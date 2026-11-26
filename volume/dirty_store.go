// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// dirtyStore is disposable, UNSYNCED writeback storage, never recovery authority.
// Its RAM directory contains at most limit pages. After spilling, a sparse byte
// index and a unique-offset log remain on disk; there is no unbounded RAM map of
// dirty addresses. Overwriting a hot address does not grow the log. RAM volumes
// return backpressure instead of ever creating these files.
// Owning Volume/transaction locking serializes puts; immutable checkpoint views
// permit concurrent gets/iteration. Returned pages are immutable after publish.
type dirtyStore struct {
	pages            map[uint64][]byte
	flags            map[uint64]byte
	order            []uint64
	cursor           int
	limit            int
	noSpill          bool
	count            uint64
	data, index, log *os.File
	closed           bool
	failed           error
	createTemp       func() (*os.File, error)
	sortRun          int
}

func newDirtyStore(limit int, noSpill bool) *dirtyStore {
	return &dirtyStore{pages: make(map[uint64][]byte), flags: make(map[uint64]byte), limit: max(1, limit), noSpill: noSpill}
}
func (s *dirtyStore) close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	for _, f := range []*os.File{s.data, s.index, s.log} {
		if f != nil {
			name := f.Name()
			f.Close()
			os.Remove(name)
		}
	}
	s.data, s.index, s.log = nil, nil, nil
}
func (s *dirtyStore) openSpill() error {
	if s.closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	if s.data != nil {
		return nil
	}
	if s.noSpill {
		return ErrBusy
	}
	files := []**os.File{&s.data, &s.index, &s.log}
	for _, p := range files {
		var f *os.File
		var err error
		if s.createTemp != nil {
			f, err = s.createTemp()
		} else {
			f, err = os.CreateTemp("", "stqr-unsynced-*")
		}
		if err != nil {
			s.close()
			return err
		}
		*p = f
	}
	for off, p := range s.pages {
		if err := s.writeData(off, p); err != nil {
			return err
		}
		if err := s.addIndex(off, s.flags[off]); err != nil {
			return err
		}
	}
	return nil
}
func (s *dirtyStore) writeData(off uint64, p []byte) error {
	if len(p) != PageSize {
		return ErrCorrupt
	}
	n, err := s.data.WriteAt(p, int64(off))
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return err
}
func (s *dirtyStore) addIndex(off uint64, flag byte) error {
	if _, err := s.index.WriteAt([]byte{flag}, int64(off/PageSize)); err != nil {
		return err
	}
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], off)
	n, err := s.log.Write(raw[:])
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	return err
}
func (s *dirtyStore) flag(off uint64) (byte, error) {
	if s != nil {
		if s.closed {
			return 0, ErrClosed
		}
		if s.failed != nil {
			return 0, s.failed
		}
	}
	if s == nil {
		return 0, nil
	}
	if _, ok := s.pages[off]; ok {
		return s.flags[off], nil
	}
	if s.index == nil {
		return 0, nil
	}
	var b [1]byte
	n, err := s.index.ReadAt(b[:], int64(off/PageSize))
	if err == io.EOF && n == 0 {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return b[0], nil
}
func (s *dirtyStore) get(off uint64) ([]byte, bool, error) {
	if s != nil {
		if s.closed {
			return nil, false, ErrClosed
		}
		if s.failed != nil {
			return nil, false, s.failed
		}
	}
	if s == nil {
		return nil, false, nil
	}
	if p, ok := s.pages[off]; ok {
		return p, true, nil
	}
	flag, err := s.flag(off)
	if err != nil || flag == 0 {
		return nil, false, err
	}
	p := make([]byte, PageSize)
	_, err = io.ReadFull(io.NewSectionReader(s.data, int64(off), PageSize), p)
	return p, true, err
}

// flag=1 is wholly free in the previous durable image; flag=2 requires redo.
// Transaction staging uses flag=1 temporarily, before baseline classification.
func (s *dirtyStore) put(off uint64, p []byte, flag byte) (resultErr error) {
	if s.closed {
		return ErrClosed
	}
	if s.failed != nil {
		return s.failed
	}
	defer func() {
		if resultErr != nil && !errors.Is(resultErr, ErrBusy) {
			s.failed = resultErr
		}
	}()
	if off%PageSize != 0 || len(p) != PageSize || flag == 0 {
		return ErrCorrupt
	}
	if _, ok := s.pages[off]; ok {
		s.pages[off] = p
		s.flags[off] = flag
		return nil
	}
	prior, err := s.flag(off)
	if err != nil {
		return err
	}
	if len(s.pages) >= s.limit {
		if err = s.openSpill(); err != nil {
			return err
		}
		victim := s.order[s.cursor]
		if err = s.writeData(victim, s.pages[victim]); err != nil {
			return err
		}
		delete(s.pages, victim)
		delete(s.flags, victim)
		s.order[s.cursor] = off
		s.cursor = (s.cursor + 1) % s.limit
	} else {
		s.order = append(s.order, off)
	}
	if prior == 0 {
		if s.index != nil {
			if err = s.addIndex(off, flag); err != nil {
				return err
			}
		}
		s.count++
	}
	// A flag is fixed by this group's baseline; it cannot change on later writes.
	if prior != 0 && prior != flag {
		return ErrCorrupt
	}
	s.pages[off], s.flags[off] = p, flag
	return nil
}
func (s *dirtyStore) each(fn func(uint64, byte, []byte) error) error {
	if s != nil {
		if s.closed {
			return ErrClosed
		}
		if s.failed != nil {
			return s.failed
		}
	}
	if s == nil {
		return nil
	}
	if s.log == nil {
		for off, p := range s.pages {
			if err := fn(off, s.flags[off], p); err != nil {
				return err
			}
		}
		return nil
	}
	for at := int64(0); at < int64(s.count)*8; at += 8 {
		var raw [8]byte
		if _, err := s.log.ReadAt(raw[:], at); err != nil {
			return err
		}
		off := binary.LittleEndian.Uint64(raw[:])
		flag, err := s.flag(off)
		if err != nil {
			return err
		}
		p, ok, err := s.get(off)
		if err != nil {
			return err
		}
		if !ok {
			return ErrCorrupt
		}
		if err = fn(off, flag, p); err != nil {
			return err
		}
	}
	return nil
}

// eachSorted keeps checkpoint writes in physical order without an all-dirty-page
// heap directory. Sort bounded runs, then merge at most 32 streams per pass.
func (s *dirtyStore) eachSorted(fn func(uint64, byte, []byte) error) error {
	if s != nil {
		if s.closed {
			return ErrClosed
		}
		if s.failed != nil {
			return s.failed
		}
	}
	if s == nil {
		return nil
	}
	yield := func(off uint64) error {
		flag, err := s.flag(off)
		if err != nil {
			return err
		}
		p, ok, err := s.get(off)
		if err != nil {
			return err
		}
		if !ok {
			return ErrCorrupt
		}
		return fn(off, flag, p)
	}
	if s.log == nil {
		offsets := make([]uint64, 0, len(s.pages))
		for off := range s.pages {
			offsets = append(offsets, off)
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		for _, off := range offsets {
			if err := yield(off); err != nil {
				return err
			}
		}
		return nil
	}
	dir, err := os.MkdirTemp("", "stqr-unsynced-sort-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	name := func(pass int, run uint64) string { return filepath.Join(dir, fmt.Sprintf("%d-%d", pass, run)) }
	perRun := uint64(65536)
	if s.sortRun > 0 {
		perRun = uint64(s.sortRun)
	}
	runs := uint64(0)
	for start := uint64(0); start < s.count; start += perRun {
		n := min(uint64(perRun), s.count-start)
		raw := make([]byte, n*8)
		if _, err = s.log.ReadAt(raw, int64(start*8)); err != nil {
			return err
		}
		offsets := make([]uint64, n)
		for i := range offsets {
			offsets[i] = binary.LittleEndian.Uint64(raw[i*8:])
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		for i, off := range offsets {
			binary.LittleEndian.PutUint64(raw[i*8:], off)
		}
		if err = os.WriteFile(name(0, runs), raw, 0600); err != nil {
			return err
		}
		runs++
	}
	pass := 0
	for runs > 1 {
		next := uint64(0)
		for start := uint64(0); start < runs; start += 32 {
			stop := min(start+32, runs)
			if err = mergeDirtyRuns(name, pass, start, stop, next); err != nil {
				return err
			}
			for i := start; i < stop; i++ {
				if err = os.Remove(name(pass, i)); err != nil {
					return err
				}
			}
			next++
		}
		runs = next
		pass++
	}
	f, err := os.Open(name(pass, 0))
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		var raw [8]byte
		_, err = io.ReadFull(f, raw[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = yield(binary.LittleEndian.Uint64(raw[:])); err != nil {
			return err
		}
	}
}
func mergeDirtyRuns(name func(int, uint64) string, pass int, start, stop, out uint64) error {
	type input struct {
		f     *os.File
		value uint64
		valid bool
	}
	inputs := make([]input, stop-start)
	defer func() {
		for _, in := range inputs {
			if in.f != nil {
				in.f.Close()
			}
		}
	}()
	next := func(in *input) error {
		var raw [8]byte
		_, err := io.ReadFull(in.f, raw[:])
		if err == io.EOF {
			in.valid = false
			return nil
		}
		if err != nil {
			return err
		}
		in.value = binary.LittleEndian.Uint64(raw[:])
		in.valid = true
		return nil
	}
	for i := range inputs {
		f, err := os.Open(name(pass, start+uint64(i)))
		if err != nil {
			return err
		}
		inputs[i].f = f
		if err = next(&inputs[i]); err != nil {
			return err
		}
	}
	target, err := os.OpenFile(name(pass+1, out), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer target.Close()
	for {
		chosen := -1
		for i := range inputs {
			if inputs[i].valid && (chosen < 0 || inputs[i].value < inputs[chosen].value) {
				chosen = i
			}
		}
		if chosen < 0 {
			return target.Close()
		}
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], inputs[chosen].value)
		if _, err = target.Write(raw[:]); err != nil {
			return err
		}
		if err = next(&inputs[chosen]); err != nil {
			return err
		}
	}
}
