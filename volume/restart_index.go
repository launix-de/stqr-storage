// SPDX-License-Identifier: GPL-3.0-only
package volume

// restartIndex stores only retained dirty pages in an immutable, checksummed
// tree. Opening reads its seal and root only; page payloads are verified lazily.
// It is OS writeback retention, never a physical durability acknowledgement.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
)

const restartIndexFanout = 64
const restartIndexRecord = 56

var errRestartIndex = errors.New("volumes: invalid retained writeback index")

type restartIndexRef struct {
	min, max, pos uint64
	hash          [32]byte
	level         byte
}
type restartIndexEntry struct {
	off, pos, flag uint64
	hash           [32]byte
}
type restartIndexBuilder struct {
	file       *os.File
	end, count uint64
	last       uint64
	leaves     []restartIndexEntry
	levels     [][]restartIndexRef
	headerData []byte
}

func (b *restartIndexBuilder) writePage(page []byte) (uint64, error) {
	if len(page) != PageSize || b.end > math.MaxInt64-PageSize {
		return 0, errRestartIndex
	}
	pos := b.end
	n, e := b.file.WriteAt(page, int64(pos))
	if e == nil && n != len(page) {
		e = io.ErrShortWrite
	}
	if e == nil {
		b.end += PageSize
	}
	return pos, e
}
func (b *restartIndexBuilder) node(level byte, entries []restartIndexEntry, children []restartIndexRef) (restartIndexRef, error) {
	var out restartIndexRef
	count := len(entries)
	if level > 0 {
		count = len(children)
	}
	if count < 1 || count > restartIndexFanout {
		return out, errRestartIndex
	}
	p := make([]byte, PageSize)
	copy(p, "WBINDEX1")
	p[8] = level
	binary.LittleEndian.PutUint16(p[10:], uint16(count))
	if level == 0 {
		out.min = entries[0].off
		out.max = entries[count-1].off
	} else {
		out.min = children[0].min
		out.max = children[count-1].max
	}
	binary.LittleEndian.PutUint64(p[16:], out.min)
	binary.LittleEndian.PutUint64(p[24:], out.max)
	for i := 0; i < count; i++ {
		raw := p[32+i*restartIndexRecord:]
		if level == 0 {
			e := entries[i]
			binary.LittleEndian.PutUint64(raw, e.off)
			binary.LittleEndian.PutUint64(raw[8:], e.pos)
			binary.LittleEndian.PutUint64(raw[16:], e.flag)
			copy(raw[24:], e.hash[:])
		} else {
			r := children[i]
			if r.level+1 != level {
				return out, errRestartIndex
			}
			binary.LittleEndian.PutUint64(raw, r.min)
			binary.LittleEndian.PutUint64(raw[8:], r.max)
			binary.LittleEndian.PutUint64(raw[16:], r.pos)
			copy(raw[24:], r.hash[:])
		}
	}
	var e error
	out.pos, e = b.writePage(p)
	out.hash = sha256.Sum256(p)
	out.level = level
	return out, e
}
func (b *restartIndexBuilder) push(r restartIndexRef) error {
	level := int(r.level)
	for level >= len(b.levels) {
		b.levels = append(b.levels, nil)
	}
	if level == 255 {
		return errRestartIndex
	}
	b.levels[level] = append(b.levels[level], r)
	if len(b.levels[level]) < restartIndexFanout {
		return nil
	}
	next, e := b.node(r.level+1, nil, b.levels[level])
	if e != nil {
		return e
	}
	b.levels[level] = nil
	return b.push(next)
}
func (b *restartIndexBuilder) add(off uint64, flag byte, page []byte) error {
	if off < PageSize*12 || off%PageSize != 0 || flag < 1 || flag > 2 {
		return errRestartIndex
	}
	if b.count > 0 && off <= b.last {
		return errRestartIndex
	}
	pos, e := b.writePage(page)
	if e != nil {
		return e
	}
	b.leaves = append(b.leaves, restartIndexEntry{off: off, pos: pos, flag: uint64(flag), hash: sha256.Sum256(page)})
	b.count++
	b.last = off
	if len(b.leaves) < restartIndexFanout {
		return nil
	}
	leaf, e := b.node(0, b.leaves, nil)
	if e != nil {
		return e
	}
	b.leaves = nil
	return b.push(leaf)
}
func (b *restartIndexBuilder) seal() error {
	if len(b.leaves) > 0 {
		leaf, e := b.node(0, b.leaves, nil)
		if e != nil {
			return e
		}
		b.leaves = nil
		if e = b.push(leaf); e != nil {
			return e
		}
	}
	var root restartIndexRef
	for {
		total, low := 0, -1
		for i, refs := range b.levels {
			total += len(refs)
			if low < 0 && len(refs) > 0 {
				low = i
			}
		}
		if total == 0 {
			break
		}
		if total == 1 {
			root = b.levels[low][0]
			break
		}
		next, e := b.node(byte(low+1), nil, b.levels[low])
		if e != nil {
			return e
		}
		b.levels[low] = nil
		if e = b.push(next); e != nil {
			return e
		}
	}
	header := make([]byte, PageSize)
	copy(header, "STQRWBC1")
	binary.LittleEndian.PutUint64(header[8:], b.end+PageSize)
	binary.LittleEndian.PutUint64(header[16:], b.count)
	binary.LittleEndian.PutUint64(header[24:], root.pos)
	binary.LittleEndian.PutUint64(header[32:], root.min)
	binary.LittleEndian.PutUint64(header[40:], root.max)
	copy(header[48:], root.hash[:])
	header[80] = root.level
	if len(b.headerData) > PageSize-96 {
		return errRestartIndex
	}
	copy(header[96:], b.headerData)
	footer := make([]byte, PageSize)
	copy(footer, "WBSEALED")
	sum := sha256.Sum256(header)
	copy(footer[8:], sum[:])
	if _, e := b.writePage(footer); e != nil {
		return e
	}
	n, e := b.file.WriteAt(header, 0)
	if e == nil && n != len(header) {
		e = io.ErrShortWrite
	}
	return e
}

type restartIndexReader struct {
	source      io.ReaderAt
	size, count uint64
	root        restartIndexRef
	header      []byte
	close       func() error
}

func restartIndexOpen(source io.ReaderAt, size uint64) (*restartIndexReader, error) {
	if size < 4*PageSize || size > math.MaxInt64 || size%PageSize != 0 {
		return nil, errRestartIndex
	}
	header := make([]byte, PageSize)
	footer := make([]byte, PageSize)
	if e := restartIndexRead(source, header, 0); e != nil {
		return nil, e
	}
	if e := restartIndexRead(source, footer, size-PageSize); e != nil {
		return nil, e
	}
	sum := sha256.Sum256(header)
	var blank [PageSize]byte
	if !bytes.Equal(footer[40:], blank[40:]) {
		return nil, errRestartIndex
	}
	if string(header[:8]) != "STQRWBC1" || string(footer[:8]) != "WBSEALED" || !bytes.Equal(sum[:], footer[8:40]) || binary.LittleEndian.Uint64(header[8:]) != size {
		return nil, errRestartIndex
	}
	r := &restartIndexReader{source: source, size: size, count: binary.LittleEndian.Uint64(header[16:]), header: header}
	r.root = restartIndexRef{pos: binary.LittleEndian.Uint64(header[24:]), min: binary.LittleEndian.Uint64(header[32:]), max: binary.LittleEndian.Uint64(header[40:]), level: header[80]}
	copy(r.root.hash[:], header[48:80])
	if r.count > size/PageSize {
		return nil, errRestartIndex
	}
	if r.count == 0 {
		if r.root != (restartIndexRef{}) {
			return nil, errRestartIndex
		}
	} else if _, e := r.node(r.root); e != nil {
		return nil, e
	}
	return r, nil
}
func restartIndexRead(src io.ReaderAt, p []byte, off uint64) error {
	n, e := src.ReadAt(p, int64(off))
	if n == len(p) && e == io.EOF {
		return nil
	}
	if e == nil && n != len(p) {
		e = io.ErrUnexpectedEOF
	}
	return e
}
func (r *restartIndexReader) node(ref restartIndexRef) ([]byte, error) {
	if ref.pos < 3*PageSize || ref.pos%PageSize != 0 || ref.pos > r.size-2*PageSize || ref.min > ref.max {
		return nil, errRestartIndex
	}
	p, e := r.page(ref.pos)
	if e != nil {
		return nil, e
	}
	sum := sha256.Sum256(p)
	count := int(binary.LittleEndian.Uint16(p[10:]))
	if sum != ref.hash || string(p[:8]) != "WBINDEX1" || p[8] != ref.level || count < 1 || count > restartIndexFanout || binary.LittleEndian.Uint64(p[16:]) != ref.min || binary.LittleEndian.Uint64(p[24:]) != ref.max {
		return nil, errRestartIndex
	}
	var prior uint64
	for i := 0; i < count; i++ {
		raw := p[32+i*restartIndexRecord:]
		first := binary.LittleEndian.Uint64(raw)
		last := first
		if ref.level > 0 {
			last = binary.LittleEndian.Uint64(raw[8:])
		}
		if first < PageSize*12 || first%PageSize != 0 || last%PageSize != 0 || first > last || i > 0 && first <= prior || i == 0 && first != ref.min || i == count-1 && last != ref.max {
			return nil, errRestartIndex
		}
		prior = last
	}
	return p, nil
}
func (r *restartIndexReader) get(off uint64) ([]byte, byte, bool, error) {
	if r.count == 0 {
		return nil, 0, false, nil
	}
	ref := r.root
	for {
		p, e := r.node(ref)
		if e != nil {
			return nil, 0, false, e
		}
		count := int(binary.LittleEndian.Uint16(p[10:]))
		chosen := -1
		for i := 0; i < count; i++ {
			raw := p[32+i*restartIndexRecord:]
			first := binary.LittleEndian.Uint64(raw)
			last := first
			if ref.level > 0 {
				last = binary.LittleEndian.Uint64(raw[8:])
			}
			if off >= first && off <= last {
				chosen = i
				break
			}
		}
		if chosen < 0 {
			return nil, 0, false, nil
		}
		raw := p[32+chosen*restartIndexRecord:]
		if ref.level > 0 {
			ref = restartIndexRef{min: binary.LittleEndian.Uint64(raw), max: binary.LittleEndian.Uint64(raw[8:]), pos: binary.LittleEndian.Uint64(raw[16:]), level: ref.level - 1}
			copy(ref.hash[:], raw[24:56])
			continue
		}
		data, flag, err := r.data(raw)
		return data, flag, err == nil, err
	}
}

func (r *restartIndexReader) data(raw []byte) ([]byte, byte, error) {
	pos := binary.LittleEndian.Uint64(raw[8:])
	flag := binary.LittleEndian.Uint64(raw[16:])
	if pos < 3*PageSize || pos%PageSize != 0 || pos > r.size-2*PageSize || flag < 1 || flag > 2 {
		return nil, 0, errRestartIndex
	}
	data, err := r.page(pos)
	if err != nil {
		return nil, 0, err
	}
	hash := sha256.Sum256(data)
	if !bytes.Equal(hash[:], raw[24:56]) {
		return nil, 0, errRestartIndex
	}
	return data, byte(flag), nil
}

// An explicit checkpoint must consume and verify every retained dependency,
// even if startup and earlier reads never touched a corrupt page.
func (r *restartIndexReader) each(fn func(uint64, byte, []byte) error) error {
	if r.count == 0 {
		return nil
	}
	var count uint64
	var walk func(restartIndexRef) error
	walk = func(ref restartIndexRef) error {
		page, err := r.node(ref)
		if err != nil {
			return err
		}
		entries := int(binary.LittleEndian.Uint16(page[10:]))
		for i := 0; i < entries; i++ {
			raw := page[32+i*restartIndexRecord:]
			if ref.level > 0 {
				child := restartIndexRef{min: binary.LittleEndian.Uint64(raw), max: binary.LittleEndian.Uint64(raw[8:]), pos: binary.LittleEndian.Uint64(raw[16:]), level: ref.level - 1}
				copy(child.hash[:], raw[24:56])
				if err = walk(child); err != nil {
					return err
				}
			} else {
				off := binary.LittleEndian.Uint64(raw)
				data, flag, err := r.data(raw)
				if err != nil {
					return err
				}
				count++
				if count > r.count {
					return errRestartIndex
				}
				if err = fn(off, flag, data); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(r.root); err != nil {
		return err
	}
	if count != r.count {
		return errRestartIndex
	}
	return nil
}

func (r *restartIndexReader) flag(off uint64) (byte, error) {
	if r.count == 0 {
		return 0, nil
	}
	ref := r.root
	for {
		p, err := r.node(ref)
		if err != nil {
			return 0, err
		}
		count := int(binary.LittleEndian.Uint16(p[10:]))
		chosen := -1
		for i := 0; i < count; i++ {
			raw := p[32+i*restartIndexRecord:]
			first := binary.LittleEndian.Uint64(raw)
			last := first
			if ref.level > 0 {
				last = binary.LittleEndian.Uint64(raw[8:])
			}
			if off >= first && off <= last {
				chosen = i
				break
			}
		}
		if chosen < 0 {
			return 0, nil
		}
		raw := p[32+chosen*restartIndexRecord:]
		if ref.level > 0 {
			ref = restartIndexRef{min: binary.LittleEndian.Uint64(raw), max: binary.LittleEndian.Uint64(raw[8:]), pos: binary.LittleEndian.Uint64(raw[16:]), level: ref.level - 1}
			copy(ref.hash[:], raw[24:56])
			continue
		}
		pos, flag := binary.LittleEndian.Uint64(raw[8:]), binary.LittleEndian.Uint64(raw[16:])
		if pos < 3*PageSize || pos%PageSize != 0 || pos > r.size-2*PageSize || flag < 1 || flag > 2 {
			return 0, errRestartIndex
		}
		return byte(flag), nil
	}
}

func (r *restartIndexReader) page(off uint64) ([]byte, error) {
	if src, ok := r.source.(interface{ retainedPage(uint64) ([]byte, error) }); ok {
		return src.retainedPage(off)
	}
	p := make([]byte, PageSize)
	if err := restartIndexRead(r.source, p, off); err != nil {
		return nil, err
	}
	return p, nil
}
