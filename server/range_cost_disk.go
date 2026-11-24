// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const rangeCostMemoryRows = 16384
const rangeCostDiskRow = 56

type rangeCostRun struct {
	volume                  volume.ID
	offset                  int64
	count                   int
	first, last             volume.ObjectID
	physical, cost, objects uint64
}

// Very large ranges spill sorted metadata runs. This is a planning scratch
// index, never a second copy of payload or a permanent in-RAM object index.
type rangeCostRows struct {
	rows []rangeCostRow
	file *os.File
	runs []rangeCostRun
}

func (s *rangeCostRows) close() {
	if s.file != nil {
		name := s.file.Name()
		s.file.Close()
		os.Remove(name)
		s.file = nil
	}
}
func (s *rangeCostRows) add(row rangeCostRow) error {
	s.rows = append(s.rows, row)
	if len(s.rows) >= rangeCostMemoryRows {
		return s.flush()
	}
	return nil
}
func costAdd(a, b uint64) (uint64, error) {
	if b > ^uint64(0)-a {
		return 0, volume.ErrRange
	}
	return a + b, nil
}
func (s *rangeCostRows) flush() error {
	if len(s.rows) == 0 {
		return nil
	}
	if s.file == nil {
		var err error
		s.file, err = os.CreateTemp("", "stqr-plan-cost-*")
		if err != nil {
			return err
		}
	}
	sort.Slice(s.rows, func(i, j int) bool {
		a, b := s.rows[i], s.rows[j]
		if a.volume != b.volume {
			return bytes.Compare(a.volume[:], b.volume[:]) < 0
		}
		return bytes.Compare(a.id[:], b.id[:]) < 0
	})
	for start := 0; start < len(s.rows); {
		stop := start + 1
		for stop < len(s.rows) && s.rows[stop].volume == s.rows[start].volume {
			stop++
		}
		offset, err := s.file.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		run := rangeCostRun{volume: s.rows[start].volume, offset: offset, count: stop - start, first: s.rows[start].id, last: s.rows[stop-1].id}
		data := make([]byte, 0, (stop-start)*rangeCostDiskRow)
		for _, row := range s.rows[start:stop] {
			run.physical, err = costAdd(run.physical, row.physical)
			if err != nil {
				return err
			}
			for _, n := range []uint64{row.physical, row.receipts, row.count * 256} {
				run.cost, err = costAdd(run.cost, n)
				if err != nil {
					return err
				}
			}
			run.objects, err = costAdd(run.objects, row.count)
			if err != nil {
				return err
			}
			data = append(data, row.id[:]...)
			data = binary.LittleEndian.AppendUint64(data, run.physical)
			data = binary.LittleEndian.AppendUint64(data, run.cost)
			data = binary.LittleEndian.AppendUint64(data, run.objects)
		}
		if _, err = s.file.Write(data); err != nil {
			return err
		}
		s.runs = append(s.runs, run)
		start = stop
	}
	s.rows = s.rows[:0]
	return nil
}
func (s *rangeCostRows) bound(run rangeCostRun, id client.ObjectID) (int, error) {
	low, high := 0, run.count
	for low < high {
		mid := (low + high) / 2
		var key [32]byte
		if _, err := s.file.ReadAt(key[:], run.offset+int64(mid*rangeCostDiskRow)); err != nil {
			return 0, err
		}
		if bytes.Compare(key[:], id[:]) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low, nil
}
func (s *rangeCostRows) prefix(run rangeCostRun, count int) ([3]uint64, error) {
	if count == 0 {
		return [3]uint64{}, nil
	}
	if count == run.count {
		return [3]uint64{run.physical, run.cost, run.objects}, nil
	}
	var raw [24]byte
	_, err := s.file.ReadAt(raw[:], run.offset+int64((count-1)*rangeCostDiskRow)+32)
	return [3]uint64{binary.LittleEndian.Uint64(raw[:]), binary.LittleEndian.Uint64(raw[8:]), binary.LittleEndian.Uint64(raw[16:])}, err
}
func (s *rangeCostRows) measure(entry client.RangeEntry, only *volume.ID) (uint64, uint64, error) {
	end, toEnd, err := client.RangeEnd(entry.Start, entry.PrefixBits)
	if err != nil {
		return 0, 0, err
	}
	var cost, count uint64
	for _, run := range s.runs {
		if only != nil && run.volume != *only || bytes.Compare(run.last[:], entry.Start[:]) < 0 || !toEnd && bytes.Compare(run.first[:], end[:]) >= 0 {
			continue
		}
		a, b := 0, run.count
		if bytes.Compare(run.first[:], entry.Start[:]) < 0 {
			a, err = s.bound(run, entry.Start)
			if err != nil {
				return 0, 0, err
			}
		}
		if !toEnd && bytes.Compare(run.last[:], end[:]) >= 0 {
			b, err = s.bound(run, end)
			if err != nil {
				return 0, 0, err
			}
		}
		lo, e := s.prefix(run, a)
		if e != nil {
			return 0, 0, e
		}
		hi, e := s.prefix(run, b)
		if e != nil {
			return 0, 0, e
		}
		n := hi[1] - lo[1]
		if only != nil {
			n = hi[0] - lo[0]
		}
		cost, err = costAdd(cost, n)
		if err != nil {
			return 0, 0, err
		}
		count, err = costAdd(count, hi[2]-lo[2])
		if err != nil {
			return 0, 0, err
		}
	}
	for _, row := range s.rows {
		if only != nil && row.volume != *only || bytes.Compare(row.id[:], entry.Start[:]) < 0 || !toEnd && bytes.Compare(row.id[:], end[:]) >= 0 {
			continue
		}
		values := []uint64{row.physical}
		if only == nil {
			values = append(values, row.receipts, row.count*256)
		}
		for _, n := range values {
			cost, err = costAdd(cost, n)
			if err != nil {
				return 0, 0, err
			}
		}
		count, err = costAdd(count, row.count)
		if err != nil {
			return 0, 0, err
		}
	}
	if only == nil {
		cost, err = costAdd(cost, 64<<10)
	}
	return cost, count, err
}
