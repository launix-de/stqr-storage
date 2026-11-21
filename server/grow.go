// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"os"

	"github.com/launix-de/stqr-storage/volume"
)

type volumeGrowInfo struct {
	Kind                        uint8 // 0 regular file, 1 blockdevice, 2 RAM
	Current, Available, Maximum uint64
}

func growthMaximum(current, available uint64) uint64 {
	const maximum = uint64((math.MaxInt64 - volume.PageSize) / volume.PageSize * volume.PageSize)
	if current >= maximum {
		return current
	}
	if available > maximum-current {
		return maximum
	}
	return alignedCapacity(current + available)
}
func (s *Server) growthInfo(vid string) (volumeGrowInfo, error) {
	s.mu.RLock()
	m := s.volumes[vid]
	s.mu.RUnlock()
	if m == nil {
		return volumeGrowInfo{}, fail(404, "volume not found")
	}
	current := m.store.Size()
	out := volumeGrowInfo{Current: current, Maximum: current}
	if m.store.State() != volume.Active {
		return out, fail(409, "only active volumes can grow")
	}
	if m.store.Identity().Class == volume.RAM {
		c, e := s.volumeCapacity()
		if e != nil {
			return out, e
		}
		out.Kind = 2
		out.Available = c.RAMLimit
		out.Maximum = growthMaximum(current, out.Available)
		return out, nil
	}
	st, e := os.Stat(m.config.Path)
	if e != nil {
		return out, e
	}
	if st.Mode().IsRegular() {
		out.Available, e = filesystemAvailable(m.config.Path)
		if e != nil {
			return out, e
		}
		out.Maximum = growthMaximum(current, out.Available)
		return out, nil
	}
	// Query the existing device without acquiring a second claim. The library
	// keeps its exclusive handle; opening the path again could fail O_EXCL.
	physical, e := m.store.BackingSize()
	if e != nil {
		return out, e
	}
	out.Kind = 1
	if physical < current {
		return out, fail(409, "backing device has become smaller than the committed volume")
	}
	out.Maximum = physical
	out.Available = physical - current
	return out, nil
}
func (s *Server) GrowVolume(id, vid string, sizeBytes int64) (Operation, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return Operation{}, fail(503, "server is stopping")
	}
	input := struct {
		Volume    string
		SizeBytes int64
	}{vid, sizeBytes}
	op, exists, e := s.beginOperation(id, "grow", input)
	if e != nil || exists {
		return op, e
	}
	op.VolumeID = vid
	op.SizeBytes = sizeBytes
	if e = s.saveOperation(op); e != nil {
		return op, e
	}
	info, e := s.growthInfo(vid)
	if e != nil {
		return s.finishOperation(op, e)
	}
	s.mu.RLock()
	m := s.volumes[vid]
	s.mu.RUnlock()
	if info.Kind == 1 {
		if sizeBytes != 0 {
			return s.finishOperation(op, fail(400, "blockdevices always grow to their full physical capacity"))
		}
		_, e = m.store.Rescan()
	} else {
		if sizeBytes < 0 || uint64(sizeBytes) < info.Current {
			return s.finishOperation(op, fail(400, "new size must not be smaller than the current volume"))
		}
		if uint64(sizeBytes) > info.Maximum {
			return s.finishOperation(op, fail(507, "new size exceeds current size plus available space"))
		}
		e = m.store.Grow(sizeBytes)
	}
	if e != nil {
		return s.finishOperation(op, e)
	}
	actual := int64(m.store.Size())
	s.mu.Lock()
	c := copyConfig(s.cfg)
	for i := range c.Volumes {
		if c.Volumes[i].ID == vid {
			c.Volumes[i].SizeBytes = actual
		}
	}
	c.Generation++
	e = saveConfig(s.configPath, c)
	if e == nil {
		s.cfg = c
		m.config.SizeBytes = actual
	}
	s.mu.Unlock()
	return s.finishOperation(op, e)
}
