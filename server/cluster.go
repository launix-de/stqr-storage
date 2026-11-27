// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/volume"
)

// startDrain is the single-node coordinator stub. TODO(multinode): feed DRAINING
// into the next statistics fragment, wait for committed placement and source
// fencing, transfer replicas/dedupe/subscriptions, then publish detach. No peer
// failure timeout or coordinator-race algorithm is invented here.
func (s *Server) startDrain(operation, vid string) {
	if s.controller != nil {
		s.notifyStats()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.workers.Add(1)
	s.mu.Unlock()
	go func() { defer s.workers.Done(); s.drain(operation, vid) }()
}
func (s *Server) drain(id, vid string) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		s.opMu.Lock()
		e := s.drainOnce(id, vid)
		s.opMu.Unlock()
		if e == nil {
			return
		}
		s.opMu.Lock()
		op, er := s.operation(id)
		if er == nil {
			op.State = "running"
			op.Status = 202
			op.Detail = "Drain wartet: " + e.Error()
			s.saveOperation(op)
		}
		s.opMu.Unlock()
		// Waiting for capacity is not data loss or a membership timeout. Retry also
		// permits an administrator to attach a suitable local target volume.
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}
func (s *Server) drainOnce(id, vid string) error {
	s.mu.RLock()
	src := s.volumes[vid]
	targets := make([]*managedVolume, 0, len(s.volumes))
	for _, v := range s.volumes {
		if v.config.ID != vid && !v.config.System && v.config.State == "active" {
			targets = append(targets, v)
		}
	}
	s.mu.RUnlock()
	if src == nil {
		return nil
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].config.ID < targets[j].config.ID })
	class := src.store.Identity().Class
	moved := map[volume.ID]map[string]bool{}
	var after *volume.Key
	for {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		h, e := src.store.Open(false)
		if e != nil {
			return e
		}
		records, e := h.ScanObjects(after, 128)
		h.Close()
		if e != nil {
			return e
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if b, err := s.Bucket(record.Key.Bucket); err == nil {
				d := b.Describe()
				owned := len(d.LocalVolumes) == 0
				for _, location := range d.LocalVolumes {
					owned = owned || location == vid
				}
				if !owned {
					continue
				}
			}
			copied := false
			var last error
			for _, dst := range targets {
				if !class.Allows(dst.store.Identity().Class) {
					continue
				}
				if e = copyObject(s.ctx, src.store, dst.store, record.Key, record.Info); e == nil {
					copied = true
					if moved[record.Key.Bucket] == nil {
						moved[record.Key.Bucket] = map[string]bool{}
					}
					moved[record.Key.Bucket][dst.config.ID] = true
					break
				}
				last = e
			}
			if !copied {
				if last != nil {
					return last
				}
				return errors.New("kein passendes Zielvolume mit freiem Platz verfügbar")
			}
		}
		last := records[len(records)-1].Key
		after = &last
	}
	if err := s.noteBucketDrain(vid, moved); err != nil {
		return err
	}
	return s.completeDetach(id, vid, src)
}
func (s *Server) completeDetach(id, vid string, src *managedVolume) error {
	if e := src.store.Sync(); e != nil {
		return e
	}
	s.mu.RLock()
	c := copyConfig(s.cfg)
	s.mu.RUnlock()
	next := make([]VolumeConfig, 0, len(c.Volumes))
	for _, v := range c.Volumes {
		if v.ID != vid {
			next = append(next, v)
		}
	}
	c.Volumes = next
	c.Generation++
	if e := saveConfig(s.configPath, c); e != nil {
		return e
	}
	// All object copies and retry records have synced on their destinations. The
	// source bytes remain on disk for explicit reuse/recovery; removal never unlinks
	// a file or wipes a raw device. Library Close releases the exclusive device claim.
	s.stopVolumeMembership(src.store)
	counters := volumeAccessIO(src.store.IOStats())
	e := src.store.Close()
	s.mu.Lock()
	s.retiredIO.PhysicalReadBytes += counters.PhysicalReadBytes
	s.retiredIO.PhysicalWriteBytes += counters.PhysicalWriteBytes
	s.retiredIO.Errors += counters.Errors
	s.retiredIO.Flushes += counters.Flushes
	s.retiredIO.FlushNS += counters.FlushNS
	s.cfg = c
	delete(s.volumes, vid)
	s.mu.Unlock()
	op, er := s.operation(id)
	if er != nil {
		return er
	}
	_, er = s.finishOperation(op, e)
	return er
}

type extentReader struct {
	ctx       context.Context
	h         *volume.Handle
	k         volume.Key
	off, left uint64
}

func (r *extentReader) Read(b []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	if r.left == 0 {
		return 0, io.EOF
	}
	if uint64(len(b)) > r.left {
		b = b[:r.left]
	}
	n, e := r.h.ReadAt(r.k, b, r.off)
	r.off += uint64(n)
	r.left -= uint64(n)
	return n, e
}
func copyObject(ctx context.Context, source, target *volume.Volume, k volume.Key, info volume.Info) error {
	src, e := source.Open(false)
	if e != nil {
		return e
	}
	defer src.Close()
	var extents []volume.Extent
	if !info.Deleted {
		extents, e = src.Extents(k)
		if e != nil {
			return e
		}
	}
	dst, e := target.Open(true)
	if e != nil {
		return e
	}
	defer dst.Close()
	existing, e := dst.Stat(k)
	if info.Deleted && errors.Is(e, volume.ErrNotFound) {
		// Stat deliberately hides tombstones; Restore rejects an identical version,
		// so locate its migration metadata through the bounded record scan.
		var cursor *volume.Key
		for {
			records, er := dst.ScanObjects(cursor, 128)
			if er != nil {
				return er
			}
			if len(records) == 0 {
				break
			}
			found := false
			for _, x := range records {
				if x.Key == k {
					existing = x.Info
					e = nil
					found = true
					break
				}
			}
			if found {
				break
			}
			last := records[len(records)-1].Key
			cursor = &last
		}
	}
	if e == nil {
		if existing != info {
			return errors.New("target contains conflicting object metadata")
		}
		// A replay after crash must verify content, not infer equality from length.
		a, b := make([]byte, volume.BlockSize), make([]byte, volume.BlockSize)
		comparison := append([]volume.Extent(nil), extents...)
		if !info.Deleted {
			other, er := dst.Extents(k)
			if er != nil {
				return er
			}
			comparison = append(comparison, other...)
		}
		for _, x := range comparison {
			if x.Zero {
				continue
			}
			for off := x.Offset; off < x.Offset+x.Length; {
				if e := ctx.Err(); e != nil {
					return e
				}
				n := min(uint64(len(a)), x.Offset+x.Length-off)
				if _, e = src.ReadAt(k, a[:n], off); e != nil {
					return e
				}
				if _, e = dst.ReadAt(k, b[:n], off); e != nil {
					return e
				}
				if !bytes.Equal(a[:n], b[:n]) {
					return errors.New("target object differs from drain source")
				}
				off += n
			}
		}
	} else if errors.Is(e, volume.ErrNotFound) {
		input := func(yield func(uint64, io.Reader, uint64) error) error {
			for _, x := range extents {
				if !x.Zero {
					if e := yield(x.Offset, &extentReader{ctx: ctx, h: src, k: k, off: x.Offset, left: x.Length}, x.Length); e != nil {
						return e
					}
				}
			}
			return nil
		}
		if e = dst.Restore(k, info, input, volume.Options{}); e != nil {
			return e
		}
	} else {
		return e
	}
	return src.ScanRequests(func(x volume.RequestRecord) error {
		if x.Key == k {
			return dst.RestoreRequest(x)
		}
		return nil
	})
}
