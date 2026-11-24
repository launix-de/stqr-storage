// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type durabilityVolume struct {
	v          *volume.Volume
	generation uint64
}
type bucketDurability struct {
	active            *volume.Volume
	sequence, durable uint64
	volumes           map[*volume.Volume]uint64
}
type durabilityState struct {
	mu      sync.Mutex
	buckets map[volume.ID]*bucketDurability
}

// Called under opMu before every object mutation. Keeping just one volatile
// volume per bucket preserves causal data/index/inode ordering across local
// volumes: a change of destination checkpoints the preceding volume first.
func (s *Server) switchAcceptedVolume(bucket volume.ID, v *volume.Volume, accepted bool) error {
	d := &s.durability
	d.mu.Lock()
	if d.buckets == nil {
		d.buckets = map[volume.ID]*bucketDurability{}
	}
	state := d.buckets[bucket]
	if state == nil && accepted {
		state = &bucketDurability{volumes: map[*volume.Volume]uint64{}}
		d.buckets[bucket] = state
	}
	var previous *volume.Volume
	if state != nil {
		previous = state.active
	}
	d.mu.Unlock()
	if previous != nil && previous != v {
		if err := previous.CheckpointThrough(previous.Header().Generation); err != nil {
			return err
		}
	}
	if state != nil {
		d.mu.Lock()
		if accepted {
			state.active = v
		} else if previous != v {
			state.active = nil
		}
		d.mu.Unlock()
	}
	return nil
}
func (s *Server) recordAccepted(bucket volume.ID, v *volume.Volume, generation uint64) {
	d := &s.durability
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.buckets == nil {
		d.buckets = map[volume.ID]*bucketDurability{}
	}
	b := d.buckets[bucket]
	if b == nil {
		b = &bucketDurability{volumes: map[*volume.Volume]uint64{}}
		d.buckets[bucket] = b
	}
	b.active = v
	b.sequence++
	b.volumes[v] = generation
}
func (s *Server) acceptedMark(bucket volume.ID) (client.CommitMark, bool) {
	d := &s.durability
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.buckets[bucket]
	if b == nil || b.sequence <= b.durable {
		return client.CommitMark{}, false
	}
	return client.CommitMark{Boot: s.boot, Sequence: b.sequence}, true
}
func (s *Server) durabilityResponse(status int, result []byte, bucket volume.ID) []byte {
	mark, ok := s.acceptedMark(bucket)
	if !ok {
		return client.ResponseBody(status, result, "")
	}
	e := client.Encoder{}
	e.V(uint64(status))
	h := client.Encoder{}
	h.V(1)
	h.V(client.DurabilityHeader)
	h.Blob(mark.MarshalBinary())
	e.Blob(h.B)
	e.B = append(e.B, result...)
	return e.B
}
func (s *Server) executeSync(ctx context.Context, m client.Message, ws *wsSession, p principal) ([]byte, int, error) {
	d := client.Decoder{B: m.Body}
	bucket := volume.ID(d.ID())
	boot, seq := d.ID(), d.U64()
	if d.Done() != nil || seq == 0 || boot == (client.ID{}) {
		return nil, 0, fail(400, "invalid durability mark")
	}
	if boot != s.boot {
		return nil, 410, fail(410, "durability mark belongs to another server incarnation")
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, 0, err
	}
	if ws != nil && (ws.authorization == nil || ws.authorization.bucket != b) {
		return nil, 0, ErrBucketForbidden
	}
	permission := BucketRead
	if ws != nil && ws.authorization != nil {
		for _, candidate := range []BucketPermissions{BucketRead, BucketWrite, BucketAppend, BucketDelete} {
			if ws.authorization.permissions&candidate != 0 {
				permission = candidate
				break
			}
		}
	}
	if _, err = b.authorize(p, permission); err != nil {
		return nil, 0, err
	}
	s.opMu.Lock()
	s.durability.mu.Lock()
	state := s.durability.buckets[bucket]
	if state == nil || seq > state.sequence {
		s.durability.mu.Unlock()
		s.opMu.Unlock()
		return nil, 0, fail(412, "unknown durability mark")
	}
	cut := state.sequence
	volumes := make([]durabilityVolume, 0, len(state.volumes))
	for v, g := range state.volumes {
		volumes = append(volumes, durabilityVolume{v, g})
	}
	s.durability.mu.Unlock()
	s.opMu.Unlock()
	for _, v := range volumes {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if err := v.v.CheckpointThrough(v.generation); err != nil {
			return nil, 0, err
		}
	}
	s.durability.mu.Lock()
	for _, ref := range volumes {
		if state.volumes[ref.v] == ref.generation {
			delete(state.volumes, ref.v)
		}
	}
	if len(state.volumes) == 0 {
		state.active = nil
	}
	if state.durable < cut {
		state.durable = cut
	}
	s.durability.mu.Unlock()
	return (client.CommitMark{Boot: s.boot, Sequence: cut}).MarshalBinary(), 200, nil
}
func (s *Server) startAcceptedCheckpoints() {
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-tick.C:
				s.mu.RLock()
				stores := make([]*volume.Volume, 0, len(s.volumes))
				for _, v := range s.volumes {
					stores = append(stores, v.store)
				}
				s.mu.RUnlock()
				for _, v := range stores {
					_ = v.FlushAccepted()
				}
				// Prune confirmed volume references, including closed/detached
				// stores, so a long-running bucket does not retain old devices.
				s.opMu.Lock()
				s.durability.mu.Lock()
				type cleanup struct {
					state *bucketDurability
					cut   uint64
					refs  []durabilityVolume
				}
				work := []cleanup{}
				for _, state := range s.durability.buckets {
					if len(state.volumes) == 0 {
						continue
					}
					item := cleanup{state: state, cut: state.sequence}
					for v, g := range state.volumes {
						item.refs = append(item.refs, durabilityVolume{v, g})
					}
					work = append(work, item)
				}
				s.durability.mu.Unlock()
				s.opMu.Unlock()
				for _, item := range work {
					ok := true
					for _, ref := range item.refs {
						if err := ref.v.CheckpointThrough(ref.generation); err != nil {
							ok = false
							break
						}
					}
					if !ok {
						continue
					}
					s.durability.mu.Lock()
					for _, ref := range item.refs {
						if item.state.volumes[ref.v] == ref.generation {
							delete(item.state.volumes, ref.v)
						}
					}
					if item.state.durable < item.cut {
						item.state.durable = item.cut
					}
					if len(item.state.volumes) == 0 {
						item.state.active = nil
					}
					s.durability.mu.Unlock()
				}

			}
		}
	}()
}
