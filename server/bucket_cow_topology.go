// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Topology is configuration, not an object cache. A published image is immutable
// and retained by readers until their I/O finishes. Only clone edges, namespace
// identity and deletion change its revision; ACL, quota and receipt updates do
// not invalidate it. Publication never takes this cache's build mutex while
// holding a bucket mutex.
type cowTopologyCache struct {
	revision atomic.Uint64
	image    atomic.Pointer[cowTopologySnapshot]
	build    sync.Mutex
}
type cowTopologySnapshot struct {
	revision   uint64
	graph      cowGraph
	proofs     map[volume.ID][32]byte
	readPeers  map[volume.ID][]volume.ID
	writePeers map[volume.ID][]volume.ID
	err        error
}

func (g *cowTopologySnapshot) proof(bucket volume.ID) ([32]byte, error) {
	proof, ok := g.proofs[bucket]
	if !ok {
		return [32]byte{}, ErrBucketNotFound
	}
	return proof, nil
}
func (s *Server) cowTopology() (*cowTopologySnapshot, error) {
	cache := &s.cowTopologyCache
	if image := cache.image.Load(); image != nil && image.revision == cache.revision.Load() {
		return image, image.err
	}
	cache.build.Lock()
	defer cache.build.Unlock()
	for range 8 {
		// Copies only the fields needed by topology validation, on a topology edit.
		// ClonePeers belongs to an immutable description and is never modified here.
		s.bucketMu.RLock()
		revision := cache.revision.Load()
		if image := cache.image.Load(); image != nil && image.revision == revision {
			s.bucketMu.RUnlock()
			return image, image.err
		}
		descriptions := make([]BucketDescription, 0, len(s.bucketsByID))
		for _, bucket := range s.bucketsByID {
			bucket.mu.RLock()
			d := bucket.description
			descriptions = append(descriptions, BucketDescription{ID: d.ID, DeletionComplete: d.DeletionComplete, ClonePeers: d.ClonePeers})
			bucket.mu.RUnlock()
		}
		s.bucketMu.RUnlock()
		if revision != cache.revision.Load() {
			continue
		}
		image := buildCowTopology(descriptions)
		image.revision = revision
		// The captured configuration is a valid immutable cut even if another
		// edit arrives while it is validated. Do not starve readers behind a
		// stream of edits; the next acquisition (including post-I/O validation)
		// checks this revision and obtains the replacement image.
		cache.image.Store(image)
		return image, image.err
	}
	return nil, fail(503, "clone graph changed repeatedly during publication")
}
func buildCowTopology(descriptions []BucketDescription) *cowTopologySnapshot {
	image := &cowTopologySnapshot{proofs: make(map[volume.ID][32]byte, len(descriptions)), readPeers: make(map[volume.ID][]volume.ID), writePeers: make(map[volume.ID][]volume.ID)}
	image.graph, image.err = cowGraphDescriptions(descriptions)
	if image.err != nil {
		return image
	}
	live := make(map[volume.ID]BucketDescription, len(descriptions))
	for _, d := range descriptions {
		if d.DeletionComplete {
			continue
		}
		id, err := bucketID(d.ID)
		if err != nil {
			image.err = err
			return image
		}
		live[id] = d
		for _, edge := range d.ClonePeers {
			peer, err := bucketID(edge.Bucket)
			if err != nil {
				image.err = err
				return image
			}
			if edge.Direction != "child" {
				image.readPeers[id] = append(image.readPeers[id], peer)
			}
			if edge.Direction != "parent" {
				image.writePeers[id] = append(image.writePeers[id], peer)
			}
		}
	}
	// Every member of an undirected lifetime component has the same canonical
	// proof. Compute it once when configuration changes, never per object read.
	for start := range live {
		if _, done := image.proofs[start]; done {
			continue
		}
		component := image.graph.component(start)
		e := client.Encoder{}
		for _, id := range component {
			d, ok := live[id]
			if !ok {
				image.err = ErrBucketNotFound
				return image
			}
			e.ID(client.ID(id))
			for _, edge := range d.ClonePeers {
				peer, err := bucketID(edge.Bucket)
				if err != nil {
					image.err = err
					return image
				}
				link, err := bucketID(edge.Link)
				if err != nil {
					image.err = err
					return image
				}
				e.ID(client.ID(peer))
				e.ID(client.ID(link))
				e.Text(edge.Direction)
			}
			e.ID(client.ID{})
		}
		proof := sha256.Sum256(e.B)
		for _, id := range component {
			image.proofs[id] = proof
		}
	}
	return image
}
