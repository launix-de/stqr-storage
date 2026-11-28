// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func topologyDescriptions() []BucketDescription {
	ids := []volume.ID{{1}, {2}, {3}, {4}, {5}, {6}}
	descriptions := make([]BucketDescription, len(ids))
	for i, id := range ids {
		descriptions[i] = BucketDescription{ID: id.String(), Name: fmt.Sprint("topology-", i), Revision: 1, State: BucketActive, Policy: DefaultBucketPolicy()}
	}
	edge := func(from, to int, link byte, direction string) {
		descriptions[from].ClonePeers = append(descriptions[from].ClonePeers, BucketClonePeer{Bucket: ids[to].String(), Link: (client.ID{link}).String(), Direction: direction})
	}
	edge(0, 1, 1, "child")
	edge(0, 2, 2, "child")
	edge(1, 0, 1, "parent")
	edge(2, 0, 2, "parent")
	edge(3, 4, 3, "")
	edge(4, 3, 3, "")
	return descriptions
}
func topologyServer(descriptions []BucketDescription) *Server {
	s := &Server{bucketsByID: map[volume.ID]*Bucket{}, bucketsByName: map[string]*Bucket{}}
	for _, d := range descriptions {
		b := bucketSnapshot(d)
		b.server = s
		s.bucketsByID[b.ID()] = b
		s.bucketsByName[d.Name] = b
	}
	return s
}
func replaceTopologyDescription(s *Server, d BucketDescription) {
	id, _ := bucketID(d.ID)
	b := s.bucketsByID[id]
	b.mu.Lock()
	b.setDescription(d)
	b.mu.Unlock()
}

func TestCOWTopologyProofMatchesPreviousAlgorithm(t *testing.T) {
	d := topologyDescriptions()
	s := topologyServer(d)
	image, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	for _, description := range d {
		id, _ := bucketID(description.ID)
		want, err := originalServerTopologyProof(s, image.graph, id)
		if err != nil {
			t.Fatal(err)
		}
		got, err := image.proof(id)
		if err != nil || got != want {
			t.Fatal("changed wire proof", id, got, want, err)
		}
	}
	// Peer order participates in the existing proof and must invalidate the image.
	old := image
	d[0].ClonePeers = slices.Clone(d[0].ClonePeers)
	slices.Reverse(d[0].ClonePeers)
	replaceTopologyDescription(s, d[0])
	image, err = s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := old.proof(volume.ID{1})
	after, _ := image.proof(volume.ID{1})
	want, _ := originalServerTopologyProof(s, image.graph, volume.ID{1})
	if image == old || before == after || after != want {
		t.Fatal("peer reordering reused old proof")
	}
}
func TestCOWTopologyIgnoresNonTopologyChanges(t *testing.T) {
	d := topologyDescriptions()
	s := topologyServer(d)
	image, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	d[0].Revision++
	d[0].Name = "renamed"
	d[0].State = BucketLocked
	d[0].Grants = []BucketGrant{{UserID: "user", Permissions: BucketRead}}
	d[0].Receipts = make([]BucketReceipt, 4096)
	d[0].Policy.Limits.Absolute = 12345
	replaceTopologyDescription(s, d[0])
	current, err := s.cowTopology()
	if err != nil || current != image {
		t.Fatal("unrelated metadata rebuilt topology", err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		got, err := s.cowTopology()
		if err != nil || got != image {
			panic("snapshot changed")
		}
		if _, err = got.proof(volume.ID{1}); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warm topology allocated %.1f objects per lookup", allocations)
	}
}
func TestCOWTopologyInvalidatesEdgesAndRetainsOldDirections(t *testing.T) {
	d := topologyDescriptions()
	s := topologyServer(d)
	old, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	oldProof, _ := old.proof(volume.ID{1})
	for i := 0; i < 3; i++ {
		d[i].ClonePeers = slices.Clone(d[i].ClonePeers)
	}
	// Publish one endpoint first: do not hide an incomplete graph with old cache.
	d[0].ClonePeers[0].Link = (client.ID{9}).String()
	replaceTopologyDescription(s, d[0])
	if _, err = s.cowTopology(); statusOf(err) != 503 {
		t.Fatalf("incomplete graph accepted: %v", err)
	}
	d[1].ClonePeers[0].Link = d[0].ClonePeers[0].Link
	replaceTopologyDescription(s, d[1])
	current, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	if proof, _ := current.proof(volume.ID{1}); proof == oldProof {
		t.Fatal("link edit retained proof")
	}
	// Reverse a directed edge atomically, retaining the previous snapshot for I/O.
	d[0].ClonePeers[0].Direction = "parent"
	d[1].ClonePeers[0].Direction = "child"
	s.bucketMu.Lock()
	replaceTopologyDescription(s, d[0])
	replaceTopologyDescription(s, d[1])
	s.bucketMu.Unlock()
	current, err = s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	beforePeers, err := (cowServerObjects{topology: old}).cloneNeighbors(volume.ID{2}, false)
	if err != nil || !slices.Equal(beforePeers, []volume.ID{{1}}) {
		t.Fatal("old read directions changed", beforePeers, err)
	}
	nowPeers, err := (cowServerObjects{topology: current}).cloneNeighbors(volume.ID{2}, false)
	if err != nil || len(nowPeers) != 0 {
		t.Fatal("new direction not published", nowPeers, err)
	}
	d[3].DeletionComplete = true
	replaceTopologyDescription(s, d[3])
	if _, err = s.cowTopology(); statusOf(err) != 503 {
		t.Fatal("half-deleted link accepted", err)
	}
	d[4].DeletionComplete = true
	replaceTopologyDescription(s, d[4])
	current, err = s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = current.proof(volume.ID{4}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("deleted namespace retained proof", err)
	}
}
func TestCOWTopologyConcurrentPublication(t *testing.T) {
	d := topologyDescriptions()
	s := topologyServer(d)
	old, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	wantA, _ := old.proof(volume.ID{1})
	changed := slices.Clone(d)
	changed[0].ClonePeers = slices.Clone(d[0].ClonePeers)
	changed[1].ClonePeers = slices.Clone(d[1].ClonePeers)
	changed[0].ClonePeers[0].Link = (client.ID{9}).String()
	changed[1].ClonePeers[0].Link = changed[0].ClonePeers[0].Link
	other := buildCowTopology(changed)
	if other.err != nil {
		t.Fatal(other.err)
	}
	wantB, _ := other.proof(volume.ID{1})
	var workers sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 1000 {
				image, err := s.cowTopology()
				if err != nil {
					errors <- err
					return
				}
				p, err := image.proof(volume.ID{1})
				if err != nil || p != wantA && p != wantB {
					errors <- fmt.Errorf("mixed snapshot proof: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		next := d
		if i%2 == 0 {
			next = changed
		}
		s.bucketMu.Lock()
		replaceTopologyDescription(s, next[0])
		replaceTopologyDescription(s, next[1])
		s.bucketMu.Unlock()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

// Exact former server digest path, separate from the pure description oracle.
func originalServerTopologyProof(s *Server, g cowGraph, bucket volume.ID) ([32]byte, error) {
	e := client.Encoder{}
	for _, id := range g.component(bucket) {
		b, err := s.Bucket(id)
		if err != nil {
			return [32]byte{}, err
		}
		e.ID(client.ID(id))
		for _, edge := range b.storageDescription().ClonePeers {
			peer, err := bucketID(edge.Bucket)
			if err != nil {
				return [32]byte{}, err
			}
			link, err := bucketID(edge.Link)
			if err != nil {
				return [32]byte{}, err
			}
			other, err := s.Bucket(peer)
			if err != nil {
				return [32]byte{}, err
			}
			symmetric := false
			for _, back := range other.storageDescription().ClonePeers {
				if back.Bucket == id.String() && back.Link == edge.Link && cowDirectionsMatch(edge.Direction, back.Direction) {
					symmetric = true
					break
				}
			}
			if !symmetric {
				return [32]byte{}, fail(503, "clone connection is being published")
			}
			e.ID(client.ID(peer))
			e.ID(client.ID(link))
			e.Text(edge.Direction)
		}
		e.ID(client.ID{})
	}
	return sha256.Sum256(e.B), nil
}
func BenchmarkCOWTopologyLookup(b *testing.B) {
	for _, n := range []int{2, 128, 1024} {
		b.Run(fmt.Sprintf("buckets-%d", n), func(b *testing.B) {
			d := topologyDescriptions()[:2]
			d[0].ClonePeers = d[0].ClonePeers[:1]
			for i := 2; i < n; i++ {
				id := volume.ID{0xff}
				id[1] = byte(i)
				id[2] = byte(i >> 8)
				d = append(d, BucketDescription{ID: id.String(), Receipts: make([]BucketReceipt, 128)})
			}
			s := topologyServer(d)
			image, err := s.cowTopology()
			if err != nil {
				b.Fatal(err)
			}
			_ = image
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				g, err := s.cowTopology()
				if err != nil {
					b.Fatal(err)
				}
				if _, err = g.proof(volume.ID{1}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func BenchmarkCOWTopologyPrevious(b *testing.B) {
	for _, n := range []int{2, 128, 1024} {
		b.Run(fmt.Sprintf("buckets-%d", n), func(b *testing.B) {
			d := topologyDescriptions()[:2]
			d[0].ClonePeers = d[0].ClonePeers[:1]
			for i := 2; i < n; i++ {
				id := volume.ID{0xff}
				id[1] = byte(i)
				id[2] = byte(i >> 8)
				d = append(d, BucketDescription{ID: id.String(), Receipts: make([]BucketReceipt, 128)})
			}
			s := topologyServer(d)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				descriptions := s.bucketSnapshots()
				g, err := cowGraphDescriptions(descriptions)
				if err != nil {
					b.Fatal(err)
				}
				if _, err = originalServerTopologyProof(s, g, volume.ID{1}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCOWTopologyPublicationAddsAndDeletesNamespaces(t *testing.T) {
	s := topologyServer(nil)
	empty, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	d := topologyDescriptions()[5]
	s.publishBucket(d)
	added, err := s.cowTopology()
	if err != nil || added == empty {
		t.Fatal("new namespace did not invalidate", err)
	}
	id, _ := bucketID(d.ID)
	if _, err = added.proof(id); err != nil {
		t.Fatal(err)
	}
	d.Revision++
	d.Receipts = make([]BucketReceipt, 128)
	s.publishBucket(d)
	current, err := s.cowTopology()
	if err != nil || current != added {
		t.Fatal("receipt publication invalidated topology", err)
	}
	d.Revision++
	d.DeletionComplete = true
	s.publishBucket(d)
	deleted, err := s.cowTopology()
	if err != nil || deleted == added {
		t.Fatal("deletion publication not observed", err)
	}
	if _, err = deleted.proof(id); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("deleted namespace exposed proof", err)
	}
}

func TestCOWTopologyKeepsUnrelatedComponentProof(t *testing.T) {
	d := topologyDescriptions()
	s := topologyServer(d)
	before, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	wanted, _ := before.proof(volume.ID{1})
	otherBefore, _ := before.proof(volume.ID{4})
	for _, i := range []int{3, 4} {
		d[i].ClonePeers = slices.Clone(d[i].ClonePeers)
		d[i].ClonePeers[0].Link = (client.ID{0xaa}).String()
	}
	s.bucketMu.Lock()
	replaceTopologyDescription(s, d[3])
	replaceTopologyDescription(s, d[4])
	s.bucketMu.Unlock()
	after, err := s.cowTopology()
	if err != nil {
		t.Fatal(err)
	}
	got, _ := after.proof(volume.ID{1})
	otherAfter, _ := after.proof(volume.ID{4})
	if before == after || got != wanted || otherBefore == otherAfter {
		t.Fatal("component proofs do not isolate unrelated topology changes")
	}
}
