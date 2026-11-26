// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func TestDurabilityResponseConfirmsOnlyCheckpointedPrefix(t *testing.T) {
	s := testServer(t)
	var bucket volume.ID
	bucket[0] = 19
	stores := make([]*volume.Volume, 2)
	gens := make([]uint64, 2)
	for i := range stores {
		mem, err := volume.NewMemory(16 << 20)
		if err != nil {
			t.Fatal(err)
		}
		v, err := volume.Format(mem, volume.FormatOptions{Class: volume.SSD})
		if err != nil {
			t.Fatal(err)
		}
		defer v.Close()
		stores[i] = v
		gens[i] = connectionTestPut(t, v, volume.Key{Object: volume.ObjectID{byte(i + 1)}}, "accepted")
		s.recordAccepted(bucket, v, gens[i])
	}
	decodeHeaders := func(body []byte) map[uint64]client.CommitMark {
		d := client.Decoder{B: body}
		d.V()
		h := client.Decoder{B: d.Blob()}
		count := h.V()
		result := map[uint64]client.CommitMark{}
		for i := uint64(0); i < count; i++ {
			tag := h.V()
			m, err := client.DecodeCommitMark(h.Blob())
			if err != nil {
				t.Fatal(err)
			}
			result[tag] = m
		}
		if err := h.Done(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	headers := func() map[uint64]client.CommitMark { return decodeHeaders(s.durabilityResponse(200, nil, bucket)) }
	if err := stores[0].CheckpointThrough(gens[0]); err != nil {
		t.Fatal(err)
	}
	h := headers()
	if h[client.DurabilityHeader].Sequence != 2 || h[durabilityConfirmedHeader].Sequence != 0 {
		t.Fatal("one durable volume confirmed multi-volume prefix", h)
	}
	if err := stores[1].CheckpointThrough(gens[1]); err != nil {
		t.Fatal(err)
	}
	h = headers()
	if h[durabilityConfirmedHeader] != (client.CommitMark{Boot: s.boot, Sequence: 2}) {
		t.Fatal("missing exact prefix confirmation", h)
	}
	// An already-durable prefix is still only a best-effort response hint.
	// A locked unrelated volume must suppress confirmation rather than queue
	// the response behind its storage work.
	hold, err := stores[0].Open(true)
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan []byte, 1)
	go func() { response <- s.durabilityResponse(200, nil, bucket) }()
	select {
	case body := <-response:
		hold.Close()
		h = decodeHeaders(body)
		if h[durabilityConfirmedHeader].Sequence != 0 || h[client.DurabilityHeader].Sequence != 2 {
			t.Fatal("busy volume did not conservatively omit optional confirmation", h)
		}
	case <-time.After(time.Second):
		hold.Close()
		<-response
		t.Fatal("response waited for an unrelated volume lock")
	}
	if h = headers(); h[durabilityConfirmedHeader].Sequence != 2 {
		t.Fatal("idle volume confirmation did not resume", h)
	}
	// Background/explicit pruning must not remove confirmation for a client
	// which still retains an earlier accepted mark.
	s.durability.mu.Lock()
	state := s.durability.buckets[bucket]
	state.durable = 2
	state.volumes = map[*volume.Volume]uint64{}
	s.durability.mu.Unlock()
	h = headers()
	if h[client.DurabilityHeader].Sequence != 0 || h[durabilityConfirmedHeader].Sequence != 2 {
		t.Fatal("pruned prefix lost confirmation", h)
	}
	g := connectionTestPut(t, stores[0], volume.Key{Object: volume.ObjectID{8}}, "later")
	s.recordAccepted(bucket, stores[0], g)
	h = headers()
	if h[client.DurabilityHeader].Sequence != 3 || h[durabilityConfirmedHeader].Sequence != 0 {
		t.Fatal("later uncheckpointed write inherited earlier confirmation", h)
	}
}
