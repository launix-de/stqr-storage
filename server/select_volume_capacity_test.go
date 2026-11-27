// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/volume"
)

func capacitySelectionFixture(t testing.TB, count int) (*Server, *Bucket, []*volume.Volume, volume.Key) {
	return capacitySelectionWithGate(t, count, nil)
}
func capacitySelectionWithGate(t testing.TB, count int, gate *selectionReadGate) (*Server, *Bucket, []*volume.Volume, volume.Key) {
	t.Helper()
	s := &Server{volumes: make(map[string]*managedVolume)}
	b, e := NewBucket(volume.ID{99}, "capacity")
	if e != nil {
		t.Fatal(e)
	}
	b.server = s
	b.description.Policy.PlacementGroup = ""
	stores := make([]*volume.Volume, count)
	for i := range stores {
		dev, e := volume.NewMemory(4 << 20)
		if e != nil {
			t.Fatal(e)
		}
		id := volume.ID{byte(i + 1)}
		var backend volume.VolumeIO = dev
		if i == 0 && gate != nil {
			gate.VolumeIO = dev
			backend = gate
		}
		v, e := volume.Format(backend, volume.FormatOptions{Class: volume.HDD, VolumeID: id})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { v.Close() })
		stores[i] = v
		s.volumes[id.String()] = &managedVolume{store: v, config: VolumeConfig{ID: id.String(), State: "active"}}
	}
	return s, b, stores, volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
}
func selectionPut(t testing.TB, v *volume.Volume, key volume.Key, req volume.ID) {
	t.Helper()
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if _, e = h.Put(key, []byte("data"), volume.Options{RequestID: req, Accepted: true}); e != nil {
		t.Fatal(e)
	}
}
func TestSelectVolumeCapacity(t *testing.T) {
	t.Run("existing-full-and-draining", func(t *testing.T) {
		_, b, vs, key := capacitySelectionFixture(t, 3)
		selectionPut(t, vs[0], key, volume.ID{})
		for _, v := range vs {
			if e := v.SetGuaranteedReserve(v.Stats().AdmissionFreePhysical); e != nil {
				t.Fatal(e)
			}
			if e := v.SetState(volume.Draining); e != nil {
				t.Fatal(e)
			}
		}
		for _, create := range []bool{false, true} {
			got, info, found, e := b.selectVolume(b.Describe(), key, volume.ID{}, create)
			if e != nil || !found || got != vs[0] || info.Length != 4 {
				t.Fatal(create, found, e)
			}
		}
	})
	t.Run("duplicates", func(t *testing.T) {
		_, b, vs, key := capacitySelectionFixture(t, 3)
		selectionPut(t, vs[0], key, volume.ID{})
		selectionPut(t, vs[2], key, volume.ID{})
		for _, create := range []bool{false, true} {
			_, _, _, e := b.selectVolume(b.Describe(), key, volume.ID{}, create)
			if statusOf(e) != 503 {
				t.Fatal("duplicate not rejected", create, e)
			}
		}
	})
	t.Run("later-closed-probe", func(t *testing.T) {
		_, b, vs, key := capacitySelectionFixture(t, 3)
		selectionPut(t, vs[0], key, volume.ID{})
		if e := vs[2].Close(); e != nil {
			t.Fatal(e)
		}
		_, _, _, e := b.selectVolume(b.Describe(), key, volume.ID{}, false)
		if !errors.Is(e, volume.ErrClosed) {
			t.Fatal("mandatory later probe skipped", e)
		}
	})
	t.Run("receipt-owner", func(t *testing.T) {
		_, b, vs, key := capacitySelectionFixture(t, 3)
		req := volume.ID{33}
		selectionPut(t, vs[1], key, req)
		got, info, found, e := b.selectVolume(b.Describe(), key, req, true)
		if e != nil || !found || got != vs[1] || info.Length != 4 {
			t.Fatal(found, e)
		}
	})
	t.Run("miss-and-capacity", func(t *testing.T) {
		_, b, vs, key := capacitySelectionFixture(t, 3)
		if e := vs[0].SetGuaranteedReserve(1 << 20); e != nil {
			t.Fatal(e)
		}
		if e := vs[1].SetInFlightReserve(2 << 20); e != nil {
			t.Fatal(e)
		}
		_, _, _, e := b.selectVolume(b.Describe(), key, volume.ID{}, false)
		if !errors.Is(e, volume.ErrNotFound) {
			t.Fatal(e)
		}
		got, _, found, e := b.selectVolume(b.Describe(), key, volume.ID{}, true)
		if e != nil || found || got != vs[2] {
			t.Fatal("wrong free target", found, e)
		}
		vs[2].SetState(volume.Draining)
		got, _, _, e = b.selectVolume(b.Describe(), key, volume.ID{}, true)
		if e != nil || got != vs[0] {
			t.Fatal("draining target selected", e)
		}
	})
	t.Run("managed-drain-and-local-filter", func(t *testing.T) {
		s, b, vs, key := capacitySelectionFixture(t, 3)
		s.controller = &clusterController{}
		s.volumes[vs[0].Identity().VolumeID.String()].config.State = "draining"
		vs[1].SetState(volume.Draining)
		got, _, _, e := b.selectVolume(b.Describe(), key, volume.ID{}, true)
		if e != nil || got != vs[2] {
			t.Fatal("inactive volume selected", e)
		}
		d := b.Describe()
		d.LocalVolumes = []string{vs[0].Identity().VolumeID.String()}
		_, _, _, e = b.selectVolume(d, key, volume.ID{}, true)
		if !errors.Is(e, volume.ErrFull) {
			t.Fatal("local filter bypassed", e)
		}
	})
}
func BenchmarkSelectVolumeCapacity(b *testing.B) {
	for _, count := range []int{2, 8} {
		b.Run(fmt.Sprintf("volumes%d", count), func(b *testing.B) {
			_, bucket, vs, key := capacitySelectionFixture(b, count)
			selectionPut(b, vs[0], key, volume.ID{})
			description := bucket.Describe()
			for _, op := range []string{"GET", "existing-CAS", "new-create"} {
				b.Run(op, func(b *testing.B) {
					probe := key
					if op == "new-create" {
						probe.Object[0] = 2
					}
					create := op != "GET"
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						_, _, _, e := bucket.selectVolume(description, probe, volume.ID{}, create)
						if e != nil {
							b.Fatal(e)
						}
					}
				})
			}
		})
	}
}

// The first volume's required existence probe may complete just before an
// unrelated writer starts. Existing/read selection must not acquire its lock a
// second time merely to rank an allocation target which will never be used.
type selectionReadGate struct {
	volume.VolumeIO
	armed            atomic.Bool
	entered, release chan struct{}
}

func (g *selectionReadGate) ReadAt(p []byte, off int64) (int, error) {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
	return g.VolumeIO.ReadAt(p, off)
}
func TestSelectVolumeCapacityDoesNotReenterUnrelatedWriter(t *testing.T) {
	for _, create := range []bool{false, true} {
		t.Run(fmt.Sprintf("create=%v", create), func(t *testing.T) {
			gate := &selectionReadGate{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseProbe sync.Once
			defer releaseProbe.Do(func() { close(gate.release) })
			_, b, vs, key := capacitySelectionWithGate(t, 2, gate)
			selectionPut(t, vs[1], key, volume.ID{})
			vs[0].SetCacheLimits(-1, 1)
			gate.armed.Store(true)
			selected := make(chan error, 1)
			go func() {
				v, _, found, e := b.selectVolume(b.Describe(), key, volume.ID{}, create)
				if e == nil && (!found || v != vs[1]) {
					e = errors.New("wrong owner")
				}
				if e == nil {
					h, err := v.Open(false)
					if err == nil {
						_, _, err = h.Get(key)
						h.Close()
					}
					e = err
				}
				selected <- e
			}()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("required probe not reached")
			}
			writerEntered := make(chan struct{})
			releaseWriter := make(chan struct{})
			writerDone := make(chan error, 1)
			defer func() {
				close(releaseWriter)
				releaseProbe.Do(func() { close(gate.release) })
				select {
				case <-writerDone:
				case <-time.After(time.Second):
					t.Error("writer cleanup timed out")
				}
			}()
			go func() {
				h, e := vs[0].Open(true)
				if e != nil {
					writerDone <- e
					return
				}
				close(writerEntered)
				<-releaseWriter
				h.Close()
				writerDone <- nil
			}()
			// The first probe still holds RLock. A failed TryOpen(false) therefore proves
			// the writer is queued rather than relying on a scheduling delay.
			deadline := time.Now().Add(time.Second)
			for {
				h, ok, e := vs[0].TryOpen(false)
				if e != nil {
					t.Fatal(e)
				}
				if !ok {
					break
				}
				h.Close()
				if time.Now().After(deadline) {
					t.Fatal("writer not queued")
				}
				runtime.Gosched()
			}
			releaseProbe.Do(func() { close(gate.release) })
			select {
			case <-writerEntered:
			case <-time.After(time.Second):
				t.Fatal("writer did not acquire empty volume")
			}
			select {
			case e := <-selected:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("existing/read request waited for unrelated capacity ranking")
			}
		})
	}
}
