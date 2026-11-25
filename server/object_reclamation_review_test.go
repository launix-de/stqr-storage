// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A local unpinned range can have an obsolete import shadow on another eligible
// volume. It is not authoritative even though node, storage class and PG match.
// Remove it before releasing the canonical deletion marker.
func TestTicket32MetadataReclamationRemovesImportShadowBeforeTombstone(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	first := addTestVolume(t, s, "canonical", "hdd", 4<<20)
	second := addTestVolume(t, s, "obsolete-shadow", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "shadow-reclamation")
	entry, known := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{})
	if !known || len(entry.Locations) != 1 || entry.Locations[0].Node != s.control.ledger.node || entry.Locations[0].Volume != (client.ID{}) {
		t.Fatalf("fixture needs one local unpinned location: %+v", entry)
	}
	canonical := s.volumes[first.VolumeID].store
	shadow := s.volumes[second.VolumeID].store
	key := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
	live := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{2}}
	data := bytes.Repeat([]byte("stale"), 512)
	h, err := canonical.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := h.Put(key, data, volume.Options{})
	if err == nil {
		_, err = h.Put(live, []byte("keep authoritative live data"), volume.Options{})
	}
	if err == nil {
		err = h.Delete(key, volume.Options{})
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	h, err = shadow.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Restore(key, info, func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader(data), uint64(len(data)))
	}, volume.Options{ImportShadow: true})
	if err == nil {
		var marked bool
		marked, err = h.ImportShadow(key)
		if err == nil && !marked {
			t.Error("fixture did not mark the stale import")
		}
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	phase := func(number uint8) {
		t.Helper()
		s.opMu.Lock()
		err := s.reclaimBucketMetadataLocked(context.Background(), b, number)
		s.opMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	assertMissing := func(v *volume.Volume, label string) {
		t.Helper()
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.RecordInfo(key)
		h.Close()
		if !errors.Is(err, volume.ErrNotFound) {
			t.Fatalf("%s still has the physical record: %v", label, err)
		}
	}
	phase(5)
	assertMissing(shadow, "inactive import shadow")
	h, err = canonical.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := h.RecordInfo(key)
	h.Close()
	if err != nil || !tombstone.Deleted {
		t.Fatalf("phase5 prematurely released canonical deletion marker: %+v %v", tombstone, err)
	}
	phase(6)
	assertMissing(canonical, "canonical tombstone")
	actor := userPrincipal(t, s)
	if _, _, err := b.Get(context.Background(), actor, key.Object); !errors.Is(err, volume.ErrNotFound) {
		t.Fatalf("deleted data became visible after both GC phases: %v", err)
	}
	got, _, err := b.Get(context.Background(), actor, live.Object)
	if err != nil || string(got) != "keep authoritative live data" {
		t.Fatalf("collection damaged unrelated authoritative data: %q %v", got, err)
	}
	for _, store := range []*volume.Volume{canonical, shadow} {
		if err := store.Scrub(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTicket32MetadataReclamationPreservesUnavailableRangeSalvage(t *testing.T) {
	for _, state := range []uint8{4, 5} {
		t.Run(fmt.Sprintf("state-%d", state), func(t *testing.T) {
			node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
			s := node.node
			first := addTestVolume(t, s, "unavailable-canonical", "hdd", 4<<20)
			second := addTestVolume(t, s, "recovery-salvage", "hdd", 4<<20)
			b, _ := createProtocolBucket(t, s, "salvage-reclamation")
			canonical := s.volumes[first.VolumeID].store
			shadow := s.volumes[second.VolumeID].store
			live := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}
			deleted := volume.Key{Bucket: b.ID(), Object: volume.ObjectID{2}}
			data := []byte("recoverable payload")
			h, err := canonical.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			info, err := h.Put(live, data, volume.Options{})
			if err == nil {
				_, err = h.Put(deleted, []byte("old deleted data"), volume.Options{})
			}
			if err == nil {
				err = h.Delete(deleted, volume.Options{})
			}
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			h, err = shadow.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			err = h.Restore(live, info, func(yield func(uint64, io.Reader, uint64) error) error {
				return yield(0, bytes.NewReader(data), uint64(len(data)))
			}, volume.Options{ImportShadow: true})
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			// Model committed unavailable/lost placement, as the loss-map tests
			// do. This is not authority to discard surviving recovery media.
			patches := append([]client.MapPatch(nil), s.currentSystem.Load().patches...)
			for i := range patches {
				if patches[i].Bucket != client.ID(b.ID()) {
					continue
				}
				patches[i].Entries = append([]client.RangeEntry(nil), patches[i].Entries...)
				for j := range patches[i].Entries {
					patches[i].Entries[j].State = state
					patches[i].Entries[j].OwnerEpoch++
				}
			}
			image, err := buildPlacement(patches)
			if err != nil {
				t.Fatal(err)
			}
			s.placement.Store(image)
			s.mu.Lock()
			s.volumes[second.VolumeID].config.RecoveryOnly = true
			s.mu.Unlock()
			for _, phase := range []uint8{5, 6} {
				s.opMu.Lock()
				err := s.reclaimBucketMetadataLocked(context.Background(), b, phase)
				s.opMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, store := range []*volume.Volume{canonical, shadow} {
				h, err := store.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				got, _, err := h.Get(live)
				h.Close()
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("unavailable range salvage was reclaimed: %q %v", got, err)
				}
				if err := store.Scrub(); err != nil {
					t.Fatal(err)
				}
			}
			h, err = canonical.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			tombstone, err := h.RecordInfo(deleted)
			h.Close()
			if err != nil || !tombstone.Deleted {
				t.Fatalf("unavailable range deletion evidence was reclaimed: %+v %v", tombstone, err)
			}
		})
	}
}
