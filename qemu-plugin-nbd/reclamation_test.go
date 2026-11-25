// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

// The adapter must send repeated writes to the same native stripe identities.
// Physical allocation reuse is checked independently by the volume tests.
func TestRepeatedOverwriteTrimAndCollectKeepBoundedStripeObjects(t *testing.T) {
	ctx := context.Background()
	store := newTestStore()
	catalog := NewCatalog(store)
	device, err := catalog.Create(ctx, Metadata{Name: "reused", Size: 2 * StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	metadata := MetadataID(device.Name)
	stripe := StripeID(device.ID, 0)
	payload := make([]byte, StripeSize)
	for iteration := 0; iteration < 100; iteration++ {
		for i := range payload {
			payload[i] = byte(iteration)
		}
		if err := catalog.WriteAt(ctx, device, payload, 0); err != nil {
			t.Fatal(err)
		}
		if err := catalog.WriteAt(ctx, device, []byte("range update"), 8192); err != nil {
			t.Fatal(err)
		}
		copy(payload[8192:], []byte("range update"))
		if len(store.objects) != 2 || store.objects[metadata].data == nil || store.objects[stripe].data == nil {
			t.Fatalf("iteration %d changed stripe identity or grew graph: %d", iteration, len(store.objects))
		}
		got := make([]byte, len(payload))
		// A fresh catalog must decode/read the current native object graph.
		if err := NewCatalog(store).ReadAt(ctx, device, got, 0); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("iteration %d data mismatch after reopen: %v", iteration, err)
		}
		if err := catalog.Trim(ctx, device, 0, StripeSize); err != nil {
			t.Fatal(err)
		}
		if len(store.objects) != 1 {
			t.Fatalf("iteration %d did not release trimmed stripe", iteration)
		}
	}
	// Descriptor CAS changes its bytes without changing the descriptor identity.
	for iteration := 1; iteration <= 100; iteration++ {
		device, err = catalog.Resize(ctx, device, 2*StripeSize+uint64(iteration)*512)
		if err != nil {
			t.Fatal(err)
		}
		if len(store.objects) != 1 || store.objects[metadata].data == nil {
			t.Fatal("descriptor CAS allocated a new metadata identity")
		}
	}
	if err := catalog.WriteAt(ctx, device, []byte("live stripe"), StripeSize); err != nil {
		t.Fatal(err)
	}
	old := device
	if err := store.Delete(ctx, metadata, client.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	device, err = catalog.Create(ctx, Metadata{Name: old.Name, Size: old.Size})
	if err != nil || device.ID == old.ID {
		t.Fatalf("recreated descriptor reused a deleted generation: %v", err)
	}
	// Collection is an explicitly offline/exclusive step. It must preserve
	// unrelated application roots and reclaim only the old device's stripes.
	opaque := client.ObjectID{7}
	if _, err := store.Put(ctx, opaque, []byte("application root"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	deleted, err := catalog.Collect(ctx, []client.ObjectID{opaque})
	if err != nil || deleted != 1 || len(store.objects) != 2 {
		t.Fatalf("orphan collection: deleted=%d remaining=%d err=%v", deleted, len(store.objects), err)
	}
	got := make([]byte, len("live stripe"))
	if err := catalog.ReadAt(ctx, device, got, StripeSize); err != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatalf("new device exposed deleted generation: %v", err)
	}
}

func TestCollectionKeepsOpenExportGenerations(t *testing.T) {
	for _, readonly := range []bool{false, true} {
		t.Run(map[bool]string{false: "writer", true: "readers"}[readonly], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore()
			catalog := NewCatalog(store)
			device, err := catalog.Create(ctx, Metadata{Name: "open", Size: StripeSize})
			if err != nil {
				t.Fatal(err)
			}
			if err := catalog.WriteAt(ctx, device, []byte("open generation"), 0); err != nil {
				t.Fatal(err)
			}
			release, err := catalog.claim(device.ID, readonly)
			if err != nil {
				t.Fatal(err)
			}
			var second func()
			if readonly {
				second, err = catalog.claim(device.ID, true)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Delete(ctx, MetadataID(device.Name), client.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if deleted, err := catalog.Collect(ctx, nil); err != ErrBusy || deleted != 0 {
				t.Fatalf("collection ignored open export: %d %v", deleted, err)
			}
			release()
			release() // Releasing one claim twice must not release another reader.
			if second != nil {
				if deleted, err := catalog.Collect(ctx, nil); err != ErrBusy || deleted != 0 {
					t.Fatalf("collection ignored second reader: %d %v", deleted, err)
				}
				second()
			}
			if deleted, err := catalog.Collect(ctx, nil); err != nil || deleted != 1 {
				t.Fatalf("collection after last export: %d %v", deleted, err)
			}
		})
	}
}
