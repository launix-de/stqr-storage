// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestLostRangeDistinguishesRepairFromPartialCreate(t *testing.T) {
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	s := n.node
	addTestVolume(t, s, "data", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "lost-range-contract")
	// Model an already committed loss map. Membership-reset/recovery has its
	// own tests; this fixture does not manufacture a force-disconnect ACK.
	view := s.currentSystem.Load()
	patches := append([]client.MapPatch{}, view.patches...)
	for i := range patches {
		if patches[i].Bucket == client.ID(b.ID()) {
			patches[i].Entries = append([]client.RangeEntry{}, patches[i].Entries...)
			for j := range patches[i].Entries {
				patches[i].Entries[j].State = 5
				patches[i].Entries[j].OwnerEpoch++
			}
		}
	}
	image, err := buildPlacement(patches)
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{1}
	if _, _, err := c.Get(ctx, id); !errors.Is(err, client.ErrGone) {
		t.Fatal("lost GET became absence", err)
	}
	if _, err := c.Stat(ctx, id); !errors.Is(err, client.ErrGone) {
		t.Fatal("lost STAT became absence", err)
	}
	if _, err := c.PutRange(ctx, id, 3, []byte{1}, client.PutOptions{}); !errors.Is(err, client.ErrGone) {
		t.Fatal("partial PUT created a fragment", err)
	}
	if _, err := c.Append(ctx, id, []byte{1}, client.AppendOptions{}); !errors.Is(err, client.ErrGone) {
		t.Fatal("APPEND created a fragment", err)
	}
	if _, err := c.Trunc(ctx, id, 42, client.TruncOptions{}); !errors.Is(err, client.ErrGone) {
		t.Fatal("TRUNC fabricated an empty object", err)
	}
	if _, err := c.Put(ctx, id, nil, client.PutOptions{}); err != nil {
		t.Fatal("full empty PUT did not restore a known object", err)
	}
	if _, err := c.Append(ctx, id, []byte("ok"), client.AppendOptions{}); err != nil {
		t.Fatal("partial update after repair", err)
	}
	if data, _, err := c.Get(ctx, id); err != nil || string(data) != "ok" {
		t.Fatal("repaired object", string(data), err)
	}
	absent := client.ObjectID{2}
	if err := c.Delete(ctx, absent, client.DeleteOptions{}); err != nil {
		t.Fatal("explicit absence", err)
	}
	if _, _, err := c.Get(ctx, absent); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("known tombstone remained unknown", err)
	}
	if _, err := c.Append(ctx, absent, []byte{1}, client.AppendOptions{}); err != nil {
		t.Fatal("APPEND after explicit known absence", err)
	}
}
