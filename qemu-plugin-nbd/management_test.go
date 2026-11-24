// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"context"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestManagementPagesAndConditionalGrowth(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	var disk Metadata
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		m, err := c.Create(ctx, Metadata{Name: name, Size: 4096})
		if err != nil {
			t.Fatal(err)
		}
		disk = m
	}
	// One descriptor per page, including empty filtered pages, with no full scan.
	after := ""
	seen := map[client.ID]bool{}
	for {
		before := s.get
		body, err := c.Manage(ctx, (ManagementRequest{Action: 1, Limit: 1, After: after}).MarshalBinary(), false)
		if err != nil {
			t.Fatal(err)
		}
		items, next, err := DecodeManagementResult(body)
		if err != nil || len(items) != 1 || s.get-before != 1 || seen[items[0].ID] {
			t.Fatal(items, next, err, s.get-before)
		}
		seen[items[0].ID] = true
		if next == "" {
			break
		}
		if next == after {
			t.Fatal("repeated cursor")
		}
		after = next
	}
	if len(seen) != 5 {
		t.Fatal(seen)
	}
	items, next, err := c.ListPage(ctx, "", 1, "not-present")
	if err != nil || len(items) != 0 || next == "" {
		t.Fatal(items, next, err)
	}
	for _, limit := range []uint32{0, 101} {
		if _, _, err = c.ListPage(ctx, "", limit, ""); !errors.Is(err, client.ErrRange) {
			t.Fatal(limit, err)
		}
	}
	r := ManagementRequest{Action: 2, Name: disk.Name, Device: disk.ID, Expected: disk.Size, Size: 8192}
	if _, err = c.Manage(ctx, r.MarshalBinary(), true); err == nil {
		t.Fatal("readonly mutation")
	}
	if _, err = c.Manage(ctx, r.MarshalBinary(), false); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Manage(ctx, r.MarshalBinary(), false); err != nil {
		t.Fatal("retry", err)
	}
	r.Size = 16384
	if _, err = c.Manage(ctx, r.MarshalBinary(), false); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("stale size", err)
	}
	r.Device = client.NewRequestID()
	r.Expected = 8192
	if _, err = c.Manage(ctx, r.MarshalBinary(), false); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("stale device", err)
	}
}
