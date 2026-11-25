// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"testing"
)

func TestOverlayUpperWriteDoesNotWalkUnrelatedLowerFiles(t *testing.T) {
	ctx := context.Background()
	_, o, lower, _, unrelated, _ := overlayFixture(t, nil)
	file, err := o.Create(ctx, FilesystemRootID(), "zzz-new", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	lower.mu.Lock()
	before := lower.reads[unrelated.ID]
	lower.mu.Unlock()
	mode := uint32(0640)
	if _, err = o.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if _, err = o.WriteAt(ctx, file.ID, []byte("tiny"), 0); err != nil {
		t.Fatal(err)
	}
	lower.mu.Lock()
	after := lower.reads[unrelated.ID]
	lower.mu.Unlock()
	if after != before {
		t.Fatalf("upper update scanned unrelated lower inode: before=%d after=%d", before, after)
	}
	// A root publication may add unrelated names; validated hints remain useful.
	if _, err = o.Create(ctx, FilesystemRootID(), "aaa-other", 0600, 0, 0, false); err != nil {
		t.Fatal(err)
	}
	lower.mu.Lock()
	before = lower.reads[unrelated.ID]
	lower.mu.Unlock()
	if _, err = o.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	lower.mu.Lock()
	after = lower.reads[unrelated.ID]
	lower.mu.Unlock()
	if after != before {
		t.Fatal("unrelated root publication forced a full lower walk")
	}
}
