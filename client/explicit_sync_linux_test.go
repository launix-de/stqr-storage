// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"syscall"
	"testing"
)

func TestFilesystemFUSECloseDoesNotSyncButOSyncDoes(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	p, host := newDeferredFSPeer(t, caps)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	f, err := OpenFilesystem(ctx, c, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Create(ctx, FilesystemRootID(), "file", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	node := &FSNode{filesystem: f, id: info.ID}
	h, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatal(errno)
	}
	if _, errno = node.Write(ctx, h, []byte("burst"), 0); errno != 0 {
		t.Fatal(errno)
	}
	if errno = node.Flush(ctx, h); errno != 0 {
		t.Fatal(errno)
	}
	if errno = h.(*fsOpenFile).Release(ctx); errno != 0 {
		t.Fatal(errno)
	}
	if p.fences.Load() != 0 || p.durable.Load() != 0 || p.refsDurable.Load() != 0 {
		t.Fatal("ordinary write/flush/release persisted", p.fences.Load(), p.durable.Load(), p.refsDurable.Load())
	}
	h, _, errno = node.Open(ctx, syscall.O_RDWR|syscall.O_SYNC)
	if errno != 0 {
		t.Fatal(errno)
	}
	if _, errno = node.Write(ctx, h, []byte("sync"), 0); errno != 0 {
		t.Fatal(errno)
	}
	if p.fences.Load() != 1 {
		t.Fatal("O_SYNC write did not fence", p.fences.Load())
	}
	if errno = h.(*fsOpenFile).Release(ctx); errno != 0 {
		t.Fatal(errno)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if p.fences.Load() != 1 {
		t.Fatal("release/client close implicitly synced", p.fences.Load())
	}
}
