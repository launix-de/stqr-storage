// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

func TestReadonlyFilesystemNeverSendsMutationEvenForPendingIntent(t *testing.T) {
	ctx := context.Background()
	base, store := newTestFS(t)
	root := FilesystemRootID()
	file := createTestFile(t, base, "file")
	if _, err := base.WriteAt(ctx, file.ID, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	_, _ = pendingTestTransaction(t, base)
	guard := &overlayReadStore{MemoryFilesystemStore: store, reads: map[ObjectID]int{}}
	ro, err := OpenFilesystem(ctx, guard, FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := ro.Stat(ctx, root); err != nil || info.Mode != 0755 {
		t.Fatal(info, err)
	}
	if _, err = ro.ReadDir(ctx, root); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 7)
	if _, err = ro.ReadAt(ctx, file.ID, buf, 0); err != nil || string(buf) != "payload" {
		t.Fatal(string(buf), err)
	}
	mode := uint32(0600)
	for _, operation := range []func() error{
		func() error { _, e := ro.Create(ctx, root, "denied", 0600, 0, 0, false); return e },
		func() error { _, e := ro.WriteAt(ctx, file.ID, []byte("denied"), 0); return e },
		func() error { _, e := ro.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode}); return e },
		func() error { return ro.Unlink(ctx, root, "file") },
		func() error { return ro.Rmdir(ctx, root, "file") },
		func() error { return ro.Rename(ctx, root, "file", root, "other") },
	} {
		if err := operation(); !errors.Is(err, syscall.EROFS) {
			t.Fatal(err)
		}
	}
	if guard.writes != 0 {
		t.Fatal("readonly used mutating communication", guard.writes)
	}
}
