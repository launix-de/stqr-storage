// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"testing"
)

func TestTicket32SpecialInodesRequireOfflineReclamation(t *testing.T) {
	for _, kind := range []FSKind{FSSymlink, FSFIFO, FSSocket} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			ctx := context.Background()
			f, store := newTestFS(t)
			var info FSInfo
			var err error
			if kind == FSSymlink {
				info, err = f.Symlink(ctx, FilesystemRootID(), "special", "target", 42, 43)
			} else {
				info, err = f.Mknod(ctx, FilesystemRootID(), "special", kind, 0600, 42, 43)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Get(ctx, info.ID); err != nil {
				t.Fatal("positive inode-presence control", err)
			}
			if err := f.Unlink(ctx, FilesystemRootID(), "special"); err != nil {
				t.Fatal(err)
			}
			if err := f.CollectOrphans(ctx); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Get(ctx, info.ID); err != nil {
				t.Fatal("online collection discarded special inode", err)
			}
			// All holders are explicitly offline at this boundary.
			if _, err := checkFilesystem(ctx, &fsckMemory{MemoryFilesystemStore: store}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Get(ctx, info.ID); !errors.Is(err, ErrNotFound) {
				t.Fatal("offline collection retained unreachable inode", err)
			}
		})
	}
}
