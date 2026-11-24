//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	winfs "github.com/launix-de/stqr-storage/windows"
	"github.com/winfsp/cgofuse/fuse"
	"time"
)

func watchWindowsChanges(ctx context.Context, store *client.SubscriptionStore, fs *winfs.Filesystem, host *fuse.FileSystemHost) func() {
	ctx, cancel := context.WithCancel(ctx)
	pending := make(chan struct{}, 1)
	finished := make(chan struct{})
	unwatch := store.WatchUpdates(func(client.ObjectID) {
		select {
		case pending <- struct{}{}:
		default:
		}
	})
	go func() {
		defer close(finished)
		for {
			select {
			case <-ctx.Done():
				return
			case <-pending:
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			for _, change := range fs.Changes(ctx) {
				action := uint32(fuse.NOTIFY_CHMOD | fuse.NOTIFY_CHOWN | fuse.NOTIFY_UTIME | fuse.NOTIFY_TRUNCATE)
				if change.Created {
					action = fuse.NOTIFY_CREATE
					if change.Directory {
						action = fuse.NOTIFY_MKDIR
					}
				}
				if change.Removed {
					action = fuse.NOTIFY_UNLINK
					if change.Directory {
						action = fuse.NOTIFY_RMDIR
					}
				}
				host.Notify(change.Path, action)
			}
		}
	}()
	return func() { unwatch(); cancel(); <-finished }
}
