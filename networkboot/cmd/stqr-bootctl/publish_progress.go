// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Closing the fd, including on process exit, releases the local publish lock.
func lockPublication(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, ".stqr-publish.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("a publisher is already running for %s", dir)
		}
		return nil, err
	}
	return func() { f.Close() }, nil
}
func reportImportProgress(ctx context.Context, f *os.File) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var once sync.Once
	info, err := f.Stat()
	if err != nil {
		cancel()
		return func() {}
	}
	go func() {
		defer close(done)
		timer := time.NewTicker(10 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				pos, err := f.Seek(0, io.SeekCurrent)
				if err == nil {
					fmt.Fprintf(os.Stderr, "Rootfs archive read: %.1f / %.1f MiB (%.1f%%); waiting for storage as needed\n", float64(pos)/(1<<20), float64(info.Size())/(1<<20), 100*float64(pos)/float64(max(int64(1), info.Size())))
				}
			}
		}
	}()
	return func() { once.Do(func() { cancel(); <-done }) }
}
