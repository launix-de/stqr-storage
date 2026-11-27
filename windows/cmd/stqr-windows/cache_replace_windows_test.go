// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/windows"
)

func TestExecutableReplacementNeverExposesMissingOrPartialTarget(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "cache-東京-📦")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "cache.exe")
	old, fresh := bytes.Repeat([]byte("old-cache"), 256), bytes.Repeat([]byte("new-cache"), 256)
	if err := os.WriteFile(target, old, 0600); err != nil {
		t.Fatal(err)
	}
	held, err := openExecutableForDigest(target)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	var observations atomic.Uint64
	go func() {
		first := true
		for {
			select {
			case <-ctx.Done():
				done <- nil
				return
			default:
			}
			f, e := openExecutableForDigest(target)
			if e != nil {
				done <- e
				return
			}
			data, e := io.ReadAll(f)
			closeErr := f.Close()
			if e == nil {
				e = closeErr
			}
			if e != nil {
				done <- e
				return
			}
			if !bytes.Equal(data, old) && !bytes.Equal(data, fresh) {
				done <- errors.New("reader observed partial replacement bytes")
				return
			}
			observations.Add(1)
			if first {
				close(ready)
				first = false
			}
		}
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error("target disappeared or changed in place", err)
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		done <- err
		t.Fatal("observer failed before publication", err)
	}
	for i := 0; i < 64; i++ {
		replacement := filepath.Join(directory, "replacement.exe")
		data := fresh
		if i%2 == 1 {
			data = old
		}
		if err := os.WriteFile(replacement, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := replaceExecutable(replacement, target); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	data, err := io.ReadAll(held)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("replacement changed an existing handle", err)
	}
	if observations.Load() < 2 {
		t.Fatal("observer did not run concurrently")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "cache.exe" {
		t.Fatal("replacement retained extra names", entries, err)
	}
}

func TestUnsupportedExecutableReplacementLeavesBothFilesIntact(t *testing.T) {
	for _, native := range []error{windows.ERROR_NOT_SUPPORTED, windows.ERROR_INVALID_PARAMETER, windows.ERROR_INVALID_FUNCTION} {
		t.Run(native.Error(), func(t *testing.T) {
			directory := t.TempDir()
			source, target := filepath.Join(directory, "new.exe"), filepath.Join(directory, "cache.exe")
			old, fresh := []byte("previous cache"), []byte("verified replacement")
			if err := os.WriteFile(source, fresh, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, old, 0600); err != nil {
				t.Fatal(err)
			}
			err := replaceExecutableWith(source, target, func(windows.Handle, uint32, *byte, uint32) error { return native })
			if !errors.Is(err, native) || !strings.Contains(err.Error(), "unsupported") {
				t.Fatal("unsupported capability was hidden", err)
			}
			for path, want := range map[string][]byte{source: fresh, target: old} {
				got, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(got, want) {
					t.Fatal("failed replacement changed a file", path, e)
				}
			}
			entries, e := os.ReadDir(directory)
			if e != nil || len(entries) != 2 {
				t.Fatal("failed replacement changed namespace", entries, e)
			}
		})
	}
}
