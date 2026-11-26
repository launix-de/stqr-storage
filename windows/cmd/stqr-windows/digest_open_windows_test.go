// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDigestReaderAllowsConcurrentAtomicReplacement(t *testing.T) {
	for _, kind := range []string{"short", "long"} {
		t.Run(kind, func(t *testing.T) { testDigestReaderReplacement(t, kind == "long") })
	}
}

func testDigestReaderReplacement(t *testing.T, long bool) {
	t.Helper()
	directory := t.TempDir()
	if long {
		for i := 0; i < 10; i++ {
			directory = filepath.Join(directory, strings.Repeat("segment", 4))
		}
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	target, replacement := filepath.Join(directory, "cache.exe"), filepath.Join(directory, "replacement.exe")
	old, fresh := []byte("old damaged cache"), []byte("verified replacement")
	if err := os.WriteFile(target, old, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(replacement, fresh, 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := openExecutableForDigest(target)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := os.Rename(replacement, target); err != nil {
		t.Fatal("integrity reader blocked atomic cache replacement", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal("replacement changed an active integrity reader's bytes", string(got), err)
	}
	got, err = os.ReadFile(target)
	if err != nil || !bytes.Equal(got, fresh) {
		t.Fatal("atomic replacement did not publish verified bytes", string(got), err)
	}
}

func TestOrdinaryWindowsReaderBlocksReplacementControl(t *testing.T) {
	directory := t.TempDir()
	target, replacement := filepath.Join(directory, "cache.exe"), filepath.Join(directory, "replacement.exe")
	if err := os.WriteFile(target, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(replacement, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, target); err == nil {
		reader.Close()
		t.Fatal("negative control: ordinary non-delete-shared reader allowed replacement")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, target); err != nil {
		t.Fatal("negative control: replacement remained unavailable after reader closed", err)
	}
}
