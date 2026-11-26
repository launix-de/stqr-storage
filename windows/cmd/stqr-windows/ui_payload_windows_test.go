// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestEmbeddedUIExtractionIntegrityAndReuse(t *testing.T) {
	data := []byte("MZ embedded UI test fixture")
	digest := sha256.Sum256(data)
	expected := hex.EncodeToString(digest[:])
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := z.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path, err := unpackWinUI(root, compressed.Bytes(), expected)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "stqr-windows.ui.exe" {
		t.Fatal("unstable internal WinUI basename", path)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	same, err := unpackWinUI(root, compressed.Bytes(), expected)
	if err != nil || same != path {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("valid cached resource was replaced", err)
	}
	if err := os.WriteFile(path, []byte("damaged cache"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := unpackWinUI(root, compressed.Bytes(), expected); err != nil {
		t.Fatal(err)
	}
	if repaired, err := os.ReadFile(path); err != nil || !bytes.Equal(repaired, data) {
		t.Fatal("cache integrity was not restored", err)
	}
	wrong := hex.EncodeToString(make([]byte, 32))
	if _, err := unpackWinUI(root, compressed.Bytes(), wrong); err == nil {
		t.Fatal("invalid embedded payload accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "runtime", wrong, "stqr-windows.ui.exe")); !os.IsNotExist(err) {
		t.Fatal("invalid UI was published", err)
	}
}
