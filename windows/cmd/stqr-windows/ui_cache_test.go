// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

func TestEmbeddedUIConcurrentExtractionAndRepair(t *testing.T) {
	data := bytes.Repeat([]byte("MZ concurrent embedded UI fixture"), 128)
	sum := sha256.Sum256(data)
	expected := hex.EncodeToString(sum[:])
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := z.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	target := filepath.Join(root, "runtime", expected, "stqr-windows.ui.exe")
	for _, repair := range []bool{false, true} {
		if repair {
			if err := os.WriteFile(target, []byte("damaged cache"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		failures := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			go func() {
				path, err := unpackWinUI(root, compressed.Bytes(), expected)
				if err == nil && path != target {
					err = fmt.Errorf("unexpected runtime target %q", path)
				}
				failures <- err
			}()
		}
		for worker := 0; worker < 8; worker++ {
			if err := <-failures; err != nil {
				t.Fatal("concurrent extraction/repair", repair, err)
			}
		}
		raw, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(raw, data) {
			t.Fatal("concurrent extraction exposed invalid bytes", repair, err)
		}
		entries, err := os.ReadDir(filepath.Dir(target))
		if err != nil || len(entries) != 1 || entries[0].Name() != "stqr-windows.ui.exe" {
			t.Fatal("runtime cache retained incomplete files", entries, err)
		}
	}
}

func TestEmbeddedUIFailureDoesNotPublishOrLeaveTemporaryFiles(t *testing.T) {
	data := []byte("MZ verified UI fixture")
	sum := sha256.Sum256(data)
	expected := hex.EncodeToString(sum[:])
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := z.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"malformed":            []byte("not a gzip executable"),
		"missing gzip trailer": compressed.Bytes()[:compressed.Len()-4],
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if _, err := unpackWinUI(root, payload, expected); err == nil {
				t.Fatal("corrupt payload accepted")
			}
			entries, err := os.ReadDir(filepath.Join(root, "runtime", expected))
			if err != nil || len(entries) != 0 {
				t.Fatal("failed payload left published/temporary bytes", entries, err)
			}
		})
	}
	root := t.TempDir()
	target := filepath.Join(root, "runtime", expected, "stqr-windows.ui.exe")
	if err := os.MkdirAll(target, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := unpackWinUI(root, compressed.Bytes(), expected); err == nil {
		t.Fatal("failed replacement accepted")
	}
	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil || len(entries) != 1 || !entries[0].IsDir() {
		t.Fatal("failed replacement left an incomplete executable", entries, err)
	}
	if _, err := unpackWinUI(t.TempDir(), compressed.Bytes(), "../invalid"); err == nil {
		t.Fatal("non-digest path accepted")
	}
}
