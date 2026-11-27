// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
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
	directory := digestReplacementDirectory(t, long)
	testDigestReaderReplacementPaths(t, filepath.Join(directory, "cache.exe"), filepath.Join(directory, "replacement.exe"))
}

func digestReplacementDirectory(t *testing.T, long bool) string {
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
	return directory
}

func testDigestReaderReplacementPaths(t *testing.T, target, replacement string) {
	t.Helper()
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
	if err := replaceExecutable(replacement, target); err != nil {
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

func TestDigestReaderAllowsExtendedAtomicReplacement(t *testing.T) {
	for _, long := range []bool{false, true} {
		t.Run(map[bool]string{false: "short", true: "long"}[long], func(t *testing.T) {
			directory := `\\?\` + digestReplacementDirectory(t, long)
			testDigestReaderReplacementPaths(t, filepath.Join(directory, "cache.exe"), filepath.Join(directory, "replacement.exe"))
		})
	}
}

func TestExecutableWindowsPathNamespaces(t *testing.T) {
	for _, test := range []struct{ input, expected string }{
		{`C:\cache\ui.exe`, `\\?\C:\cache\ui.exe`},
		{`\\server\share\cache\ui.exe`, `\\?\UNC\server\share\cache\ui.exe`},
		{`\\?\C:\cache\ui.exe`, `\\?\C:\cache\ui.exe`},
		{`\\?\UNC\server\share\cache\ui.exe`, `\\?\UNC\server\share\cache\ui.exe`},
		{`\\.\C:\cache\ui.exe`, `\\.\C:\cache\ui.exe`},
	} {
		got, err := executableWindowsPath(test.input)
		if err != nil || got != test.expected {
			t.Fatal(test.input, got, err)
		}
	}
}

// Exercise the complete production repair while a separate integrity reader
// holds the damaged inode. The reader is kept open through successful publish.
func TestEmbeddedUIRepairWithActiveIntegrityReader(t *testing.T) {
	for _, long := range []bool{false, true} {
		t.Run(map[bool]string{false: "short", true: "long"}[long], func(t *testing.T) {
			root := digestReplacementDirectory(t, long)
			data := []byte("MZ verified replacement UI")
			sum := sha256.Sum256(data)
			expected := hex.EncodeToString(sum[:])
			var payload bytes.Buffer
			z := gzip.NewWriter(&payload)
			if _, err := z.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := z.Close(); err != nil {
				t.Fatal(err)
			}
			target, err := unpackWinUI(root, payload.Bytes(), expected)
			if err != nil {
				t.Fatal(err)
			}
			old := []byte("damaged cache held by another integrity check")
			if err := os.WriteFile(target, old, 0600); err != nil {
				t.Fatal(err)
			}
			reader, err := openExecutableForDigest(target)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if repaired, err := unpackWinUI(root, payload.Bytes(), expected); err != nil || repaired != target {
				t.Fatal("repair failed while a separate integrity reader remained open", repaired, err)
			}
			if got, err := io.ReadAll(reader); err != nil || !bytes.Equal(got, old) {
				t.Fatal("repair changed the old reader's bytes", got, err)
			}
			if digest, err := executableDigest(target); err != nil || digest != sum {
				t.Fatal("repair did not publish the verified executable", digest, err)
			}
			if entries, err := os.ReadDir(filepath.Dir(target)); err != nil || len(entries) != 1 {
				t.Fatal("repair left temporary/unpublished files", entries, err)
			}
		})
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
