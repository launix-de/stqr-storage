// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestKernelCacheRemovalInvalidationHost(t *testing.T) {
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("POSIX C compiler required")
	}
	source, err := os.ReadFile("../kernel/kstqr/cache.c")
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(source, []byte("static void remove_entry("))
	if start < 0 {
		t.Fatal("missing actual remove_entry")
	}
	end := bytes.Index(source[start:], []byte("\nvoid ks_cache_forget("))
	if end < 0 {
		t.Fatal("missing remove_entry boundary")
	}
	dir := t.TempDir()
	body := filepath.Join(dir, "remove_entry.inc")
	if err = os.WriteFile(body, source[start:start+end], 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "cache-removal")
	args := []string{"-std=c11", "-Wall", "-Wextra", "-Werror", "-Wno-unused-parameter", "-DREMOVE_ENTRY_SOURCE=" + strconv.Quote(body), "../kernel/kstqr/tests/cache_removal_invalidation.c", "-o", binary}
	if output, err := exec.Command(compiler, args...).CombinedOutput(); err != nil {
		t.Fatalf("compile actual removal: %v\n%s", err, output)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("CACHE_REMOVAL_INVALIDATION_PASS")) {
		t.Fatalf("cache removal invariants: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
