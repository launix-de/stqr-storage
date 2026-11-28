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
	hashStart := bytes.Index(source, []byte("static u32 subscription_hash("))
	updateStart := bytes.Index(source, []byte("static void update("))
	if hashStart < 0 || updateStart < 0 {
		t.Fatal("missing actual cache update functions")
	}
	hashEnd := bytes.Index(source[hashStart:], []byte("static struct cached_inode *find("))
	updateEnd := bytes.Index(source[updateStart:], []byte("\n/* Caller holds wire"))
	if hashEnd < 0 || updateEnd < 0 {
		t.Fatal("missing actual cache update boundaries")
	}
	updateBody := filepath.Join(dir, "update.inc")
	code := append(bytes.Clone(source[hashStart:hashStart+hashEnd]), source[updateStart:updateStart+updateEnd]...)
	if err = os.WriteFile(updateBody, code, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "cache-removal")
	args := []string{"-std=c11", "-Wall", "-Wextra", "-Werror", "-Wno-unused-parameter", "-DREMOVE_ENTRY_SOURCE=" + strconv.Quote(body), "-DUPDATE_SOURCE=" + strconv.Quote(updateBody), "../kernel/kstqr/tests/cache_removal_invalidation.c", "-o", binary}
	if output, err := exec.Command(compiler, args...).CombinedOutput(); err != nil {
		t.Fatalf("compile actual removal: %v\n%s", err, output)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("CACHE_REMOVAL_INVALIDATION_PASS")) {
		t.Fatalf("cache removal invariants: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
