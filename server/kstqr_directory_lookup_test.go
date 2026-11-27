// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestKSTQRDirectoryLookupAlgorithms(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler unavailable")
	}
	exe := filepath.Join(t.TempDir(), "directory-lookup")
	if out, err := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "../kernel/kstqr/tests/directory_lookup.c", "-o", exe).CombinedOutput(); err != nil {
		t.Fatalf("compile: %v\n%s", err, out)
	}
	out, err := exec.Command(exe).CombinedOutput()
	if err != nil {
		t.Fatalf("directory lookup helpers: %v\n%s", err, out)
	}
	t.Logf("%s", out)
}
