// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// The test includes the same private lookup helpers as the module. It exercises
// immutable/mutable semantics and conservative bounded-allocation fallbacks
// without loading a module or requiring a guest.
func TestKSTQROverlayLookupAlgorithms(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler unavailable")
	}
	exe := filepath.Join(t.TempDir(), "overlay-lookup")
	if out, err := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "../kernel/kstqr/tests/overlay_lookup.c", "-o", exe).CombinedOutput(); err != nil {
		t.Fatalf("compile: %v\n%s", err, out)
	}
	for _, args := range [][]string{nil, {"--benchmark"}} {
		out, err := exec.Command(exe, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("lookup helpers: %v\n%s", err, out)
		}
		t.Logf("%s", out)
	}
}
