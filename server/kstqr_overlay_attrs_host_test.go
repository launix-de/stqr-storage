// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestKernelOverlayDirectoryAttributeGuardsHost(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler unavailable")
	}
	raw, err := os.ReadFile("../kernel/kstqr/overlay.c")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "int ks_overlay_refresh_directory_attrs(")
	if start < 0 {
		t.Fatal("actual attribute guard missing")
	}
	end := strings.Index(source[start:], "static int ov_edit(") + start
	if end <= start {
		t.Fatal("actual attribute guard extraction")
	}
	work := t.TempDir()
	if err = os.WriteFile(filepath.Join(work, "overlay_attrs_guard_actual.h"), []byte(source[start:end]), 0600); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(work, "guard")
	if out, e := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-I", work, "../kernel/kstqr/tests/overlay_attrs_guard.c", "-o", exe).CombinedOutput(); e != nil {
		t.Fatalf("compile: %v\n%s", e, out)
	}
	if out, e := exec.Command(exe).CombinedOutput(); e != nil {
		t.Fatalf("guard: %v\n%s", e, out)
	} else {
		t.Log(string(out))
	}
}
