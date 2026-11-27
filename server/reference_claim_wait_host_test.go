//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestKernelReferenceClaimWaitHost(t *testing.T) {
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("POSIX C compiler is required for the kernel-helper host test")
	}
	binary := filepath.Join(t.TempDir(), "reference-claim-wait")
	build := exec.Command(compiler, "-std=c11", "-Wall", "-Wextra", "-Werror", "-Wno-unused-parameter", "-pthread", "../kernel/kstqr/tests/reference_claim_wait.c", "-o", binary)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("compile actual-helper harness: %v\n%s", err, output)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "KSTQR_REFERENCE_CLAIM_WAIT_PASS") {
		t.Fatalf("actual-helper claim/wait regression: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
