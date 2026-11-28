// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestKernelInvalidationQueueHost(t *testing.T) {
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("POSIX C compiler required")
	}
	binary := filepath.Join(t.TempDir(), "invalidation-queue")
	args := []string{"-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "../kernel/kstqr/tests/invalidation_queue.c", "-o", binary}
	if output, err := exec.Command(compiler, args...).CombinedOutput(); err != nil {
		t.Fatalf("compile actual invalidation queue: %v\n%s", err, output)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("INVALIDATION_QUEUE_PASS")) {
		t.Fatalf("invalidation invariants: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
