//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestKernelOverlayTemporaryPinRetirementHost(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler required")
	}
	raw, err := os.ReadFile("../kernel/kstqr/overlay.c")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	callers := ""
	for _, span := range [][2]string{{"static int ov_dir(", "static int ov_inode_match("}, {"static int ov_copy_inode(", "static int ov_mark_orphan("}} {
		start := strings.Index(source, span[0])
		if start < 0 {
			t.Fatal("missing actual caller", span[0])
		}
		end := strings.Index(source[start:], span[1])
		if end < 0 {
			t.Fatal("missing caller boundary", span[1])
		}
		callers += source[start:start+end] + "\n"
	}
	t.Run("old-null-lower-queue-control", func(t *testing.T) {
		dir := t.TempDir()
		header := filepath.Join(dir, "overlay_pin_callers.h")
		if err := os.WriteFile(header, []byte(callers), 0600); err != nil {
			t.Fatal(err)
		}
		cleanup, err := os.ReadFile("../kernel/kstqr/reference_cleanup.c")
		if err != nil {
			t.Fatal(err)
		}
		old := strings.ReplaceAll(string(cleanup), "queue_work(ks_reference_workqueue(f),", "queue_work(f->reference_queue,")
		oldPath := filepath.Join(dir, "old_cleanup.c")
		if err := os.WriteFile(oldPath, []byte(old), 0600); err != nil {
			t.Fatal(err)
		}
		binary := filepath.Join(dir, "old-lower-queue")
		cmd := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread", "-I"+dir, `-DREFERENCE_CLEANUP_SOURCE="`+oldPath+`"`, "../kernel/kstqr/tests/overlay_pin_retirement.c", "-o", binary)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("compile old queue-owner control: %v\n%s", err, output)
		}
		output, err := exec.Command(binary).CombinedOutput()
		if err == nil || !strings.Contains(string(output), "q==&queue") {
			t.Fatalf("old lower queue did not demonstrate NULL owner: %v\n%s", err, output)
		}
	})
	for _, old := range []bool{false, true} {
		label := "fixed"
		body := callers
		if old {
			label = "old-direct-unpin-control"
			body = strings.ReplaceAll(body, "ks_retire_pin(f->lower, id, temporary)", "ks_object_reference(f->lower, id, temporary, 1)")
		}
		t.Run(label, func(t *testing.T) {
			dir := t.TempDir()
			header := filepath.Join(dir, "overlay_pin_callers.h")
			if err := os.WriteFile(header, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(dir, "overlay-pins")
			cmd := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread", "-I"+dir, "../kernel/kstqr/tests/overlay_pin_retirement.c", "-o", binary)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("compile actual overlay callers: %v\n%s", err, output)
			}
			if old {
				for scenario := 0; scenario < 3; scenario++ {
					output, err := exec.Command(binary, strconv.Itoa(scenario)).CombinedOutput()
					if err == nil || !strings.Contains(string(output), "!pins&&unpins==1&&queued==1&&flushed==1") {
						t.Fatalf("old caller %d did not demonstrate retained PIN: %v\n%s", scenario, err, output)
					}
				}
				return
			}
			output, err := exec.Command(binary).CombinedOutput()
			if err != nil || !strings.Contains(string(output), "KSTQR_OVERLAY_PIN_RETIREMENT_PASS") {
				t.Fatalf("actual overlay retirement: %v\n%s", err, output)
			}
			t.Log(string(output))
		})
	}
}
