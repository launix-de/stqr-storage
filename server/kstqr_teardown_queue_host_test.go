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

func teardownQueueBlock(t *testing.T, source, condition string) (int, int) {
	t.Helper()
	start := strings.Index(source, condition)
	if start < 0 {
		t.Fatal("missing actual queue drain", condition)
	}
	open := strings.Index(source[start:], "{") + start
	depth := 0
	for i := open; i < len(source); i++ {
		if source[i] == '{' {
			depth++
		}
		if source[i] == '}' {
			depth--
			if depth == 0 {
				return start, i + 1
			}
		}
	}
	t.Fatal("unclosed actual queue drain")
	return 0, 0
}
func TestKernelTeardownQueueOrderHost(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler required")
	}
	raw, err := os.ReadFile("../kernel/kstqr/vfs.c")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "static void ks_put_super(")
	end := strings.Index(source[start:], "static int ks_show_options(")
	if start < 0 || end < 0 {
		t.Fatal("actual teardown caller missing")
	}
	body := source[start : start+end]
	a, b := teardownQueueBlock(t, body, "if (f->read_queue)")
	c, d := teardownQueueBlock(t, body, "if (f->reference_queue)")
	if a >= c {
		t.Fatal("production producer drain still follows its consumer")
	}
	old := body[:a] + body[c:d] + body[b:c] + body[a:b] + body[d:]
	for _, scenario := range []struct {
		name, body string
		old        bool
	}{{"actual", body, false}, {"old-consumer-first-control", old, true}} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "teardown_caller.h"), []byte(scenario.body), 0600); err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(dir, "teardown")
			cmd := exec.Command(cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-I"+dir, "../kernel/kstqr/tests/teardown_queue_order.c", "-o", binary)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("compile actual caller: %v\n%s", err, output)
			}
			for mode := 0; mode < 3; mode++ {
				output, err := exec.Command(binary, strconv.Itoa(mode)).CombinedOutput()
				if scenario.old && mode != 2 {
					if err == nil || !strings.Contains(string(output), "REFERENCE_DRAIN_PRECEDED_READ_PRODUCER") {
						t.Fatalf("old order did not demonstrate pending producer: %v\n%s", err, output)
					}
				} else if err != nil || !strings.Contains(string(output), "KSTQR_TEARDOWN_QUEUE_PASS") {
					t.Fatalf("actual teardown failed: %v\n%s", err, output)
				}
			}
		})
	}
}
