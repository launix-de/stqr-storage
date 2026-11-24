// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"strings"
	"testing"
)

func TestPasswordInput(t *testing.T) {
	for _, line := range []string{"secret\n", "secret\r\n", "secret"} {
		b, e := readPassword(strings.NewReader(line))
		if e != nil || string(b) != "secret" {
			t.Fatal(e)
		}
	}
	if _, e := readPassword(strings.NewReader(strings.Repeat("x", 5000))); e == nil {
		t.Fatal("unbounded password accepted")
	}
}
