// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"strings"
	"testing"
)

func TestBucketAccessDoesNotRouteLongNamesAsObjects(t *testing.T) {
	e := Encoder{}
	e.Text(strings.Repeat("bucket", 30))
	if objectMessage(Message{Opcode: BucketInfoGet, Body: e.B}) {
		t.Fatal("long bucket name interpreted as object routing reference")
	}
	if !objectMessage(Message{Opcode: objectGet, Body: make([]byte, 80)}) {
		t.Fatal("object routing disabled for actual object requests")
	}
}
