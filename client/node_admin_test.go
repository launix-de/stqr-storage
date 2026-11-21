// SPDX-License-Identifier: GPL-3.0-only
package client

import "testing"

func TestNodeAdminCodecRejectsPrivilegeAndProxyRecursion(t *testing.T) {
	for _, op := range []uint16{NodeAdminRequest, PeerAdminRequest, UserCreate, BucketCreate, 0x0101, NodeHelloMessage} {
		if _, err := (NodeAdmin{Target: ID{1}, Opcode: op}).MarshalBinary(); err == nil {
			t.Fatalf("allowed nonlocal administration opcode %x", op)
		}
	}
	want := NodeAdmin{Target: ID{1}, Opcode: VolumeAdd, Body: []byte{1, 2, 3}}
	b, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeNodeAdmin(b)
	if err != nil || got.Target != want.Target || got.Opcode != want.Opcode || string(got.Body) != string(want.Body) {
		t.Fatal("node admin roundtrip", err)
	}
	for i := range b {
		if _, err := DecodeNodeAdmin(b[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
}
