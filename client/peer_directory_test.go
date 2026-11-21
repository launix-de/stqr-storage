// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"reflect"
	"testing"
)

func TestPeerDirectoryCodecAndTruncations(t *testing.T) {
	want := PeerDirectoryAnnouncement{Cluster: ID{1}, Sender: ID{2}, Nodes: []NodeAddress{{Node: ID{2}, Addresses: []string{"http://node-a:7419"}}, {Node: ID{3}, Addresses: []string{"http://[::1]:7419"}}}}
	b, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePeerDirectory(b)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("peer directory roundtrip", err)
	}
	for i := range b {
		if _, err := DecodePeerDirectory(b[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	want.Nodes[1].Node = want.Nodes[0].Node
	if _, err := want.MarshalBinary(); err == nil {
		t.Fatal("duplicate identity accepted")
	}
}
