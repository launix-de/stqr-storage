// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"reflect"
	"testing"
)

func TestNodeHelloCodec(t *testing.T) {
	m := NodeHello{Cluster: ID{1}, Node: ID{2}, Membership: 3, Addresses: []string{"http://127.0.0.1:7419", "http://[::1]:7419"}, Boot: ID{4}}
	b, e := m.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeNodeHello(b)
	if e != nil || !reflect.DeepEqual(got, m) {
		t.Fatal(got, e)
	}
	for i := range b {
		if _, e = DecodeNodeHello(b[:i]); e == nil {
			t.Fatal("truncated", i)
		}
	}
	if _, e = DecodeNodeHello(append(b, 0)); e == nil {
		t.Fatal("trailing bytes")
	}
	for _, address := range []string{"https://host", "http://user:secret@host", "http://host/path", "http://host?secret=x", "file:///tmp/x", "http://:7419"} {
		m.Addresses = []string{address}
		if _, e = m.MarshalBinary(); e == nil {
			t.Fatal("accepted invalid address", address)
		}
	}
}
func TestNodeHelloAckCodec(t *testing.T) {
	m := NodeHelloAck{Membership: Membership{Epoch: 1, Cluster: ID{1}, Members: []Member{{ID: ID{2}, Addresses: []string{"http://host:7419"}, Tags: []MemberTag{{"rack", "r1"}}}}, Excluded: []ID{{3}}}, ControlSequence: 7, Generation: 8, MapRevision: 9}
	b, e := m.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeNodeHelloAck(b)
	if e != nil || !reflect.DeepEqual(got, m) {
		t.Fatal(got, e)
	}
	for i := range b {
		if _, e = DecodeNodeHelloAck(b[:i]); e == nil {
			t.Fatal("truncated", i)
		}
	}
	if _, e = DecodeNodeHelloAck(append(b, 0)); e == nil {
		t.Fatal("trailing bytes")
	}
	m.Membership.Excluded = []ID{{2}}
	if _, e = m.MarshalBinary(); e == nil {
		t.Fatal("member also excluded")
	}
}

func TestPendingNodeHelloHasNoCommittedMembership(t *testing.T) {
	message := NodeHello{Cluster: ID{1}, Node: ID{2}, Boot: ID{3}}
	b, err := message.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeNodeHello(b); err != nil || got.Membership != 0 {
		t.Fatal("prejoin identity cannot claim a membership", err)
	}
	ack := NodeHelloAck{Membership: Membership{Cluster: ID{1}}}
	b, err = ack.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeNodeHelloAck(b); err != nil || got.Membership.Epoch != 0 || len(got.Membership.Members) != 0 || got.Generation != 0 {
		t.Fatal("prejoin ACK claimed membership", err)
	}
	ack.Generation = 1
	if _, err := ack.MarshalBinary(); err == nil {
		t.Fatal("unknown membership carried an authoritative generation")
	}
}
func FuzzPeerIdentityPayloads(f *testing.F) {
	b, _ := (NodeHello{Cluster: ID{1}, Node: ID{2}, Boot: ID{3}, Membership: 1}).MarshalBinary()
	f.Add(b)
	a, _ := (NodeHelloAck{Membership: Membership{Epoch: 1, Cluster: ID{1}, Members: []Member{{ID: ID{2}}}}, Generation: 1}).MarshalBinary()
	f.Add(a)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxMessage {
			return
		}
		if m, e := DecodeNodeHello(b); e == nil {
			encoded, e := m.MarshalBinary()
			if e != nil || !bytes.Equal(encoded, b) {
				t.Fatal("hello roundtrip", e)
			}
		}
		if m, e := DecodeNodeHelloAck(b); e == nil {
			encoded, e := m.MarshalBinary()
			if e != nil || !bytes.Equal(encoded, b) {
				t.Fatal("ack roundtrip", e)
			}
		}
	})
}
