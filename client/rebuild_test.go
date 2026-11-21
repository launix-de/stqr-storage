// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRebuildOfferCodec(t *testing.T) {
	id := ID{1}
	offer := RebuildOffer{Round: RoundID{1, 2, id, 3}, Candidate: id, Reasons: 5}
	body, err := offer.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := Encoder{}
	want.U64(1)
	want.U64(2)
	want.ID(id)
	want.U64(3)
	want.ID(id)
	want.U64(5)
	if !bytes.Equal(body, want.B) {
		t.Fatal("wire field order differs from spec")
	}
	got, err := DecodeRebuildOffer(body)
	if err != nil || got != offer {
		t.Fatal(got, err)
	}
	for size := 0; size < len(body); size++ {
		if _, err = DecodeRebuildOffer(body[:size]); err == nil {
			t.Fatal("accepted truncated offer", size)
		}
	}
	if _, err = DecodeRebuildOffer(append(body, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	offer.Candidate = ID{2}
	if _, err = offer.MarshalBinary(); err == nil {
		t.Fatal("accepted mismatched initiator")
	}
}
func TestStatsFragmentCodecAndBounds(t *testing.T) {
	id := ID{1}
	m := StatsFragment{Round: RoundID{1, 2, id, 3}, Part: StatsPart{Source: id, Boot: ID{2}, Sequence: 9, Index: 1, Count: 3, Digest: [32]byte{4}, Data: []byte("fragment")}, ControlSequence: 7, PendingChanges: []ID{{5}}}
	body, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStatsFragment(body)
	if err != nil || !reflect.DeepEqual(m, got) {
		t.Fatal(got, err)
	}
	for size := 0; size < len(body); size++ {
		if _, err = DecodeStatsFragment(body[:size]); err == nil {
			t.Fatal("accepted truncated fragment", size)
		}
	}
	if _, err = DecodeStatsFragment(append(body, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	body[len(body)-17] = 255 // corrupt PendingChanges count without allocating from it
	if _, err = DecodeStatsFragment(body); err == nil {
		t.Fatal("accepted invalid count")
	}
	for _, bad := range []StatsPart{{}, {Source: id, Boot: id, Sequence: 1, Count: 65, Data: []byte{1}}, {Source: id, Boot: id, Sequence: 1, Count: 1, Index: 1, Data: []byte{1}}, {Source: id, Boot: id, Sequence: 1, Count: 1, Data: make([]byte, (1<<20)+1)}} {
		m.Part = bad
		if _, err = m.MarshalBinary(); err == nil {
			t.Fatal("accepted invalid fragment")
		}
	}
}
func FuzzRebuildPayloads(f *testing.F) {
	id := ID{1}
	body, _ := (RebuildOffer{Round: RoundID{1, 2, id, 1}, Candidate: id}).MarshalBinary()
	f.Add(body)
	fragment, _ := (StatsFragment{Round: RoundID{1, 2, id, 1}, Part: StatsPart{Source: id, Boot: id, Sequence: 1, Count: 1, Data: []byte("snapshot")}}).MarshalBinary()
	f.Add(fragment)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxMessage {
			return
		}
		if m, e := DecodeRebuildOffer(b); e == nil {
			encoded, e := m.MarshalBinary()
			if e != nil || !bytes.Equal(encoded, b) {
				t.Fatal("offer roundtrip", e)
			}
		}
		if m, e := DecodeStatsFragment(b); e == nil {
			encoded, e := m.MarshalBinary()
			if e != nil || !bytes.Equal(encoded, b) {
				t.Fatal("fragment roundtrip", e)
			}
		}
	})
}
