// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"testing"
)

func TestCanonicalEnvelopeAndPrimitives(t *testing.T) {
	id := ID{1, 2, 3}
	e := Encoder{}
	e.U16(0x1234)
	e.U64(^uint64(0))
	e.Text("Größe")
	e.V(507)
	e.Bool(true)
	m := Message{Kind: Request, Opcode: VolumeAdd, ID: id, Body: e.B}
	encoded := EncodeMessage(m)
	if len(encoded) < 20 || encoded[2] != 1 || encoded[3] != 0x20 {
		t.Fatal("wire opcode is not little endian")
	}
	got, err := DecodeMessage(encoded)
	if err != nil || got.Kind != m.Kind || got.ID != m.ID || !bytes.Equal(got.Body, m.Body) {
		t.Fatal("envelope mismatch", err)
	}
	d := Decoder{B: got.Body}
	if d.U16() != 0x1234 || d.U64() != ^uint64(0) || d.Text() != "Größe" || d.V() != 507 || !d.Bool() || d.Done() != nil {
		t.Fatal("primitive round trip failed")
	}
	for _, b := range [][]byte{{0x80, 0}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}, {0x80}} {
		d := Decoder{B: b}
		d.V()
		if d.Done() == nil {
			t.Fatal("noncanonical/overflow varint accepted")
		}
	}
	d = Decoder{B: []byte{2}}
	d.Bool()
	if d.Done() == nil {
		t.Fatal("invalid bool")
	}
	d = Decoder{B: []byte{1, 0xff}}
	d.Text()
	if d.Done() == nil {
		t.Fatal("invalid UTF8")
	}
}
func TestResponseErrorAndStatsLayout(t *testing.T) {
	body := ResponseBody(507, nil, "full")
	d := Decoder{B: body}
	if d.V() != 507 {
		t.Fatal("status truncated to u8")
	}
	h := Decoder{B: d.Blob()}
	if h.V() != 1 || h.V() != 3 {
		t.Fatal("error follow-header tag")
	}
	txt := Decoder{B: h.Blob()}
	if txt.Text() != "full" || txt.Done() != nil || h.Done() != nil || d.Done() != nil {
		t.Fatal("follow header layout")
	}
	s := StatsSnapshot{Schema: 1, Node: ID{1}, BootID: ID{2}, Sequence: 7, Volumes: []VolumeStats{{ID: ID{3}, Path: "/volumes/test", CapacityPhysical: 512 << 10, AdmissionFreePhysical: 200 << 10}}}
	b := EncodeStats(s)
	if len(b) < 900 {
		t.Fatal("statistics fixed fields missing")
	}
	d = Decoder{B: b}
	if d.U16() != 1 || d.ID() != s.Node || d.ID() != s.BootID || d.U64() != 7 {
		t.Fatal("stats prefix layout")
	}
}
func FuzzDecodeEnvelope(f *testing.F) {
	f.Add(EncodeMessage(Message{Kind: Request, Opcode: Ping, Body: make([]byte, 8)}))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, e := DecodeMessage(b)
		if e == nil && !bytes.Equal(EncodeMessage(m), b) {
			t.Fatal("noncanonical envelope")
		}
	})
}
