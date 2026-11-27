// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

// Matches the original writer, not the current decoder's implementation.
func legacyIndexFixture(parts []fsSegment) []byte {
	e := Encoder{B: []byte("STIX")}
	e.U16(2)
	e.U32(uint32(len(parts)))
	for _, p := range parts {
		e.U64(p.length)
		e.B = append(e.B, p.id[:]...)
		e.U64(p.offset)
		e.U64(p.legacyVersion)
	}
	return e.B
}
func TestFilesystemLegacyIndexReadAndRewrite(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "legacy-journal")
	data, index := newFSID(), newFSID()
	raw := append(bytes.Repeat([]byte{'P'}, 9), bytes.Repeat([]byte{'J'}, 16384)...)
	copy(raw[9:], []byte("\xda\x1ajour"))
	parts := []fsSegment{{length: 16384, id: data, offset: 9}, {length: 16384}}
	for id, b := range map[ObjectID][]byte{data: raw, index: legacyIndexFixture(parts)} {
		if _, err := store.CAS(ctx, id, 0, nil, b, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: true}}); err != nil {
			t.Fatal(err)
		}
	}
	n, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	n.blob = fsBlob{length: 32768, root: index, depth: 1}
	head := encodeFSNode(n)
	if _, err = store.CAS(ctx, file.ID, 0, n.head, head, CASOptions{Scope: CASObject}); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 32768)
	copy(want, raw[9:])
	if got := readTestFile(t, f, file.ID); !bytes.Equal(got, want) {
		t.Fatal("old journal data or hole changed")
	}
	if _, err = f.WriteAt(ctx, file.ID, []byte("updated"), 7000); err != nil {
		t.Fatal(err)
	}
	copy(want[7000:], []byte("updated"))
	if got := readTestFile(t, f, file.ID); !bytes.Equal(got, want) {
		t.Fatal("rewrite lost old bytes")
	}
	current, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := store.Get(ctx, current.blob.root)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint16(b[4:6]) == 2 {
		t.Fatal("new write emitted obsolete index")
	}
}
func TestFilesystemLegacyIndexStrictLayout(t *testing.T) {
	parts := []fsSegment{{length: 32768, id: ObjectID{1}, offset: 9}}
	raw := legacyIndexFixture(parts)
	got, err := decodeFSIndex(raw, 32768, 1)
	if err != nil || len(got) != 1 || got[0] != parts[0] {
		t.Fatal(got, err)
	}
	for _, b := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0), append(bytes.Clone(raw[:54]), raw[58:]...)} {
		if _, err := decodeFSIndex(b, 32768, 1); !errors.Is(err, ErrFSFormat) {
			t.Fatal("accepted malformed legacy index", err)
		}
	}
	parts[0].legacyVersion = 123
	got, err = decodeFSIndex(legacyIndexFixture(parts), 32768, 1)
	if err != nil || got[0].offset != 9 || got[0].legacyVersion != 123 {
		t.Fatal("historical token was misread as a local reference", got, err)
	}
}
