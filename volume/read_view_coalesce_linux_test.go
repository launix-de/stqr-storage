// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"bytes"
	"testing"
)

func TestMappedReadViewCoalescesPastUnrelatedAcceptedPages(t *testing.T) {
	v, file := mappedTestVolume(t, 16<<20)
	key := testKey(1)
	want := bytes.Repeat([]byte("p"), BlockSize)
	if _, err := testPut(v, key, want, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(2), []byte("pending"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	o, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.WithReadView(key, 0, BlockSize, func(view ReadView) error {
		if len(view.Segments) != 1 {
			t.Fatalf("contiguous mapped payload split into %d segments", len(view.Segments))
		}
		s := view.Segments[0]
		if !s.Borrowed || &s.Data[0] != &file.mapping[o.Spans[0].Location] || !bytes.Equal(s.Data, want) {
			t.Fatal("lost mapped payload identity")
		}
		w := &mappedPointerWriter{pointer: &s.Data[0], length: BlockSize}
		_, e := view.WriteTo(w)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMappedReadViewCoalescesAroundShadowPage(t *testing.T) {
	v, file := mappedTestVolume(t, 16<<20)
	key := testKey(1)
	want := bytes.Repeat([]byte("q"), BlockSize)
	if _, err := testPut(v, key, want, Options{}); err != nil {
		t.Fatal(err)
	}
	at := uint64(5*PageSize + 17)
	if _, err := testPutRange(v, key, at, []byte("new"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	copy(want[at:], "new")
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	o, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.WithReadView(key, 0, BlockSize, func(view ReadView) error {
		var got bytes.Buffer
		if _, err := view.WriteTo(&got); err != nil {
			return err
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatal("coalescing bypassed accepted bytes")
		}
		// Two mapped runs surround a private accepted page. It must not be
		// mistaken for the old physical bytes while the checkpoint is pending.
		if len(view.Segments) != 3 {
			t.Fatalf("wanted mapped/shadow/mapped, got %d segments", len(view.Segments))
		}
		base := o.Spans[0].Location
		if &view.Segments[0].Data[0] != &file.mapping[base] ||
			&view.Segments[1].Data[0] == &file.mapping[base+5*PageSize] ||
			&view.Segments[2].Data[0] != &file.mapping[base+6*PageSize] {
			t.Fatal("coalescing lost mapped/shadow provenance")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
