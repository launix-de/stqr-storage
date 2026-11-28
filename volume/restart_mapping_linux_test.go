//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestRestartRetentionMappedPayloadIsBorrowed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	v, err := New(path, 16<<20, SSD)
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(810)
	want := bytes.Repeat([]byte("p"), PageSize*3)
	if _, err = testPut(v, key, want, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	image := filepath.Join(t.TempDir(), "mapped.image")
	receipt, err := v.RetainAccepted(image, testID(810))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := Open(dev, OpenOptions{Restart: &RestartSource{image, receipt}})
	if err != nil {
		dev.Close()
		t.Fatal(err)
	}
	defer w.Abort()
	if w.deferred == nil || w.deferred.store.retained == nil {
		t.Fatal("missing lazy image")
	}
	mapped := w.deferred.store.retained.source.(*restartMappedSource)
	if len(mapped.mapping) == 0 || len(w.deferred.store.pages) != 0 {
		t.Fatal("payload not demand mapped")
	}
	h, err := w.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	obj, err := w.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	location := obj.Spans[0].Location
	page, _, ok, err := w.deferred.store.retained.get(location / PageSize * PageSize)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	_, err = h.WithReadView(key, 0, uint64(len(want)), func(view ReadView) error {
		if len(view.Segments) == 0 || !view.Segments[0].Borrowed || &view.Segments[0].Data[0] != &page[location%PageSize] {
			t.Fatal("retained payload copied instead of mmap borrow")
		}
		var got []byte
		for _, segment := range view.Segments {
			got = append(got, segment.Data...)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("mapped payload mismatch")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.IOStats().SyncOps != 0 {
		t.Fatal("mapping started physical fence")
	}
}
