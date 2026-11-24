// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestExportImportTranscodesSparseAndRetry(t *testing.T) {
	src, _ := memoryVolume(t, 8<<20)
	dst, d := memoryVolume(t, 8<<20)
	k := testKey(1)
	id := testID(500)
	if _, e := testPutRange(src, k, 1<<40, []byte("far"), Options{Compression: Codec(LZ4)}); e != nil {
		t.Fatal(e)
	}
	a, e := testAppend(src, k, []byte("!"), Options{RequestID: id})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(src, testKey(2), []byte("deleted"), Options{}); e != nil {
		t.Fatal(e)
	}
	if e = testDelete(src, testKey(2), Options{}); e != nil {
		t.Fatal(e)
	}
	var buf bytes.Buffer
	if e = src.Export(&buf); e != nil {
		t.Fatal(e)
	}
	if buf.Len() > 5000 {
		t.Fatal("sparse holes materialized")
	}
	if e = dst.Import(&buf, Codec(Gzip)); e != nil {
		t.Fatal(e)
	}
	dst = reopen(t, dst, d)
	b, info, e := testGetRange(dst, k, 1<<40, 4)
	if e != nil || string(b) != "far!" || info.Compression != Gzip {
		t.Fatalf("%q %+v %v", b, info, e)
	}
	retry, e := testAppend(dst, k, []byte("!"), Options{RequestID: id})
	if e != nil || retry != a {
		t.Fatalf("%+v %v", retry, e)
	}
	if _, e = dst.Stat(testKey(2)); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if e = dst.Scrub(); e != nil {
		t.Fatal(e)
	}
}
func TestImportMalformedDoesNotPublish(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.Encode(snapshotFrame{Kind: "STQR-SNAPSHOT-2"})
	enc.Encode(snapshotFrame{Kind: "object", Key: testKey(1), Info: Info{Length: 3}})
	enc.Encode(snapshotFrame{Kind: "data", Data: []byte("abc")}) // missing end/complete
	if e := v.Import(&b, nil); e == nil {
		t.Fatal("truncated import succeeded")
	}
	if _, e := v.Stat(testKey(1)); !errors.Is(e, ErrNotFound) {
		t.Fatalf("partial import visible %v", e)
	}
	if _, e := testPut(v, testKey(1), []byte("after"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, testKey(1), []byte("after"))
}

func TestRestoreIntoPopulatedVolumePreservesContentAndRetries(t *testing.T) {
	source, _ := memoryVolume(t, 4<<20)
	target, d := memoryVolume(t, 4<<20)
	k := testKey(1)
	originalID := testID(23)
	a, e := testAppend(source, k, []byte("data"), Options{RequestID: originalID})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(target, testKey(2), []byte("unrelated"), Options{}); e != nil {
		t.Fatal(e)
	}
	input := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader([]byte("data")), 4)
	}
	info := a.Info
	info.Compression = LZ4
	opts := Options{RequestID: testID(24)}
	if e = testRestore(target, k, info, input, opts); e != nil {
		t.Fatal(e)
	}
	if e = testRestore(target, k, info, input, opts); e != nil {
		t.Fatal(e)
	}
	if e = testScanRequests(source, func(r RequestRecord) error { return testRestoreRequest(target, r) }); e != nil {
		t.Fatal(e)
	}
	target = reopen(t, target, d)
	again, e := testAppend(target, k, []byte("data"), Options{RequestID: originalID})
	if e != nil || again != a {
		t.Fatalf("%+v %v", again, e)
	}
	mustGet(t, target, k, []byte("data"))
	mustGet(t, target, testKey(2), []byte("unrelated"))
	if e = testRestore(target, k, info, input, Options{CreateOnly: true}); !errors.Is(e, ErrPrecondition) {
		t.Fatal(e)
	}
}

func TestRepairRetainsContentAndRetry(t *testing.T) {
	d := newCrash(2 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	key := testKey(1)
	id := testID(5)
	info, e := testPut(v, key, []byte("correct"), Options{RequestID: id})
	if e != nil {
		t.Fatal(e)
	}
	o, e := v.loadObject(key)
	if e != nil {
		t.Fatal(e)
	}
	d.live[o.Spans[0].Location] ^= 1
	if e = v.Scrub(); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	source := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewBufferString("correct"), 7)
	}
	if e = testRepair(v, key, info, source, Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, key, []byte("correct"))
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
	again, e := testPut(v, key, []byte("correct"), Options{RequestID: id})
	if e != nil || again != info {
		t.Fatalf("retry %+v %v", again, e)
	}
}
