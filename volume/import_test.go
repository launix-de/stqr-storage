// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestExportImportTranscodesSparseAndRetry(t *testing.T) {
	src, _ := memoryVolume(t, 8<<20)
	dst, d := memoryVolume(t, 8<<20)
	k := testKey(1)
	id := testID(500)
	if _, e := src.PutRange(k, 1<<40, []byte("far"), Options{Compression: Codec(LZ4)}); e != nil {
		t.Fatal(e)
	}
	a, e := src.Append(k, []byte("!"), Options{RequestID: id})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = src.Put(testKey(2), []byte("deleted"), Options{}); e != nil {
		t.Fatal(e)
	}
	if e = src.Delete(testKey(2), Options{}); e != nil {
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
	b, info, e := dst.GetRange(k, 1<<40, 4)
	if e != nil || string(b) != "far!" || info.Compression != Gzip || info.Version != a.Info.Version {
		t.Fatalf("%q %+v %v", b, info, e)
	}
	retry, e := dst.Append(k, []byte("!"), Options{RequestID: id})
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
	b.WriteString(snapshotMagic)
	var count [8]byte
	diskOrder.PutUint64(count[:], 1)
	b.Write(count[:])
	r := record{Op: opSnapshot, Key: testKey(1), Version: 1, Length: 3}
	raw, _ := encodeRecord(r)
	writeFrame(&b, 1, raw)
	writeFrame(&b, 3, append(make([]byte, 8), []byte("abc")...)) // missing object/end markers
	if e := v.Import(&b, nil); e == nil {
		t.Fatal("truncated import succeeded")
	}
	if _, e := v.Stat(testKey(1)); !errors.Is(e, ErrNotFound) {
		t.Fatalf("partial import visible %v", e)
	}
	if _, e := v.Put(testKey(1), []byte("after"), Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, testKey(1), []byte("after"))
}

func TestRestoreIntoPopulatedVolumePreservesVersionAndRetries(t *testing.T) {
	source, _ := memoryVolume(t, 4<<20)
	target, d := memoryVolume(t, 4<<20)
	k := testKey(1)
	originalID := testID(23)
	a, e := source.Append(k, []byte("data"), Options{RequestID: originalID})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = target.Put(testKey(2), []byte("unrelated"), Options{}); e != nil {
		t.Fatal(e)
	}
	input := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader([]byte("data")), 4)
	}
	info := a.Info
	info.Compression = LZ4
	opts := Options{RequestID: testID(24)}
	if e = target.Restore(k, info, input, opts); e != nil {
		t.Fatal(e)
	}
	if e = target.Restore(k, info, input, opts); e != nil {
		t.Fatal(e)
	}
	if e = source.ScanRequests(func(r RequestRecord) error { return target.RestoreRequest(r) }); e != nil {
		t.Fatal(e)
	}
	target = reopen(t, target, d)
	again, e := target.Append(k, []byte("data"), Options{RequestID: originalID})
	if e != nil || again != a {
		t.Fatalf("%+v %v", again, e)
	}
	mustGet(t, target, k, []byte("data"))
	mustGet(t, target, testKey(2), []byte("unrelated"))
	if e = target.Restore(k, info, input, Options{}); !errors.Is(e, ErrPrecondition) {
		t.Fatal(e)
	}
}

func TestRepairRetainsLogicalVersionAndRetry(t *testing.T) {
	d := newCrash(2 << 20)
	v, e := Format(d, FormatOptions{Class: SSD})
	if e != nil {
		t.Fatal(e)
	}
	key := testKey(1)
	id := testID(5)
	info, e := v.Put(key, []byte("correct"), Options{RequestID: id})
	if e != nil {
		t.Fatal(e)
	}
	o, e := v.loadObject(key)
	if e != nil {
		t.Fatal(e)
	}
	d.live[v.sb.bankStart()+o.Extents[0].Location] ^= 1
	if e = v.Scrub(); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	source := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewBufferString("correct"), 7)
	}
	if e = v.Repair(key, info, source, Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, key, []byte("correct"))
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
	again, e := v.Put(key, []byte("correct"), Options{RequestID: id})
	if e != nil || again != info {
		t.Fatalf("retry %+v %v", again, e)
	}
}
