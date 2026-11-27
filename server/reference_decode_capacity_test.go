// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"runtime"
	"testing"
)

func TestReferenceDecodeSpareSlotKeepsLimits(t *testing.T) {
	bucket := volume.ID{1}
	for _, n := range []int{0, 1, 3218, maxObjectReferences} {
		entries := make([]objectReferenceEntry, n)
		for i := range entries {
			entries[i].object[0] = byte(i)
			entries[i].object[31] = byte(i >> 8)
		}
		raw := encodeObjectReferences(entries, bucket)
		got, err := decodeObjectReferences(raw, bucket)
		if err != nil || len(got) != n {
			t.Fatal("length changed", n, len(got), err)
		}
		if cap(got) > maxObjectReferences {
			t.Fatal("unbounded capacity", cap(got))
		}
		if n < maxObjectReferences && cap(got) < n+1 {
			t.Fatal("missing spare slot", n, cap(got))
		}
		for i := range entries {
			if got[i] != entries[i] {
				t.Fatal("decoded bytes changed")
			}
		}
	}
	raw := encodeObjectReferences(make([]objectReferenceEntry, maxObjectReferences+1), bucket)
	if _, err := decodeObjectReferences(raw, bucket); err == nil {
		t.Fatal("encoded global limit relaxed")
	}
}

var referenceDecodeAppendSink []objectReferenceEntry

func TestReferenceDecodePinAppendAvoidsSecondArray(t *testing.T) {
	bucket := volume.ID{1}
	raw := encodeObjectReferences(make([]objectReferenceEntry, 3218), bucket)
	for i := 0; i < 4; i++ {
		entries, err := decodeObjectReferences(raw, bucket)
		if err != nil {
			t.Fatal(err)
		}
		referenceDecodeAppendSink = append(entries, objectReferenceEntry{})
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const n = 64
	for i := 0; i < n; i++ {
		entries, err := decodeObjectReferences(raw, bucket)
		if err != nil {
			t.Fatal(err)
		}
		referenceDecodeAppendSink = append(entries, objectReferenceEntry{})
	}
	runtime.ReadMemStats(&after)
	perOp := (after.TotalAlloc - before.TotalAlloc) / n
	t.Logf("3218entry decode+PINappend allocated%dB/op", perOp)
	if perOp >= 400<<10 {
		t.Fatal("PIN append allocated a second root array", perOp)
	}
}
func BenchmarkReferenceDecodePinAppend(b *testing.B) {
	bucket := volume.ID{1}
	raw := encodeObjectReferences(make([]objectReferenceEntry, 3218), bucket)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := decodeObjectReferences(raw, bucket)
		if err != nil {
			b.Fatal(err)
		}
		referenceDecodeAppendSink = append(entries, objectReferenceEntry{})
	}
}

func TestReferenceDecodeSpareSlotDoesNotRelaxAdmission(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprint(global), func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "reference-limits", "hdd", 8<<20)
			b, _ := createProtocolBucket(t, s, "reference-limits")
			p := quotaPrincipal(t, s)
			uid, err := bucketID(p.User.ID)
			if err != nil {
				t.Fatal(err)
			}
			count := maxAccountReferences
			if global {
				count = maxObjectReferences
			}
			entries := make([]objectReferenceEntry, count)
			for i := range entries {
				entries[i].object[0] = byte(i)
				entries[i].object[31] = byte(i >> 8)
				entries[i].account = uid
				if global {
					entries[i].account = volume.ID{9}
				}
			}
			raw := encodeObjectReferences(entries, b.ID())
			root := volume.ObjectID(client.ObjectReferenceRootID())
			if _, err = b.Put(context.Background(), p, root, raw, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			ws := connectionTestSession(s)
			ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
			ws.authorization.objectReferences.Store(true)
			ws.acceptedReferencesCap.Store(true)
			ws.durabilityCap.Store(true)
			ws.connectionFenceCap.Store(true)
			accepted := true
			m := referenceRequest(b, client.ObjectID{0xfe, 0xff}, client.NewRequestID(), &accepted)
			if _, err = s.executeObjectReference(context.Background(), m, ws, p); statusOf(err) != 429 {
				t.Fatal("reference limit changed", global, err)
			}
			after, _, err := b.Get(context.Background(), p, root)
			if err != nil || !bytes.Equal(after, raw) {
				t.Fatal("rejected reference changed root", err)
			}
		})
	}
}
