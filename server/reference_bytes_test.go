// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/volume"
)

// This is the original decoded-entry decision path, retained as an independent
// oracle for all seven online/offline actions, rather than testing the new
// byte operations against their own output.
func legacyReferenceDecision(entries []objectReferenceEntry, target volume.ObjectID, proof [32]byte, uid volume.ID, admin bool, action byte) ([]objectReferenceEntry, bool, error) {
	own := -1
	busy := false
	for i, r := range entries {
		if action != 6 && r.proof == proof {
			if !(r.account == uid || admin && (action == 1 || action == 3 || action == 5)) || r.object != target || r.claim != (action == 2 || action == 3) || r.dependency != (action == 4 || action == 5) {
				return entries, false, ErrBucketForbidden
			}
			own = i
		}
		if r.object == target && (action == 2 || (action == 0 || action == 4) && r.claim) {
			busy = true
		}
	}
	if action == 6 {
		kept := make([]objectReferenceEntry, 0, len(entries))
		for _, r := range entries {
			if r.dependency {
				kept = append(kept, r)
			}
		}
		return kept, len(kept) != len(entries), nil
	}
	if action == 1 || action == 3 || action == 5 {
		if own < 0 {
			return entries, false, nil
		}
		return append(entries[:own], entries[own+1:]...), true, nil
	}
	if own >= 0 {
		return entries, false, nil
	}
	if busy {
		return entries, false, fail(409, "object reference is busy")
	}
	return append(entries, objectReferenceEntry{target, proof, uid, action == 2, action == 4}), true, nil
}
func TestReferenceByteDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(47913))
	bucket := volume.ID{19}
	entries := []objectReferenceEntry{}
	tokens := make([][32]byte, 80)
	for i := range tokens {
		tokens[i] = sha256.Sum256([]byte(fmt.Sprint(i)))
	}
	for i := 0; i < 12000; i++ {
		raw := encodeObjectReferences(entries, bucket)
		saved := bytes.Clone(raw)
		table, err := borrowReferenceBytes(raw, bucket, 32)
		if err != nil {
			t.Fatal(err)
		}
		target := volume.ObjectID{byte(rng.Intn(12))}
		uid := volume.ID{byte(rng.Intn(4))}
		proof := tokens[rng.Intn(len(tokens))]
		action := byte(rng.Intn(7))
		admin := rng.Intn(3) == 0
		old := append([]objectReferenceEntry(nil), entries...)
		expected, changed, wantErr := legacyReferenceDecision(old, target, proof, uid, admin, action)
		gotErr := table.apply(target, proof, uid, admin, action)
		if objectStatus(gotErr) != objectStatus(wantErr) || table.changed != changed {
			t.Fatalf("step %d action%d changed%v/%v err%v/%v", i, action, table.changed, changed, gotErr, wantErr)
		}
		got, e := decodeObjectReferences(table.raw, bucket)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got, expected) && !(len(got) == 0 && len(expected) == 0) {
			t.Fatalf("step%d action%d state differs", i, action)
		}
		if !bytes.Equal(raw, saved) {
			t.Fatal("borrowed CAS input changed", i)
		}
		entries = expected
	}
}
func TestReferenceByteFormatAndForeignRoot(t *testing.T) {
	bucket := volume.ID{1}
	entry := objectReferenceEntry{object: volume.ObjectID{2}, proof: [32]byte{3}, account: volume.ID{4}}
	raw := encodeObjectReferences([]objectReferenceEntry{entry}, bucket)
	for n := 1; n < len(raw); n++ {
		if _, e := borrowReferenceBytes(raw[:n], bucket, 1); e == nil {
			t.Fatal("accepted truncated image", n)
		}
	}
	for _, mutate := range []func([]byte){func(b []byte) { b[0] = 'X' }, func(b []byte) { b[104] = 3 }, func(b []byte) { binary.LittleEndian.PutUint32(b[20:24], historicalReferenceRootLimit+1) }} {
		bad := bytes.Clone(raw)
		mutate(bad)
		for _, id := range []volume.ID{bucket, {9}} {
			if _, e := borrowReferenceBytes(bad, id, 1); e == nil {
				t.Fatal("accepted malformed image")
			}
		}
	}
	table, e := borrowReferenceBytes(raw, volume.ID{9}, 1)
	if e != nil || table.count() != 0 {
		t.Fatal("clone inherited holders", e)
	}
	original := bytes.Clone(raw)
	if e = table.apply(entry.object, entry.proof, entry.account, false, 0); e != nil {
		t.Fatal(e)
	}
	got, e := decodeObjectReferences(table.raw, volume.ID{9})
	if e != nil || len(got) != 1 || got[0] != entry || !bytes.Equal(raw, original) {
		t.Fatal("foreign edit damaged root", e)
	}
}
func TestReferenceByteDynamicAdmissionAndReadOnly(t *testing.T) {
	bucket := volume.ID{8}
	uid := volume.ID{1}
	target := volume.ObjectID{3}
	proof := [32]byte{4}
	for _, n := range []int{historicalReferenceAccountLimit, historicalReferenceRootLimit, 2*historicalReferenceRootLimit + 1} {
		entries := make([]objectReferenceEntry, n)
		for i := range entries {
			entries[i].account = uid
		}
		raw := encodeObjectReferences(entries, bucket)
		saved := bytes.Clone(raw)
		table, e := borrowReferenceBytes(raw, bucket, 32)
		if e != nil {
			t.Fatal(e)
		}
		e = table.apply(target, proof, uid, false, 0)
		if e != nil || !table.changed || table.count() != n+1 || !bytes.Equal(raw, saved) {
			t.Fatal("dynamic reference insertion failed", n, e)
		}
	}

	entries := []objectReferenceEntry{{target, proof, uid, false, false}}
	raw := encodeObjectReferences(entries, bucket)
	for _, action := range []byte{0, 1} {
		p := proof
		if action == 1 {
			p = [32]byte{7}
		} // Existing PIN and absent UNPIN are read-only.
		alloc := testing.AllocsPerRun(100, func() {
			table, e := borrowReferenceBytes(raw, bucket, 1)
			if e != nil {
				panic(e)
			}
			e = table.apply(target, p, uid, false, action)
			if e != nil || table.changed {
				panic("read-only decision changed")
			}
		})
		if alloc != 0 {
			t.Fatal("read-only decision allocates", alloc)
		}
	}
}

var referenceByteSink []byte

func BenchmarkReferenceByteMutation(b *testing.B) {
	bucket := volume.ID{7}
	entries := make([]objectReferenceEntry, 3218)
	for i := range entries {
		entries[i] = objectReferenceEntry{object: volume.ObjectID{byte(i), byte(i >> 8)}, proof: sha256.Sum256([]byte(fmt.Sprint(i))), account: volume.ID{0xee}}
	}
	raw := encodeObjectReferences(entries, bucket)
	target, proof, uid := volume.ObjectID{0xfa, 0xff}, [32]byte{0xfa}, volume.ID{1}
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprint("decoded=", old), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if old {
					es, e := decodeObjectReferences(raw, bucket)
					if e != nil {
						b.Fatal(e)
					}
					es, _, e = legacyReferenceDecision(es, target, proof, uid, false, 0)
					if e != nil {
						b.Fatal(e)
					}
					referenceByteSink = encodeObjectReferences(es, bucket)
				} else {
					table, e := borrowReferenceBytes(raw, bucket, 1)
					if e != nil {
						b.Fatal(e)
					}
					e = table.apply(target, proof, uid, false, 0)
					if e != nil {
						b.Fatal(e)
					}
					referenceByteSink = table.raw
				}
			}
		})
	}
}

func TestReferenceByteBatchKeepsOriginalCASImage(t *testing.T) {
	bucket := volume.ID{23}
	entries := []objectReferenceEntry{{object: volume.ObjectID{8}, proof: [32]byte{9}, account: volume.ID{2}, dependency: true}}
	raw := encodeObjectReferences(entries, bucket)
	saved := bytes.Clone(raw)
	table, err := borrowReferenceBytes(raw, bucket, 32)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(90217))
	everChanged := false
	for i := 0; i < 1000; i++ {
		target := volume.ObjectID{byte(rng.Intn(30))}
		proof := sha256.Sum256([]byte(fmt.Sprint(rng.Intn(70))))
		uid := volume.ID{byte(rng.Intn(3))}
		action := byte(rng.Intn(7))
		admin := rng.Intn(2) == 0
		old := append([]objectReferenceEntry(nil), entries...)
		expected, changed, wantErr := legacyReferenceDecision(old, target, proof, uid, admin, action)
		gotErr := table.apply(target, proof, uid, admin, action)
		everChanged = everChanged || changed
		if objectStatus(gotErr) != objectStatus(wantErr) || table.changed != everChanged {
			t.Fatal("batch outcome diverged", i, gotErr, wantErr)
		}
		got, e := decodeObjectReferences(table.raw, bucket)
		if e != nil || (!reflect.DeepEqual(got, expected) && !(len(got) == 0 && len(expected) == 0)) {
			t.Fatal("batch state diverged", i, e)
		}
		if !bytes.Equal(raw, saved) {
			t.Fatal("authoritative CAS input modified", i)
		}
		entries = expected
	}
}
