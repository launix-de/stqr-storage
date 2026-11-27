// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestWholeObjectCASComparesBytesAndLength(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	if _, e := testPut(v, k, []byte("original"), Options{}); e != nil {
		t.Fatal(e)
	}
	for _, before := range [][]byte{[]byte("orig"), []byte("different"), nil} {
		if _, e := testCAS(v, k, 0, before, []byte("replacement"), Options{WholeObject: true}); !errors.Is(e, ErrPrecondition) {
			t.Fatalf("comparison %q: %v", before, e)
		}
		mustGet(t, v, k, []byte("original"))
	}
	if _, e := testCAS(v, k, 0, []byte("original"), []byte("new"), Options{WholeObject: true}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, []byte("new"))
}

func TestFullPUTReusesRawAllocationForShorterPayload(t *testing.T) {
	v, d := memoryVolume(t, 2<<20)
	k := testKey(1)
	before := bytes.Repeat([]byte("a"), 1500<<10)
	if _, e := testPut(v, k, before, Options{}); e != nil {
		t.Fatal(e)
	}
	old, e := v.loadObject(k)
	if e != nil {
		t.Fatal(e)
	}
	location := old.Spans[0].Location
	if v.Stats().AdmissionFreePhysical >= uint64(len(before)) {
		t.Fatal("fixture admits a duplicate")
	}
	next := bytes.Repeat([]byte("b"), 64<<10)
	if _, e := testPut(v, k, next, Options{}); e != nil {
		t.Fatal(e)
	}
	state, e := v.loadObject(k)
	if e != nil {
		t.Fatal(e)
	}
	if len(state.Spans) != 1 || state.Spans[0].Location != location {
		t.Fatal("full PUT relocated payload")
	}
	mustGet(t, reopen(t, v, d), k, next)
	assertFreeTree(t, v)
}

func TestFullPUTReuseAcrossLengths(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	var location uint64
	priorLength := 0
	for _, length := range []int{128 << 10, 64 << 10, 192 << 10, 0, 24 << 10} {
		data := bytes.Repeat([]byte{byte(length / 1024)}, length)
		if _, e := testPut(v, k, data, Options{}); e != nil {
			t.Fatal(length, e)
		}
		mustGet(t, v, k, data)
		state, e := v.loadObject(k)
		if e != nil {
			t.Fatal(e)
		}
		if length > 0 {
			if len(state.Spans) != 1 || state.Segmented {
				t.Fatal("whole PUT must remain one packed payload", length, len(state.Spans))
			}
			// A fitting replacement stays in place. Growth may relocate the
			// payload to retain the whole-object contiguous-storage contract.
			if location != 0 && length <= priorLength && state.Spans[0].Location != location {
				t.Fatal("fitting replacement relocated", length)
			}
			location = state.Spans[0].Location
		} else {
			location = 0
		}
		priorLength = length
		assertFreeTree(t, v)
	}
	if e := testDelete(v, k, Options{}); e != nil {
		t.Fatal(e)
	}
	if v.Stats().DataPhysical != 0 {
		t.Fatal("payload leaked", v.Stats())
	}
}

func TestReplicaCommitAppliesRawMutationsInPlace(t *testing.T) {
	for _, op := range []uint8{opPut, opPutRange, opCAS} {
		t.Run(string(rune('0'+op)), func(t *testing.T) {
			v, disk := memoryVolume(t, 8<<20)
			k := testKey(1)
			before := bytes.Repeat([]byte("a"), 64<<10)
			if _, err := testPut(v, k, before, Options{}); err != nil {
				t.Fatal(err)
			}
			original, err := v.loadObject(k)
			if err != nil {
				t.Fatal(err)
			}
			location := original.Spans[0].Location
			offset := uint64(4096)
			data := []byte("replace")
			var expected []byte
			if op == opPut {
				offset = 0
				data = bytes.Repeat([]byte("b"), len(before))
			}
			if op == opCAS {
				expected = before[offset : offset+uint64(len(data))]
			}
			id := testID(91)
			binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
			h, err := v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.PrepareMutation(op, k, offset, uint64(len(data)), bytes.NewReader(data), expected, Options{RequestID: id}, binding)
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			mustGet(t, v, k, before)
			v = reopen(t, v, disk)
			h, err = v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			err = h.DecideReplica(id, binding)
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			v = reopen(t, v, disk)
			h, err = v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.PublishReplica(id, binding)
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Clone(before)
			copy(want[offset:], data)
			mustGet(t, v, k, want)
			current, err := v.loadObject(k)
			if err != nil {
				t.Fatal(err)
			}
			if current.Spans[0].Location != location {
				t.Fatal("replica commit relocated existing RAW payload")
			}
			h, err = v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.PublishReplica(id, binding)
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			v = reopen(t, v, disk)
			mustGet(t, v, k, want)
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicaSparseMaterializationPrepareAndRecovery(t *testing.T) {
	v, disk := memoryVolume(t, 8<<20)
	key := testKey(1)
	id := testID(92)
	binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
	info := Info{Length: 2 << 40}
	source := func(yield func(uint64, io.Reader, uint64) error) error {
		if err := yield(0, bytes.NewBufferString("head"), 4); err != nil {
			return err
		}
		return yield(info.Length-4, bytes.NewBufferString("tail"), 4)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.PrepareRestore(key, info, source, Options{RequestID: id}, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Stat(key); !errors.Is(err, ErrNotFound) {
		t.Fatal("prepare became visible", err)
	}
	v = reopen(t, v, disk)
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.DecideReplica(id, binding); err == nil {
		_, err = h.PublishReplica(id, binding)
	}
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	v = reopen(t, v, disk)
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	data, got, err := h.GetRange(key, info.Length-8, 8)
	h.Close()
	if err != nil || got != info || !bytes.Equal(data, []byte{0, 0, 0, 0, 't', 'a', 'i', 'l'}) {
		t.Fatal(data, got, err)
	}
	if v.Stats().DataPhysical > 64<<10 {
		t.Fatal("sparse materialization allocated holes", v.Stats().DataPhysical)
	}
	assertFreeTree(t, v)
}

func TestCompressedFullPUTReusesEncodedPayloadChunks(t *testing.T) {
	for _, codec := range []Compression{Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) {
			v, disk := memoryVolume(t, 8<<20)
			key := testKey(1)
			before := bytes.Repeat([]byte("abcdefghijklmnop"), 512)
			if _, err := testPut(v, key, before, Options{Compression: Codec(codec)}); err != nil {
				t.Fatal(err)
			}
			object, err := v.loadObject(key)
			if err != nil {
				t.Fatal(err)
			}
			chunks, err := v.packedChunkReferences(object.Spans[0])
			if err != nil {
				t.Fatal(err)
			}
			after := bytes.Repeat([]byte("b"), len(before))
			if _, err = testPut(v, key, after, Options{}); err != nil {
				t.Fatal(err)
			}
			object, err = v.loadObject(key)
			if err != nil {
				t.Fatal(err)
			}
			if len(object.Spans) != len(chunks) {
				t.Fatal("unexpected chunk layout", len(object.Spans), len(chunks))
			}
			for i, span := range object.Spans {
				if span.Location != chunks[i].Location {
					t.Fatal("full PUT relocated a fitting encoded chunk", i, span.Location, chunks[i].Location)
				}
			}
			mustGet(t, reopen(t, v, disk), key, after)
			assertFreeTree(t, v)
		})
	}
}
