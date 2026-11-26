// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"runtime"
	"testing"
)

// Encoded transfer legitimately carries partial compressed chunks. A scoped
// view must not retain a complete decoded chunk for every small imported slice.
func TestReadViewCompressedAliasesBoundRetainedMemory(t *testing.T) {
	v, _ := memoryVolume(t, 16<<20)
	key := testKey(1)
	raw := bytes.Repeat([]byte("a"), BlockSize)
	encoded, err := compressBlock(Gzip, raw)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("a"), 24)
	writer, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.RestoreEncoded(key, Info{Length: uint64(len(want)), Compression: Gzip}, func(yield func(EncodedChunk) error) error {
		for i := range want {
			if err := yield(EncodedChunk{Offset: uint64(i), Length: 1, Skip: uint64(i), DecodedLength: BlockSize, Codec: Gzip, RawCRC: checksum(raw), EncodedCRC: checksum(encoded), Data: encoded}); err != nil {
				return err
			}
		}
		return nil
	}, Options{ImportShadow: true})
	writer.Close()
	if err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = h.WithReadView(key, 0, uint64(len(want)), func(view ReadView) error {
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(view)
		retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("%d-byte aliased compressed view retains %d additional heap bytes", view.Length, retained)
		// Generous allowance for metadata/runtime noise, far below one full decoded
		// MiB for each of the 24 partial chunks accepted by encoded transfer.
		if retained > 8*BlockSize {
			t.Errorf("small compressed aliases retained full decoded chunks: %d bytes", retained)
		}
		var got bytes.Buffer
		if _, err := view.WriteTo(&got); err != nil {
			return err
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatal("aliased view changed current object bytes")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadViewPackedCompressedPartialChunkMemory(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	key := testKey(1)
	want := bytes.Repeat([]byte("p"), BlockSize)
	if _, err := testPut(v, key, want, Options{Compression: Codec(Gzip)}); err != nil {
		t.Fatal(err)
	}
	object, err := v.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(object.Spans) != 1 || object.Spans[0].Direct {
		t.Fatal("fixture did not retain a packed compressed chunk table")
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	for _, length := range []uint64{24, BlockSize} {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := h.WithReadView(key, 0, length, func(view ReadView) error {
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(view)
			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			t.Logf("%d-byte packed compressed view retains %d additional heap bytes", length, retained)
			if length == 24 && retained > BlockSize/2 {
				t.Errorf("partial packed view retains unused decoded data: %d", retained)
			}
			// The complete chunk is a real positive control for the heap measurement;
			// the view must own its decoded bytes until the callback finishes.
			if length == BlockSize && (retained < BlockSize/2 || retained > 8*BlockSize) {
				t.Errorf("full chunk heap control outside expected range: %d", retained)
			}
			var got bytes.Buffer
			if _, err := view.WriteTo(&got); err != nil {
				return err
			}
			if !bytes.Equal(got.Bytes(), want[:length]) {
				t.Fatal("packed compressed view changed bytes")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
