// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestEncodedTransferPreservesStoredChunks(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) {
			src, _ := memoryVolume(t, 12<<20)
			dst, device := memoryVolume(t, 12<<20)
			key := testKey(1)
			payload := bytes.Repeat([]byte("compressible payload"), 130000)
			if _, err := testPut(src, key, payload, Options{Compression: &codec}); err != nil {
				t.Fatal(err)
			}
			if _, err := testPutRange(src, key, BlockSize+17, []byte("changed"), Options{}); err != nil {
				t.Fatal(err)
			}
			copy(payload[BlockSize+17:], "changed")
			h, err := src.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			var chunks []EncodedChunk
			info, err := h.ExportEncoded(key, func(c EncodedChunk) error { c.Data = bytes.Clone(c.Data); chunks = append(chunks, c); return nil })
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			w, err := dst.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			source := func(yield func(EncodedChunk) error) error {
				for _, c := range chunks {
					if err := yield(c); err != nil {
						return err
					}
				}
				return nil
			}
			if err = w.RestoreEncoded(key, info, source, Options{ImportShadow: true}); err != nil {
				t.Fatal(err)
			}
			w.Close()
			r, err := dst.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			index := 0
			_, err = r.ExportEncoded(key, func(c EncodedChunk) error {
				if index >= len(chunks) {
					t.Fatal("extra chunk")
				}
				expected := chunks[index]
				index++
				if !bytes.Equal(c.Data, expected.Data) || c.Codec != expected.Codec || c.RawCRC != expected.RawCRC || c.EncodedCRC != expected.EncodedCRC {
					t.Fatal("encoded bytes changed")
				}
				return nil
			})
			r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if index != len(chunks) {
				t.Fatal("missing chunk")
			}
			mustGet(t, reopen(t, dst, device), key, payload)
		})
	}
}
