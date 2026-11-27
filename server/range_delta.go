// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"io"

	"github.com/launix-de/stqr-storage/volume"
)

const rangeDeltaPage = 4096
const rangeDeltaMaxPages = 16384 // Bounded optional manifest: at most 512 KiB.

// Catch-up compares logical pages of the previously imported shadow. The exact
// shadow content digest fences the eventual in-place patch. Sparse/encoded objects and
// oversized manifests retain the general extent transfer path.
func (r *rangeManager) deltaHashes(t *rangeTxn, key volume.Key, source volume.Info) ([][32]byte, [32]byte, error) {
	if source.Deleted || source.Compression != volume.Raw || t.plan.Compression != volume.Raw || source.Length == 0 || source.Length > rangeDeltaPage*rangeDeltaMaxPages {
		return nil, [32]byte{}, nil
	}
	// The current shadow and its exact content hash are the precondition.
	// Advisory persisted transfer memos can outlive accepted shadow bytes.
	store, err := r.destination(t.plan)
	if err != nil {
		return nil, [32]byte{}, err
	}
	h, err := store.Open(false)
	if err != nil {
		return nil, [32]byte{}, err
	}
	defer h.Close()
	info, err := h.RecordInfo(key)
	if err != nil {
		return nil, [32]byte{}, err
	}
	if info.Length != source.Length || info.Compression != volume.Raw || info.Deleted {
		return nil, [32]byte{}, nil
	}
	extents, err := h.Extents(key)
	if err != nil {
		return nil, [32]byte{}, err
	}
	for _, e := range extents {
		if e.Zero {
			return nil, [32]byte{}, nil
		}
	}
	hash := sha256.New()
	sums := make([][32]byte, 0, (source.Length+rangeDeltaPage-1)/rangeDeltaPage)
	// Read checksum blocks once, then hash their 4-KiB logical pages in memory.
	for off := uint64(0); off < info.Length; {
		data := make([]byte, int(min(info.Length-off, uint64(volume.BlockSize))))
		n, e := h.ReadAt(key, data, off)
		if e != nil && e != io.EOF {
			return nil, [32]byte{}, e
		}
		if n != len(data) {
			return nil, [32]byte{}, io.ErrUnexpectedEOF
		}
		hash.Write(data)
		for at := 0; at < len(data); at += rangeDeltaPage {
			sums = append(sums, sha256.Sum256(data[at:min(at+rangeDeltaPage, len(data))]))
		}
		off += uint64(n)
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return sums, digest, nil
}
