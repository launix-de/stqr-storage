// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// Format-4 request receipts remain evidence of completed operations. Their old
// hash had an absent object-version predicate; decode that *request encoding*
// during upgrade, never restore a content counter or use it for synchronization.
// The current hash remains the only one emitted for new requests.
type compatibleFingerprint struct {
	hash.Hash
	legacy hash.Hash
}

func (h *compatibleFingerprint) Write(p []byte) (int, error) {
	n, e := h.Hash.Write(p)
	if e == nil {
		_, e = h.legacy.Write(p)
	}
	return n, e
}
func fingerprint(op uint8, k Key, offset, length uint64, opts Options, expected []byte) hash.Hash {
	current := fingerprintCurrent(op, k, offset, length, opts, expected)
	if opts.Compare != nil {
		return current
	}
	legacy := sha256.New()
	legacy.Write([]byte{op})
	legacy.Write(k.Bucket[:])
	legacy.Write(k.Object[:])
	var b [8]byte
	for _, n := range []uint64{offset, length} {
		binary.LittleEndian.PutUint64(b[:], n)
		legacy.Write(b[:])
	}
	legacy.Write([]byte{0}) // removed optional version predicate was absent
	if opts.ExpectedLength == nil {
		legacy.Write([]byte{0})
	} else {
		legacy.Write([]byte{1})
		binary.LittleEndian.PutUint64(b[:], *opts.ExpectedLength)
		legacy.Write(b[:])
	}
	var flags byte
	if opts.CreateOnly {
		flags |= 1
	}
	if opts.WholeObject {
		flags |= 2
	}
	legacy.Write([]byte{flags})
	if opts.Compression == nil {
		legacy.Write([]byte{0})
	} else {
		legacy.Write([]byte{1, byte(*opts.Compression)})
	}
	binary.LittleEndian.PutUint64(b[:], uint64(len(expected)))
	legacy.Write(b[:])
	legacy.Write(expected)
	return &compatibleFingerprint{Hash: current, legacy: legacy}
}
func matchesRequestFingerprint(h hash.Hash, want [32]byte) bool {
	var got [32]byte
	copy(got[:], h.Sum(nil))
	if got == want {
		return true
	}
	if compatible, ok := h.(*compatibleFingerprint); ok {
		copy(got[:], compatible.legacy.Sum(nil))
		return got == want
	}
	return false
}
