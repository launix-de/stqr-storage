// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/launix-de/stqr-storage/volume"
)

const referenceHeaderBytes = 24
const referenceEntryBytes = 81

// referenceBytes borrows one request's validated ORF1 image. It never modifies
// that image: the first actual edit allocates a bounded workspace, preserving
// the original bytes for the authoritative whole-object CAS. No ownership or
// object metadata survives the request, and no format or durability changes.
type referenceBytes struct {
	raw     []byte
	bucket  volume.ID
	spare   int
	changed bool
}

func borrowReferenceBytes(raw []byte, bucket volume.ID, spare int) (referenceBytes, error) {
	t := referenceBytes{bucket: bucket, spare: spare}
	if len(raw) == 0 {
		return t, nil
	}
	if len(raw) < referenceHeaderBytes || !bytes.Equal(raw[:4], []byte("ORF1")) {
		return t, volume.ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(raw[20:24])
	if uint64(n)*referenceEntryBytes != uint64(len(raw)-referenceHeaderBytes) {
		return t, volume.ErrCorrupt
	}
	for off := referenceHeaderBytes + 80; off < len(raw); off += referenceEntryBytes {
		if raw[off] > 2 {
			return t, volume.ErrCorrupt
		}
	}
	// A clone must not inherit the source's live holders, but corrupt foreign
	// images are still rejected before ignoring their contents.
	if bytes.Equal(raw[4:20], bucket[:]) {
		t.raw = raw
	}
	return t, nil
}
func (t *referenceBytes) count() int {
	if len(t.raw) == 0 {
		return 0
	}
	return (len(t.raw) - referenceHeaderBytes) / referenceEntryBytes
}
func (t *referenceBytes) writable() {
	if t.changed {
		return
	}
	n := t.count()
	spare := max(t.spare, 0)
	// Only representable byte lengths constrain allocation, not a holder quota.
	if spare > (math.MaxInt-referenceHeaderBytes)/referenceEntryBytes-n || uint64(n)+uint64(spare) > math.MaxUint32 {
		spare = 0
	}
	next := make([]byte, referenceHeaderBytes+n*referenceEntryBytes, referenceHeaderBytes+(n+spare)*referenceEntryBytes)
	if len(t.raw) > 0 {
		copy(next, t.raw)
	} else {
		copy(next, "ORF1")
		copy(next[4:20], t.bucket[:])
	}
	t.raw = next
	t.changed = true
}
func (t *referenceBytes) setCount() { binary.LittleEndian.PutUint32(t.raw[20:24], uint32(t.count())) }

// apply preserves proof/authentication, idempotence and exclusive retirement.
// Capacity follows the dynamic ORF1 image; ordinary admission never prunes
// another holder. Explicit guarded repair remains a separate administration op.
func (t *referenceBytes) apply(target volume.ObjectID, proof [32]byte, uid volume.ID, admin bool, action byte) error {
	own := -1
	busy := false
	for off := referenceHeaderBytes; off < len(t.raw); off += referenceEntryBytes {
		e := t.raw[off : off+referenceEntryBytes]
		accountMatch := bytes.Equal(e[64:80], uid[:])
		targetMatch := bytes.Equal(e[:32], target[:])
		if action != 6 && bytes.Equal(e[32:64], proof[:]) {
			if !(accountMatch || admin && (action == 1 || action == 3 || action == 5)) || !targetMatch || (e[80] == 1) != (action == 2 || action == 3) || (e[80] == 2) != (action == 4 || action == 5) {
				return ErrBucketForbidden
			}
			own = off
		}
		if targetMatch && (action == 2 || (action == 0 || action == 4) && e[80] == 1) {
			busy = true
		}
	}
	if action == 6 {
		keep := 0
		for off := referenceHeaderBytes; off < len(t.raw); off += referenceEntryBytes {
			if t.raw[off+80] == 2 {
				keep++
			}
		}
		if keep == t.count() {
			return nil
		}
		t.writable()
		dst := referenceHeaderBytes
		for off := referenceHeaderBytes; off < len(t.raw); off += referenceEntryBytes {
			if t.raw[off+80] == 2 {
				copy(t.raw[dst:dst+referenceEntryBytes], t.raw[off:off+referenceEntryBytes])
				dst += referenceEntryBytes
			}
		}
		t.raw = t.raw[:dst]
		t.setCount()
		return nil
	}
	if action == 1 || action == 3 || action == 5 {
		if own < 0 {
			return nil
		}
		t.writable()
		copy(t.raw[own:], t.raw[own+referenceEntryBytes:])
		t.raw = t.raw[:len(t.raw)-referenceEntryBytes]
		t.setCount()
		return nil
	}
	if own >= 0 {
		return nil
	}
	if busy {
		return fail(409, "object reference is busy")
	}
	if uint64(t.count()) == math.MaxUint32 || len(t.raw) > math.MaxInt-referenceEntryBytes {
		return volume.ErrFull // ORF1 uint32/address-space representation only.
	}
	t.writable()
	off := len(t.raw)
	t.raw = append(t.raw, make([]byte, referenceEntryBytes)...)
	copy(t.raw[off:off+32], target[:])
	copy(t.raw[off+32:off+64], proof[:])
	copy(t.raw[off+64:off+80], uid[:])
	if action == 2 {
		t.raw[off+80] = 1
	}
	if action == 4 {
		t.raw[off+80] = 2
	}
	t.setCount()
	return nil
}
