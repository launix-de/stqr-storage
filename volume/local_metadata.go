// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
)

const localMetadataPayload = PageSize - 48

// MaxLocalMetadataBytes bounds management decoding, not client object sizes.
const MaxLocalMetadataBytes = 64 << 20

// LocalMetadata is opaque, durable volume-management metadata, separate from
// client objects and retry records. Its pages count as physical metadata; they
// do not increase object counts, logical usage or payload bytes. The superblock
// points to a checksummed, volume-bound page chain. Callers own its schema and
// authority semantics; merely finding this record never grants write authority.
func (h *Handle) LocalMetadata() ([]byte, error) {
	return withHandle(h, false, func(v *Volume) ([]byte, error) { b, _, e := v.localMetadataLocked(); return b, e })
}

func (v *Volume) localMetadataLocked() ([]byte, []uint64, error) {
	size, off := v.sb.LocalMetadataSize, v.sb.LocalMetadataRoot
	if size == 0 {
		if off != 0 {
			return nil, nil, ErrCorrupt
		}
		return nil, nil, nil
	}
	if size > v.sb.Identity.Size || size > MaxLocalMetadataBytes {
		return nil, nil, ErrCorrupt
	}
	out := make([]byte, 0, int(size))
	pages := make([]uint64, 0, (size+localMetadataPayload-1)/localMetadataPayload)
	for uint64(len(out)) < size {
		if off < treeStart || off%PageSize != 0 || off > v.sb.Identity.Size-PageSize {
			return nil, nil, ErrCorrupt
		}
		b, e := v.readPage(off)
		if e != nil {
			return nil, nil, e
		}
		n := uint64(diskOrder.Uint32(b[32:]))
		next := diskOrder.Uint64(b[24:])
		if string(b[:8]) != "STQRMET1" || !bytes.Equal(b[8:24], v.sb.Identity.VolumeID[:]) || checksum(b[:PageSize-4]) != diskOrder.Uint32(b[PageSize-4:]) || n == 0 || n > localMetadataPayload || n > size-uint64(len(out)) || n != min(uint64(localMetadataPayload), size-uint64(len(out))) {
			return nil, nil, ErrCorrupt
		}
		pages = append(pages, off)
		out = append(out, b[40:40+n]...)
		off = next
	}
	if off != 0 {
		return nil, nil, ErrCorrupt
	}
	return out, pages, nil
}

// SetLocalMetadata atomically replaces the management record, retaining the old
// chain until the same redo transaction publishes its replacement and freelist.
// An empty record clears it. Like object operations it requires a write handle.
func (h *Handle) SetLocalMetadata(data []byte) error {
	return h.SetLocalMetadataWithOptions(data, Options{})
}

// SetLocalMetadataWithOptions lets a trusted importer spend its own reserved
// physical budget. Other buckets' and other imports' reserves remain protected.
func (h *Handle) SetLocalMetadataWithOptions(data []byte, options Options) error {
	return h.do(true, func(v *Volume) error { return v.setLocalMetadataLocked(data, options, nil) })
}
func (v *Volume) setLocalMetadataLocked(data []byte, options Options, node *ID) error {
	if len(data) > MaxLocalMetadataBytes {
		return ErrRange
	}
	old, pages, e := v.localMetadataLocked()
	if e != nil {
		return e
	}
	if bytes.Equal(old, data) && (node == nil || *node == v.sb.Identity.NodeID) {
		return nil
	}
	t := v.transaction()
	if node != nil {
		t.sb.Identity.NodeID = *node
	}
	defer t.close()
	// Reuse existing pages in place through redo. Metadata updates must remain
	// possible on a nearly full volume (not require a second complete chain).
	count := (len(data) + localMetadataPayload - 1) / localMetadataPayload
	offsets := append([]uint64(nil), pages[:min(count, len(pages))]...)
	for len(offsets) < count {
		off, e := t.metaPage()
		if e != nil {
			return e
		}
		offsets = append(offsets, off)
	}
	for i, off := range offsets {
		b := make([]byte, PageSize)
		copy(b, "STQRMET1")
		copy(b[8:24], v.sb.Identity.VolumeID[:])
		if i+1 < len(offsets) {
			diskOrder.PutUint64(b[24:], offsets[i+1])
		}
		from := i * localMetadataPayload
		n := min(localMetadataPayload, len(data)-from)
		diskOrder.PutUint32(b[32:], uint32(n))
		copy(b[40:], data[from:from+n])
		diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
		t.pages[off] = b
	}
	for _, off := range pages[min(count, len(pages)):] {
		t.release(off, PageSize, true)
	}
	t.sb.LocalMetadataRoot = 0
	t.sb.LocalMetadataSize = uint64(len(data))
	if len(offsets) > 0 {
		t.sb.LocalMetadataRoot = offsets[0]
	}
	return t.commitReserved(options.UseGuaranteed, options.InFlightAllowance, options.GuaranteedAllowance)
}

// ReassignIdentity changes the owning node without reformatting objects. It is
// an administrative primitive, not an ownership election. The caller MUST have
// a durable cluster takeover decision and fence the former writer first. The
// volume is exclusively claimed by its caller; outstanding handles serialize
// with this operation. Cross-cluster reassignment and stale identities fail.
func (v *Volume) ReassignIdentity(expected Identity, node ID) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if e := v.check(true); e != nil {
		return e
	}
	if node == (ID{}) || expected.ClusterID == (ID{}) {
		return ErrIdentity
	}
	if v.sb.Identity != expected {
		return fmt.Errorf("%w: volume identity changed", ErrPrecondition)
	}
	if expected.NodeID == node {
		return nil
	}
	s := v.sb
	s.Identity.NodeID = node
	s.Generation++
	return v.publish(s)
}

// ReassignIdentityWithMetadata commits the new owner and its management proof in
// ONE redo transaction. After power loss both identify the old owner or both
// identify the new owner; no intermediate provenance gap can become durable.
// The same fencing/cluster-decision contract as ReassignIdentity applies.
func (v *Volume) ReassignIdentityWithMetadata(expected Identity, node ID, data []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(true); err != nil {
		return err
	}
	if node == (ID{}) || expected.ClusterID == (ID{}) {
		return ErrIdentity
	}
	if v.sb.Identity != expected {
		return ErrPrecondition
	}
	return v.setLocalMetadataLocked(data, Options{}, &node)
}
