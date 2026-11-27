// SPDX-License-Identifier: GPL-3.0-only
package volume

import "hash/crc32"

// objectImportedAt reads only the CRC-protected OBJ2 descriptor, never external
// SEG2 pages, checksums or payload. Ownership accounting needs the header flag,
// not an extent walk. The caller holds the volume lock, and readBytes retains
// the transaction/deferred view. Full reads retain loadObjectAt layout checks.
// Segmented descriptors are always 64 bytes, independent of segment count. Older
// inline descriptors include their span bytes in the same CRC; stream those
// bytes through bounded scratch space without decoding or following spans.
func (v *Volume) objectImportedAt(ptr uint64) (bool, error) {
	// OBJ2 descriptors and SEG2 lists are metadata. Retired initial index
	// pages below dataStart may be reused for metadata, but never payload.
	if ptr < treeStart || ptr > v.sb.Identity.Size || objectHeader > v.sb.Identity.Size-ptr {
		return false, ErrCorrupt
	}
	var head [objectHeader]byte
	if err := v.readBytes(head[:], ptr); err != nil {
		return false, err
	}
	n := uint64(diskOrder.Uint32(head[:]))
	count := uint64(diskOrder.Uint32(head[28:]))
	segmented := head[25]&2 != 0
	if string(head[4:8]) != "OBJ2" || n < objectHeader || n > v.sb.Identity.Size-ptr || !Compression(head[24]).valid() {
		return false, ErrCorrupt
	}
	if segmented && n != objectHeader+24 || !segmented && n != objectHeader+count*spanBytes {
		return false, ErrCorrupt
	}
	stored := diskOrder.Uint32(head[32:])
	diskOrder.PutUint32(head[32:], 0)
	crc := checksum(head[:])
	scratch := make([]byte, min(n-objectHeader, PageSize))
	for off := uint64(objectHeader); off < n; {
		part := scratch[:min(uint64(len(scratch)), n-off)]
		if err := v.readBytes(part, ptr+off); err != nil {
			return false, err
		}
		crc = crc32.Update(crc, crcTable, part)
		if segmented {
			first, last, total := diskOrder.Uint64(part), diskOrder.Uint64(part[8:]), diskOrder.Uint64(part[16:])
			valid := func(p uint64) bool {
				return p >= treeStart && p <= v.sb.Identity.Size && listBytes <= v.sb.Identity.Size-p
			}
			if total != count || count == 0 && (first != 0 || last != 0) || count > 0 && (!valid(first) || !valid(last)) {
				return false, ErrCorrupt
			}
		}
		off += uint64(len(part))
	}
	if crc != stored {
		return false, ErrCorrupt
	}
	return head[25]&4 != 0, nil
}
