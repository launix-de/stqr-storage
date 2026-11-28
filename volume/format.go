// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var diskOrder = binary.LittleEndian
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

const scratchPages = 8
const treeStart = uint64((4 + scratchPages) * PageSize)
const dataStart = treeStart + 2*PageSize

// Superblock contains only roots, configuration and physical counters. Tree
// pages are updated in place through the redo WAL; no index directory snapshots.
type Superblock struct {
	RestartLineage                                                                                                         ID // accepted handoff ancestry, never an object content version
	Identity                                                                                                               Identity
	Generation, Sequence, ObjectRoot, FreeRoot, Objects, LogicalBytes, FreeBytes, MetadataBytes, PayloadBytes, ArenaUnused uint64
	LocalMetadataRoot, LocalMetadataSize                                                                                   uint64
	Moving                                                                                                                 bool
	MoveKey                                                                                                                Key
	MoveFrom, MoveTo, MoveLength, MoveDone, MoveSpan                                                                       uint64
}

func (s Superblock) MarshalBinary() ([]byte, error) {
	if s.Identity.Size < dataStart || s.ObjectRoot < treeStart || s.FreeRoot < treeStart || s.ObjectRoot%PageSize != 0 || s.FreeRoot%PageSize != 0 || s.ObjectRoot+PageSize > s.Identity.Size || s.FreeRoot+PageSize > s.Identity.Size || !s.Identity.Class.valid() || s.FreeBytes > s.Identity.Size {
		return nil, ErrCorrupt
	}
	if s.Generation == 0 || s.ObjectRoot == s.FreeRoot || s.MetadataBytes > s.Identity.Size-scratchPages*PageSize {
		return nil, ErrCorrupt
	}
	remaining := s.Identity.Size - scratchPages*PageSize - s.MetadataBytes
	if s.PayloadBytes > remaining || s.FreeBytes != remaining-s.PayloadBytes {
		return nil, ErrCorrupt
	}
	b := make([]byte, PageSize)
	if (s.LocalMetadataRoot == 0) != (s.LocalMetadataSize == 0) || s.LocalMetadataRoot != 0 && (s.LocalMetadataRoot < treeStart || s.LocalMetadataRoot%PageSize != 0 || s.LocalMetadataRoot > s.Identity.Size-PageSize || s.LocalMetadataSize > s.Identity.Size) {
		return nil, ErrCorrupt
	}
	// Keep the family magic: v2 readers see the newer version and refuse the
	// entire volume, including when the other redundant superblock is still v2.
	copy(b, "STQRVOL2")
	diskOrder.PutUint16(b[8:], FormatVersion)
	b[10] = byte(s.Identity.Class)
	copy(b[16:32], s.Identity.ClusterID[:])
	copy(b[32:48], s.Identity.NodeID[:])
	copy(b[48:64], s.Identity.VolumeID[:])
	for i, x := range []uint64{s.Identity.Size, s.Generation, s.Sequence, s.ObjectRoot, s.FreeRoot, s.Objects, s.LogicalBytes, s.FreeBytes, s.MetadataBytes, s.PayloadBytes, s.ArenaUnused} {
		diskOrder.PutUint64(b[64+i*8:], x)
	}
	diskOrder.PutUint64(b[160:], s.LocalMetadataRoot)
	diskOrder.PutUint64(b[168:], s.LocalMetadataSize)
	if s.Moving {
		b[256] = 1
	}
	copy(b[264:280], s.MoveKey.Bucket[:])
	copy(b[280:312], s.MoveKey.Object[:])
	for i, x := range []uint64{s.MoveFrom, s.MoveTo, s.MoveLength, s.MoveDone, s.MoveSpan} {
		diskOrder.PutUint64(b[312+i*8:], x)
	}
	copy(b[384:400], s.RestartLineage[:])
	diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
	return b, nil
}
func DecodeSuperblock(b []byte) (Superblock, error) {
	var s Superblock
	if len(b) != PageSize {
		return s, ErrUnformatted
	}
	if string(b[:8]) != "STQRVOL2" && string(b[:8]) != "STQRVOL3" {
		if string(b[:8]) == "STQRVOL1" {
			return s, ErrVersion
		}
		if bytes.HasPrefix(b, []byte("STQR")) || string(b[4:8]) == "VOL2" {
			return s, ErrCorrupt
		}
		return s, ErrUnformatted
	}
	if checksum(b[:PageSize-4]) != diskOrder.Uint32(b[PageSize-4:]) {
		return s, ErrCorrupt
	}
	version := diskOrder.Uint16(b[8:])
	if version != FormatVersion && version != 5 && version != 4 && version != 3 && version != 2 || version == 2 && string(b[:8]) != "STQRVOL2" {
		return s, ErrVersion
	}
	if version >= 6 {
		copy(s.RestartLineage[:], b[384:400])
	}
	s.Identity.Class = Class(b[10])
	copy(s.Identity.ClusterID[:], b[16:32])
	copy(s.Identity.NodeID[:], b[32:48])
	copy(s.Identity.VolumeID[:], b[48:64])
	fields := []*uint64{&s.Identity.Size, &s.Generation, &s.Sequence, &s.ObjectRoot, &s.FreeRoot, &s.Objects, &s.LogicalBytes, &s.FreeBytes, &s.MetadataBytes, &s.PayloadBytes, &s.ArenaUnused}
	for i, p := range fields {
		*p = diskOrder.Uint64(b[64+i*8:])
	}
	if version >= 3 {
		s.LocalMetadataRoot = diskOrder.Uint64(b[160:])
		s.LocalMetadataSize = diskOrder.Uint64(b[168:])
	}
	s.Moving = b[256] != 0
	copy(s.MoveKey.Bucket[:], b[264:280])
	copy(s.MoveKey.Object[:], b[280:312])
	for i, p := range []*uint64{&s.MoveFrom, &s.MoveTo, &s.MoveLength, &s.MoveDone, &s.MoveSpan} {
		*p = diskOrder.Uint64(b[312+i*8:])
	}
	if s.Moving && (s.MoveFrom < dataStart || s.MoveFrom > s.Identity.Size || s.MoveTo > s.Identity.Size || s.MoveLength > s.Identity.Size-s.MoveTo || s.MoveTo < dataStart || s.MoveTo >= s.MoveFrom || s.MoveDone > s.MoveLength || s.MoveLength > s.Identity.Size-s.MoveFrom) {
		return s, ErrCorrupt
	}
	if _, e := s.MarshalBinary(); e != nil {
		return s, e
	}
	return s, nil
}
func loadSuperblocks(dev VolumeIO) (Superblock, error) {
	var best Superblock
	var last error = ErrUnformatted
	var unsupported bool
	for i := 0; i < 2; i++ {
		b := make([]byte, PageSize)
		if e := readFullAt(dev, b, uint64(i*PageSize)); e != nil {
			last = e
			continue
		}
		s, e := DecodeSuperblock(b)
		if e != nil {
			if e == ErrVersion {
				unsupported = true
			}
			if e != ErrUnformatted {
				last = e
			}
			continue
		}
		if s.Generation > best.Generation {
			best = s
		}
	}
	if unsupported {
		return best, ErrVersion
	}
	if best.Generation == 0 {
		return best, last
	}
	return best, nil
}
func ReadHeader(dev VolumeIO) (Superblock, error) { return loadSuperblocks(dev) }
