// SPDX-License-Identifier: GPL-3.0-only
package client

// Filesystem format v1 (all fixed-width integers little endian):
//
//   inode: "STFS", version:u16, kind:u8 (1=file, 2=directory), mode:u32,
//          uid:u32, gid:u32, mtime_ns:u64, content_length:u64, layout:u8,
//          layout=0: content:Blob (at most 4096 bytes)
//          layout=1: depth:u8, segment_index:ObjectID
//          v2 only: removed:Bool=true (directory tombstone)
//   directory content: "STDR", version:u16, count:u64,
//          (name:Text, inode:ObjectID)*, sorted by name without duplicates
//   segment index: "STIX", version:u16, count:u32,
//          (length:u64, object:ObjectID, source_offset:u32)*
//
// Blob/Text use the shared codec's canonical uvarint byte length. ObjectIDs are
// always 32 raw bytes. Ordinary references stay within the connection's bucket. At index
// depth 1, references address raw data objects; higher levels address indexes.
// Entry lengths partition the parent's logical content without gaps. A zero
// ObjectID denotes a zero-filled span with no backing object. source_offset is
// only nonzero for data slices. Empty files use inline length zero. Directories
// and files share the same inline/index representation. The fixed root ID is
// SHA-256("STQR filesystem root v1"); all other object IDs are random.
//
// Ordinary data/index blocks retain their IDs and are modified in place under
// recoverable inode intents. Objects are at most 1 MiB. Fresh inode IDs are
// random; mutable block IDs derive from inode/kind/level/slot (mutable.go).
// STFS v7 retains removed/write_path, atime/ctime, links and xattrs, followed by
// mutable:Bool, digest:Bytes32, hasLowerAnchor:Bool and optionally
// lowerOwner:OID, lowerDigest:Bytes32. These are representation/integrity fields,
// never content counters. Namespace caches use actual inode metadata hashes.
// Layout 2 stores a lower index root (depth:u8, root:OID). Lower data must remain
// read-only; a writable origin requires a separate COW bucket. STIX v3 records
// source_offset:u64 and lower:Bool. Known old layouts are decoder-only; legacy
// lower inode references are materialized before writing the new format.
// Namespace intents use STFT v3, state:u8 (pending=0, committed=1, aborted=2),
// count:u32 and (inode:OID, before:Blob, after:Blob, writes:Vec)* in OID order.
// Writes contain id:OID, absent/full/remove:Bool, offset:u64, data:Blob,
// before:Blob (the preimage is used by CAS-only backends). Locked heads
// contain STFL v1 plus descriptor:OID. Helpers compare the actual head bytes,
// publish the intent decision, and then publish its before/after images. Read-only
// readers resolve the logical image without writing. Upgrade all mounts together.

import (
	"crypto/sha256"
	"errors"
	"math"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

const (
	FSObjectSize = 1 << 20
	fsInlineSize = 4096
	fsFanout     = 1024
	fsMaxDepth   = 16
	fsFile       = 1
	fsDirectory  = 2
	fsSymlink    = 3
	fsFIFO       = 4
	fsSocket     = 5
)

var ErrFSFormat = errors.New("client: invalid filesystem object")

// FilesystemRootID is the fixed root inode address, independently in each bucket.
func FilesystemRootID() ObjectID { return sha256.Sum256([]byte("STQR filesystem root v1")) }

type fsSegment struct {
	length        uint64
	id            ObjectID
	offset        uint64
	lower         bool   // data object in the read-only lower bucket
	legacyVersion uint64 // legacy lower inode reference; decode only
}
type fsBlob struct {
	length        uint64
	inline        []byte
	root          ObjectID
	depth         uint8
	source        ObjectID // lazy lower inode; no payload copy for metadata-only changes
	lower         bool
	legacyVersion uint64 // read-only compatibility field; never emitted
	mutable       bool
	digest        [32]byte
	headDigest    [32]byte
	lowerOwner    ObjectID
	lowerDigest   [32]byte
	view          map[ObjectID]fsBlockWrite
}
type fsNode struct {
	identity ObjectID
	head     []byte
	edits    []fsBlockWrite
	guardTx  ObjectID
	guardRaw []byte

	kind           uint8
	mode, uid, gid uint32
	mtime          uint64
	atime, ctime   uint64
	blob           fsBlob
	removed        bool   // v2 directory tombstone; prevents creates through stale handles
	writePath      string // original authorized overlay path for unlinked open files
	links          uint32
	extended       bool
	xattrs         map[string][]byte
}

// FSEntry addresses another inode in the same bucket; files and directories use
// the same addressing scheme. Names are sorted by their UTF-8 bytes on disk.
type FSEntry struct {
	Name string
	ID   ObjectID
}

// FSInfo describes the inode snapshot used by an operation.
type FSKind uint8

const (
	FSRegular   FSKind = fsFile
	FSDirectory FSKind = fsDirectory
	FSSymlink   FSKind = fsSymlink
	FSFIFO      FSKind = fsFIFO
	FSSocket    FSKind = fsSocket
)

type FSInfo struct {
	Kind             FSKind
	Nlink            uint32
	ID               ObjectID
	Size             uint64
	Proof            SnapshotProof
	Mode, UID, GID   uint32
	MtimeNS          uint64
	AtimeNS, CtimeNS uint64
	Directory        bool
	Removed          bool
}

func validFSName(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 255 && utf8.ValidString(name) && !strings.ContainsAny(name, "/\x00")
}
func encodeFSNode(n fsNode) []byte {
	e := Encoder{B: []byte("STFS")}
	e.U16(7)
	e.U8(n.kind)
	e.U32(n.mode)
	e.U32(n.uid)
	e.U32(n.gid)
	e.U64(n.mtime)
	e.U64(n.blob.length)
	if n.blob.source != (ObjectID{}) {
		// Legacy lower inode references are materialized before any new write.
		return nil
	} else if n.blob.lower {
		e.U8(2)
		e.U8(n.blob.depth)
		e.B = append(e.B, n.blob.root[:]...)
	} else if n.blob.root == (ObjectID{}) {
		e.U8(0)
		e.Blob(n.blob.inline)
	} else {
		e.U8(1)
		e.U8(n.blob.depth)
		e.B = append(e.B, n.blob.root[:]...)
	}
	e.Bool(n.removed)
	e.Text(n.writePath)
	e.U64(n.atime)
	e.U64(n.ctime)
	e.U32(fsLinks(n))
	names := make([]string, 0, len(n.xattrs))
	for name := range n.xattrs {
		names = append(names, name)
	}
	sort.Strings(names)
	e.U32(uint32(len(names)))
	for _, name := range names {
		e.Text(name)
		e.Blob(n.xattrs[name])
	}
	e.Bool(n.blob.mutable)
	e.B = append(e.B, n.blob.digest[:]...)
	e.Bool(n.blob.lowerOwner != (ObjectID{}))
	if n.blob.lowerOwner != (ObjectID{}) {
		e.B = append(e.B, n.blob.lowerOwner[:]...)
		e.B = append(e.B, n.blob.lowerDigest[:]...)
	}
	return e.B
}
func decodeFSNode(data []byte) (fsNode, error) {
	n, err := decodeFSNodeFormat(data, false)
	// Two development branches used v6 differently. Only these two known,
	// strictly validated layouts are accepted; all new writes use unambiguous v7.
	if err != nil && len(data) >= 6 && string(data[:6]) == "STFS\x06\x00" {
		return decodeFSNodeFormat(data, true)
	}
	return n, err
}
func decodeFSNodeFormat(data []byte, cowSix bool) (fsNode, error) {
	var n fsNode
	if len(data) > FSObjectSize {
		return n, ErrFSFormat
	}
	d := Decoder{B: data}
	if string(d.Take(4)) != "STFS" {
		return n, ErrFSFormat
	}
	n.blob.headDigest = sha256.Sum256(data)
	version := d.U16()
	if version != 1 && version != 2 && version != 3 && version != 4 && version != 5 && version != 6 && version != 7 {
		return n, ErrFSFormat
	}
	n.kind = d.U8()
	n.mode = d.U32()
	n.uid = d.U32()
	n.gid = d.U32()
	n.mtime = d.U64()
	n.blob.length = d.U64()
	switch d.U8() {
	case 0:
		n.blob.inline = append([]byte(nil), d.Blob()...)
		if uint64(len(n.blob.inline)) != n.blob.length || len(n.blob.inline) > fsInlineSize {
			return n, ErrFSFormat
		}
	case 1:
		n.blob.depth = d.U8()
		copy(n.blob.root[:], d.Take(32))
		if n.blob.depth == 0 || n.blob.depth > fsMaxDepth || n.blob.root == (ObjectID{}) || n.blob.length == 0 {
			return n, ErrFSFormat
		}
	case 2:
		if n.kind != fsFile {
			return n, ErrFSFormat
		}
		if version < 7 && !cowSix {
			if version < 3 {
				return n, ErrFSFormat
			}
			copy(n.blob.source[:], d.Take(32))
			n.blob.legacyVersion = d.U64()
			if n.blob.source == (ObjectID{}) || n.blob.legacyVersion == 0 {
				return n, ErrFSFormat
			}
		} else {
			n.blob.lower = true
			n.blob.depth = d.U8()
			copy(n.blob.root[:], d.Take(32))
			if n.blob.depth == 0 || n.blob.depth > fsMaxDepth || n.blob.root == (ObjectID{}) || n.blob.length == 0 {
				return n, ErrFSFormat
			}
		}
	default:
		return n, ErrFSFormat
	}
	if version == 2 {
		n.removed = d.Bool()
		if !n.removed || n.kind != fsDirectory {
			return n, ErrFSFormat
		}
	}
	if version >= 3 {
		n.removed = d.Bool()
		n.writePath = d.Text()
		if n.removed && n.kind != fsDirectory {
			return n, ErrFSFormat
		}
		if n.writePath != "" && !validOverlayPath(n.writePath) {
			return n, ErrFSFormat
		}
	}
	if version >= 4 {
		n.atime = d.U64()
		n.ctime = d.U64()
	} else {
		n.atime, n.ctime = n.mtime, n.mtime
	}
	if version >= 5 {
		n.extended = true
		n.links = d.U32()
		start := len(d.B)
		count := d.U32()
		if count > fsXattrCount {
			return n, ErrFSFormat
		}
		n.xattrs = make(map[string][]byte, count)
		previous := ""
		for i := uint32(0); i < count; i++ {
			name := d.Text()
			value := d.Blob()
			if d.Err != nil || !validXattrName(name) || name <= previous || len(value) > fsXattrValue || start-len(d.B) > fsXattrTotal {
				return n, ErrFSFormat
			}
			n.xattrs[name] = append([]byte(nil), value...)
			previous = name
		}
	}
	if (n.kind > fsDirectory && version < 5) || (n.kind == fsSymlink && (n.blob.root != (ObjectID{}) || n.blob.lower || n.blob.length == 0 || n.blob.length > 4095 || strings.ContainsRune(string(n.blob.inline), 0))) || ((n.kind == fsFIFO || n.kind == fsSocket) && n.blob.length != 0) {
		return n, ErrFSFormat
	}
	if n.kind == fsDirectory && !n.removed && n.extended && n.links == 0 {
		return n, ErrFSFormat
	}
	if version == 6 && !cowSix {
		n.blob.mutable = d.U64() != 0
		if !n.blob.mutable {
			return n, ErrFSFormat
		}
	}
	if version >= 7 {
		n.blob.mutable = d.Bool()
		copy(n.blob.digest[:], d.Take(32))
		if d.Bool() {
			copy(n.blob.lowerOwner[:], d.Take(32))
			copy(n.blob.lowerDigest[:], d.Take(32))
			if n.blob.lowerOwner == (ObjectID{}) {
				return n, ErrFSFormat
			}
		}
	}

	if d.Done() != nil || (n.kind < fsFile || n.kind > fsSocket) || n.mode & ^uint32(07777) != 0 || n.blob.length > math.MaxInt64 {
		return n, ErrFSFormat
	}
	return n, nil
}
func encodeFSIndex(segments []fsSegment) []byte {
	e := Encoder{B: []byte("STIX")}
	modern := false
	for _, s := range segments {
		if s.lower {
			modern = true
		}
	}
	if modern {
		e.U16(3)
	} else {
		e.U16(1)
	}
	e.U32(uint32(len(segments)))
	for _, s := range segments {
		e.U64(s.length)
		e.B = append(e.B, s.id[:]...)
		if modern {
			e.U64(s.offset)
			e.Bool(s.lower)
		} else {
			e.U32(uint32(s.offset))
		}
	}
	return e.B
}
func decodeFSIndex(data []byte, length uint64, depth uint8) ([]fsSegment, error) {
	d := Decoder{B: data}
	if len(data) > FSObjectSize || string(d.Take(4)) != "STIX" {
		return nil, ErrFSFormat
	}
	version := d.U16()
	if version != 1 && version != 2 && version != 3 {
		return nil, ErrFSFormat
	}
	count := d.U32()
	if count == 0 || count > fsFanout {
		return nil, ErrFSFormat
	}
	segments := make([]fsSegment, 0, count)
	var total uint64
	for i := uint32(0); i < count; i++ {
		s := fsSegment{length: d.U64()}
		copy(s.id[:], d.Take(32))
		if version == 3 {
			s.offset = d.U64()
			s.lower = d.Bool()
		} else {
			s.offset = uint64(d.U32())
			if version == 2 {
				s.legacyVersion = d.U64()
			}
		}
		if s.length == 0 || s.length > length-total || s.offset > math.MaxInt64 || s.length > math.MaxInt64-s.offset || (depth > 1 && (s.offset != 0 || s.lower)) || (s.id == (ObjectID{}) && (s.offset != 0 || s.lower)) || (depth == 1 && s.id != (ObjectID{}) && (s.length > FSObjectSize || s.offset+s.length > FSObjectSize)) {
			return nil, ErrFSFormat
		}
		total += s.length
		segments = append(segments, s)
	}
	if d.Done() != nil || total != length {
		return nil, ErrFSFormat
	}
	return segments, nil
}
func encodeFSDirectory(entries []FSEntry) ([]byte, error) {
	entries = append([]FSEntry(nil), entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	e := Encoder{B: []byte("STDR")}
	e.U16(1)
	e.U64(uint64(len(entries)))
	for i, x := range entries {
		if !validFSName(x.Name) || x.ID == (ObjectID{}) || (i > 0 && entries[i-1].Name == x.Name) {
			return nil, syscall.EINVAL
		}
		e.Text(x.Name)
		e.B = append(e.B, x.ID[:]...)
	}
	return e.B, nil
}
func decodeFSDirectory(data []byte) ([]FSEntry, error) {
	d := Decoder{B: data}
	if string(d.Take(4)) != "STDR" || d.U16() != 1 {
		return nil, ErrFSFormat
	}
	count := d.U64()
	if count > uint64(len(d.B))/34 {
		return nil, ErrFSFormat
	}
	entries := make([]FSEntry, 0, int(count))
	for i := uint64(0); i < count; i++ {
		x := FSEntry{Name: d.Text()}
		copy(x.ID[:], d.Take(32))
		if !validFSName(x.Name) || x.ID == (ObjectID{}) || (i > 0 && entries[len(entries)-1].Name >= x.Name) {
			return nil, ErrFSFormat
		}
		entries = append(entries, x)
	}
	if d.Done() != nil {
		return nil, ErrFSFormat
	}
	return entries, nil
}
