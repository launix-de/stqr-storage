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
// Data and index objects are immutable, including after unlink/truncate. A new
// inode version atomically publishes the complete snapshot by CAS. Every stored
// object is <= 1 MiB. This is a development format, not a frozen production ABI.
// Inode v1/v2 remain readable. v3 adds layout=2 (lower inode:OID,
// expected_version:u64) and a suffix removed:Bool, write_path:Text. v4 also
// appends atime_ns:u64, ctime_ns:u64. Legacy timestamps default to mtime.
// STIX v2 widens source_offset to u64 and appends lower_inode_version:u64
// per entry; a nonzero version denotes a range of that lower inode, not raw
// upper data. Only native overlays supply a lower store. Lower data is verified
// against the captured inode version; changed bases report ESTALE.
// v5 retains the v4 layout and appends links:u32, xattr_count:u32,
// (name:Text,value:Blob)* in sorted name order. Kind=3 is an inline symlink,
// 4 a FIFO and 5 a Unix socket; the latter two have empty content. Attribute
// limits and link publication/recovery are documented in docs/filesystem-posix.md.
// Namespace intents use "STFT", v1:u16, state:u8 (pending=0,
// committed=1, aborted=2), count:u32, (inode:OID, expected_version:u64,
// before:Blob, after:Blob)* in OID order. Locked inode heads contain "STFL",
// v1:u16, descriptor:OID. Helpers durably decide an intent before replacing its
// markers with before/after images. A reader without CAS permission can read the
// old/new logical image according to that decision. Upgrade all mounts together.

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
	length       uint64
	id           ObjectID
	offset       uint64
	lowerVersion uint64 // nonzero: range of a lower-bucket inode snapshot
}
type fsBlob struct {
	length        uint64
	inline        []byte
	root          ObjectID
	depth         uint8
	source        ObjectID // lazy lower inode; no payload copy for metadata-only changes
	sourceVersion uint64
	stamp         uint64
	view          map[ObjectID]fsBlockWrite
}
type fsNode struct {
	identity     ObjectID
	version      uint64
	edits        []fsBlockWrite
	guardTx      ObjectID
	guardVersion uint64

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
	Version, Size    uint64
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
	if n.blob.stamp != 0 && !n.extended {
		n.links = fsLinks(n)
		n.extended = true
	}
	if n.blob.stamp != 0 {
		e.U16(6)
	} else if n.extended || len(n.xattrs) != 0 || n.kind > fsDirectory {
		e.U16(5)
	} else if n.atime != 0 || n.ctime != 0 {
		e.U16(4)
	} else if n.blob.source != (ObjectID{}) || n.writePath != "" {
		e.U16(3)
	} else if n.removed {
		e.U16(2)
	} else {
		e.U16(1)
	}
	e.U8(n.kind)
	e.U32(n.mode)
	e.U32(n.uid)
	e.U32(n.gid)
	e.U64(n.mtime)
	e.U64(n.blob.length)
	if n.blob.source != (ObjectID{}) {
		e.U8(2)
		e.B = append(e.B, n.blob.source[:]...)
		e.U64(n.blob.sourceVersion)
	} else if n.blob.root == (ObjectID{}) {
		e.U8(0)
		e.Blob(n.blob.inline)
	} else {
		e.U8(1)
		e.U8(n.blob.depth)
		e.B = append(e.B, n.blob.root[:]...)
	}
	if n.extended || len(n.xattrs) != 0 || n.kind > fsDirectory || n.atime != 0 || n.ctime != 0 || n.blob.source != (ObjectID{}) || n.writePath != "" {
		e.Bool(n.removed)
		e.Text(n.writePath)
		if n.extended || len(n.xattrs) != 0 || n.kind > fsDirectory || n.atime != 0 || n.ctime != 0 {
			e.U64(n.atime)
			e.U64(n.ctime)
		}
	} else if n.removed {
		e.Bool(true)
	}
	if n.extended || len(n.xattrs) != 0 || n.kind > fsDirectory {
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
	}
	if n.blob.stamp != 0 {
		e.U64(n.blob.stamp)
	}
	return e.B
}
func decodeFSNode(data []byte) (fsNode, error) {
	var n fsNode
	if len(data) > FSObjectSize {
		return n, ErrFSFormat
	}
	d := Decoder{B: data}
	if string(d.Take(4)) != "STFS" {
		return n, ErrFSFormat
	}
	version := d.U16()
	if version != 1 && version != 2 && version != 3 && version != 4 && version != 5 && version != 6 {
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
		copy(n.blob.source[:], d.Take(32))
		n.blob.sourceVersion = d.U64()
		if version < 3 || n.blob.source == (ObjectID{}) || n.blob.sourceVersion == 0 || n.kind != fsFile {
			return n, ErrFSFormat
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
	if (n.kind > fsDirectory && version < 5) || (n.kind == fsSymlink && (n.blob.root != (ObjectID{}) || n.blob.source != (ObjectID{}) || n.blob.length == 0 || n.blob.length > 4095 || strings.ContainsRune(string(n.blob.inline), 0))) || ((n.kind == fsFIFO || n.kind == fsSocket) && n.blob.length != 0) {
		return n, ErrFSFormat
	}
	if n.kind == fsDirectory && !n.removed && n.extended && n.links == 0 {
		return n, ErrFSFormat
	}
	if version >= 6 {
		n.blob.stamp = d.U64()
		if n.blob.stamp == 0 {
			return n, ErrFSFormat
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
		if s.lowerVersion != 0 {
			modern = true
		}
	}
	if modern {
		e.U16(2)
	} else {
		e.U16(1)
	}
	e.U32(uint32(len(segments)))
	for _, s := range segments {
		e.U64(s.length)
		e.B = append(e.B, s.id[:]...)
		if modern {
			e.U64(s.offset)
			e.U64(s.lowerVersion)
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
	if version != 1 && version != 2 {
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
		if version == 2 {
			s.offset = d.U64()
			s.lowerVersion = d.U64()
		} else {
			s.offset = uint64(d.U32())
		}
		if s.length == 0 || s.length > length-total || s.offset > math.MaxInt64 || s.length > math.MaxInt64-s.offset || (depth > 1 && (s.offset != 0 || s.lowerVersion != 0)) || (s.id == (ObjectID{}) && (s.offset != 0 || s.lowerVersion != 0)) || (depth == 1 && s.id != (ObjectID{}) && s.lowerVersion == 0 && (s.length > FSObjectSize || s.offset+s.length > FSObjectSize)) {
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
