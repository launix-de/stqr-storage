// SPDX-License-Identifier: GPL-3.0-only
package client

// Filesystem format v1 (all fixed-width integers little endian):
//
//   inode: "STFS", version:u16, kind:u8 (1=file, 2=directory), mode:u32,
//          uid:u32, gid:u32, mtime_ns:u64, content_length:u64, layout:u8,
//          layout=0: content:Blob (at most 4096 bytes)
//          layout=1: depth:u8, segment_index:ObjectID
//   directory content: "STDR", version:u16, count:u64,
//          (name:Text, inode:ObjectID)*, sorted by name without duplicates
//   segment index: "STIX", version:u16, count:u32,
//          (length:u64, object:ObjectID, source_offset:u32)*
//
// Blob/Text use the shared codec's canonical uvarint byte length. ObjectIDs are
// always 32 raw bytes. References stay within the connection's bucket. At index
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
)

var ErrFSFormat = errors.New("client: invalid filesystem object")

// FilesystemRootID is the fixed root inode address, independently in each bucket.
func FilesystemRootID() ObjectID { return sha256.Sum256([]byte("STQR filesystem root v1")) }

type fsSegment struct {
	length uint64
	id     ObjectID
	offset uint32
}
type fsBlob struct {
	length uint64
	inline []byte
	root   ObjectID
	depth  uint8
}
type fsNode struct {
	kind           uint8
	mode, uid, gid uint32
	mtime          uint64
	blob           fsBlob
}

// FSEntry addresses another inode in the same bucket; files and directories use
// the same addressing scheme. Names are sorted by their UTF-8 bytes on disk.
type FSEntry struct {
	Name string
	ID   ObjectID
}

// FSInfo describes the inode snapshot used by an operation.
type FSInfo struct {
	ID             ObjectID
	Version, Size  uint64
	Mode, UID, GID uint32
	MtimeNS        uint64
	Directory      bool
}

func validFSName(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 255 && utf8.ValidString(name) && !strings.ContainsAny(name, "/\x00")
}
func encodeFSNode(n fsNode) []byte {
	e := Encoder{B: []byte("STFS")}
	e.U16(1)
	e.U8(n.kind)
	e.U32(n.mode)
	e.U32(n.uid)
	e.U32(n.gid)
	e.U64(n.mtime)
	e.U64(n.blob.length)
	if n.blob.root == (ObjectID{}) {
		e.U8(0)
		e.Blob(n.blob.inline)
	} else {
		e.U8(1)
		e.U8(n.blob.depth)
		e.B = append(e.B, n.blob.root[:]...)
	}
	return e.B
}
func decodeFSNode(data []byte) (fsNode, error) {
	var n fsNode
	if len(data) > FSObjectSize {
		return n, ErrFSFormat
	}
	d := Decoder{B: data}
	if string(d.Take(4)) != "STFS" || d.U16() != 1 {
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
	default:
		return n, ErrFSFormat
	}
	if d.Done() != nil || (n.kind != fsFile && n.kind != fsDirectory) || n.mode & ^uint32(07777) != 0 || n.blob.length > math.MaxInt64 {
		return n, ErrFSFormat
	}
	return n, nil
}
func encodeFSIndex(segments []fsSegment) []byte {
	e := Encoder{B: []byte("STIX")}
	e.U16(1)
	e.U32(uint32(len(segments)))
	for _, s := range segments {
		e.U64(s.length)
		e.B = append(e.B, s.id[:]...)
		e.U32(s.offset)
	}
	return e.B
}
func decodeFSIndex(data []byte, length uint64, depth uint8) ([]fsSegment, error) {
	d := Decoder{B: data}
	if len(data) > FSObjectSize || string(d.Take(4)) != "STIX" || d.U16() != 1 {
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
		s.offset = d.U32()
		if s.length == 0 || s.length > length-total || (depth > 1 && s.offset != 0) || (s.id == (ObjectID{}) && s.offset != 0) || (depth == 1 && s.id != (ObjectID{}) && (s.length > FSObjectSize || uint64(s.offset)+s.length > FSObjectSize)) {
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
