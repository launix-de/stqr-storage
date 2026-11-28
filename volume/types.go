// SPDX-License-Identifier: GPL-3.0-only

// Package volume implements STQR's local storage engine with a versioned disk format. It has no
// dependency on the server, client protocol, or cluster membership algorithms.
package volume

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const (
	PageSize        = 4096
	BlockSize       = 1 << 20
	MinVolumeSize   = 256 << 10
	FormatVersion   = 6
	pageHeaderSize  = 64
	pagePayload     = PageSize - pageHeaderSize
	superblockBytes = 2 * PageSize
)

var (
	ErrCorrupt           = errors.New("volumes: corrupt storage")
	ErrUnformatted       = errors.New("volumes: unformatted device")
	ErrVersion           = errors.New("volumes: unsupported format version")
	ErrIdentity          = errors.New("volumes: identity mismatch")
	ErrFull              = errors.New("volumes: insufficient physical storage")
	ErrClosed            = errors.New("volumes: closed")
	ErrReadOnly          = errors.New("volumes: read-only handle")
	ErrRetentionRequired = errors.New("volumes: accepted pages require retention before close")
	ErrRecovery          = errors.New("volumes: reopen required after I/O failure")
	ErrNotFound          = errors.New("volumes: object not found")
	ErrPrecondition      = errors.New("volumes: precondition failed")
	ErrRequestID         = errors.New("volumes: request ID reused with different parameters")
	ErrRange             = errors.New("volumes: invalid range")
	ErrBusy              = errors.New("volumes: busy")
	ErrDraining          = errors.New("volumes: draining")
	ErrUnsupported       = errors.New("volumes: unsupported operation")
)

type ID [16]byte
type ObjectID [32]byte

type Key struct {
	Bucket ID
	Object ObjectID
}

func NewID() (ID, error)     { var id ID; _, err := rand.Read(id[:]); return id, err }
func (id ID) String() string { return fmt.Sprintf("%x", id[:]) }

type Class uint8

const (
	HDD Class = iota
	SSD
	RAM
	RAMCache
)

func (c Class) String() string {
	switch c {
	case HDD:
		return "hdd"
	case SSD:
		return "ssd"
	case RAM:
		return "ram"
	case RAMCache:
		return "ramcache"
	}
	return "invalid"
}
func (c Class) valid() bool              { return c <= RAMCache }
func (c Class) Allows(actual Class) bool { return c == actual || c == HDD && actual == SSD }

type Compression uint8

const (
	Raw Compression = iota
	Gzip
	LZ4
	XZ
)

func (c Compression) String() string {
	switch c {
	case Raw:
		return "raw"
	case Gzip:
		return "gzip"
	case LZ4:
		return "lz4"
	case XZ:
		return "xz"
	}
	return "invalid"
}
func (c Compression) valid() bool      { return c <= XZ }
func Codec(c Compression) *Compression { return &c }

type VolumeIO interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Size() (int64, error)
	Close() error
}

type FormatOptions struct {
	ClusterID, NodeID, VolumeID ID
	Class                       Class
}

// RestartSource names a node-manifest-bound immutable accepted image.
// Missing/corrupt required images fail Open before ordinary index access.
type RestartSource struct {
	Path    string
	Receipt RestartReceipt
}
type OpenOptions struct {
	Restart                     *RestartSource
	ClusterID, NodeID, VolumeID ID
	CacheEntries                int
	CacheBytes                  uint64
}

type Info struct {
	Length      uint64
	Compression Compression
	Deleted     bool
}
type Result struct {
	Info   Info
	Offset uint64
}

// Options controls preconditions and retry identity. A zero RequestID disables
// deduplication for a local operation; servers must supply nonzero IDs for retries.
// A nil Compression keeps an existing object's codec, or selects Raw on creation.
type Options struct {
	Accepted bool // visible without Sync; pressure backpressures/spills; only explicit Sync confirms durability

	RequestID           ID
	Compression         *Compression
	ExpectedLength      *uint64
	CompareOffset       uint64 // optional byte predicate, checked under the same volume lock
	Compare             []byte // nil disables comparison; not an object revision
	CreateOnly          bool
	WholeObject         bool
	UseGuaranteed       bool   // trusted owner of the entire volume reserve; shared buckets use GuaranteedAllowance
	GuaranteedAllowance uint64 // trusted physical budget from this caller's guarantee; never consumes another bucket's reserve
	ImportShadow        bool   // trusted fenced incoming copy; becomes authoritative only through committed placement
	InFlightAllowance   uint64 // trusted physical allowance; never consumes bucket guarantees
	restoreSegmented    bool
	prepare             *ReplicaBinding
}

type Extent struct {
	Offset, Length uint64
	Zero           bool
}
type Identity struct {
	ClusterID, NodeID, VolumeID ID
	Class                       Class
	Size                        uint64
}
type Stats struct {
	IO                                                                           IOStats
	Identity                                                                     Identity
	Generation, Objects, LogicalBytes                                            uint64
	CapacityPhysical, AllocatedPhysical, MetadataPhysical, DataPhysical          uint64
	ArenaUnusedPhysical, ReclaimablePhysical, OperatingReservePhysical           uint64
	GuaranteedRemainingPhysical, InFlightReservedPhysical, AdmissionFreePhysical uint64
}

// Object layout is explicit: PUT owns one contiguous allocation; APPEND uses
// a segment list and byte-packed, growable tail arenas. Compression chunks are
// positions inside an allocation, not separate allocator extents.
type object struct {
	importShadow                  bool
	Info                          Info
	Segmented                     bool
	Spans                         []dataSpan
	ListHead, ListTail, ListCount uint64
	ListBlocks                    []uint64
	dirtyFrom, dirtyUntil         int
	Base, Size                    uint64 // packed descriptor's location and encoded length
}
type dataSpan struct {
	// Direct is a standalone encoded chunk. Checksums packs decoded length
	// (high u32) and encoded CRC32C (low u32), not a metadata pointer.
	Offset, Length, Location, Stored, Capacity, Checksums uint64
	Skip                                                  uint64
	Codec                                                 Compression
	Direct                                                bool
	CRC                                                   uint32
}
type dedupe struct {
	importShadow bool
	Key          Key
	Fingerprint  [32]byte
	Result       Result
}

func add(a, b uint64) (uint64, error) {
	c := a + b
	if c < a {
		return 0, ErrRange
	}
	return c, nil
}
func align(n uint64) uint64 { return (n + PageSize - 1) &^ (PageSize - 1) }
func readFullAt(d io.ReaderAt, b []byte, off uint64) error {
	if off > uint64(^uint64(0)>>1) {
		return ErrRange
	}
	n, e := d.ReadAt(b, int64(off))
	if n == len(b) {
		return nil
	}
	if e == nil {
		e = io.ErrUnexpectedEOF
	}
	return e
}
func writeFullAt(d io.WriterAt, b []byte, off uint64) error {
	if off > uint64(^uint64(0)>>1) {
		return ErrRange
	}
	n, e := d.WriteAt(b, int64(off))
	if e == nil && n != len(b) {
		e = io.ErrShortWrite
	}
	return e
}
