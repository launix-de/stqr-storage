// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// BucketID and RequestID share the protocol's 128-bit representation.
type BucketID = ID
type RequestID = ID

// ObjectID is a native address, used directly without hashing it again.
type ObjectID [32]byte

func (id ObjectID) String() string { return hex.EncodeToString(id[:]) }

// NewRequestID creates the retry identity for one logical mutation.
// Reuse it after an unknown outcome; use a new ID for a new operation.
func NewRequestID() RequestID {
	var id RequestID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}

var (
	ErrNotImplemented = errors.New("client: operation not implemented by server")
	ErrNotFound       = errors.New("client: object not found")
	ErrPrecondition   = errors.New("client: precondition failed")
	ErrFull           = errors.New("client: quota or physical capacity exhausted")
	ErrClosed         = errors.New("client: closed")
	ErrProtocol       = errors.New("client: invalid protocol message")
	ErrWouldBlock     = errors.New("client: send queue full")
	ErrUnauthorized   = errors.New("client: unauthorized")
	ErrRedirect       = errors.New("client: object routing redirect")
	ErrTooLarge       = errors.New("client: use a streaming read for payloads above 16 MiB")
	ErrRange          = errors.New("client: invalid object range or precondition")
	ErrGone           = errors.New("client: object range or bucket runtime no longer valid")
)

// SnapshotProof identifies a position in a live subscription, not object contents.
// A new subscription gets a new ID, even when its cursor starts at the same value.
type SnapshotProof struct {
	Subscription ID
	Cursor       uint64
}

// ObjectInfo describes an atomic read. Proof is local and never sent on the wire.
type ObjectInfo struct {
	Length        uint64
	Proof         SnapshotProof
	PayloadDigest *[32]byte
	Flags         uint32
	RuntimeID     ID
	Compression   uint8
	LayoutVersion uint16
}
type AppendResult struct {
	ObjectInfo
	StartOffset uint64
}

// MutationOptions must be preserved across retries. Nil preconditions are absent;
// pointers allow an explicit expected length of zero.
type MutationOptions struct {
	Accepted bool // opt-in weaker ACK; Sync confirms durability

	RequestID      RequestID
	ExpectedLength *uint64
	CompareOffset  uint64
	Compare        []byte
	Absent         bool
}

// ReadCondition compares application bytes and length under the same read handle
// as the payload transfer. It does not name a historical object state.
type ReadCondition struct {
	Offset uint64
	Bytes  []byte
	Length *uint64
}

type PutOptions = MutationOptions
type DeleteOptions = MutationOptions
type AppendOptions = MutationOptions
type TruncOptions = MutationOptions

type CASScope uint8

const (
	CASRange CASScope = iota
	CASObject
)

// CASRange compares equal-sized byte ranges (at most 1 MiB). CASObject replaces
// the whole object, permits length changes, and compares the complete old bytes.
// Use Absent for creation.
type CASOptions struct {
	MutationOptions
	Scope CASScope
}
