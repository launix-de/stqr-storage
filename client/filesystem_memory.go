// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
)

// MemoryFilesystemStore is an explicitly volatile backend for dry filesystem
// development. It implements just the Get/CAS subset, not a network connection.
// All handles sharing this instance share atomic version checks and object data.
// It retains immutable/orphan objects and is not intended for unbounded workloads.
type MemoryFilesystemStore struct {
	mu       sync.RWMutex
	objects  map[ObjectID]fsMemoryObject
	requests map[RequestID]fsMemoryRequest
}
type fsMemoryObject struct {
	data    []byte
	version uint64
}
type fsMemoryRequest struct {
	digest [32]byte
	info   ObjectInfo
}

func NewMemoryFilesystemStore() *MemoryFilesystemStore {
	return &MemoryFilesystemStore{objects: make(map[ObjectID]fsMemoryObject), requests: make(map[RequestID]fsMemoryRequest)}
}
func (m *MemoryFilesystemStore) Get(ctx context.Context, id ObjectID) ([]byte, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[id]
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}
	return append([]byte(nil), o.data...), ObjectInfo{Version: o.version, Length: uint64(len(o.data))}, nil
}

// GetRange is the optional ranged-read fast path for file data objects.
func (m *MemoryFilesystemStore) GetRange(ctx context.Context, id ObjectID, offset, length uint64) ([]byte, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[id]
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}
	if offset > uint64(len(o.data)) || length > uint64(len(o.data))-offset {
		return nil, ObjectInfo{}, ErrRange
	}
	return append([]byte(nil), o.data[offset:offset+length]...), ObjectInfo{Version: o.version, Length: uint64(len(o.data))}, nil
}
func (m *MemoryFilesystemStore) CAS(ctx context.Context, id ObjectID, offset uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if opt.Scope != CASObject || offset != 0 || len(expected) != 0 || len(replacement) > FSObjectSize || (opt.Absent && opt.ExpectedVersion != nil) {
		return ObjectInfo{}, errors.New("client: unsupported dry filesystem CAS")
	}
	e := Encoder{B: append([]byte(nil), id[:]...)}
	e.Bool(opt.Absent)
	for _, v := range []*uint64{opt.ExpectedVersion, opt.ExpectedLength} {
		e.Bool(v != nil)
		if v != nil {
			e.U64(*v)
		}
	}
	e.Blob(replacement)
	digest := sha256.Sum256(e.B)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if opt.RequestID != (RequestID{}) {
		if r, ok := m.requests[opt.RequestID]; ok {
			if r.digest != digest {
				return ObjectInfo{}, errors.New("client: request ID reused")
			}
			return r.info, nil
		}
	}
	old, exists := m.objects[id]
	if (opt.Absent && exists) || (!exists && !opt.Absent) || (opt.ExpectedVersion != nil && *opt.ExpectedVersion != old.version) || (opt.ExpectedLength != nil && *opt.ExpectedLength != uint64(len(old.data))) {
		return ObjectInfo{}, ErrPrecondition
	}
	if !opt.Absent && opt.ExpectedVersion == nil {
		return ObjectInfo{}, ErrPrecondition
	}
	o := fsMemoryObject{data: append([]byte(nil), replacement...), version: old.version + 1}
	m.objects[id] = o
	info := ObjectInfo{Version: o.version, Length: uint64(len(o.data))}
	if opt.RequestID != (RequestID{}) {
		m.requests[opt.RequestID] = fsMemoryRequest{digest: digest, info: info}
	}
	return info, nil
}

var _ FilesystemStore = (*MemoryFilesystemStore)(nil)
