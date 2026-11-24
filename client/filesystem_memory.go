// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
)

// MemoryFilesystemStore is an explicitly volatile backend for dry filesystem
// development. It implements local object access, not a network connection.
// All handles sharing this instance share atomic byte comparisons and object data.
// It retains immutable/orphan objects and is not intended for unbounded workloads.
type MemoryFilesystemStore struct {
	mu       sync.RWMutex
	objects  map[ObjectID]fsMemoryObject
	requests map[RequestID]fsMemoryRequest
}
type fsMemoryObject struct {
	data []byte
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
	return append([]byte(nil), o.data...), ObjectInfo{Length: uint64(len(o.data))}, nil
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
	return append([]byte(nil), o.data[offset:offset+length]...), ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (m *MemoryFilesystemStore) CAS(ctx context.Context, id ObjectID, offset uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if opt.Scope != CASObject || offset != 0 || len(replacement) > FSObjectSize {
		return ObjectInfo{}, errors.New("client: unsupported dry filesystem CAS")
	}
	e := Encoder{B: append([]byte(nil), id[:]...)}
	e.Bool(opt.Absent)
	for _, v := range []*uint64{opt.ExpectedLength} {
		e.Bool(v != nil)
		if v != nil {
			e.U64(*v)
		}
	}
	e.Blob(expected)
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
	if (opt.Absent && exists) || (!exists && !opt.Absent) || (opt.ExpectedLength != nil && *opt.ExpectedLength != uint64(len(old.data))) {
		return ObjectInfo{}, ErrPrecondition
	}
	if !opt.Absent && !bytes.Equal(expected, old.data) {
		return ObjectInfo{}, ErrPrecondition
	}
	o := fsMemoryObject{data: append([]byte(nil), replacement...)}
	m.objects[id] = o
	info := ObjectInfo{Length: uint64(len(o.data))}
	if opt.RequestID != (RequestID{}) {
		m.requests[opt.RequestID] = fsMemoryRequest{digest: digest, info: info}
	}
	return info, nil
}

var _ FilesystemStore = (*MemoryFilesystemStore)(nil)

func (m *MemoryFilesystemStore) Stat(ctx context.Context, id ObjectID) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	o, ok := m.objects[id]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (m *MemoryFilesystemStore) PutRange(ctx context.Context, id ObjectID, offset uint64, data []byte, opt PutOptions) (ObjectInfo, error) {
	if offset > FSObjectSize || uint64(len(data)) > FSObjectSize-offset {
		return ObjectInfo{}, ErrRange
	}
	// This helper uses one CAS image but retains the caller's logical request
	// fingerprint through a separate operation namespace, just like native PUT_RANGE.
	e := Encoder{B: append([]byte("range"), id[:]...)}
	e.U64(offset)
	e.Blob(data)
	e.Bool(opt.Absent)
	e.Bool(opt.ExpectedLength != nil)
	if opt.ExpectedLength != nil {
		e.U64(*opt.ExpectedLength)
	}
	e.U64(opt.CompareOffset)
	e.Blob(opt.Compare)
	digest := sha256.Sum256(e.B)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if prior, ok := m.requests[opt.RequestID]; opt.RequestID != (RequestID{}) && ok {
		if prior.digest != digest {
			return ObjectInfo{}, errors.New("client: reused request ID")
		}
		return prior.info, nil
	}
	old, exists := m.objects[id]
	if opt.Absent && exists || opt.ExpectedLength != nil && (!exists || uint64(len(old.data)) != *opt.ExpectedLength) {
		return ObjectInfo{}, ErrPrecondition
	}
	if opt.Compare != nil && (opt.CompareOffset > uint64(len(old.data)) || uint64(len(opt.Compare)) > uint64(len(old.data))-opt.CompareOffset || !bytes.Equal(old.data[opt.CompareOffset:opt.CompareOffset+uint64(len(opt.Compare))], opt.Compare)) {
		return ObjectInfo{}, ErrPrecondition
	}
	body := append([]byte(nil), old.data...)
	end := offset + uint64(len(data))
	if end > uint64(len(body)) {
		body = append(body, make([]byte, end-uint64(len(body)))...)
	}
	copy(body[offset:], data)
	next := fsMemoryObject{data: body}
	m.objects[id] = next
	info := ObjectInfo{Length: uint64(len(body))}
	if opt.RequestID != (RequestID{}) {
		m.requests[opt.RequestID] = fsMemoryRequest{digest: digest, info: info}
	}
	return info, nil
}
