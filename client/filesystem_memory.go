// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"sync"
)

// MemoryFilesystemStore is an explicitly volatile backend for dry filesystem
// development. It implements local object access, not a network connection.
// All handles sharing this instance share atomic byte comparisons and object data.
// Ordinary CAS/PUT_RANGE always inspect and update the current bytes. Reference
// state is bounded by open holders and outstanding claims, with no retry history.
type MemoryFilesystemStore struct {
	mu           sync.RWMutex
	objects      map[ObjectID]fsMemoryObject
	references   map[ObjectID]map[ID]bool
	claims       map[ObjectID]ID
	dependencies map[ObjectID]map[ID]bool
}

func (m *MemoryFilesystemStore) PinObject(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if token == (ID{}) {
		return ErrRange
	}
	if m.claims[id] != (ID{}) {
		return ErrWouldBlock
	}
	if m.references == nil {
		m.references = make(map[ObjectID]map[ID]bool)
	}
	if m.references[id] == nil {
		m.references[id] = make(map[ID]bool)
	}
	m.references[id][token] = true
	return nil
}

func (m *MemoryFilesystemStore) UnpinObject(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.references[id], token)
	if len(m.references[id]) == 0 {
		delete(m.references, id)
	}
	return nil
}

func (m *MemoryFilesystemStore) ClaimObjectRetirement(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if token == (ID{}) {
		return ErrRange
	}
	if prior := m.claims[id]; prior != (ID{}) && prior != token || len(m.references[id]) != 0 || len(m.dependencies[id]) != 0 {
		return ErrWouldBlock
	}
	if m.claims == nil {
		m.claims = make(map[ObjectID]ID)
	}
	m.claims[id] = token
	return nil
}

func (m *MemoryFilesystemStore) PinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if token == (ID{}) {
		return ErrRange
	}
	if m.claims[id] != (ID{}) {
		return ErrWouldBlock
	}
	if m.dependencies == nil {
		m.dependencies = map[ObjectID]map[ID]bool{}
	}
	if m.dependencies[id] == nil {
		m.dependencies[id] = map[ID]bool{}
	}
	m.dependencies[id][token] = true
	return nil
}
func (m *MemoryFilesystemStore) UnpinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.dependencies[id], token)
	if len(m.dependencies[id]) == 0 {
		delete(m.dependencies, id)
	}
	return nil
}
func (m *MemoryFilesystemStore) ReadObjectDependencies(ctx context.Context) ([]ObjectID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ObjectID, 0, len(m.dependencies))
	for id := range m.dependencies {
		out = append(out, id)
	}
	return out, nil
}

func (m *MemoryFilesystemStore) ReleaseObjectRetirement(ctx context.Context, id ObjectID, token ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claims[id] == token {
		delete(m.claims, id)
	}
	return nil
}

type fsMemoryObject struct {
	data []byte
}

func NewMemoryFilesystemStore() *MemoryFilesystemStore {
	return &MemoryFilesystemStore{objects: make(map[ObjectID]fsMemoryObject)}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
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
	return info, nil
}

func (m *MemoryFilesystemStore) ObjectReferencesSupported() bool { return true }

// The volatile memory backend has no bucket-COW clone operation.
func (m *MemoryFilesystemStore) CanReclaimDependencies(ctx context.Context) (bool, error) {
	return true, ctx.Err()
}

func (m *MemoryFilesystemStore) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[id]; !ok {
		return ErrNotFound
	}
	delete(m.objects, id)
	return nil
}
