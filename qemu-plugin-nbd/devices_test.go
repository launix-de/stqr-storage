// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"sort"
	"sync"
	"testing"
)

type testObject struct {
	data []byte
}

func TestTrimDeletesWholeStripesAndZeroesEdges(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "trim", Size: 4*StripeSize + 512})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x5a}, int(3*StripeSize))
	if err = c.WriteAt(ctx, m, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err = c.Trim(ctx, m, 512, 3*StripeSize-1024); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Get(ctx, StripeID(m.ID, 1)); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("full stripe retained", err)
	}
	got := make([]byte, len(payload))
	if err = c.ReadAt(ctx, m, got, 0); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, len(payload))
	copy(want[:512], payload[:512])
	copy(want[len(want)-512:], payload[:512])
	if !bytes.Equal(got, want) {
		t.Fatal("trim damaged adjacent bytes or left discarded data")
	}
	if s.rangedPut != 2 || s.put != 3 {
		t.Fatal("trim must zero edges with ranged writes", s.rangedPut, s.put)
	}
	// Entirely sparse and repeated discards must not allocate new stripe objects.
	if err = c.Trim(ctx, m, 0, m.Size); err != nil {
		t.Fatal(err)
	}
	if err = c.Trim(ctx, m, 0, m.Size); err != nil {
		t.Fatal(err)
	}
	if len(s.objects) != 1 {
		t.Fatal("discard recreated sparse stripes", len(s.objects))
	}
	if err = c.WriteAt(ctx, m, []byte{7}, 3*StripeSize); err != nil {
		t.Fatal(err)
	}
	if err = c.Trim(ctx, m, 3*StripeSize+512, 512); err != nil {
		t.Fatal(err)
	}
	if info, err := s.Stat(ctx, StripeID(m.ID, 3)); err != nil || info.Length != 1 {
		t.Fatal("zeroing sparse tail grew stripe", info, err)
	}
	if err = c.Trim(ctx, m, m.Size-512, ^uint64(0)); !errors.Is(err, client.ErrRange) {
		t.Fatal("trim overflow", err)
	}
}

type testStore struct {
	mu                                    sync.Mutex
	objects                               map[client.ObjectID]testObject
	get, rangedGet, put, rangedPut, syncs int
	offsets                               []uint64
}

func newTestStore() *testStore { return &testStore{objects: map[client.ObjectID]testObject{}} }
func (s *testStore) Get(_ context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.get++
	o, ok := s.objects[id]
	if !ok {
		return nil, client.ObjectInfo{}, client.ErrNotFound
	}
	return bytes.Clone(o.data), client.ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (s *testStore) GetRange(_ context.Context, id client.ObjectID, off, n uint64) ([]byte, client.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rangedGet++
	o, ok := s.objects[id]
	if !ok {
		return nil, client.ObjectInfo{}, client.ErrNotFound
	}
	end := min(off+n, uint64(len(o.data)))
	off = min(off, end)
	return bytes.Clone(o.data[off:end]), client.ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (s *testStore) Put(_ context.Context, id client.ObjectID, data []byte, _ client.PutOptions) (client.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.put++
	o := testObject{bytes.Clone(data)}
	s.objects[id] = o
	return client.ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (s *testStore) PutRange(_ context.Context, id client.ObjectID, off uint64, data []byte, _ client.PutOptions) (client.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rangedPut++
	s.offsets = append(s.offsets, off)
	o := s.objects[id]
	if end := off + uint64(len(data)); end > uint64(len(o.data)) {
		o.data = append(o.data, make([]byte, end-uint64(len(o.data)))...)
	}
	copy(o.data[off:], data)
	s.objects[id] = o
	return client.ObjectInfo{Length: uint64(len(o.data))}, nil
}
func (s *testStore) CAS(ctx context.Context, id client.ObjectID, offset uint64, expected, data []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, exists := s.objects[id]
	if opt.Absent {
		if exists {
			return client.ObjectInfo{}, client.ErrPrecondition
		}
	} else if opt.Scope == client.CASObject {
		if !exists || offset != 0 || !bytes.Equal(o.data, expected) {
			return client.ObjectInfo{}, client.ErrPrecondition
		}
	} else {
		if !exists || offset > uint64(len(o.data)) || uint64(len(expected)) > uint64(len(o.data))-offset || !bytes.Equal(o.data[offset:offset+uint64(len(expected))], expected) {
			return client.ObjectInfo{}, client.ErrPrecondition
		}
		if len(expected) != len(data) {
			return client.ObjectInfo{}, client.ErrRange
		}
		copy(o.data[offset:], data)
		s.objects[id] = o
		return client.ObjectInfo{Length: uint64(len(o.data))}, nil
	}
	o = testObject{bytes.Clone(data)}
	s.objects[id] = o
	return client.ObjectInfo{Length: uint64(len(o.data))}, nil
}

func (s *testStore) Stat(ctx context.Context, id client.ObjectID) (client.ObjectInfo, error) {
	_, i, e := s.Get(ctx, id)
	return i, e
}
func (s *testStore) Delete(_ context.Context, id client.ObjectID, opt client.DeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[id]
	if !ok {
		return client.ErrNotFound
	}
	delete(s.objects, id)
	return nil
}
func (s *testStore) ScanObjects(_ context.Context, after *client.ObjectID, limit uint32) ([]client.ObjectEntry, *client.ObjectID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []client.ObjectID
	for id := range s.objects {
		if after == nil || bytes.Compare(id[:], after[:]) > 0 {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	var next *client.ObjectID
	if len(ids) > int(limit) {
		ids = ids[:limit]
		last := ids[len(ids)-1]
		next = &last
	}
	out := make([]client.ObjectEntry, len(ids))
	for i, id := range ids {
		o := s.objects[id]
		out[i] = client.ObjectEntry{ID: id, Info: client.ObjectInfo{Length: uint64(len(o.data))}}
	}
	return out, next, nil
}
func (s *testStore) Sync(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncs++
	return nil
}
func TestStripedDevices(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	a, e := c.Create(ctx, Metadata{Name: "system", Size: 32 << 30})
	if e != nil {
		t.Fatal(e)
	}
	if len(s.objects) != 1 {
		t.Fatal("creating sparse disk must store only metadata")
	}
	b, e := c.Create(ctx, Metadata{Name: "data", Size: 8 << 30})
	if e != nil {
		t.Fatal(e)
	}
	if a.ID == b.ID {
		t.Fatal("device generations collide")
	}
	if _, e = c.Create(ctx, Metadata{Name: "system", Size: 512}); e != client.ErrPrecondition {
		t.Fatal("duplicate", e)
	}
	listed, e := c.List(ctx)
	if e != nil || len(listed) != 2 {
		t.Fatal(listed, e)
	}
	payload := bytes.Repeat([]byte{0x5a}, 4096)
	before := s.get
	if e = c.WriteAt(ctx, a, payload, StripeSize-512); e != nil {
		t.Fatal(e)
	}
	if s.get != before || s.put != 0 || s.rangedPut != 2 || s.offsets[0] != StripeSize-512 || s.offsets[1] != 0 {
		t.Fatal("small write performed RMW or incorrect split", s)
	}
	got := make([]byte, 8192)
	if e = c.ReadAt(ctx, a, got, StripeSize-1024); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(got[512:4608], payload) || !bytes.Equal(got[:512], make([]byte, 512)) || !bytes.Equal(got[4608:], make([]byte, 3584)) {
		t.Fatal("sparse stripe padding or boundary error")
	}
	reads := s.rangedGet
	if e = c.ReadAt(ctx, a, got, StripeSize-1024); e != nil || s.rangedGet != reads+2 {
		t.Fatal("unexpected data cache", e)
	}
	if e = c.ReadAt(ctx, b, got, 0); e != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatal("exports share data", e)
	}
	whole := bytes.Repeat([]byte{9}, int(StripeSize))
	if e = c.WriteAt(ctx, a, whole, StripeSize*4); e != nil {
		t.Fatal(e)
	}
	if s.put != 1 {
		t.Fatal("full stripe must use put")
	}
	if e = c.ReadAt(ctx, a, got, a.Size); e != client.ErrRange {
		t.Fatal("bounds", e)
	}
	release, e := c.claim(a.ID, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.claim(a.ID, false); e != ErrBusy {
		t.Fatal("duplicate writer", e)
	}
	release()
	release, e = c.claim(a.ID, false)
	if e != nil {
		t.Fatal(e)
	}
	release()
}
func TestCollectWithoutSegmentLists(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	m, e := c.Create(ctx, Metadata{Name: "huge", Size: 1 << 50})
	if e != nil {
		t.Fatal(e)
	}
	live := StripeID(m.ID, 123)
	outOfRange := StripeID(m.ID, 1<<50)
	orphan := StripeID(client.NewRequestID(), 0)
	opaque := client.ObjectID{88}
	for _, id := range []client.ObjectID{live, outOfRange, orphan, opaque} {
		s.Put(ctx, id, []byte{1}, client.PutOptions{})
	}
	n, e := c.Collect(ctx, []client.ObjectID{opaque})
	if e != nil || n != 2 || len(s.objects) != 3 {
		t.Fatal(n, e, len(s.objects))
	}
	s.Put(ctx, client.FilesystemRootID(), []byte("legacy"), client.PutOptions{})
	if _, e = c.Collect(ctx, nil); e != ErrLegacyFormat {
		t.Fatal("legacy filesystem protection", e)
	}
}

type writeOnlyStore struct{ *testStore }

func (s writeOnlyStore) Delete(context.Context, client.ObjectID, client.DeleteOptions) error {
	return client.ErrUnauthorized
}
func TestWriteOnlyTrimReleasesPayloadWithoutServerDeviceRules(t *testing.T) {
	ctx := context.Background()
	store := writeOnlyStore{newTestStore()}
	c := NewCatalog(store)
	disk, err := c.Create(ctx, Metadata{Name: "write-only", Size: StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.WriteAt(ctx, disk, bytes.Repeat([]byte("p"), int(StripeSize)), 0); err != nil {
		t.Fatal(err)
	}
	if err = c.Trim(ctx, disk, 0, StripeSize); err != nil {
		t.Fatal(err)
	}
	info, err := store.Stat(ctx, StripeID(disk.ID, 0))
	if err != nil || info.Length != 0 {
		t.Fatal("payload not released", info, err)
	}
	writes := store.put
	if err = c.Trim(ctx, disk, 0, StripeSize); err != nil || store.put != writes {
		t.Fatal("repeated discard rewrote empty stripe", err)
	}
	zeros := make([]byte, 4096)
	if err = c.ReadAt(ctx, disk, zeros, 512); err != nil || !bytes.Equal(zeros, make([]byte, len(zeros))) {
		t.Fatal("trimmed range not zero", err)
	}
}
