// SPDX-License-Identifier: GPL-3.0-only
// Package nbd exposes named striped block devices in one authenticated bucket.
package nbd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
)

const StripeSize uint64 = 1 << 20

var ErrLegacyFormat = errors.New("legacy filesystem-backed disk layout; export/migrate with the previous version before using this format")
var ErrBusy = errors.New("device already has a writer")

// Store has no content cache. Each block request goes directly to native object
// reads/writes; callers must hold a block-maintenance lease for mutation.
type Store interface {
	Get(context.Context, client.ObjectID) ([]byte, client.ObjectInfo, error)
	GetRange(context.Context, client.ObjectID, uint64, uint64) ([]byte, client.ObjectInfo, error)
	Put(context.Context, client.ObjectID, []byte, client.PutOptions) (client.ObjectInfo, error)
	PutRange(context.Context, client.ObjectID, uint64, []byte, client.PutOptions) (client.ObjectInfo, error)
	CAS(context.Context, client.ObjectID, uint64, []byte, []byte, client.CASOptions) (client.ObjectInfo, error)
	Delete(context.Context, client.ObjectID, client.DeleteOptions) error
	Stat(context.Context, client.ObjectID) (client.ObjectInfo, error)
	ScanObjects(context.Context, *client.ObjectID, uint32) ([]client.ObjectEntry, *client.ObjectID, error)
	Sync(context.Context) error
}

// Metadata is the only per-device descriptor. No stripe/segment list is stored.
// ID is the disk identity, unchanged by writes or resize. Recreating a deleted
// name creates a new disk identity so it cannot expose the old disk's stripes.
type Metadata struct {
	ID           client.ID `json:"id"`
	Name         string    `json:"name"`
	Size         uint64    `json:"size"`
	Manufacturer string    `json:"manufacturer"`
	Model        string    `json:"model"`
	Serial       string    `json:"serial"`
}

func (m Metadata) validate() error {
	if m.ID == (client.ID{}) || m.Size == 0 || m.Size%512 != 0 || m.Size > math.MaxInt64 || m.Name == "" || len(m.Name) > 255 {
		return client.ErrRange
	}
	for _, s := range []string{m.Name, m.Manufacturer, m.Model, m.Serial} {
		if len(s) > 255 || !utf8.ValidString(s) || strings.ContainsAny(s, "\x00\r\n") {
			return client.ErrProtocol
		}
	}
	return nil
}

// MetadataID reserves the lowest 64-bit prefix for descriptors. Sorted native
// scans can enumerate devices without walking the (uniformly spread) stripes.
func MetadataID(name string) client.ObjectID {
	sum := sha256.Sum256(append([]byte("STQR NBD metadata v1\x00"), []byte(name)...))
	var id client.ObjectID
	copy(id[8:], sum[:24])
	return id
}

// StripeID is a systematic address, not an entry in an index object. Its hashed
// high half distributes placement. The low half contains a device tag and the
// stripe number, allowing bounded-memory orphan collection without a stripe list.
func StripeID(device client.ID, index uint64) client.ObjectID {
	input := append([]byte("STQR NBD stripe v1\x00"), device[:]...)
	input = binary.BigEndian.AppendUint64(input, index)
	sum := sha256.Sum256(input)
	var id client.ObjectID
	copy(id[:16], sum[:16])
	if bytes.Equal(id[:8], make([]byte, 8)) {
		id[0] = 1
	}
	copy(id[16:24], device[:8])
	binary.BigEndian.PutUint64(id[24:], index)
	return id
}
func encodeMetadata(m Metadata) []byte {
	e := client.Encoder{B: []byte("STNBD001")}
	e.U16(1)
	e.U32(uint32(StripeSize))
	e.U64(m.Size)
	e.ID(m.ID)
	e.Text(m.Name)
	e.Text(m.Manufacturer)
	e.Text(m.Model)
	e.Text(m.Serial)
	return e.B
}
func decodeMetadata(raw []byte) (Metadata, error) {
	d := client.Decoder{B: raw}
	if string(d.Take(8)) != "STNBD001" || d.U16() != 1 || d.U32() != uint32(StripeSize) {
		return Metadata{}, client.ErrProtocol
	}
	m := Metadata{Size: d.U64(), ID: d.ID(), Name: d.Text(), Manufacturer: d.Text(), Model: d.Text(), Serial: d.Text()}
	if d.Done() != nil {
		return m, client.ErrProtocol
	}
	return m, m.validate()
}

type blockOperation struct {
	device     client.ID
	begin, end uint64
	write, all bool
	done       chan struct{}
}
type Catalog struct {
	writeMu    sync.Mutex // protects in-memory causal completion dependencies only
	operations map[*blockOperation]struct{}

	store   Store
	mu      sync.Mutex
	writers map[client.ID]bool
	readers map[client.ID]uint64
}

func NewCatalog(s Store) *Catalog {
	return &Catalog{store: s, writers: map[client.ID]bool{}, readers: map[client.ID]uint64{}}
}
func (c *Catalog) CheckFormat(ctx context.Context) error {
	_, e := c.store.Stat(ctx, client.FilesystemRootID())
	if e == nil {
		return ErrLegacyFormat
	}
	if !errors.Is(e, client.ErrNotFound) {
		return e
	}
	return nil
}
func (c *Catalog) Create(ctx context.Context, m Metadata) (Metadata, error) {
	// Each new disk gets its own identity, including callers reusing Metadata.
	m.ID = client.NewRequestID()
	if m.Manufacturer == "" {
		m.Manufacturer = "STQR"
	}
	if m.Model == "" {
		m.Model = "Striped block device"
	}
	if m.Serial == "" {
		m.Serial = m.ID.String()
	}
	if e := m.validate(); e != nil {
		return m, e
	}
	if e := c.CheckFormat(ctx); e != nil {
		return m, e
	}
	_, e := c.store.CAS(ctx, MetadataID(m.Name), 0, nil, encodeMetadata(m), client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{Accepted: true, Absent: true, RequestID: client.NewRequestID()}})
	return m, e
}
func (c *Catalog) Lookup(ctx context.Context, name string) (Metadata, error) {
	raw, _, e := c.store.Get(ctx, MetadataID(name))
	if e != nil {
		return Metadata{}, e
	}
	m, e := decodeMetadata(raw)
	if e == nil && m.Name != name {
		return m, client.ErrProtocol
	}
	return m, e
}

// Resize grows a device without changing its generation or allocating stripes.
// The caller must hold the bucket's writable block-maintenance lease. Existing
// Metadata handles retain their previous bounds; look up the device again to
// access the extension. Shrinking is deliberately rejected.
// A failed Sync has an unknown durable outcome; retrying the same size is safe.
func (c *Catalog) Resize(ctx context.Context, device Metadata, size uint64) (Metadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	raw, _, err := c.store.Get(ctx, MetadataID(device.Name))
	if err != nil {
		return Metadata{}, err
	}
	m, err := decodeMetadata(raw)
	if err != nil {
		return Metadata{}, err
	}
	if m.ID != device.ID || m.Name != device.Name {
		return Metadata{}, client.ErrPrecondition
	}
	if size < m.Size {
		return Metadata{}, client.ErrRange
	}
	oldSize := m.Size
	m.Size = size
	if err = m.validate(); err != nil {
		return Metadata{}, err
	}
	if size != oldSize {
		_, err = c.store.CAS(ctx, MetadataID(m.Name), 0, raw, encodeMetadata(m), client.CASOptions{
			Scope:           client.CASObject,
			MutationOptions: client.MutationOptions{Accepted: true, RequestID: client.NewRequestID()},
		})
		if err != nil {
			return Metadata{}, err
		}
	}
	return m, nil
}
func (c *Catalog) List(ctx context.Context) ([]Metadata, error) {
	var out []Metadata
	var after *client.ObjectID
	for {
		page, next, e := c.store.ScanObjects(ctx, after, 512)
		if e != nil {
			return nil, e
		}
		for _, entry := range page {
			if !bytes.Equal(entry.ID[:8], make([]byte, 8)) {
				return out, nil
			}
			raw, _, e := c.store.Get(ctx, entry.ID)
			if e != nil {
				return nil, e
			}
			m, e := decodeMetadata(raw)
			if e != nil {
				return nil, e
			}
			if MetadataID(m.Name) != entry.ID {
				return nil, client.ErrProtocol
			}
			out = append(out, m)
		}
		if next == nil {
			return out, nil
		}
		after = next
	}
}
func (c *Catalog) claim(id client.ID, readonly bool) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var once sync.Once
	if readonly {
		c.readers[id]++
		return func() {
			once.Do(func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.readers[id] <= 1 {
					delete(c.readers, id)
				} else {
					c.readers[id]--
				}
			})
		}, nil
	}
	if c.writers[id] {
		return nil, ErrBusy
	}
	c.writers[id] = true
	return func() { once.Do(func() { c.mu.Lock(); delete(c.writers, id); c.mu.Unlock() }) }, nil
}
func (c *Catalog) ReadAt(ctx context.Context, m Metadata, dst []byte, off uint64) error {
	if off > m.Size || uint64(len(dst)) > m.Size-off {
		return client.ErrRange
	}
	complete, err := c.beginOperation(ctx, m.ID, off, off+uint64(len(dst)), false, false)
	if err != nil {
		return err
	}
	defer complete()
	clear(dst)
	parallel, ok := c.store.(interface{ ConcurrentObjectRequests() bool })
	if !ok || !parallel.ConcurrentObjectRequests() {
		for len(dst) > 0 {
			at := off % StripeSize
			n := min(uint64(len(dst)), StripeSize-at)
			if err := c.readStripe(ctx, StripeID(m.ID, off/StripeSize), at, dst[:n]); err != nil {
				return err
			}
			dst = dst[n:]
			off += n
		}
		return nil
	}
	batch := newStripeReadScatter(ctx)
	defer batch.cancel()
	for len(dst) > 0 && batch.ctx.Err() == nil {
		at := off % StripeSize
		n := min(uint64(len(dst)), StripeSize-at)
		id, part := StripeID(m.ID, off/StripeSize), dst[:n]
		batch.run(func() error { return c.readStripe(batch.ctx, id, at, part) })
		dst = dst[n:]
		off += n
	}
	batch.work.Wait()
	if batch.err != nil {
		return batch.err
	}
	return batch.ctx.Err()
}
func (c *Catalog) readStripe(ctx context.Context, id client.ObjectID, at uint64, dst []byte) error {
	n := uint64(len(dst))

	var data []byte
	var info client.ObjectInfo
	var e error
	if at == 0 && n == StripeSize {
		data, info, e = c.store.Get(ctx, id)
	} else {
		data, info, e = c.store.GetRange(ctx, id, at, n)
		var status *client.StatusError
		if errors.Is(e, client.ErrRange) || (errors.As(e, &status) && status.Status == 400) {
			// Object ranges are bounded by payload length. Disk holes/tails are
			// interpreted here, never by the storage server. No extra Stat is needed
			// for ordinary reads within a populated stripe.
			info, e = c.store.Stat(ctx, id)
			if e == nil {
				if info.Length > StripeSize {
					return client.ErrProtocol
				}
				if at >= info.Length {
					data = nil
				} else {
					data, info, e = c.store.GetRange(ctx, id, at, min(n, info.Length-at))
				}
			}
		}
	}
	if e != nil && !errors.Is(e, client.ErrNotFound) {
		return e
	}
	if e == nil {
		if info.Length > StripeSize || uint64(len(data)) > n {
			return client.ErrProtocol
		}
		copy(dst[:n], data)
	}
	return nil
}

func (c *Catalog) WriteAt(ctx context.Context, m Metadata, data []byte, off uint64) error {
	if off > m.Size || uint64(len(data)) > m.Size-off {
		return client.ErrRange
	}
	complete, err := c.beginOperation(ctx, m.ID, off, off+uint64(len(data)), true, false)
	if err != nil {
		return err
	}
	defer complete()
	parallel, ok := c.store.(interface{ ConcurrentObjectRequests() bool })
	concurrent := ok && parallel.ConcurrentObjectRequests()
	var calls sync.WaitGroup
	var results []*error
	write := func(id client.ObjectID, at, n uint64, payload []byte) error {
		opt := client.PutOptions{Accepted: true, RequestID: client.NewRequestID()}
		if at == 0 && n == StripeSize {
			_, err := c.store.Put(ctx, id, payload, opt)
			return err
		}
		_, err := c.store.PutRange(ctx, id, at, payload, opt)
		return err
	}
	for len(data) > 0 {
		at := off % StripeSize
		n := min(uint64(len(data)), StripeSize-at)
		id := StripeID(m.ID, off/StripeSize)
		payload := data[:n]
		if concurrent {
			calls.Add(1)
			// Each result slot is allocated before launch; do not capture the slice
			// header while another stripe is appending to it.
			slot := new(error)
			go func() { defer calls.Done(); *slot = write(id, at, n, payload) }()
			results = append(results, slot)
		} else if err := write(id, at, n, payload); err != nil {
			return err
		}
		data = data[n:]
		off += n
	}
	calls.Wait()
	for _, failure := range results {
		if *failure != nil {
			return *failure
		}
	}

	return nil
}

// Sync is the explicit durability boundary. It captures locally admitted work
// and waits only for completion before sending the native owner fences.
func (c *Catalog) Sync(ctx context.Context) error {
	complete, err := c.beginOperation(ctx, client.ID{}, 0, 0, true, true)
	if err != nil {
		return err
	}
	defer complete()
	return c.store.Sync(ctx)
}

// beginOperation records visibility dependencies, never storage barriers. A
// later overlapping operation waits for actual results; disjoint ranges proceed.
// The global form is used only for explicit flush or administrative inventory.
func (c *Catalog) beginOperation(ctx context.Context, device client.ID, begin, end uint64, write, all bool) (func(), error) {
	op := &blockOperation{device: device, begin: begin, end: end, write: write, all: all, done: make(chan struct{})}
	c.writeMu.Lock()
	var predecessors []<-chan struct{}
	for old := range c.operations {
		if all || old.all || device == old.device && (write || old.write) && begin < old.end && old.begin < end {
			predecessors = append(predecessors, old.done)
		}
	}
	if c.operations == nil {
		c.operations = map[*blockOperation]struct{}{}
	}
	c.operations[op] = struct{}{}
	c.writeMu.Unlock()
	var once sync.Once
	complete := func() {
		once.Do(func() { c.writeMu.Lock(); delete(c.operations, op); close(op.done); c.writeMu.Unlock() })
	}
	for _, prior := range predecessors {
		select {
		case <-prior:
		case <-ctx.Done():
			complete()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		complete()
		return nil, err
	}
	return complete, nil
}

// Trim deletes complete 1-MiB stripes and zeroes only the stored portion of
// boundary stripes. Missing objects/tails already read as zero and stay sparse.
// As with WriteAt, the caller uses Sync for FLUSH/FUA durability.
func (c *Catalog) Trim(ctx context.Context, m Metadata, off, length uint64) error {
	if off > m.Size || length > m.Size-off {
		return client.ErrRange
	}
	complete, err := c.beginOperation(ctx, m.ID, off, off+length, true, false)
	if err != nil {
		return err
	}
	defer complete()
	var zeros []byte
	for length > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		at := off % StripeSize
		n := min(length, StripeSize-at)
		id := StripeID(m.ID, off/StripeSize)
		info, err := c.store.Stat(ctx, id)
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			return err
		}
		if err == nil && info.Length > 0 {
			if at == 0 && n == StripeSize {
				err = c.store.Delete(ctx, id, client.DeleteOptions{Accepted: true, RequestID: client.NewRequestID()})
				if errors.Is(err, client.ErrNotFound) {
					err = nil
				} else if errors.Is(err, client.ErrUnauthorized) {
					// WRITE permits replacing stripe payload, not native DELETE. Keep
					// the representation decision here instead of a server device exception.
					_, err = c.store.Put(ctx, id, nil, client.PutOptions{Accepted: true, RequestID: client.NewRequestID()})
				}
			} else if at < info.Length {
				count := min(n, info.Length-at)
				if uint64(len(zeros)) < count {
					zeros = make([]byte, count)
				}
				_, err = c.store.PutRange(ctx, id, at, zeros[:count], client.PutOptions{Accepted: true, RequestID: client.NewRequestID()})
			}
			if err != nil {
				return err
			}
		}
		off += n
		length -= n
	}
	return nil
}

// Collect keeps named device metadata/current stripes plus application-owned
// opaque roots. Acquire maintenance and read the application manifest first.
// Memory is bounded by device metadata and one native scan page, not disk size.
func (c *Catalog) Collect(ctx context.Context, opaque []client.ObjectID) (uint64, error) {
	complete, err := c.beginOperation(ctx, client.ID{}, 0, 0, true, true)
	if err != nil {
		return 0, err
	}
	defer complete()
	// Local exports, including read-only ones, can still address a generation
	// whose descriptor was removed. Keep their stripes until the last export
	// releases it and exclude new claims for the complete inventory/sweep.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.writers) != 0 || len(c.readers) != 0 {
		return 0, ErrBusy
	}
	if e := c.CheckFormat(ctx); e != nil {
		return 0, e
	}
	devices, e := c.List(ctx)
	if e != nil {
		return 0, e
	}
	keep := map[client.ObjectID]bool{}
	tags := map[[8]byte][]Metadata{}
	for _, m := range devices {
		keep[MetadataID(m.Name)] = true
		var tag [8]byte
		copy(tag[:], m.ID[:8])
		tags[tag] = append(tags[tag], m)
	}
	for _, id := range opaque {
		if _, e := c.store.Stat(ctx, id); e != nil {
			return 0, fmt.Errorf("missing opaque root: %w", e)
		}
		keep[id] = true
	}
	var after *client.ObjectID
	var deleted uint64
	for {
		page, next, e := c.store.ScanObjects(ctx, after, 512)
		if e != nil {
			return deleted, e
		}
		for _, entry := range page {
			if keep[entry.ID] {
				continue
			}
			var tag [8]byte
			copy(tag[:], entry.ID[16:24])
			index := binary.BigEndian.Uint64(entry.ID[24:])
			live := false
			for _, m := range tags[tag] {
				if index < (m.Size+StripeSize-1)/StripeSize && StripeID(m.ID, index) == entry.ID {
					live = true
					break
				}
			}
			if !live {
				if e := c.store.Delete(ctx, entry.ID, client.DeleteOptions{Accepted: true, RequestID: client.NewRequestID()}); e != nil {
					return deleted, e
				}
				deleted++
			}
		}
		if next == nil {
			return deleted, nil
		}
		after = next
	}
}
