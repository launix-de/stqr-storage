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
// ID is a generation identifier so a recreated name never exposes older stripes.
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

type Catalog struct {
	store   Store
	mu      sync.Mutex
	writers map[client.ID]bool
}

func NewCatalog(s Store) *Catalog { return &Catalog{store: s, writers: map[client.ID]bool{}} }
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
	// Each new name gets its own generation, including callers reusing Metadata.
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
	_, e := c.store.CAS(ctx, MetadataID(m.Name), 0, nil, encodeMetadata(m), client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{Absent: true, RequestID: client.NewRequestID()}})
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
	if readonly {
		return func() {}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writers[id] {
		return nil, ErrBusy
	}
	c.writers[id] = true
	return func() { c.mu.Lock(); delete(c.writers, id); c.mu.Unlock() }, nil
}
func (c *Catalog) ReadAt(ctx context.Context, m Metadata, dst []byte, off uint64) error {
	if off > m.Size || uint64(len(dst)) > m.Size-off {
		return client.ErrRange
	}
	clear(dst)
	for len(dst) > 0 {
		index, at := off/StripeSize, off%StripeSize
		n := min(uint64(len(dst)), StripeSize-at)
		id := StripeID(m.ID, index)
		var data []byte
		var info client.ObjectInfo
		var e error
		if at == 0 && n == StripeSize {
			data, info, e = c.store.Get(ctx, id)
		} else {
			data, info, e = c.store.GetRange(ctx, id, at, n)
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
		dst = dst[n:]
		off += n
	}
	return nil
}
func (c *Catalog) WriteAt(ctx context.Context, m Metadata, data []byte, off uint64) error {
	if off > m.Size || uint64(len(data)) > m.Size-off {
		return client.ErrRange
	}
	for len(data) > 0 {
		at := off % StripeSize
		n := min(uint64(len(data)), StripeSize-at)
		id := StripeID(m.ID, off/StripeSize)
		opt := client.PutOptions{Accepted: true, RequestID: client.NewRequestID()}
		var e error
		if at == 0 && n == StripeSize {
			_, e = c.store.Put(ctx, id, data[:n], opt)
		} else {
			_, e = c.store.PutRange(ctx, id, at, data[:n], opt)
		}
		if e != nil {
			return e
		}
		data = data[n:]
		off += n
	}
	return nil
}
func (c *Catalog) Sync(ctx context.Context) error { return c.store.Sync(ctx) }

// Trim deletes complete 1-MiB stripes and zeroes only the stored portion of
// boundary stripes. Missing objects/tails already read as zero and stay sparse.
// As with WriteAt, the caller uses Sync for FLUSH/FUA durability.
func (c *Catalog) Trim(ctx context.Context, m Metadata, off, length uint64) error {
	if off > m.Size || length > m.Size-off {
		return client.ErrRange
	}
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
		if err == nil {
			if at == 0 && n == StripeSize {
				err = c.store.Delete(ctx, id, client.DeleteOptions{Accepted: true, RequestID: client.NewRequestID()})
				if errors.Is(err, client.ErrNotFound) {
					err = nil
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
				v := entry.Info.Version
				if e := c.store.Delete(ctx, entry.ID, client.DeleteOptions{ExpectedVersion: &v, RequestID: client.NewRequestID()}); e != nil {
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
