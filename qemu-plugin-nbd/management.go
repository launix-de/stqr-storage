// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"

	"github.com/launix-de/stqr-storage/client"
)

// ManagementRequest uses the native codec; sizes never pass through JSON floats.
// Action 1 lists descriptors, action 2 conditionally grows one generation.
type ManagementRequest struct {
	Action         uint8
	After, Query   string
	Limit          uint32
	Name           string
	Device         client.ID
	Expected, Size uint64
}

func DecodeManagementRequest(body []byte) (ManagementRequest, error) {
	d := client.Decoder{B: body}
	version := d.U16()
	r := ManagementRequest{Action: d.U8()}
	switch r.Action {
	case 1:
		r.After, r.Limit, r.Query = d.Text(), d.U32(), d.Text()
		if r.Limit == 0 || r.Limit > 100 || len(r.Query) > 255 {
			return r, client.ErrRange
		}
		if r.After != "" {
			if raw, err := hex.DecodeString(r.After); err != nil || len(raw) != 32 {
				return r, client.ErrProtocol
			}
		}
	case 2:
		r.Name, r.Device, r.Expected, r.Size = d.Text(), d.ID(), d.U64(), d.U64()
		if err := (Metadata{ID: r.Device, Name: r.Name, Size: r.Size}).validate(); err != nil {
			return r, err
		}
		if r.Expected == 0 || r.Expected%512 != 0 || r.Size < r.Expected {
			return r, client.ErrRange
		}
	default:
		return r, client.ErrProtocol
	}
	if version != 1 || d.Done() != nil || len(body) > 2048 {
		return r, client.ErrProtocol
	}
	return r, nil
}

func (r ManagementRequest) MarshalBinary() []byte {
	e := client.Encoder{}
	e.U16(1)
	e.U8(r.Action)
	if r.Action == 1 {
		e.Text(r.After)
		e.U32(r.Limit)
		e.Text(r.Query)
	} else {
		e.Text(r.Name)
		e.ID(r.Device)
		e.U64(r.Expected)
		e.U64(r.Size)
	}
	return e.B
}

// ListPage bounds both scanning and returned descriptors. A filtered page may
// be empty and still carry a cursor. Non-descriptor stripes are never fetched.
func (c *Catalog) ListPage(ctx context.Context, after string, limit uint32, query string) ([]Metadata, string, error) {
	if limit == 0 || limit > 100 || len(query) > 255 {
		return nil, "", client.ErrRange
	}
	var cursor *client.ObjectID
	if after != "" {
		raw, err := hex.DecodeString(after)
		if err != nil || len(raw) != 32 {
			return nil, "", client.ErrProtocol
		}
		var id client.ObjectID
		copy(id[:], raw)
		cursor = &id
	}
	page, next, err := c.store.ScanObjects(ctx, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	items := make([]Metadata, 0, len(page))
	for _, entry := range page {
		if !bytes.Equal(entry.ID[:8], make([]byte, 8)) {
			return items, "", nil
		}
		raw, _, err := c.store.Get(ctx, entry.ID)
		if err != nil {
			return nil, "", err
		}
		m, err := decodeMetadata(raw)
		if err != nil || MetadataID(m.Name) != entry.ID {
			return nil, "", client.ErrProtocol
		}
		if strings.Contains(strings.ToLower(m.Name), strings.ToLower(query)) {
			items = append(items, m)
		}
	}
	if next != nil {
		return items, hex.EncodeToString(next[:]), nil
	}
	return items, "", nil
}

func DecodeManagementResult(body []byte) ([]Metadata, string, error) {
	d := client.Decoder{B: body}
	version, count := d.U16(), d.U32()
	if version != 1 || count > 100 {
		return nil, "", client.ErrProtocol
	}
	items := make([]Metadata, 0, count)
	for i := uint32(0); i < count; i++ {
		m, err := decodeMetadata(d.Blob())
		if err != nil {
			return nil, "", err
		}
		items = append(items, m)
	}
	next := d.Text()
	if d.Done() != nil {
		return nil, "", client.ErrProtocol
	}
	if next != "" {
		if raw, err := hex.DecodeString(next); err != nil || len(raw) != 32 {
			return nil, "", client.ErrProtocol
		}
	}
	return items, next, nil
}

func (c *Catalog) Manage(ctx context.Context, body []byte, readonly bool) ([]byte, error) {
	r, err := DecodeManagementRequest(body)
	if err != nil {
		return nil, err
	}
	var items []Metadata
	next := ""
	if r.Action == 1 {
		items, next, err = c.ListPage(ctx, r.After, r.Limit, r.Query)
	} else {
		if readonly {
			return nil, &client.StatusError{Status: 403, Message: "readonly exporter cannot resize devices"}
		}
		var m Metadata
		m, err = c.Lookup(ctx, r.Name)
		if err == nil && (m.ID != r.Device || m.Size != r.Expected && m.Size != r.Size) {
			err = client.ErrPrecondition
		}
		if err == nil {
			m, err = c.Resize(ctx, m, r.Size)
		}
		items = []Metadata{m}
	}
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.U16(1)
	e.U32(uint32(len(items)))
	for _, m := range items {
		e.Blob(encodeMetadata(m))
	}
	e.Text(next)
	return e.B, nil
}
