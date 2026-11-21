// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/launix-de/stqr-storage/client"
)

const placementHeaderBytes = 96
const placementLocationBytes = 40

// Headers and locations share one immutable allocation. Readers retain this
// image through their operation; publication replaces the image atomically.
type placementImage struct {
	Membership, Generation, Revision uint64
	data                             []byte
	count                            int
}

func buildPlacement(patches []client.MapPatch) (*placementImage, error) {
	image := &placementImage{}
	type item struct {
		bucket client.ID
		entry  client.RangeEntry
	}
	var entries []item
	locations := 0
	seen := map[client.ID]bool{}
	for _, p := range patches {
		if err := p.Validate(); err != nil {
			return nil, err
		}
		if p.Begin != (client.ObjectID{}) || !p.ToEnd || seen[p.Bucket] {
			return nil, client.ErrProtocol
		}
		seen[p.Bucket] = true
		if image.Membership == 0 {
			image.Membership = p.Membership
			image.Generation = p.Generation
			image.Revision = p.Revision
		} else if image.Membership != p.Membership || image.Generation != p.Generation || image.Revision != p.Revision {
			return nil, client.ErrProtocol
		}
		for _, entry := range p.Entries {
			entries = append(entries, item{p.Bucket, entry})
			locations += len(entry.Locations)
		}
	}
	size := uint64(len(entries))*placementHeaderBytes + uint64(locations)*placementLocationBytes
	if size > 1<<30 {
		return nil, client.ErrProtocol
	}
	image.count = len(entries)
	image.data = make([]byte, int(size))
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].bucket != entries[j].bucket {
			return bytes.Compare(entries[i].bucket[:], entries[j].bucket[:]) < 0
		}
		return bytes.Compare(entries[i].entry.Start[:], entries[j].entry.Start[:]) < 0
	})
	at := len(entries) * placementHeaderBytes
	for i, item := range entries {
		r := item.entry
		b := image.data[i*placementHeaderBytes : (i+1)*placementHeaderBytes]
		copy(b, item.bucket[:])
		copy(b[16:], r.Start[:])
		binary.LittleEndian.PutUint16(b[48:], r.PrefixBits)
		b[50] = r.State
		binary.LittleEndian.PutUint64(b[56:], r.PolicyVersion)
		binary.LittleEndian.PutUint64(b[64:], r.OwnerEpoch)
		copy(b[72:], r.Runtime[:])
		binary.LittleEndian.PutUint32(b[88:], uint32(at))
		binary.LittleEndian.PutUint32(b[92:], uint32(len(r.Locations)))
		for _, loc := range r.Locations {
			b = image.data[at : at+placementLocationBytes]
			copy(b, loc.Node[:])
			copy(b[16:], loc.Volume[:])
			b[32] = loc.Role
			b[33] = loc.Class
			binary.LittleEndian.PutUint16(b[34:], loc.Fragment)
			binary.LittleEndian.PutUint32(b[36:], loc.Flags)
			at += placementLocationBytes
		}
	}
	return image, nil
}
func (p *placementImage) entry(i int) (client.ID, client.RangeEntry) {
	b := p.data[i*placementHeaderBytes : (i+1)*placementHeaderBytes]
	var bucket client.ID
	copy(bucket[:], b[:16])
	r := client.RangeEntry{PrefixBits: binary.LittleEndian.Uint16(b[48:]), State: b[50], PolicyVersion: binary.LittleEndian.Uint64(b[56:]), OwnerEpoch: binary.LittleEndian.Uint64(b[64:])}
	copy(r.Start[:], b[16:48])
	copy(r.Runtime[:], b[72:88])
	at, count := int(binary.LittleEndian.Uint32(b[88:])), int(binary.LittleEndian.Uint32(b[92:]))
	r.Locations = make([]client.Location, count)
	for i := range r.Locations {
		b = p.data[at : at+placementLocationBytes]
		loc := &r.Locations[i]
		copy(loc.Node[:], b[:16])
		copy(loc.Volume[:], b[16:32])
		loc.Role = b[32]
		loc.Class = b[33]
		loc.Fragment = binary.LittleEndian.Uint16(b[34:])
		loc.Flags = binary.LittleEndian.Uint32(b[36:])
		at += placementLocationBytes
	}
	return bucket, r
}
func (p *placementImage) lookup(bucket client.ID, hash client.ObjectID) (client.RangeEntry, bool) {
	if p == nil {
		return client.RangeEntry{}, false
	}
	i := sort.Search(p.count, func(i int) bool {
		b := p.data[i*placementHeaderBytes:]
		if compare := bytes.Compare(b[:16], bucket[:]); compare != 0 {
			return compare > 0
		}
		return bytes.Compare(b[16:48], hash[:]) > 0
	}) - 1
	if i < 0 {
		return client.RangeEntry{}, false
	}
	bid, r := p.entry(i)
	return r, bid == bucket
}
func (p *placementImage) bucketEntries(bucket client.ID) []client.RangeEntry {
	if p == nil {
		return nil
	}
	start := sort.Search(p.count, func(i int) bool {
		return bytes.Compare(p.data[i*placementHeaderBytes:i*placementHeaderBytes+16], bucket[:]) >= 0
	})
	var out []client.RangeEntry
	for i := start; i < p.count; i++ {
		bid, r := p.entry(i)
		if bid != bucket {
			break
		}
		out = append(out, r)
	}
	return out
}
