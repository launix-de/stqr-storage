// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// v1 data movement has one authoritative primary (data replication is optional
// and not advertised here). Only that primary can initiate its handoff. Generic
// multi-replica coordinator races remain a separate measured design; this plan
// never elects another writer when its source is offline.
type rangePlan struct {
	Attempt        client.ID
	Before, After  client.MapPatch
	Source, Target client.ID
	Volume         volume.ID
	Moving         client.RangeEntry
	Compression    volume.Compression
	Physical       uint64
}

func (p rangePlan) marshal() ([]byte, error) {
	before, err := p.Before.MarshalBinary()
	if err != nil {
		return nil, err
	}
	after, err := p.After.MarshalBinary()
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	schema := uint16(1)
	if p.Attempt != (client.ID{}) {
		schema = 2
	}
	e.U16(schema)
	e.Blob(before)
	e.Blob(after)
	e.ID(p.Source)
	e.ID(p.Target)
	e.ID(client.ID(p.Volume))
	e.B = append(e.B, p.Moving.Start[:]...)
	e.U16(p.Moving.PrefixBits)
	e.U8(uint8(p.Compression))
	e.U64(p.Physical)
	if schema == 2 {
		e.ID(p.Attempt)
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return e.B, nil
}
func decodeRangePlan(b []byte) (rangePlan, error) {
	d := client.Decoder{B: b}
	schema := d.U16()
	before, a := client.DecodeMapPatch(d.Blob())
	after, bErr := client.DecodeMapPatch(d.Blob())
	p := rangePlan{Before: before, After: after, Source: d.ID(), Target: d.ID(), Volume: volume.ID(d.ID())}
	copy(p.Moving.Start[:], d.Take(32))
	p.Moving.PrefixBits = d.U16()
	p.Compression = volume.Compression(d.U8())
	p.Physical = d.U64()
	if schema == 2 {
		p.Attempt = d.ID()
		if p.Attempt == (client.ID{}) {
			return rangePlan{}, client.ErrProtocol
		}
	}
	if (schema != 1 && schema != 2) || a != nil || bErr != nil || d.Done() != nil {
		return rangePlan{}, client.ErrProtocol
	}
	if err := p.validate(); err != nil {
		return rangePlan{}, err
	}
	return p, nil
}
func (p rangePlan) id() [32]byte { wire, _ := p.marshal(); return sha256.Sum256(wire) }
func (p rangePlan) contains(id volume.ObjectID) bool {
	end, toEnd, err := client.RangeEnd(p.Moving.Start, p.Moving.PrefixBits)
	return err == nil && bytes.Compare(id[:], p.Moving.Start[:]) >= 0 && (toEnd || bytes.Compare(id[:], end[:]) < 0)
}
func (p rangePlan) validate() error {
	if p.Before.Validate() != nil || p.After.Validate() != nil || p.Before.Generation != p.After.Generation || p.Before.Revision != p.After.Revision || p.Source == (client.ID{}) || p.Target == (client.ID{}) || p.Volume == (volume.ID{}) || p.Compression > volume.XZ || len(p.Before.Entries) != 1 || p.Before.Bucket != p.After.Bucket || p.Before.Membership != p.After.Membership || p.Before.Begin != p.After.Begin || p.Before.ToEnd != p.After.ToEnd || !p.Before.ToEnd && p.Before.End != p.After.End {
		return client.ErrProtocol
	}
	old := p.Before.Entries[0]
	if len(old.Locations) != 1 || old.Locations[0].Role != 0 || old.Locations[0].Node != p.Source || old.OwnerEpoch == ^uint64(0) || p.Moving.PrefixBits < old.PrefixBits {
		return client.ErrProtocol
	}
	end, toEnd, err := client.RangeEnd(p.Moving.Start, p.Moving.PrefixBits)
	if err != nil || bytes.Compare(p.Moving.Start[:], p.Before.Begin[:]) < 0 || !p.Before.ToEnd && (toEnd || bytes.Compare(end[:], p.Before.End[:]) > 0) {
		return client.ErrProtocol
	}
	moving := false
	reset := old.Runtime != (client.ID{}) && len(p.After.Entries) == 1 && p.Moving.Start == old.Start && p.Moving.PrefixBits == old.PrefixBits && p.After.Entries[0].Runtime != (client.ID{})
	for _, entry := range p.After.Entries {
		if entry.State != old.State || entry.OwnerEpoch != old.OwnerEpoch+1 || entry.PolicyVersion < old.PolicyVersion || (!reset && entry.Runtime != old.Runtime) || len(entry.Locations) != 1 || entry.Locations[0].Role != 0 {
			return client.ErrProtocol
		}
		if entry.Start == p.Moving.Start && entry.PrefixBits == p.Moving.PrefixBits {
			if entry.Locations[0].Node != p.Target || volume.ID(entry.Locations[0].Volume) != p.Volume {
				return client.ErrProtocol
			}
			moving = true
		} else if entry.PolicyVersion != old.PolicyVersion || !reflect.DeepEqual(entry.Locations, old.Locations) {
			return client.ErrProtocol
		}
	}
	if !moving {
		return client.ErrProtocol
	}
	return nil
}

func (p rangePlan) movingRuntime() client.ID {
	for _, entry := range p.After.Entries {
		if entry.Start == p.Moving.Start && entry.PrefixBits == p.Moving.PrefixBits {
			return entry.Runtime
		}
	}
	return client.ID{}
}

func (p rangePlan) ram() bool {
	for _, entry := range p.After.Entries {
		if entry.Start == p.Moving.Start && entry.PrefixBits == p.Moving.PrefixBits {
			return entry.Runtime != (client.ID{}) && entry.Locations[0].Class == uint8(volume.RAM)
		}
	}
	return false
}

func (p rangePlan) movingPolicyVersion() uint64 {
	for _, entry := range p.After.Entries {
		if entry.Start == p.Moving.Start && entry.PrefixBits == p.Moving.PrefixBits {
			return entry.PolicyVersion
		}
	}
	return 0
}

// Split only along the selected path, retaining at most 256 sibling ranges.
// Claiming the complete old group prevents competing imagined child owners.
func planRangeMove(before client.MapPatch, moving client.RangeEntry, target client.ID, volumeID volume.ID, class uint8, codec volume.Compression, physical uint64) (rangePlan, error) {
	if before.Validate() != nil || len(before.Entries) != 1 || len(before.Entries[0].Locations) != 1 {
		return rangePlan{}, client.ErrProtocol
	}
	old := before.Entries[0]
	source := old.Locations[0].Node
	p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: before, Source: source, Target: target, Volume: volumeID, Moving: moving, Compression: codec, Physical: physical}
	p.After.Entries = nil
	current := old
	for current.PrefixBits < moving.PrefixBits {
		left, right, err := client.SplitRange(current)
		if err != nil {
			return rangePlan{}, err
		}
		if bytes.Compare(moving.Start[:], right.Start[:]) < 0 {
			p.After.Entries = append(p.After.Entries, right)
			current = left
		} else {
			p.After.Entries = append(p.After.Entries, left)
			current = right
		}
	}
	if current.Start != moving.Start {
		return rangePlan{}, client.ErrProtocol
	}
	current.Locations = []client.Location{{Node: target, Volume: client.ID(volumeID), Class: class}}
	p.After.Entries = append(p.After.Entries, current)
	for i := range p.After.Entries {
		p.After.Entries[i].OwnerEpoch = old.OwnerEpoch + 1
	}
	sort.Slice(p.After.Entries, func(i, j int) bool {
		return bytes.Compare(p.After.Entries[i].Start[:], p.After.Entries[j].Start[:]) < 0
	})
	return p, p.validate()
}
