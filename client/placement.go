// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"reflect"
	"sort"
	"sync"
	"unicode/utf8"
)

const MapGetMessage uint16 = 0x010b
const MapPatchMessage uint16 = 0x000c

type Location struct {
	Node, Volume ID
	Role, Class  uint8
	Fragment     uint16
	Flags        uint32
}
type RangeEntry struct {
	Start                     ObjectID
	PrefixBits                uint16
	PolicyVersion, OwnerEpoch uint64
	State                     uint8
	Runtime                   ID
	Locations                 []Location
}
type NodeAddress struct {
	Node      ID
	Addresses []string
}
type PlacementPolicy struct {
	Bucket                                                  ID
	Version                                                 uint64
	Name                                                    string
	State, Strategy, Compression, PlacementMode, Redundancy uint8
	Replicas, ECK, ECM                                      uint16
	CacheEnabled                                            bool
	CacheCopies                                             uint16
	CacheBudget                                             uint64
	Accounting                                              uint8
	Guaranteed                                              uint64
	Unlimited                                               bool
	Absolute                                                uint64
	MetadataReservePPM                                      uint32
}
type MapPatch struct {
	Membership, Generation, Revision uint64
	Bucket                           ID
	Begin                            ObjectID
	ToEnd                            bool
	End                              ObjectID
	Entries                          []RangeEntry
	Addresses                        []NodeAddress
	Policies                         []PlacementPolicy
}

func compareObject(a, b ObjectID) int { return bytes.Compare(a[:], b[:]) }
func RangeEnd(start ObjectID, prefix uint16) (ObjectID, bool, error) {
	if prefix > 256 {
		return ObjectID{}, false, ErrProtocol
	}
	if prefix == 0 {
		if start != (ObjectID{}) {
			return ObjectID{}, false, ErrProtocol
		}
		return ObjectID{}, true, nil
	}
	for bit := int(prefix); bit < 256; bit++ {
		if start[bit/8]&(1<<uint(7-bit%8)) != 0 {
			return ObjectID{}, false, ErrProtocol
		}
	}
	end := start
	bit := 256 - int(prefix)
	index := 31 - bit/8
	carry := uint16(1) << uint(bit%8)
	for index >= 0 && carry > 0 {
		sum := uint16(end[index]) + carry
		end[index] = byte(sum)
		carry = sum >> 8
		index--
	}
	return end, carry != 0, nil
}
func SplitRange(entry RangeEntry) (RangeEntry, RangeEntry, error) {
	if entry.PrefixBits >= 256 {
		return RangeEntry{}, RangeEntry{}, ErrRange
	}
	if _, _, err := RangeEnd(entry.Start, entry.PrefixBits); err != nil {
		return RangeEntry{}, RangeEntry{}, err
	}
	left, right := entry, entry
	left.Locations = append([]Location(nil), entry.Locations...)
	right.Locations = append([]Location(nil), entry.Locations...)
	left.PrefixBits++
	right.PrefixBits++
	bit := int(entry.PrefixBits)
	right.Start[bit/8] |= 1 << uint(7-bit%8)
	return left, right, nil
}
func (p MapPatch) Validate() error {
	if p.Membership == 0 || p.Generation == 0 || p.Revision == 0 || p.Bucket == (ID{}) || len(p.Entries) == 0 || len(p.Entries) > 200000 || len(p.Addresses) > 10000 || len(p.Policies) > 10000 || (!p.ToEnd && compareObject(p.Begin, p.End) >= 0) {
		return ErrProtocol
	}
	nodes := map[ID]bool{}
	for _, node := range p.Addresses {
		if node.Node == (ID{}) || nodes[node.Node] || len(node.Addresses) == 0 || len(node.Addresses) > 64 {
			return ErrProtocol
		}
		nodes[node.Node] = true
		for _, a := range node.Addresses {
			if !validPeerAddress(a) {
				return ErrProtocol
			}
		}
	}
	expected := p.Begin
	toEnd := false
	for _, entry := range p.Entries {
		if toEnd || entry.Start != expected || entry.PolicyVersion == 0 || entry.OwnerEpoch == 0 || entry.State > 5 || len(entry.Locations) > 256 {
			return ErrProtocol
		}
		primary := 0
		seen := map[Location]bool{}
		for _, loc := range entry.Locations {
			if loc.Node == (ID{}) || loc.Role > 4 || loc.Class > 3 || !nodes[loc.Node] || seen[loc] {
				return ErrProtocol
			}
			seen[loc] = true
			if loc.Role == 0 {
				primary++
			}
		}
		if entry.State != 5 && primary != 1 || entry.State == 5 && primary > 1 {
			return ErrProtocol
		}
		var err error
		expected, toEnd, err = RangeEnd(entry.Start, entry.PrefixBits)
		if err != nil {
			return err
		}
	}
	if toEnd != p.ToEnd || !toEnd && expected != p.End {
		return ErrProtocol
	}
	for _, policy := range p.Policies {
		if policy.Bucket != p.Bucket || policy.Version == 0 || len(policy.Name) == 0 || len(policy.Name) > 255 || !utf8.ValidString(policy.Name) || policy.State > 2 || policy.Strategy > 3 || policy.Compression > 3 || policy.PlacementMode > 1 || policy.Redundancy > 2 || policy.Accounting > 1 {
			return ErrProtocol
		}
	}
	return nil
}
func writePlacementPolicy(e *Encoder, p PlacementPolicy) {
	e.ID(p.Bucket)
	e.U64(p.Version)
	e.Text(p.Name)
	for _, v := range []uint8{p.State, p.Strategy, p.Compression, p.PlacementMode, p.Redundancy} {
		e.U8(v)
	}
	e.U16(p.Replicas)
	e.U16(p.ECK)
	e.U16(p.ECM)
	e.Bool(p.CacheEnabled)
	e.U16(p.CacheCopies)
	e.U64(p.CacheBudget)
	e.U8(p.Accounting)
	e.U64(p.Guaranteed)
	e.Bool(p.Unlimited)
	e.U64(p.Absolute)
	e.U32(p.MetadataReservePPM)
}
func readPlacementPolicy(d *Decoder) PlacementPolicy {
	return PlacementPolicy{Bucket: d.ID(), Version: d.U64(), Name: d.Text(), State: d.U8(), Strategy: d.U8(), Compression: d.U8(), PlacementMode: d.U8(), Redundancy: d.U8(), Replicas: d.U16(), ECK: d.U16(), ECM: d.U16(), CacheEnabled: d.Bool(), CacheCopies: d.U16(), CacheBudget: d.U64(), Accounting: d.U8(), Guaranteed: d.U64(), Unlimited: d.Bool(), Absolute: d.U64(), MetadataReservePPM: d.U32()}
}
func (p MapPatch) MarshalBinary() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	e := Encoder{}
	e.U64(p.Membership)
	e.U64(p.Generation)
	e.U64(p.Revision)
	e.ID(p.Bucket)
	e.B = append(e.B, p.Begin[:]...)
	e.Bool(p.ToEnd)
	e.B = append(e.B, p.End[:]...)
	e.V(uint64(len(p.Entries)))
	for _, r := range p.Entries {
		e.B = append(e.B, r.Start[:]...)
		e.U16(r.PrefixBits)
		e.U64(r.PolicyVersion)
		e.U64(r.OwnerEpoch)
		e.U8(r.State)
		e.ID(r.Runtime)
		e.V(uint64(len(r.Locations)))
		for _, l := range r.Locations {
			e.ID(l.Node)
			e.ID(l.Volume)
			e.U8(l.Role)
			e.U8(l.Class)
			e.U16(l.Fragment)
			e.U32(l.Flags)
		}
		if len(e.B) > MaxMessage-20 {
			return nil, ErrProtocol
		}
	}
	e.V(uint64(len(p.Addresses)))
	for _, node := range p.Addresses {
		e.ID(node.Node)
		if err := writeAddresses(&e, node.Addresses); err != nil {
			return nil, err
		}
	}
	e.V(uint64(len(p.Policies)))
	for _, policy := range p.Policies {
		writePlacementPolicy(&e, policy)
	}
	if len(e.B) > MaxMessage-20 {
		return nil, ErrProtocol
	}
	return e.B, nil
}
func DecodeMapPatch(body []byte) (MapPatch, error) {
	if len(body) > MaxMessage-20 {
		return MapPatch{}, ErrProtocol
	}
	d := Decoder{B: body}
	p := MapPatch{Membership: d.U64(), Generation: d.U64(), Revision: d.U64(), Bucket: d.ID()}
	copy(p.Begin[:], d.Take(32))
	p.ToEnd = d.Bool()
	copy(p.End[:], d.Take(32))
	count := d.Count(200000)
	if d.Err != nil || count > len(d.B)/68 {
		return p, ErrProtocol
	}
	p.Entries = make([]RangeEntry, count)
	for i := range p.Entries {
		r := &p.Entries[i]
		copy(r.Start[:], d.Take(32))
		r.PrefixBits = d.U16()
		r.PolicyVersion = d.U64()
		r.OwnerEpoch = d.U64()
		r.State = d.U8()
		r.Runtime = d.ID()
		n := d.Count(256)
		if d.Err != nil || n > len(d.B)/40 {
			return p, ErrProtocol
		}
		r.Locations = make([]Location, n)
		for j := range r.Locations {
			r.Locations[j] = Location{Node: d.ID(), Volume: d.ID(), Role: d.U8(), Class: d.U8(), Fragment: d.U16(), Flags: d.U32()}
		}
	}
	count = d.Count(10000)
	if d.Err != nil || count > len(d.B)/17 {
		return p, ErrProtocol
	}
	p.Addresses = make([]NodeAddress, count)
	for i := range p.Addresses {
		p.Addresses[i] = NodeAddress{d.ID(), readAddresses(&d)}
	}
	count = d.Count(10000)
	if d.Err != nil || count > len(d.B)/69 {
		return p, ErrProtocol
	}
	p.Policies = make([]PlacementPolicy, count)
	for i := range p.Policies {
		p.Policies[i] = readPlacementPolicy(&d)
	}
	if d.Done() != nil {
		return p, ErrProtocol
	}
	return p, p.Validate()
}

// A segment has explicit learned bounds. Retaining half of an old segment never
// makes its last entry accidentally authoritative through the end of the hashspace.
type mapSegment struct {
	begin, end ObjectID
	toEnd      bool
	patch      MapPatch
}
type PartialMap struct {
	mu         sync.RWMutex
	membership uint64
	segments   map[ID][]mapSegment
}

func beforeEnd(value, end ObjectID, toEnd bool) bool { return toEnd || compareObject(value, end) < 0 }
func segmentsOverlap(a, b mapSegment) bool {
	return beforeEnd(a.begin, b.end, b.toEnd) && beforeEnd(b.begin, a.end, a.toEnd)
}
func cutSegment(s, cut mapSegment) []mapSegment {
	if !segmentsOverlap(s, cut) {
		return []mapSegment{s}
	}
	var out []mapSegment
	if compareObject(s.begin, cut.begin) < 0 {
		left := s
		left.end = cut.begin
		left.toEnd = false
		out = append(out, left)
	}
	if !cut.toEnd && beforeEnd(cut.end, s.end, s.toEnd) {
		right := s
		right.begin = cut.end
		out = append(out, right)
	}
	return out
}
func conflictingSegments(a, b mapSegment) bool {
	for _, x := range a.patch.Entries {
		xe, xt, _ := RangeEnd(x.Start, x.PrefixBits)
		for _, y := range b.patch.Entries {
			ye, yt, _ := RangeEnd(y.Start, y.PrefixBits)
			if beforeEnd(x.Start, ye, yt) && beforeEnd(y.Start, xe, xt) && !reflect.DeepEqual(x, y) {
				return true
			}
		}
	}
	return false
}
func (m *PartialMap) Apply(p MapPatch) (bool, error) {
	if err := p.Validate(); err != nil {
		return false, err
	}
	// Detach caller-owned slices before publishing the immutable learned segment.
	body, err := p.MarshalBinary()
	if err != nil {
		return false, err
	}
	p, err = DecodeMapPatch(body)
	if err != nil {
		return false, err
	}
	incoming := mapSegment{p.Begin, p.End, p.ToEnd, p}
	m.mu.Lock()
	defer m.mu.Unlock()
	if p.Membership < m.membership {
		return false, nil
	}
	if p.Membership > m.membership {
		m.segments = map[ID][]mapSegment{}
		m.membership = p.Membership
	}
	if m.segments == nil {
		m.segments = map[ID][]mapSegment{}
	}
	old := m.segments[p.Bucket]
	conflict := false
	for _, s := range old {
		if !segmentsOverlap(s, incoming) {
			continue
		}
		if s.patch.Revision > p.Revision {
			return false, nil
		}
		if s.patch.Revision == p.Revision && conflictingSegments(s, incoming) {
			conflict = true
		}
	}
	var next []mapSegment
	for _, s := range old {
		next = append(next, cutSegment(s, incoming)...)
	}
	if !conflict {
		next = append(next, incoming)
	}
	sort.Slice(next, func(i, j int) bool { return compareObject(next[i].begin, next[j].begin) < 0 })
	m.segments[p.Bucket] = next
	if conflict {
		return false, ErrProtocol
	}
	return true, nil
}
func (m *PartialMap) Lookup(bucket ID, object ObjectID) (RangeEntry, uint64, uint64, []string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	segments := m.segments[bucket]
	i := sort.Search(len(segments), func(i int) bool { return compareObject(segments[i].begin, object) > 0 }) - 1
	if i < 0 || !beforeEnd(object, segments[i].end, segments[i].toEnd) {
		return RangeEntry{}, 0, 0, nil, false
	}
	p := segments[i].patch
	j := sort.Search(len(p.Entries), func(i int) bool { return compareObject(p.Entries[i].Start, object) > 0 }) - 1
	if j < 0 {
		return RangeEntry{}, 0, 0, nil, false
	}
	entry := p.Entries[j]
	entry.Locations = append([]Location(nil), entry.Locations...)
	var addresses []string
	for _, loc := range entry.Locations {
		if loc.Role != 0 && loc.Role != 1 {
			continue
		}
		for _, node := range p.Addresses {
			if node.Node == loc.Node {
				addresses = append(addresses, node.Addresses...)
			}
		}
	}
	return entry, p.Revision, p.Membership, addresses, true
}
