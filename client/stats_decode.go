// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"crypto/sha256"
)

// Stats snapshots can span up to 64 one-MiB messages. Counts are also checked
// against the remaining wire bytes before allocating decoded slices.
const MaxStatsBytes = 64 << 20

func decodeIO(d *Decoder) IOStats {
	var s IOStats
	for i := range s.Ops {
		s.Ops[i] = d.U64()
	}
	for _, p := range []*uint64{&s.LogicalReadBytes, &s.LogicalWriteBytes, &s.PhysicalReadBytes, &s.PhysicalWriteBytes, &s.Errors, &s.InFlight, &s.Queued, &s.Flushes, &s.FlushNS} {
		*p = d.U64()
	}
	for i := range s.LatencyNS {
		for j := range s.LatencyNS[i] {
			s.LatencyNS[i][j] = d.U64()
		}
	}
	for i := range s.LatencySamples {
		s.LatencySamples[i] = d.U64()
	}
	return s
}

func statsCount(d *Decoder, minimumBytes int) int {
	n := d.Count(65536)
	if n > len(d.B)/minimumBytes {
		d.Err = ErrProtocol
		return 0
	}
	return n
}

// DecodeStats is shared by peer telemetry and generation snapshot collection.
// It decodes the exact §7.1 field order used by EncodeStats and the dashboard.
func DecodeStats(b []byte) (StatsSnapshot, error) {
	if len(b) > MaxStatsBytes {
		return StatsSnapshot{}, ErrProtocol
	}
	d := Decoder{B: b}
	s := StatsSnapshot{Schema: d.U16(), Node: d.ID(), BootID: d.ID()}
	for _, p := range []*uint64{&s.Sequence, &s.CapturedUnixNS, &s.WindowNS, &s.MembershipEpoch, &s.Generation, &s.MapRevision} {
		*p = d.U64()
	}
	s.NodeState = d.U8()
	s.NeedRebuild = d.Bool()
	s.ReasonBits = d.U64()
	s.Coordinator = d.ID()
	s.Round = readRound(&d)
	for _, p := range []*uint64{&s.CPUTimeNS, &s.ResidentBytes, &s.NetReadBytes, &s.NetWriteBytes} {
		*p = d.U64()
	}
	s.IO = decodeIO(&d)
	s.Volumes = make([]VolumeStats, statsCount(&d, 412))
	for i := range s.Volumes {
		v := &s.Volumes[i]
		v.ID, v.Path, v.Class, v.State = d.ID(), d.Text(), d.U8(), d.U8()
		for _, p := range []*uint64{&v.CapacityPhysical, &v.AllocatedPhysical, &v.ReclaimablePhysical, &v.PayloadPhysical, &v.MetadataPhysical, &v.ArenaUnusedPhysical, &v.GuaranteedRemainingPhysical, &v.InFlightReservedPhysical, &v.OperatingReservePhysical, &v.AdmissionFreePhysical} {
			*p = d.U64()
		}
		v.Health, v.IO = d.U8(), decodeIO(&d)
		if v.ID == (ID{}) || v.Class > 3 || v.State > 4 || v.Health > 2 {
			d.Err = ErrProtocol
		}
	}
	s.Buckets = make([]BucketStats, statsCount(&d, 387))
	for i := range s.Buckets {
		v := &s.Buckets[i]
		v.ID, v.Accounting, v.Strategy = d.ID(), d.U8(), d.U8()
		for _, p := range []*uint64{&v.LogicalBytes, &v.AttributedPhysical, &v.ObjectCount, &v.Guaranteed, &v.Absolute} {
			*p = d.U64()
		}
		v.AbsoluteUnlimited = d.Bool()
		v.GuaranteedReservedPhysical, v.CacheBytes = d.U64(), d.U64()
		v.IO = decodeIO(&d)
		if v.ID == (ID{}) || v.Accounting > 1 || v.Strategy > 3 {
			d.Err = ErrProtocol
		}
	}
	s.Ranges = make([]RangeStats, statsCount(&d, 501))
	for i := range s.Ranges {
		v := &s.Ranges[i]
		v.Bucket = d.ID()
		copy(v.Start[:], d.Take(32))
		v.PrefixBits, v.Volume = d.U16(), d.ID()
		v.Role, v.State, v.ActualClass, v.RuntimeID = d.U8(), d.U8(), d.U8(), d.ID()
		for _, p := range []*uint64{&v.OwnerEpoch, &v.ObjectCount, &v.LogicalLiveBytes, &v.AllocatedPhysical, &v.MetadataPhysical, &v.ArenaUnusedPhysical, &v.TombstonePhysical, &v.MigrationPinnedPhysical, &v.CacheBytes, &v.CacheHits, &v.CacheMisses, &v.SubscriptionCount, &v.HandoffPending} {
			*p = d.U64()
		}
		v.IO = decodeIO(&d)
		if v.Bucket == (ID{}) || v.Volume == (ID{}) || v.PrefixBits > 256 || v.Role > 4 || v.State > 5 || v.ActualClass > 3 {
			d.Err = ErrProtocol
		}
	}
	if d.Done() != nil || s.Schema != 1 || s.Node == (ID{}) || s.BootID == (ID{}) || s.Sequence == 0 || s.NodeState > 5 {
		return StatsSnapshot{}, ErrProtocol
	}
	return s, nil
}

// StatsAssembler is connection-local and is not safe for concurrent use. Each
// source has one in-progress snapshot. The caller binds allowed sources/boots to
// authenticated peers or membership; telemetry never grants placement authority.
// The global byte budget also bounds memory when many sources interleave.
type StatsAssembler struct {
	maxSources int
	used       int
	sources    map[ID]*statsAssembly
}

type statsAssembly struct {
	boot     ID
	sequence uint64
	digest   [32]byte
	parts    [][]byte
	received int
	bytes    int
	complete bool
}

func NewStatsAssembler(maxSources int) *StatsAssembler {
	return &StatsAssembler{maxSources: maxSources, sources: make(map[ID]*statsAssembly)}
}

// Add accepts duplicates only when byte-identical. Earlier sequences are ignored;
// a new boot requires a fresh assembler (or Forget after verified reconnect).
func (a *StatsAssembler) Add(p StatsPart) (*StatsSnapshot, error) {
	if p.Source == (ID{}) || p.Boot == (ID{}) || p.Sequence == 0 || p.Count == 0 || p.Count > 64 || p.Index >= p.Count || len(p.Data) == 0 || len(p.Data) > 1<<20 {
		return nil, ErrProtocol
	}
	s := a.sources[p.Source]
	if s != nil && s.boot != p.Boot {
		return nil, ErrProtocol
	}
	if s != nil && p.Sequence < s.sequence {
		return nil, nil
	}
	if s == nil || p.Sequence > s.sequence {
		if s == nil && (a.maxSources < 1 || len(a.sources) >= a.maxSources) {
			return nil, ErrProtocol
		}
		if s != nil {
			a.used -= s.bytes
		}
		s = &statsAssembly{boot: p.Boot, sequence: p.Sequence, digest: p.Digest, parts: make([][]byte, p.Count)}
		a.sources[p.Source] = s
	}
	if s.digest != p.Digest || len(s.parts) != int(p.Count) {
		return nil, ErrProtocol
	}
	if s.complete {
		return nil, nil
	}
	if prior := s.parts[p.Index]; prior != nil {
		if !bytes.Equal(prior, p.Data) {
			return nil, ErrProtocol
		}
		return nil, nil
	}
	if a.used > MaxStatsBytes-len(p.Data) {
		return nil, ErrProtocol
	}
	s.parts[p.Index] = bytes.Clone(p.Data)
	s.received++
	s.bytes += len(p.Data)
	a.used += len(p.Data)
	if s.received != len(s.parts) {
		return nil, nil
	}
	joined := bytes.Join(s.parts, nil)
	if sha256.Sum256(joined) != s.digest {
		return nil, ErrProtocol
	}
	snapshot, err := DecodeStats(joined)
	if err != nil || snapshot.Node != p.Source || snapshot.BootID != p.Boot || snapshot.Sequence != p.Sequence {
		return nil, ErrProtocol
	}
	a.used -= s.bytes
	s.bytes = 0
	for i := range s.parts {
		s.parts[i] = nil
	}
	s.complete = true
	return &snapshot, nil
}

func (a *StatsAssembler) Forget(source ID) {
	if s := a.sources[source]; s != nil {
		a.used -= s.bytes
		delete(a.sources, source)
	}
}

// DecodeStatsPush unwraps a STATS_SNAPSHOT payload without trusting its source.
func DecodeStatsPush(body []byte) (ID, StatsPart, error) {
	d := Decoder{B: body}
	sub := d.ID()
	p := StatsPart{Source: d.ID(), Boot: d.ID(), Sequence: d.U64(), Index: d.U32(), Count: d.U32()}
	copy(p.Digest[:], d.Take(32))
	p.Data = d.Blob()
	if d.Done() != nil || sub == (ID{}) {
		return ID{}, StatsPart{}, ErrProtocol
	}
	return sub, p, nil
}
