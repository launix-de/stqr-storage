// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"crypto/sha256"
	"reflect"
	"testing"
)

func statsFixture(n byte) StatsSnapshot {
	s := StatsSnapshot{Schema: 1, Node: ID{n}, BootID: ID{n, 2}, Sequence: 7, CapturedUnixNS: 123, WindowNS: 3e9, MembershipEpoch: 5, Generation: 4, MapRevision: 8, NodeState: 1, NeedRebuild: true, ReasonBits: 3, Coordinator: ID{4}, Round: RoundID{5, 4, ID{6}, 9}, CPUTimeNS: 12, ResidentBytes: 18, NetReadBytes: 20, NetWriteBytes: 21}
	io := IOStats{LogicalReadBytes: 11, LogicalWriteBytes: 12, PhysicalReadBytes: 13, PhysicalWriteBytes: 14, Errors: 15, InFlight: 16, Queued: 17, Flushes: 18, FlushNS: 19}
	for i := range io.Ops {
		io.Ops[i] = uint64(i + 1)
		io.LatencyNS[i] = [3]uint64{uint64(i + 101), uint64(i + 201), uint64(i + 301)}
		io.LatencySamples[i] = uint64(i + 401)
	}
	s.IO = io
	s.Volumes = []VolumeStats{{ID: ID{8}, Path: "/dev/mapper/test", Class: 1, State: 2, CapacityPhysical: 1, AllocatedPhysical: 2, ReclaimablePhysical: 3, PayloadPhysical: 4, MetadataPhysical: 5, ArenaUnusedPhysical: 6, GuaranteedRemainingPhysical: 7, InFlightReservedPhysical: 8, OperatingReservePhysical: 9, AdmissionFreePhysical: 10, Health: 1, IO: io}}
	s.Buckets = []BucketStats{{ID: ID{9}, Accounting: 1, Strategy: 2, LogicalBytes: 1, AttributedPhysical: 2, ObjectCount: 3, Guaranteed: 4, Absolute: 5, AbsoluteUnlimited: true, GuaranteedReservedPhysical: 6, CacheBytes: 7, IO: io}}
	s.Ranges = []RangeStats{{Bucket: ID{9}, Start: [32]byte{128}, PrefixBits: 1, Volume: ID{8}, Role: 1, State: 2, ActualClass: 3, RuntimeID: ID{11}, OwnerEpoch: 1, ObjectCount: 2, LogicalLiveBytes: 3, AllocatedPhysical: 4, MetadataPhysical: 5, ArenaUnusedPhysical: 6, TombstonePhysical: 7, MigrationPinnedPhysical: 8, CacheBytes: 9, CacheHits: 10, CacheMisses: 11, SubscriptionCount: 12, HandoffPending: 13, IO: io}}
	return s
}

func snapshotParts(s StatsSnapshot, chunk int) []StatsPart {
	b := EncodeStats(s)
	count := (len(b) + chunk - 1) / chunk
	parts := make([]StatsPart, count)
	for i := range parts {
		parts[i] = StatsPart{Source: s.Node, Boot: s.BootID, Sequence: s.Sequence, Index: uint32(i), Count: uint32(count), Digest: sha256.Sum256(b), Data: b[i*chunk : min(len(b), (i+1)*chunk)]}
	}
	return parts
}

func TestStatsDecodeAllFieldsAndTruncations(t *testing.T) {
	want := statsFixture(1)
	b := EncodeStats(want)
	got, err := DecodeStats(b)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("snapshot field order", err, got)
	}
	for i := range b {
		if _, err := DecodeStats(b[:i]); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
	if _, err := DecodeStats(append(b, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
}

func TestStatsAssemblerInterleavingDuplicatesAndSupersession(t *testing.T) {
	a := NewStatsAssembler(2)
	x, y := statsFixture(1), statsFixture(2)
	px, py := snapshotParts(x, 100), snapshotParts(y, 100)
	for i := range px {
		for _, p := range []StatsPart{px[len(px)-1-i], py[i]} {
			got, err := a.Add(p)
			if err != nil {
				t.Fatal(err)
			}
			if i == len(px)-1 && (got == nil || got.Node != p.Source || got.Sequence != p.Sequence) {
				t.Fatal("lost interleaved source")
			}
			if repeated, err := a.Add(p); err != nil || repeated != nil {
				t.Fatal("duplicate", err)
			}
		}
	}
	if a.used != 0 {
		t.Fatal("completed snapshot retained payload bytes")
	}
	x.Sequence++
	next := snapshotParts(x, 100)
	if _, err := a.Add(next[0]); err != nil {
		t.Fatal(err)
	}
	if old, err := a.Add(px[0]); err != nil || old != nil || a.sources[x.Node].sequence != x.Sequence {
		t.Fatal("stale fragment replaced current assembly", err)
	}
	wrong := next[0]
	wrong.Data = append([]byte(nil), wrong.Data...)
	wrong.Data[0] ^= 1
	if _, err := a.Add(wrong); err == nil {
		t.Fatal("accepted conflicting duplicate")
	}
	z := snapshotParts(statsFixture(3), 100)
	if _, err := a.Add(z[0]); err == nil {
		t.Fatal("source count budget ignored")
	}
	a.Forget(x.Node)
	if a.used != 0 {
		t.Fatal("Forget leaked bytes")
	}
}

func TestStatsAssemblerBindsDigestAndIdentity(t *testing.T) {
	s := statsFixture(1)
	for _, change := range []func(*StatsPart){
		func(p *StatsPart) { p.Digest[0] ^= 1 },
		func(p *StatsPart) { p.Source = ID{7} },
		func(p *StatsPart) { p.Boot = ID{7} },
		func(p *StatsPart) { p.Sequence++ },
	} {
		p := snapshotParts(s, 1<<20)[0]
		change(&p)
		if _, err := NewStatsAssembler(1).Add(p); err == nil {
			t.Fatal("accepted corrupt or misattributed snapshot")
		}
	}
}

func FuzzStatsDecode(f *testing.F) {
	f.Add(EncodeStats(statsFixture(1)))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := DecodeStats(b)
		if err == nil && !reflect.DeepEqual(EncodeStats(got), b) {
			t.Fatal("noncanonical snapshot accepted")
		}
	})
}
