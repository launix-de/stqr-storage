// SPDX-License-Identifier: GPL-3.0-only
package client

// Wire field order follows architecture specification §7.1. This is not the Go
// memory layout. Unimplemented single-node counters are zero, never estimates.
// PhysicalReadBytes/PhysicalWriteBytes are legacy wire names for volume storage
// ACCESS bytes, including metadata, migration and cached mmap/RAM reads. They
// measure neither OS page faults nor bytes transferred by physical disks. The
// positioned and view read families are mutually exclusive and summed once.
// LogicalReadBytes/LogicalWriteBytes retain delivered object-payload semantics.
type IOStats struct {
	Ops                                        [6]uint64
	LogicalReadBytes, LogicalWriteBytes        uint64
	PhysicalReadBytes, PhysicalWriteBytes      uint64
	Errors, InFlight, Queued, Flushes, FlushNS uint64
	LatencyNS                                  [6][3]uint64
	LatencySamples                             [6]uint64
}
type VolumeStats struct {
	ID                                                                              ID
	Path                                                                            string
	Class, State                                                                    uint8
	CapacityPhysical, AllocatedPhysical, ReclaimablePhysical                        uint64
	PayloadPhysical, MetadataPhysical, ArenaUnusedPhysical                          uint64
	GuaranteedRemainingPhysical, InFlightReservedPhysical, OperatingReservePhysical uint64
	AdmissionFreePhysical                                                           uint64
	Health                                                                          uint8
	IO                                                                              IOStats
}
type RangeStats struct {
	Bucket                                                   ID
	Start                                                    [32]byte
	PrefixBits                                               uint16
	Volume                                                   ID
	Role, State, ActualClass                                 uint8
	RuntimeID                                                ID
	OwnerEpoch, ObjectCount, LogicalLiveBytes                uint64
	AllocatedPhysical, MetadataPhysical, ArenaUnusedPhysical uint64
	TombstonePhysical, MigrationPinnedPhysical               uint64
	CacheBytes, CacheHits, CacheMisses                       uint64
	SubscriptionCount, HandoffPending                        uint64
	IO                                                       IOStats
}
type BucketStats struct {
	ID                                            ID
	Accounting, Strategy                          uint8
	LogicalBytes, AttributedPhysical, ObjectCount uint64
	Guaranteed, Absolute                          uint64
	AbsoluteUnlimited                             bool
	GuaranteedReservedPhysical, CacheBytes        uint64
	IO                                            IOStats
}
type RoundID struct {
	Membership, Generation uint64
	Initiator              ID
	Attempt                uint64
}
type StatsSnapshot struct {
	Schema                                                uint16
	Node, BootID                                          ID
	Sequence, CapturedUnixNS, WindowNS                    uint64
	MembershipEpoch, Generation, MapRevision              uint64
	NodeState                                             uint8
	NeedRebuild                                           bool
	ReasonBits                                            uint64
	Coordinator                                           ID
	Round                                                 RoundID
	CPUTimeNS, ResidentBytes, NetReadBytes, NetWriteBytes uint64
	IO                                                    IOStats
	Volumes                                               []VolumeStats
	Buckets                                               []BucketStats
	Ranges                                                []RangeStats
}

func writeU64(e *Encoder, values ...uint64) {
	for _, v := range values {
		e.U64(v)
	}
}
func encodeIO(e *Encoder, s IOStats) {
	for _, v := range s.Ops {
		e.U64(v)
	}
	writeU64(e, s.LogicalReadBytes, s.LogicalWriteBytes, s.PhysicalReadBytes, s.PhysicalWriteBytes, s.Errors, s.InFlight, s.Queued, s.Flushes, s.FlushNS)
	for _, a := range s.LatencyNS {
		for _, v := range a {
			e.U64(v)
		}
	}
	for _, v := range s.LatencySamples {
		e.U64(v)
	}
}
func EncodeStats(s StatsSnapshot) []byte {
	e := Encoder{}
	e.U16(s.Schema)
	e.ID(s.Node)
	e.ID(s.BootID)
	writeU64(&e, s.Sequence, s.CapturedUnixNS, s.WindowNS, s.MembershipEpoch, s.Generation, s.MapRevision)
	e.U8(s.NodeState)
	e.Bool(s.NeedRebuild)
	e.U64(s.ReasonBits)
	e.ID(s.Coordinator)
	writeU64(&e, s.Round.Membership, s.Round.Generation)
	e.ID(s.Round.Initiator)
	e.U64(s.Round.Attempt)
	writeU64(&e, s.CPUTimeNS, s.ResidentBytes, s.NetReadBytes, s.NetWriteBytes)
	encodeIO(&e, s.IO)
	e.V(uint64(len(s.Volumes)))
	for _, v := range s.Volumes {
		e.ID(v.ID)
		e.Text(v.Path)
		e.U8(v.Class)
		e.U8(v.State)
		writeU64(&e, v.CapacityPhysical, v.AllocatedPhysical, v.ReclaimablePhysical, v.PayloadPhysical, v.MetadataPhysical, v.ArenaUnusedPhysical, v.GuaranteedRemainingPhysical, v.InFlightReservedPhysical, v.OperatingReservePhysical, v.AdmissionFreePhysical)
		e.U8(v.Health)
		encodeIO(&e, v.IO)
	}
	e.V(uint64(len(s.Buckets)))
	for _, v := range s.Buckets {
		e.ID(v.ID)
		e.U8(v.Accounting)
		e.U8(v.Strategy)
		writeU64(&e, v.LogicalBytes, v.AttributedPhysical, v.ObjectCount, v.Guaranteed, v.Absolute)
		e.Bool(v.AbsoluteUnlimited)
		writeU64(&e, v.GuaranteedReservedPhysical, v.CacheBytes)
		encodeIO(&e, v.IO)
	}
	e.V(uint64(len(s.Ranges)))
	for _, v := range s.Ranges {
		e.ID(v.Bucket)
		e.B = append(e.B, v.Start[:]...)
		e.U16(v.PrefixBits)
		e.ID(v.Volume)
		e.U8(v.Role)
		e.U8(v.State)
		e.U8(v.ActualClass)
		e.ID(v.RuntimeID)
		writeU64(&e, v.OwnerEpoch, v.ObjectCount, v.LogicalLiveBytes, v.AllocatedPhysical, v.MetadataPhysical, v.ArenaUnusedPhysical, v.TombstonePhysical, v.MigrationPinnedPhysical, v.CacheBytes, v.CacheHits, v.CacheMisses, v.SubscriptionCount, v.HandoffPending)
		encodeIO(&e, v.IO)
	}
	return e.B
}
