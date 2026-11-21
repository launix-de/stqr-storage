// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math"
	"math/big"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type quotaNodeCapacity struct{ Free, Reserved [3]uint64 }
type bucketQuotaCapacity struct {
	Used, Available, ReservedOther, Maximum uint64
	Nodes                                   uint32
}

func addCapacity(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}
func subtractCapacity(a, b uint64) uint64 {
	if b >= a {
		return 0
	}
	return a - b
}

// Logical guarantees need physical backing including their configured metadata
// allowance. This is an estimate until the cluster allocator attributes exact
// physical reservations per bucket; no compression savings are assumed.
func unusedGuarantee(p BucketPolicy, used uint64) uint64 {
	remaining := subtractCapacity(p.Limits.Guaranteed, used)
	if p.Limits.Accounting == "physical" {
		return remaining
	}
	n := new(big.Int).SetUint64(remaining)
	n.Mul(n, new(big.Int).SetUint64(1000000+uint64(p.Limits.MetadataReservePPM)))
	n.Add(n, big.NewInt(999999))
	n.Div(n, big.NewInt(1000000))
	if !n.IsUint64() {
		return math.MaxUint64
	}
	return n.Uint64()
}

// Aggregate physical free space across validated node images. SSD reservations
// stay on SSD; HDD reservations consume HDD first and spill to SSD. RAM has a
// separate pool. Volume reservations are already included in Free and are
// protected once, rather than deducted again alongside the bucket policy.
func quotaCapacity(nodes []quotaNodeCapacity, strategy uint8, used uint64, guarantees [3]uint64) bucketQuotaCapacity {
	var free, reserved [3]uint64
	for _, node := range nodes {
		for i := range free {
			free[i] = addCapacity(free[i], node.Free[i])
			reserved[i] = addCapacity(reserved[i], node.Reserved[i])
		}
	}
	guarantees[1] = addCapacity(guarantees[1], subtractCapacity(guarantees[0], free[0]))
	for i := range reserved {
		reserved[i] = max(reserved[i], guarantees[i])
	}
	indices := []int{int(strategy)}
	if strategy == 0 {
		indices = []int{0, 1}
	}
	out := bucketQuotaCapacity{Used: used, Nodes: uint32(len(nodes))}
	for _, i := range indices {
		out.Available = addCapacity(out.Available, subtractCapacity(free[i], reserved[i]))
		out.ReservedOther = addCapacity(out.ReservedOther, min(free[i], reserved[i]))
	}
	out.Maximum = addCapacity(used, out.Available)
	return out
}

// Called under opMu, like the bucket/object mutation paths. TODO(multinode):
// add every admitted peer's validated capacity image and bucket-usage totals;
// do not omit an unavailable peer or fabricate capacity on its behalf. Current
// guaranteed quotas remain unsupported by admission; this preview also protects
// reservations present in catalog metadata and volume-level reserve counters.
func (s *Server) bucketQuotaCapacity(id volume.ID, strategy uint8) (bucketQuotaCapacity, error) {
	if strategy > 2 {
		return bucketQuotaCapacity{}, fail(400, "invalid storage strategy")
	}
	var used uint64
	if id != (volume.ID{}) {
		b, e := s.Bucket(id)
		if e != nil {
			return bucketQuotaCapacity{}, e
		}
		var err error
		used, err = b.logicalUsage()
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
	}
	var node quotaNodeCapacity
	s.mu.RLock()
	for _, v := range s.volumes {
		if v.config.System || v.store.State() != volume.Active {
			continue
		}
		stats := v.store.Stats()
		class := int(stats.Identity.Class)
		if class > 2 {
			continue
		}
		node.Free[class] = addCapacity(node.Free[class], addCapacity(stats.AdmissionFreePhysical, stats.GuaranteedRemainingPhysical))
		node.Reserved[class] = addCapacity(node.Reserved[class], stats.GuaranteedRemainingPhysical)
	}
	s.mu.RUnlock()
	var guarantees [3]uint64
	for _, d := range s.bucketSnapshots() {
		if d.ID == id.String() || d.ID == systemBucket.String() || d.DeletionComplete {
			continue
		}
		p := d.Policy
		if p.Limits.Guaranteed == 0 {
			continue
		}
		bid, _ := bucketID(d.ID)
		b, e := s.Bucket(bid)
		if e != nil {
			return bucketQuotaCapacity{}, e
		}
		usage, e := b.logicalUsage()
		if e != nil {
			return bucketQuotaCapacity{}, e
		}
		class := int(bucketClass(p))
		guarantees[class] = addCapacity(guarantees[class], unusedGuarantee(p, usage))
	}
	return quotaCapacity([]quotaNodeCapacity{node}, strategy, used, guarantees), nil
}

type objectSizeProfile struct {
	Label string
	Size  uint64
}

var objectSizeProfiles = []objectSizeProfile{
	{"< 1 KB", 512}, {"1–5 KB", 3 << 10}, {"5–20 KB", 12 << 10}, {"20–100 KB", 64 << 10}, {"100 KB–1 MB", 512 << 10}, {"1M (Blockdevices)", 1 << 20}, {"> 1 MB", 4 << 20},
}

func logicalCapacity(physical uint64, ppm uint32) uint64 {
	n := new(big.Int).SetUint64(physical)
	n.Mul(n, big.NewInt(1000000))
	n.Div(n, new(big.Int).SetUint64(1000000+uint64(ppm)))
	return n.Uint64()
}
func (s *Server) executeBucketQuotaCapacity(m client.Message, p principal) ([]byte, int, error) {
	if err := s.requireAdmin(p); err != nil {
		return nil, 0, err
	}
	d := client.Decoder{B: m.Body}
	id := volume.ID(d.ID())
	strategy := d.U8()
	profile := d.U8()
	ppm := d.U32()
	if d.Done() != nil {
		return nil, 0, fail(400, "invalid bucket capacity request")
	}
	if profile != 255 {
		if int(profile) >= len(objectSizeProfiles) {
			return nil, 0, fail(400, "invalid typical object size")
		}
		ppm, _ = volume.EstimatePutOverheadPPM(objectSizeProfiles[profile].Size)
	}
	c, err := s.bucketQuotaCapacity(id, strategy)
	if err != nil {
		return nil, 0, err
	}
	c.Maximum = addCapacity(c.Used, logicalCapacity(c.Available, ppm))
	e := client.Encoder{}
	e.U64(c.Used)
	e.U64(c.Available)
	e.U64(c.ReservedOther)
	e.U64(c.Maximum)
	e.U32(c.Nodes)
	e.U32(ppm)
	e.V(uint64(len(objectSizeProfiles)))
	for key, p := range objectSizeProfiles {
		overhead, _ := volume.EstimatePutOverheadPPM(p.Size)
		e.U8(uint8(key))
		e.Text(p.Label)
		e.U64(p.Size)
		e.U32(overhead)
	}
	return e.B, 200, nil
}
