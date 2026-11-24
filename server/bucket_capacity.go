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
func (s *Server) bucketQuotaCapacity(id volume.ID, strategy uint8, selection ...BucketPolicy) (bucketQuotaCapacity, error) {
	if strategy > 2 {
		return bucketQuotaCapacity{}, fail(400, "invalid storage strategy")
	}
	var used uint64
	group := ""
	maskVersion := uint64(0)
	if id != (volume.ID{}) {
		b, e := s.Bucket(id)
		if e != nil {
			return bucketQuotaCapacity{}, e
		}
		group = b.Describe().Policy.PlacementGroup
		maskVersion = b.Describe().Policy.PlacementMaskVersion
		var err error
		policy := b.storageDescription().Policy
		if len(selection) > 0 {
			policy.Limits.Accounting = selection[0].Limits.Accounting
		}
		used, err = b.accountedUsageFor(policy)
		if err != nil {
			return bucketQuotaCapacity{}, err
		}
	}
	if len(selection) > 0 {
		group, maskVersion = selection[0].PlacementGroup, selection[0].PlacementMaskVersion
	}
	var node quotaNodeCapacity
	for _, store := range s.placementVolumesAt(group, maskVersion) {
		if store.State() != volume.Active {
			continue
		}
		stats := store.Stats()
		class := int(stats.Identity.Class)
		if class > 2 {
			continue
		}
		node.Free[class] = addCapacity(node.Free[class], addCapacity(stats.AdmissionFreePhysical, stats.GuaranteedRemainingPhysical))
		own := uint64(0)
		if view := s.currentSystem.Load(); view != nil && id != (volume.ID{}) {
			h, err := store.Open(false)
			if err != nil {
				return bucketQuotaCapacity{}, err
			}
			u, err := h.StorageUsage()
			h.Close()
			if err != nil {
				return bucketQuotaCapacity{}, err
			}
			for _, r := range view.manifest.QuotaReserves {
				if r.Bucket == client.ID(id) && r.Node == s.quotaNode && r.Volume == client.ID(stats.Identity.VolumeID) {
					own = addCapacity(own, subtractCapacity(r.Backing, storagePhysical(u, id)))
				}
			}
		}
		node.Reserved[class] = addCapacity(node.Reserved[class], subtractCapacity(stats.GuaranteedRemainingPhysical, own))
	}
	var guarantees [3]uint64
	for _, d := range s.bucketSnapshots() {
		if s.currentSystem.Load() != nil {
			break
		}
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
	group := ""
	schema := uint16(1)
	accounting := "logical"
	if m.Opcode == client.BucketGroupCapacity {
		schema = d.U16()
		if schema != 1 && schema != 2 {
			return nil, 0, client.ErrProtocol
		}
		group = d.ID().String()
	}
	id := volume.ID(d.ID())
	strategy := d.U8()
	profile := d.U8()
	ppm := d.U32()
	if schema == 2 {
		a := d.U8()
		if a > 1 {
			return nil, 0, client.ErrProtocol
		}
		accounting = []string{"logical", "physical"}[a]
	}
	if d.Done() != nil {
		return nil, 0, fail(400, "invalid bucket capacity request")
	}
	if profile != 255 {
		if int(profile) >= len(objectSizeProfiles) {
			return nil, 0, fail(400, "invalid typical object size")
		}
		ppm, _ = volume.EstimatePutOverheadPPM(objectSizeProfiles[profile].Size)
	}
	var selection []BucketPolicy
	if group != "" {
		policy, err := s.policyForGroup(DefaultBucketPolicy(), group, true)
		if err != nil {
			return nil, 0, err
		}
		strategy = uint8(bucketClass(policy))
		policy.Limits.Accounting = accounting
		selection = append(selection, policy)
	}
	c, err := s.administrativeQuotaCapacity(id, strategy, p, selection...)
	if err != nil {
		return nil, 0, err
	}
	if accounting == "physical" {
		c.Maximum = addCapacity(c.Used, c.Available)
	} else {
		c.Maximum = addCapacity(c.Used, logicalCapacity(c.Available, ppm))
	}
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
