// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerQuotaPhysical uint16 = 0x1072

type quotaPhysicalReservation struct {
	Bucket, Node, Volume client.ID
	Backing              uint64
}
type quotaPhysicalVolume struct {
	ID     client.ID
	Class  volume.Class
	Free   uint64
	Active bool
	Usage  []volume.BucketStorage
}
type quotaPhysicalSnapshot struct {
	Node    client.ID
	Volumes []quotaPhysicalVolume
}

func (b *Bucket) physicalUsage() (uint64, error) {
	var total uint64
	for _, v := range b.server.quotaVolumes() {
		if err := b.server.refreshQuotaOwnership(v); err != nil {
			return 0, err
		}
		h, err := v.Open(false)
		if err != nil {
			return 0, err
		}
		usage, err := h.OwnedStorageUsage()
		h.Close()
		if err != nil {
			return 0, err
		}
		total = addCapacity(total, storagePhysical(usage, b.ID()))
	}
	return total, nil
}
func (b *Bucket) accountedUsage() (uint64, error) {
	return b.accountedUsageFor(quotaPolicy(b.storageDescription()))
}
func (b *Bucket) accountedUsageFor(p BucketPolicy) (uint64, error) {
	if p.Limits.Accounting == "physical" {
		return b.physicalUsage()
	}
	return b.logicalUsage()
}
func storagePhysical(usage []volume.BucketStorage, id volume.ID) uint64 {
	for _, u := range usage {
		if u.Bucket == id {
			return u.Physical
		}
	}
	return 0
}
func guaranteedPhysical(p BucketPolicy) uint64 { return unusedGuarantee(p, 0) }

func (s *Server) quotaPhysicalSnapshot(body []byte, actor client.ID) ([]byte, error) {
	var claim controlClaim
	if err := decodeControlRecord(body, &claim); err != nil || claim.Round.Initiator != actor {
		return nil, errControlConflict
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	l := s.control.ledger
	l.mu.Lock()
	valid := l.slot != nil && equalControlClaim(l.slot.Claim, claim)
	l.mu.Unlock()
	if !valid {
		return nil, errControlConflict
	}
	snap := quotaPhysicalSnapshot{Node: l.node}
	s.mu.RLock()
	stores := make([]*managedVolume, 0, len(s.volumes))
	for _, m := range s.volumes {
		stores = append(stores, m)
	}
	s.mu.RUnlock()
	for _, m := range stores {
		stats := m.store.Stats()
		h, err := m.store.Open(false)
		if err != nil {
			return nil, err
		}
		usage, err := h.StorageUsage()
		h.Close()
		if err != nil {
			return nil, err
		}
		snap.Volumes = append(snap.Volumes, quotaPhysicalVolume{ID: client.ID(stats.Identity.VolumeID), Class: stats.Identity.Class, Active: !m.config.RecoveryOnly && !s.cfg.Node.Draining && !s.cfg.Node.Paused && m.config.State == "active" && m.store.State() == volume.Active, Free: addCapacity(stats.AdmissionFreePhysical, stats.GuaranteedRemainingPhysical), Usage: usage})
	}
	sort.Slice(snap.Volumes, func(i, j int) bool { return snap.Volumes[i].ID.String() < snap.Volumes[j].ID.String() })
	return json.Marshal(snap)
}

func (c *clusterController) capturePhysicalReservations(ctx context.Context, claim controlClaim, membership client.Membership, descriptions []BucketDescription, source ...*Server) ([]quotaPhysicalReservation, error) {
	var buckets []BucketDescription
	for _, d := range descriptions {
		if !d.DeletionComplete && d.ID != systemBucket.String() && quotaPolicy(d).Limits.Guaranteed != 0 {
			buckets = append(buckets, d)
		}
	}
	if len(buckets) == 0 {
		return nil, nil
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].ID < buckets[j].ID })
	raw, err := json.Marshal(claim)
	if err != nil {
		return nil, err
	}
	type device struct {
		node client.ID
		v    quotaPhysicalVolume
	}
	var devices []device
	for _, node := range claim.Required {
		body, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerQuotaPhysical, raw)
		if err != nil {
			return nil, err
		}
		var snap quotaPhysicalSnapshot
		if decodeControlRecord(body, &snap) != nil || snap.Node != node {
			return nil, client.ErrProtocol
		}
		if !hasControlNode(membershipIDs(membership), node) {
			continue
		}
		for _, v := range snap.Volumes {
			if v.Active {
				devices = append(devices, device{node, v})
			}
		}
	}
	groups := c.server.placementGroups.Load()
	if len(source) > 0 {
		groups = source[0].placementGroups.Load()
	}
	// Existing bucket-owned allocation covers its promise first. The remaining
	// bipartite max-flow handles overlapping PG masks and HDD->SSD fallback; a
	// greedy sum can incorrectly approve incompatible promises or reject a fit.
	g := newQuotaFlow(2 + len(buckets) + len(devices))
	sink := len(g.edges) - 1
	var out []quotaPhysicalReservation
	for j, d := range devices {
		g.add(1+len(buckets)+j, sink, d.v.Free)
	}
	for i, d := range buckets {
		p := quotaPolicy(d)
		remaining := guaranteedPhysical(p)
		bid, _ := bucketID(d.ID)
		for j, dev := range devices {
			if !bucketClass(p).Allows(dev.v.Class) || !groups.allowsVersion(p.PlacementGroup, p.PlacementMaskVersion, dev.node.String(), dev.v.ID.String()) {
				continue
			}
			covered := min(remaining, storagePhysical(dev.v.Usage, bid))
			remaining -= covered
			if covered != 0 {
				out = append(out, quotaPhysicalReservation{Bucket: client.ID(bid), Node: dev.node, Volume: dev.v.ID, Backing: covered})
			}
			g.add(1+i, 1+len(buckets)+j, ^uint64(0))
		}
		g.add(0, 1+i, remaining)
	}
	g.run(0, sink)
	for i, d := range buckets {
		bid, _ := bucketID(d.ID)
		for _, e := range g.edges[0] {
			if e.to == 1+i && e.cap != 0 {
				return nil, volume.ErrFull
			}
		}
		for _, e := range g.edges[1+i] {
			if e.to < 1+len(buckets) || e.to >= sink {
				continue
			}
			n := g.edges[e.to][e.rev].cap
			if n == 0 {
				continue
			}
			dev := devices[e.to-1-len(buckets)]
			out = append(out, quotaPhysicalReservation{Bucket: client.ID(bid), Node: dev.node, Volume: dev.v.ID, Backing: n})
		}
	}
	// Combine covered bytes and new reservations for the same stable identity.
	sortPhysicalReservations(out)
	merged := out[:0]
	for _, r := range out {
		if len(merged) > 0 && samePhysicalReservation(merged[len(merged)-1], r) {
			merged[len(merged)-1].Backing = addCapacity(merged[len(merged)-1].Backing, r.Backing)
		} else {
			merged = append(merged, r)
		}
	}
	return merged, nil
}
func samePhysicalReservation(a, b quotaPhysicalReservation) bool {
	return a.Bucket == b.Bucket && a.Node == b.Node && a.Volume == b.Volume
}
func sortPhysicalReservations(a []quotaPhysicalReservation) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].Bucket != a[j].Bucket {
			return a[i].Bucket.String() < a[j].Bucket.String()
		}
		if a[i].Node != a[j].Node {
			return a[i].Node.String() < a[j].Node.String()
		}
		return a[i].Volume.String() < a[j].Volume.String()
	})
}

func validatePhysicalReservations(view *systemView) error {
	if view.manifest.QuotaProtocol > 1 || quotaPoliciesRequired(view.descriptions) && view.manifest.QuotaProtocol != 1 {
		return volume.ErrCorrupt
	}
	totals := make(map[client.ID]uint64)
	seen := make(map[[3]client.ID]bool)
	for i, r := range view.manifest.QuotaReserves {
		key := [3]client.ID{r.Bucket, r.Node, r.Volume}
		if seen[key] {
			return volume.ErrCorrupt
		}
		seen[key] = true
		if r.Backing == 0 || r.Volume == (client.ID{}) || !hasControlNode(membershipIDs(view.manifest.Membership), r.Node) || i > 0 && samePhysicalReservation(view.manifest.QuotaReserves[i-1], r) {
			return volume.ErrCorrupt
		}
		var policy *BucketPolicy
		for _, d := range view.descriptions {
			if d.ID == r.Bucket.String() && !d.DeletionComplete && d.ID != systemBucket.String() {
				p := quotaPolicy(d)
				policy = &p
				break
			}
		}
		if policy == nil || policy.Limits.Guaranteed == 0 || !view.groupImage.allowsVersion(policy.PlacementGroup, policy.PlacementMaskVersion, r.Node.String(), r.Volume.String()) {
			return volume.ErrCorrupt
		}
		prior := totals[r.Bucket]
		totals[r.Bucket] = addCapacity(prior, r.Backing)
		if totals[r.Bucket] < prior {
			return volume.ErrCorrupt
		}
	}
	for _, d := range view.descriptions {
		if d.ID == systemBucket.String() || d.DeletionComplete {
			continue
		}
		p := quotaPolicy(d)
		id, _ := bucketID(d.ID)
		if totals[client.ID(id)] != guaranteedPhysical(p) {
			return volume.ErrCorrupt
		}
	}
	return nil
}

// A residual network uses uint64 capacities without ever summing unrelated
// unlimited edges, so volumes larger than MaxInt64 cannot overflow a flow sum.
type quotaFlowEdge struct {
	to, rev int
	cap     uint64
}
type quotaFlow struct{ edges [][]quotaFlowEdge }

func newQuotaFlow(n int) *quotaFlow { return &quotaFlow{edges: make([][]quotaFlowEdge, n)} }
func (g *quotaFlow) add(a, b int, n uint64) {
	g.edges[a] = append(g.edges[a], quotaFlowEdge{b, len(g.edges[b]), n})
	g.edges[b] = append(g.edges[b], quotaFlowEdge{a, len(g.edges[a]) - 1, 0})
}
func (g *quotaFlow) run(source, sink int) {
	for {
		parent := make([]int, len(g.edges))
		edge := make([]int, len(g.edges))
		for i := range parent {
			parent[i] = -1
		}
		parent[source] = source
		q := []int{source}
		for len(q) > 0 && parent[sink] < 0 {
			a := q[0]
			q = q[1:]
			for i, e := range g.edges[a] {
				if e.cap > 0 && parent[e.to] < 0 {
					parent[e.to] = a
					edge[e.to] = i
					q = append(q, e.to)
				}
			}
		}
		if parent[sink] < 0 {
			return
		}
		n := ^uint64(0)
		for b := sink; b != source; b = parent[b] {
			n = min(n, g.edges[parent[b]][edge[b]].cap)
		}
		for b := sink; b != source; b = parent[b] {
			a := parent[b]
			i := edge[b]
			r := g.edges[a][i].rev
			g.edges[a][i].cap -= n
			g.edges[b][r].cap += n
		}
	}
}

func quotaAccounting(a string) string {
	if a == "" {
		return "logical"
	}
	return a
}
func (s *Server) quotaVolumes() []*volume.Volume {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*volume.Volume
	for _, m := range s.volumes {
		if m.store != s.system {
			out = append(out, m.store)
		}
	}
	return out
}

func quotaPoliciesRequired(descriptions []BucketDescription) bool {
	for _, d := range descriptions {
		p := quotaPolicy(d)
		if !d.DeletionComplete && d.ID != systemBucket.String() && (p.Limits.Guaranteed != 0 || !p.Limits.Unlimited && p.Limits.Accounting == "physical") {
			return true
		}
	}
	return false
}
func supportsQuotaPolicies(view *systemView, caps uint64) bool {
	return view == nil || view.manifest.QuotaProtocol == 0 && !quotaPoliciesRequired(view.descriptions) || caps&client.CapabilityPhysicalQuotas != 0
}
func (s *Server) requireQuotaCapabilities(descriptions []BucketDescription, nodes []client.ID) error {
	if !quotaPoliciesRequired(descriptions) && (s.currentSystem.Load() == nil || s.currentSystem.Load().manifest.QuotaProtocol == 0) {
		return nil
	}
	local, _ := parseID(s.cfg.Node.ID)
	s.peerStatsMu.RLock()
	defer s.peerStatsMu.RUnlock()
	for _, node := range nodes {
		if node == client.ID(local) {
			continue
		}
		p := s.peerStats[node]
		if p == nil || !p.online || p.connection.Capabilities&client.CapabilityPhysicalQuotas == 0 {
			return fail(503, "all participating nodes must support physical quotas and guarantees")
		}
	}
	return nil
}

const peerQuotaPreview uint16 = 0x1073

// Advisory per-volume ownership of the aggregate reserve. Returning the actual
// unconsumed share lets the editor reuse its OWN promise without subtracting or
// exposing another bucket's protected space as available capacity.
func (s *Server) quotaPreview(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	bucket := volume.ID(d.ID())
	if d.Done() != nil {
		return nil, client.ErrProtocol
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, ErrBucketForbidden
	}
	amounts := make(map[string]uint64)
	for _, v := range s.quotaVolumes() {
		h, err := v.Open(false)
		if err != nil {
			return nil, err
		}
		u, err := h.StorageUsage()
		h.Close()
		if err != nil {
			return nil, err
		}
		id := v.Identity().VolumeID
		n := uint64(0)
		for _, r := range view.manifest.QuotaReserves {
			if r.Bucket == client.ID(bucket) && r.Node == s.quotaNode && r.Volume == client.ID(id) {
				n = addCapacity(n, subtractCapacity(r.Backing, storagePhysical(u, bucket)))
			}
		}
		amounts[id.String()] = n
	}
	return json.Marshal(amounts)
}
func (s *Server) quotaPreviewShares(ctx context.Context, view *systemView, bucket volume.ID) (map[client.ID]map[string]uint64, error) {
	out := make(map[client.ID]map[string]uint64)
	if bucket == (volume.ID{}) || view == nil || len(view.manifest.QuotaReserves) == 0 {
		return out, nil
	}
	e := client.Encoder{}
	e.ID(client.ID(bucket))
	for _, m := range view.manifest.Membership.Members {
		raw, err := (peerControlParticipants{runtime: s.control}).call(ctx, m.ID, peerQuotaPreview, e.B)
		if err != nil {
			return nil, err
		}
		var amounts map[string]uint64
		if decodeControlRecord(raw, &amounts) != nil {
			return nil, client.ErrProtocol
		}
		out[m.ID] = amounts
	}
	return out, nil
}

// Enabling the feature is monotonic. Even after deleting its last bucket, older
// peers must not receive historical physical-credit commits they cannot decode.
func (s *Server) preserveQuotaProtocol(bundle *systemBundle, descriptions []BucketDescription) error {
	prior := s.currentSystem.Load()
	if !quotaPoliciesRequired(descriptions) && (prior == nil || prior.manifest.QuotaProtocol == 0) {
		return nil
	}
	var m systemManifest
	if err := decodeControlRecord(bundle.Objects[bundle.Root], &m); err != nil {
		return err
	}
	if m.QuotaProtocol == 1 {
		return nil
	}
	m.QuotaProtocol = 1
	raw, err := json.Marshal(m)
	if err == nil {
		bundle.Root = bundle.add(raw)
	}
	return err
}
