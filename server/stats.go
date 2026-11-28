// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// CollectStats is shared by dashboard and placement planning. The peer transport
// pushes these same source-bound snapshots; liveness never changes membership.
func (s *Server) CollectStats(filter client.StatsFilter) client.StatsSnapshot {
	// Pin the configuration/cohort and retired counters together, then release
	// the node lock before native Volume accessors can wait for storage locks.
	// Otherwise a writer holding a Volume handle and needing node admission can
	// deadlock with telemetry. The copied fields contain no mutable slice/map
	// aliases. A detached/closed store remains a valid Go instance with readable
	// final counters; counting this cohort with its matching retired snapshot
	// avoids double-counting a concurrent detach's transferred I/O history.
	type volumeInput struct {
		store             *volume.Volume
		path              string
		draining, pending bool
	}
	s.mu.RLock()
	cfgNode, generation, joinPending := s.cfg.Node, s.cfg.Generation, s.cfg.JoinPending
	retired := s.retiredIO
	control, scopes, hasController := s.control, s.scopes, s.controller != nil
	volumes := make([]volumeInput, 0, len(s.volumes))
	for _, m := range s.volumes {
		volumes = append(volumes, volumeInput{store: m.store, path: m.config.Path, draining: m.config.State == "draining", pending: len(m.config.PlacementGroupsPending) > 0})
	}
	s.mu.RUnlock()
	node, _ := parseID(cfgNode.ID)
	snap := client.StatsSnapshot{Schema: 1, Node: client.ID(node), BootID: s.boot, Sequence: s.sequence.Add(1), CapturedUnixNS: uint64(time.Now().UnixNano()), WindowNS: uint64(3 * time.Second), MembershipEpoch: 1, Generation: generation, MapRevision: generation, Volumes: []client.VolumeStats{}, Buckets: []client.BucketStats{}, Ranges: []client.RangeStats{}}
	snap.IO = retired
	if scopes != nil {
		if activity := scopes.activity.Load(); activity != nil && activity.Active > 0 {
			snap.NeedRebuild = true
			snap.ReasonBits |= 1 << 4
			snap.Coordinator = activity.Coordinator
			snap.Round = activity.Round
		}
	}
	if control != nil {
		if activity := control.ledger.activity.Load(); activity != nil && activity.Busy {
			snap.NeedRebuild = true
			snap.ReasonBits |= 1 << 9 // a durable global control decision is pending
			snap.Coordinator, snap.Round = activity.Coordinator, activity.Round
		}
	}
	if cfgNode.Paused {
		snap.NodeState = 2
	}
	if cfgNode.Draining && !cfgNode.Paused {
		snap.NodeState = 1
		snap.NeedRebuild = true
		snap.ReasonBits |= 1 << 1
	}
	if view := s.currentSystem.Load(); view != nil {
		if stop := view.manifest.Shutdown; stop != nil && stop.Membership == view.manifest.Membership.Epoch && cfgNode.ClusterResumedID != stop.ID.String() {
			snap.NodeState = 2
		}
		snap.MembershipEpoch = view.manifest.Membership.Epoch
		snap.Generation = view.manifest.Generation
		snap.MapRevision = view.manifest.Revision
		for _, description := range view.descriptions {
			if description.DesiredPolicy != nil {
				snap.NeedRebuild = true
				snap.ReasonBits |= 1 << 6
			}
			if description.State == BucketDeleting && !description.DeletionComplete {
				// Background reclamation is carried by the catalog, not a generation barrier.
				snap.ReasonBits |= 1 << 8
			}
		}
	}
	if joinPending {
		snap.NodeState = 4
		snap.MembershipEpoch, snap.Generation, snap.MapRevision = 0, 0, 0
		snap.NeedRebuild = true
		snap.ReasonBits |= 1 // JOIN; pending until committed membership activation
	}
	if groups := s.placementGroups.Load(); groups != nil {
		for _, group := range groups.object.Groups {
			if group.Desired != nil {
				snap.NeedRebuild = true
				snap.ReasonBits |= 1 << 6 // POLICY; storing desired is not applying it
			}
		}
	}
	if s.storageWarming.Load() {
		snap.NodeState = 2         // runtime pause only; never persistent drain/admin intent
		snap.ReasonBits |= 1 << 10 // accounting/recovery warmup
	}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		snap.CPUTimeNS = uint64(usage.Utime.Nano() + usage.Stime.Nano())
	}
	if b, e := os.ReadFile("/proc/self/statm"); e == nil {
		f := strings.Fields(string(b))
		if len(f) > 1 {
			pages, _ := strconv.ParseUint(f[1], 10, 64)
			snap.ResidentBytes = pages * uint64(os.Getpagesize())
		}
	}
	if !matches(filter.Nodes, snap.Node) {
		return snap
	}
	for _, m := range volumes {
		v := m.store.Stats()
		id := client.ID(v.Identity.VolumeID)
		io := volumeAccessIO(v.IO)
		state := uint8(m.store.State())
		if s.storageWarming.Load() || m.pending {
			state = 0
		}
		if hasController && m.draining {
			state = 2
		}
		health := uint8(0)
		if state == 4 {
			health = 2
		}
		if matches(filter.Volumes, id) {
			snap.Volumes = append(snap.Volumes, client.VolumeStats{ID: id, Path: m.path, Class: uint8(v.Identity.Class), State: state, CapacityPhysical: v.CapacityPhysical, AllocatedPhysical: v.AllocatedPhysical, ReclaimablePhysical: v.ReclaimablePhysical, PayloadPhysical: v.DataPhysical, MetadataPhysical: v.MetadataPhysical, ArenaUnusedPhysical: v.ArenaUnusedPhysical, GuaranteedRemainingPhysical: v.GuaranteedRemainingPhysical, InFlightReservedPhysical: v.InFlightReservedPhysical, OperatingReservePhysical: v.OperatingReservePhysical, AdmissionFreePhysical: v.AdmissionFreePhysical, Health: health, IO: io})
		}
		snap.IO.PhysicalReadBytes += io.PhysicalReadBytes
		snap.IO.PhysicalWriteBytes += io.PhysicalWriteBytes
		snap.IO.Errors += io.Errors
		snap.IO.Flushes += io.Flushes
		snap.IO.FlushNS += io.FlushNS
		if state == 2 {
			snap.NeedRebuild = true
			snap.ReasonBits |= 1 << 3
		}
	}
	s.collectQuotaStats(&snap, filter)
	if filter.IncludeRanges && scopes != nil {
		scopes.collectRangeTelemetry(&snap)
	}
	sort.Slice(snap.Volumes, func(i, j int) bool { return snap.Volumes[i].ID.String() < snap.Volumes[j].ID.String() })
	return snap
}
func matches(ids []client.ID, id client.ID) bool {
	if len(ids) == 0 {
		return true
	}
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
func filterSnapshot(snap client.StatsSnapshot, f client.StatsFilter) client.StatsSnapshot {
	out := snap
	out.Volumes = []client.VolumeStats{}
	out.Buckets = []client.BucketStats{}
	out.Ranges = []client.RangeStats{}
	if !matches(f.Nodes, snap.Node) {
		return out
	}
	for _, v := range snap.Volumes {
		if matches(f.Volumes, v.ID) {
			out.Volumes = append(out.Volumes, v)
		}
	}
	for _, b := range snap.Buckets {
		if matches(f.Buckets, b.ID) {
			out.Buckets = append(out.Buckets, b)
		}
	}
	if f.IncludeRanges {
		for _, r := range snap.Ranges {
			if matches(f.Buckets, r.Bucket) && matches(f.Volumes, r.Volume) {
				out.Ranges = append(out.Ranges, r)
			}
		}
	}
	return out
}
func (s *Server) publishStats() {
	s.publishVersions()
	snap := s.CollectStats(client.StatsFilter{IncludeRanges: true})
	s.observeDashboardRates(snap)
	s.latest.Store(&snap)
	s.broadcastStats(snap)
}

func (s *Server) broadcastStats(snap client.StatsSnapshot) {
	s.wsMu.Lock()
	sockets := make([]*wsSession, 0, len(s.sockets))
	for ws := range s.sockets {
		sockets = append(sockets, ws)
	}
	s.wsMu.Unlock()
	for _, ws := range sockets {
		ws.pushStats(snap)
	}
}
func (s *Server) statsLoop() {
	defer s.workers.Done()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.statsWake:
			s.publishStats()
		case <-ticker.C:
			s.publishStats()
		}
	}
}

func (s *Server) notifyStats() {
	select {
	case s.statsWake <- struct{}{}:
	default:
	}
}
