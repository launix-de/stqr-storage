// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type clusterController struct {
	finishedExclusions map[client.ID]bool
	stopSender         func(context.Context, client.ID, []byte) error
	stopNotified       map[client.ID]shutdownDelivery
	mu                 sync.Mutex
	server             *Server
	coordinator        *controlCoordinator
	wake               chan struct{}
}

func (s *Server) initClusterControl(dataDir string) error {
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	base := controlBase{Membership: client.Membership{Epoch: 1, Cluster: client.ID(cluster), Members: []client.Member{{ID: client.ID(node), Addresses: []string{s.localAddress()}}}}, Generation: s.cfg.Generation, MapRevision: s.cfg.Generation}
	if s.cfg.JoinPending {
		base = controlBase{Membership: client.Membership{Cluster: client.ID(cluster)}}
	}
	var initial *controlBase
	if s.cfg.ControlVersion == 0 {
		initial = &base
	} else if s.cfg.ControlVersion != 1 {
		return volume.ErrUnsupported
	}
	ledger, err := openControlLedger(filepath.Join(dataDir, "control.stqr"), cluster, node, initial)
	if err != nil {
		return err
	}
	s.control = &controlRuntime{server: s, ledger: ledger, install: s.installSystemView}
	s.controller = &clusterController{server: s, wake: make(chan struct{}, 1)}
	s.controller.coordinator = &controlCoordinator{ledger: ledger, participants: peerControlParticipants{runtime: s.control}}
	s.cfg.ControlVersion = 1
	if err := saveConfig(s.configPath, s.cfg); err != nil {
		return err
	}
	return s.control.restore()
}

func (s *Server) localAddress() string {
	if s.cfg.Node.Advertise != "" {
		return s.cfg.Node.Advertise
	}
	host, port, err := net.SplitHostPort(s.cfg.Node.Listen)
	if err != nil {
		return "http://127.0.0.1:7419"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func (c *clusterController) initialCheckpoint() error {
	l := c.coordinator.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	pending := l.request != nil
	l.mu.Unlock()
	if pending {
		// Remote transports start after Open. A recovering coordinator must be
		// reachable first so its required peers can reconnect; the worker then
		// resumes this exact request. Initial self-bootstrap has no dependency.
		r := c.coordinator.requestSnapshot()
		if base.Sequence == 0 && r != nil && len(r.Claim.Required) == 1 && r.Claim.Required[0] == l.node {
			_, err := c.resume(c.server.ctx)
			return err
		}
		return nil
	}
	if base.Membership.Epoch == 0 || base.Sequence != 0 {
		return nil
	}
	_, _, err := c.change(c.server.ctx, nil, nil)
	return err
}

func (c *clusterController) start() {
	s := c.server
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
			case <-c.wake:
			}
			c.mu.Lock()
			_, _ = c.resume(s.ctx)
			c.finishExclusionReceipts()
			if c.processClusterShutdown(s.ctx) {
				c.mu.Unlock()
				continue
			}
			s.mu.RLock()
			draining := s.cfg.Node.Draining
			s.mu.RUnlock()
			if !s.joinPending() && !s.retiring() && !s.shortStopping.Load() {
				if !draining {
					c.ensureQuotaAllocations(s.ctx)
					c.admitDiscovered(s.ctx)
				}
				c.finishBucketDeletions(s.ctx)
				c.finishBucketPolicies(s.ctx)
				c.finishPlacementGroups(s.ctx)
			}
			c.mu.Unlock()
		}
	}()
}

// Directory/transport liveness is not membership. Only a committed change
// admits a candidate. A missing old member leaves this operation pending.
func (c *clusterController) admitDiscovered(ctx context.Context) {
	l := c.coordinator.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	busy := l.request != nil || l.slot != nil
	l.mu.Unlock()
	if busy || base.Membership.Epoch == 0 {
		return
	}
	s := c.server
	s.peerStatsMu.RLock()
	var peers []*peerConnection
	for _, observation := range s.peerStats {
		if observation.online && observation.connection.Capabilities&client.CapabilityClusterControl != 0 {
			peers = append(peers, observation.connection)
		}
	}
	s.peerStatsMu.RUnlock()
	sort.Slice(peers, func(i, j int) bool { return bytes.Compare(peers[i].Node[:], peers[j].Node[:]) < 0 })
	for _, peer := range peers {
		if hasControlNode(membershipIDs(base.Membership), peer.Node) {
			continue
		}
		if !supportsRecoveryOrigins(s.currentSystem.Load(), peer.Capabilities) {
			// Do not bind a join that cannot decode the immutable parent graph.
			// It may reconnect after upgrading; no membership reset is implied.
			continue
		}
		info, err := c.info(ctx, peer.Node)
		if err != nil || info.base.Membership.Epoch != 0 {
			continue
		}
		next := base.Membership
		next.Epoch++
		next.Members = append(append([]client.Member{}, base.Membership.Members...), client.Member{ID: peer.Node, Addresses: c.addresses(peer.Node)})
		sort.Slice(next.Members, func(i, j int) bool { return bytes.Compare(next.Members[i].ID[:], next.Members[j].ID[:]) < 0 })
		_, _, _ = c.change(ctx, &next, nil)
		return
	}
}

func (c *clusterController) addresses(node client.ID) []string {
	image := c.server.directory.Load()
	if image != nil {
		for _, known := range image.Nodes {
			if known.Node == node {
				return append([]string{}, known.Addresses...)
			}
		}
	}
	if node == c.coordinator.ledger.node {
		return []string{c.server.localAddress()}
	}
	return nil
}

func (c *clusterController) patches(membership client.Membership, descriptions []BucketDescription) []client.MapPatch {
	var patches []client.MapPatch
	view := c.server.currentSystem.Load()
	for _, description := range descriptions {
		if description.ID == systemBucket.String() || description.DeletionComplete {
			continue
		}
		id, _ := bucketID(description.ID)
		var patch client.MapPatch
		if view != nil {
			for _, p := range view.patches {
				if p.Bucket == client.ID(id) {
					patch = p
					break
				}
			}
		}
		if image := c.server.placement.Load(); image != nil {
			if entries := image.bucketEntries(client.ID(id)); len(entries) > 0 {
				patch.Entries = entries
			}
		}
		if patch.Bucket == (client.ID{}) {
			patch = client.MapPatch{Bucket: client.ID(id), ToEnd: true, Entries: []client.RangeEntry{{PolicyVersion: description.Policy.Version, OwnerEpoch: 1, Locations: []client.Location{{Node: c.coordinator.ledger.node, Class: uint8(bucketClass(description.Policy))}}}}}
			if description.Policy.StorageStrategy == "ram" {
				patch.Entries[0].Runtime = c.server.boot
			}
		}
		for i := range patch.Entries {
			if description.DesiredPolicy == nil || patch.Entries[i].PolicyVersion != description.DesiredPolicy.Version {
				patch.Entries[i].PolicyVersion = description.Policy.Version
			}
		}
		patch.Membership = membership.Epoch
		c.coordinator.ledger.mu.Lock()
		patch.Generation = c.coordinator.ledger.current.Generation
		patch.Revision = c.coordinator.ledger.current.MapRevision
		c.coordinator.ledger.mu.Unlock()
		patch.Addresses = nil
		for _, member := range membership.Members {
			addresses := member.Addresses
			if len(addresses) == 0 {
				addresses = c.addresses(member.ID)
			}
			if len(addresses) > 0 {
				patch.Addresses = append(patch.Addresses, client.NodeAddress{Node: member.ID, Addresses: addresses})
			}
		}
		patches = append(patches, patch)
	}
	return patches
}

// change runs with the local controller mutex held (Open is single threaded).
// The builder is isolated; no live account/catalog mutation precedes COMMIT.
func (c *clusterController) change(ctx context.Context, next *client.Membership, build func() (*adminDraft, error), frozen ...client.ID) ([]byte, int, error) {
	return c.changeWithShutdown(ctx, next, build, nil, frozen...)
}

func (c *clusterController) changeWithShutdown(ctx context.Context, next *client.Membership, build func() (*adminDraft, error), stop *clusterShutdown, frozen ...client.ID) ([]byte, int, error) {
	return c.changeWithExclusion(ctx, next, build, stop, nil, frozen...)
}
func (c *clusterController) changeWithExclusion(ctx context.Context, next *client.Membership, build func() (*adminDraft, error), stop *clusterShutdown, exclusion *controlExclusion, frozen ...client.ID) ([]byte, int, error) {
	l := c.coordinator.ledger
	l.mu.Lock()
	base := cloneControlBase(l.current)
	l.mu.Unlock()
	membership := base.Membership
	kind := controlAdministration
	if stop != nil {
		kind = controlShutdown
	}
	if next != nil {
		membership = *next
		kind = controlMembership
	}
	// Older checkpoints predate transferable quotas. Freeze existing finite
	// buckets while establishing their first exact allocation.
	for _, d := range c.server.bucketSnapshots() {
		if !d.DeletionComplete && !quotaPolicy(d).Limits.Unlimited {
			id, _ := bucketID(d.ID)
			if findQuota(c.server.currentSystem.Load(), client.ID(id)) == nil && !hasControlNode(frozen, client.ID(id)) {
				frozen = append(frozen, client.ID(id))
			}
		}
	}
	required := membershipIDs(base.Membership)
	for _, id := range membershipIDs(membership) {
		if !hasControlNode(required, id) {
			required = append(required, id)
		}
	}
	if exclusion != nil {
		required = membershipIDs(membership)
		kind = controlForceDisconnect
	}
	sort.Slice(required, func(i, j int) bool { return bytes.Compare(required[i][:], required[j][:]) < 0 })
	sort.Slice(frozen, func(i, j int) bool { return bytes.Compare(frozen[i][:], frozen[j][:]) < 0 })
	claim, err := c.coordinator.beginWithExclusion(ctx, kind, required, exclusion, frozen...)
	if err != nil {
		if exclusion != nil && claim.Exclusion != nil {
			// No scope reconciliation has started yet. Close partial force
			// promises on ALL survivors so an existing global round is not
			// trapped behind a second, unprepared force requester. If a peer
			// is unavailable, retain the journal request for exact recovery.
			_ = c.coordinator.cancelBuilding(ctx)
		}
		return nil, 0, err
	}
	return c.finishReservedChange(ctx, claim, membership, kind, next, build, stop, exclusion)
}

func (c *clusterController) finishReservedChange(ctx context.Context, claim controlClaim, membership client.Membership, kind uint16, next *client.Membership, build func() (*adminDraft, error), stop *clusterShutdown, exclusion *controlExclusion) ([]byte, int, error) {
	l := c.coordinator.ledger
	required := claim.Required
	base := cloneControlBase(claim.Base)
	if next == nil {
		membership = base.Membership
	}
	building := true
	defer func() {
		if building {
			if exclusion != nil && exclusion.Restore != nil {
				request := c.coordinator.requestSnapshot()
				if request != nil && request.Proposal == nil {
					if c.coordinator.cancelBuilding(ctx) == nil {
						c.server.opMu.Lock()
						if op, err := c.server.operation(exclusion.Operation.String()); err == nil {
							_, _ = c.server.finishOperation(op, fail(409, "takeover cancelled before READY; check volume, placement and quota prerequisites"))
						}
						c.server.opMu.Unlock()
					}
				} else {
					c.coordinator.suspendBuilding()
				}
			} else if exclusion != nil {
				c.coordinator.suspendBuilding()
			} else {
				_ = c.coordinator.cancelBuilding(ctx)
			}
		}
	}()
	// ALL reservations fence new scoped handoffs. Collect committed histories
	// before capturing the checkpoint, including disjoint remote rebuilds.
	var recovered []forceRecoveredRange
	if exclusion != nil {
		if err := l.startForceConstruction(claim); err != nil {
			return nil, 0, err
		}
		wire, err := marshalControlClaim(claim)
		if err != nil {
			return nil, 0, err
		}
		for _, node := range required {
			if _, err := (peerControlParticipants{runtime: c.server.control}).call(ctx, node, peerForceConstruction, wire); err != nil {
				return nil, 0, err
			}
		}
		if recovered, err = c.reconcileExcludedScopes(ctx, claim); err != nil {
			return nil, 0, err
		}
	}
	if c.server.scopes != nil {
		if exclusion != nil {
			for _, node := range required {
				if err := c.server.scopes.syncArchiveFrom(ctx, node); err != nil {
					return nil, 0, err
				}
			}
		} else if err := c.server.scopes.syncArchives(ctx); err != nil {
			return nil, 0, err
		}
	}
	var err error
	roots := make(map[client.ID]volume.ID)
	for _, node := range required {
		info, err := c.info(ctx, node)
		if err != nil {
			return nil, 0, err
		}
		roots[node] = info.root
	}
	source := c.server
	status := 200
	var result []byte
	if build != nil {
		draft, err := build()
		if err != nil {
			return nil, 0, err
		}
		if draft != nil {
			defer draft.close()
			// A PG edit can affect a bucket without going through its direct
			// admin opcode. Only receipt updates may change a leased descriptor.
			for _, live := range c.server.leaseBuckets(false) {
				if !live.underMaintenance() {
					continue
				}
				candidate, e := draft.server.Bucket(live.ID())
				if e != nil {
					return nil, 0, fail(409, "leased bucket cannot be removed")
				}
				before, after := live.Describe(), candidate.Describe()
				before.Revision = 0
				after.Revision = 0
				before.Receipts = nil
				after.Receipts = nil
				if !reflect.DeepEqual(before, after) {
					return nil, 0, fail(409, "bucket configuration is under exclusive maintenance")
				}
			}
			source = draft.server
			result, status = draft.result, draft.status
		}
	}
	descriptions := source.bucketSnapshots()
	for _, description := range descriptions {
		if description.ID != systemBucket.String() && !description.DeletionComplete && description.Policy.PlacementMode == "everywhere" && len(membership.Members) > 1 {
			return nil, 0, fail(501, "ordinary EVERYWHERE buckets require data replication support")
		}
	}
	patches := c.patches(membership, descriptions)
	origins, err := c.server.recoveryOriginsForChange(patches, exclusion)
	if err != nil {
		return nil, 0, err
	}
	if exclusion != nil {
		if err := c.applyRecoveredRanges(patches, recovered); err != nil {
			return nil, 0, err
		}
		origins, err = c.server.recoveryOriginsForChange(patches, exclusion)
		if err != nil {
			return nil, 0, err
		}
		if exclusion.RecoverOnly {
			if err := c.collectRecoveryCancellations(ctx, claim, origins); err != nil {
				return nil, 0, err
			}
		}
		if err := c.markExcludedRangesLost(patches, descriptions, exclusion, membership); err != nil {
			return nil, 0, err
		}
	}
	bundle, err := source.captureSystemImage(membership, base.Sequence+1, base.Generation, base.MapRevision, roots, patches)
	if err != nil {
		return nil, 0, err
	}
	if err := c.server.setRecoveryBundle(bundle, patches, origins, exclusion); err != nil {
		return nil, 0, err
	}
	var planned volume.ObjectID
	if exclusion != nil {
		if err := c.server.stageSystemBundle(bundle); err != nil {
			return nil, 0, err
		}
		for _, node := range required {
			if node != l.node {
				if err := c.sendBundle(ctx, node, bundle); err != nil {
					return nil, 0, err
				}
			}
		}
		planned = bundle.Root
	}
	allocations, err := c.captureQuotaAllocations(ctx, claim, membership, descriptions, planned)
	if err != nil {
		return nil, 0, err
	}
	if err := setBundleQuotas(bundle, allocations); err != nil {
		return nil, 0, err
	}
	if stop != nil {
		stop.Coordinator, stop.Sequence, stop.Membership = l.node, base.Sequence+1, membership.Epoch
		if err := setBundleShutdown(bundle, stop); err != nil {
			return nil, 0, err
		}
	}
	if err = c.server.stageSystemBundle(bundle); err != nil {
		return nil, 0, err
	}
	e := client.Encoder{}
	e.V(uint64(status))
	e.Blob(result)
	if err = c.coordinator.prepared(ctx, controlProposal{Claim: claim, Kind: kind, Image: [32]byte(bundle.Root), NextMembership: next}, e.B); err != nil {
		return nil, 0, err
	}
	building = false
	public, err := c.resume(ctx)
	if err != nil {
		return nil, 0, err
	}
	d := client.Decoder{B: public}
	status = int(d.V())
	result = d.Blob()
	if d.Done() != nil {
		return nil, 0, client.ErrProtocol
	}
	return result, status, nil
}

func (c *clusterController) resume(ctx context.Context) ([]byte, error) {
	r := c.coordinator.requestSnapshot()
	if r == nil {
		return nil, nil
	}
	if r.Kind == controlForceDisconnect && !r.ResultReady {
		return c.resumeExclusionBuild(ctx, r)
	}
	if r.ResultReady && r.Proposal != nil {
		bundle, err := c.server.exportSystemBundle(volume.ObjectID(r.Proposal.Image))
		if err != nil {
			return nil, err
		}
		for _, node := range r.Claim.Required {
			if node == c.coordinator.ledger.node {
				continue
			}
			if err := c.sendBundle(ctx, node, bundle); err != nil {
				return nil, err
			}
		}
	}
	return c.coordinator.resume(ctx)
}

func (s *Server) executeGlobalAdmin(m client.Message, actor principal) ([]byte, int, error) {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.resume(s.ctx); err != nil {
		return nil, 0, err
	}
	var frozen []client.ID
	if m.Opcode == client.BucketSetQuota || m.Opcode == client.BucketSetPolicy {
		if len(m.Body) < 16 {
			return nil, 0, client.ErrProtocol
		}
		var bucket client.ID
		copy(bucket[:], m.Body[:16])
		frozen = []client.ID{bucket}
	}
	if m.Opcode == bucketSetPlacementGroup {
		d := client.Decoder{B: m.Body}
		schema := d.U16()
		id := d.ID()
		if d.Err != nil || schema != 1 && schema != 2 {
			return nil, 0, client.ErrProtocol
		}
		frozen = append(frozen, id)
	}
	return c.change(s.ctx, nil, func() (*adminDraft, error) {
		draft, err := s.prepareAdminDraft(m, actor)
		if err != nil {
			return nil, err
		}
		if m.Opcode == placementGroupSet {
			for _, d := range draft.server.bucketSnapshots() {
				if d.DesiredPolicy != nil && (d.Policy.StorageStrategy == "ram") != (d.DesiredPolicy.StorageStrategy == "ram") {
					draft.close()
					return nil, fail(501, "cluster RAM/disk policy conversion requires runtime handoff")
				}
			}
		}
		for _, id := range frozen {
			usage, err := c.frozenUsage(s.ctx, id)
			if err != nil {
				draft.close()
				return nil, err
			}
			candidate, err := draft.server.Bucket(volume.ID(id))
			if err != nil {
				draft.close()
				return nil, err
			}
			description := candidate.Describe()
			policy := description.Policy
			if description.DesiredPolicy != nil {
				policy = *description.DesiredPolicy
			}
			if !policy.Limits.Unlimited && usage > policy.Limits.Absolute {
				draft.close()
				return nil, volume.ErrFull
			}
			original, err := s.Bucket(volume.ID(id))
			if err != nil {
				draft.close()
				return nil, err
			}
			if (original.Describe().Policy.StorageStrategy == "ram") != (policy.StorageStrategy == "ram") {
				draft.close()
				return nil, fail(501, "cluster RAM/disk policy conversion requires runtime handoff")
			}
		}
		return draft, nil
	}, frozen...)
}
