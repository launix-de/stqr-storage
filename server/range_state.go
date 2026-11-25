// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	rangeGenesis uint16 = 1 + iota
	rangeReserved
	rangeFenced
	rangeSourceReady
	rangeTargetReady
	rangeCommitted
	rangeTargetDone
	rangeSourceDone
	rangeObserved
	rangeObjectStored
	rangeAborted
	rangeTargetRestored
	rangeRAMTargetLost
	rangeRAMOfferLost
	rangeRAMCommitLost
	rangeForceCancelled
	rangeForceRetired
	rangeForcePromoted
	rangePGPrepared
	rangePGStarted
	rangePGCompleted
	rangePGImported
	rangePGSourceCancelled
	rangePGOffered
	rangePGOfferRetired
	rangePGRetired
)

type rangeCommit struct {
	ID, Digest     [32]byte
	LogicalUsed    uint64
	QuotaUsed      uint64
	Source, Target client.Proof
}
type rangeEvent struct {
	RetiredMembership uint64          `json:",omitempty"`
	PGGroup           client.ID       `json:",omitempty"`
	PG                *pgRebuildRound `json:",omitempty"`
	Exclusion         *controlClaim
	Boot, SourceBoot  client.ID
	LogicalUsed       uint64
	QuotaUsed         uint64
	PeerBoot          client.ID
	Object            *rangeObjectMemo
	ID, Digest        [32]byte
	Plan              []byte
	Commit            *rangeCommit
	TargetDone        client.Proof
}
type rangeTxn struct {
	sourceBoot, targetBoot                                        client.ID
	logicalUsed, pendingUsed                                      uint64
	quotaUsed, pendingQuota                                       uint64
	aborted                                                       bool
	forceRetired                                                  bool
	forcePromotion                                                *controlClaim
	forceCancellation                                             *controlClaim
	cancellationProof                                             client.Proof
	abortObjectCursor                                             *volume.ObjectID
	abortRequestCursor                                            *volume.ID
	abortReplicaCursor                                            *volume.ID
	abortObjectsDone, abortRequestsDone                           bool
	abortReplicasDone                                             bool
	snapshot                                                      *rangeSnapshot
	plan                                                          rangePlan
	lease                                                         sync.RWMutex
	fenced                                                        bool
	digest                                                        [32]byte
	sourceReady, targetReady, commitProof, targetDone, sourceDone client.Proof
	decision                                                      *rangeCommit
	remaining                                                     uint64
}
type rangeManager struct {
	metrics          sync.Map
	measurements     sync.Map
	improveSequence  atomic.Uint64
	pgOffers         map[client.ID]rangePlan
	pgCatalog        atomic.Pointer[pgRebuildCatalog]
	pgRuntime        pgRebuildRuntime
	pgStates         map[client.ID]*pgRebuildState
	pgRounds         map[[32]byte]pgRebuildRound
	pgOrder          [][32]byte
	activity         atomic.Pointer[rangeActivity]
	lastError        error
	pressures        []rangePressure
	capacityGoals    map[volume.ID]*capacityGoal
	capacityMoveTime time.Duration
	mu               sync.Mutex
	server           *Server
	journal          *controlJournal
	node             client.ID
	txns             map[[32]byte]*rangeTxn
	order            [][32]byte
	initialized      bool
	memoCursor       *volume.Key
	capacityWake     chan struct{}
}

func (s *Server) initRanges(dataDir string) error {
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	r := &rangeManager{server: s, node: client.ID(node), txns: make(map[[32]byte]*rangeTxn), capacityWake: make(chan struct{}, 1)}
	if s.cfg.RangeVersion > 1 {
		return volume.ErrUnsupported
	}
	j, err := openControlJournal(filepath.Join(dataDir, "ranges.stqr"), cluster, node, s.cfg.RangeVersion == 0, r.replay)
	if err != nil {
		return err
	}
	r.journal = j
	s.scopes = r
	if !r.initialized {
		if _, err := r.persist(rangeGenesis, rangeEvent{}); err != nil {
			return err
		}
	}
	s.cfg.RangeVersion = 1
	if err := saveConfig(s.configPath, s.cfg); err != nil {
		return err
	}
	if err := r.retirePGMembership(); err != nil {
		return err
	}
	r.mu.Lock()
	err = r.rebuildLocked()
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if err := r.retireLostRAMTargets(); err != nil {
		return err
	}
	r.publishQuotaRangesLocked()
	if err := r.restoreReservations(); err != nil {
		return err
	}
	r.publishActivityLocked()
	r.publishQuotaImportsLocked()
	r.publishQuotaRangesLocked()
	return nil
}
func (r *rangeManager) persist(kind uint16, event rangeEvent) (client.Proof, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return client.Proof{}, err
	}
	proof, err := r.journal.append(kind, data)
	if err != nil {
		return client.Proof{}, err
	}
	if err := r.replay(controlRecord{Kind: kind, Proof: proof, Data: data}); err != nil {
		return client.Proof{}, err
	}
	r.publishActivityLocked()
	r.publishQuotaImportsLocked()
	r.publishQuotaRangesLocked()
	return proof, nil
}
func (r *rangeManager) replay(record controlRecord) error {
	var event rangeEvent
	if err := decodeControlRecord(record.Data, &event); err != nil {
		return err
	}
	if record.Kind != rangeGenesis && !r.initialized {
		return volume.ErrCorrupt
	}
	if record.Kind == rangePGRetired {
		a := event.PG
		if a == nil || !validPGRound(*a) || event.RetiredMembership <= a.Membership {
			return volume.ErrCorrupt
		}
		state := r.pgStates[a.Group]
		if state == nil || state.Active == nil || !samePGRound(state.Active.Round, *a) {
			return volume.ErrCorrupt
		}
		state.Active = nil
		delete(r.pgOffers, a.Group)
		return nil
	}
	if record.Kind == rangePGOffered || record.Kind == rangePGOfferRetired {
		if r.pgOffers == nil {
			r.pgOffers = map[client.ID]rangePlan{}
		}
		p, err := decodeRangePlan(event.Plan)
		if err != nil || event.PGGroup == (client.ID{}) || p.Source != r.node {
			return volume.ErrCorrupt
		}
		if record.Kind == rangePGOffered {
			if old, ok := r.pgOffers[event.PGGroup]; ok && old.id() != p.id() {
				return volume.ErrCorrupt
			}
			r.pgOffers[event.PGGroup] = p
		} else {
			delete(r.pgOffers, event.PGGroup)
		}
		return nil
	}
	if record.Kind >= rangePGPrepared && record.Kind <= rangePGSourceCancelled {
		return r.replayPG(record.Kind, event.PG)
	}
	if record.Kind == rangeGenesis {
		if r.initialized || record.Proof.RecordSequence != 1 {
			return volume.ErrCorrupt
		}
		r.initialized = true
		return nil
	}
	if !r.initialized {
		return volume.ErrCorrupt
	}
	if record.Kind == rangeReserved || record.Kind == rangeObserved || (record.Kind == rangeAborted || record.Kind == rangeRAMOfferLost || record.Kind == rangeForceCancelled) && r.txns[event.ID] == nil {
		plan, err := decodeRangePlan(event.Plan)
		if err != nil || plan.id() != event.ID || r.txns[event.ID] != nil {
			return volume.ErrCorrupt
		}
		r.txns[event.ID] = &rangeTxn{plan: plan, remaining: plan.Physical, sourceBoot: event.SourceBoot}
		r.order = append(r.order, event.ID)
	}
	t := r.txns[event.ID]
	if t == nil {
		return volume.ErrCorrupt
	}
	switch record.Kind {
	case rangeForcePromoted:
		if event.Exclusion == nil || !validExclusionClaim(*event.Exclusion) || !hasControlNode(event.Exclusion.Required, r.node) || !hasControlNode(event.Exclusion.Exclusion.Nodes, t.plan.Source) || t.plan.Target != r.node || t.targetReady.RecordSequence == 0 || t.decision != nil || t.aborted || t.forceRetired || t.plan.ram() && t.targetBoot != event.Boot {
			return volume.ErrCorrupt
		}
		t.forcePromotion = event.Exclusion
		t.forceRetired = true
		t.remaining = 0
	case rangeForceRetired:
		if event.Exclusion == nil || !validExclusionClaim(*event.Exclusion) || !hasControlNode(event.Exclusion.Required, r.node) || t.decision == nil || t.forceRetired || !hasControlNode(event.Exclusion.Exclusion.Nodes, t.plan.Source) && !hasControlNode(event.Exclusion.Exclusion.Nodes, t.plan.Target) {
			return volume.ErrCorrupt
		}
		t.forceRetired = true
		t.remaining = 0
	case rangeForceCancelled:
		if event.Exclusion == nil || !validExclusionClaim(*event.Exclusion) || !hasControlNode(event.Exclusion.Required, r.node) || t.decision != nil || t.aborted {
			return volume.ErrCorrupt
		}
		t.aborted = true
		t.forceCancellation = event.Exclusion
		t.cancellationProof = record.Proof
		t.remaining = 0
	case rangeRAMOfferLost:
		if !t.plan.ram() || t.aborted || t.decision != nil || event.SourceBoot != t.sourceBoot || event.PeerBoot == (client.ID{}) || event.PeerBoot == t.sourceBoot {
			return volume.ErrCorrupt
		}
		if t.plan.Target == r.node && t.targetReady.RecordSequence != 0 && t.targetBoot == event.Boot {
			return volume.ErrCorrupt
		}
		t.aborted = true
		t.remaining = 0
	case rangeAborted:
		if t.decision != nil || t.fenced || t.sourceReady.RecordSequence != 0 || t.targetReady.RecordSequence != 0 || t.aborted {
			return volume.ErrCorrupt
		}
		t.aborted = true
		t.remaining = 0
	case rangeObjectStored:
		if event.Object == nil || t.plan.Target != r.node || event.Object.Key.Bucket != volume.ID(t.plan.Before.Bucket) || !t.plan.contains(event.Object.Key.Object) || event.Object.Digest == ([32]byte{}) {
			return volume.ErrCorrupt
		}
		// Legacy progress records are validated but need not be retained in RAM.
		// A missing advisory memo safely triggers retransmission.
	case rangeReserved:
	case rangeFenced:
		if t.plan.Source != r.node || t.fenced {
			return volume.ErrCorrupt
		}
		t.fenced = true
	case rangeSourceReady:
		if !t.fenced || t.plan.Source != r.node || t.sourceReady.RecordSequence != 0 {
			return volume.ErrCorrupt
		}
		t.digest = event.Digest
		t.logicalUsed = event.LogicalUsed
		t.quotaUsed = event.QuotaUsed
		t.pendingQuota = event.QuotaUsed
		t.pendingUsed = event.LogicalUsed
		t.sourceReady = record.Proof
	case rangeTargetRestored:
		if t.plan.Target != r.node || t.targetReady.RecordSequence == 0 || t.decision != nil || !t.plan.ram() || event.Boot == (client.ID{}) || event.Boot == t.targetBoot || event.Digest != t.digest || t.aborted {
			return volume.ErrCorrupt
		}
		t.targetBoot = event.Boot
	case rangeRAMTargetLost:
		if t.plan.Target != r.node || t.decision == nil || !t.plan.ram() || t.targetDone.RecordSequence != 0 || event.Boot == (client.ID{}) || event.Boot == t.targetBoot {
			return volume.ErrCorrupt
		}
		t.targetDone = record.Proof
		t.remaining = 0
	case rangeTargetReady:
		if t.plan.Target != r.node || t.targetReady.RecordSequence != 0 {
			return volume.ErrCorrupt
		}
		if t.digest != ([32]byte{}) && t.digest != event.Digest {
			return volume.ErrCorrupt
		}
		t.digest = event.Digest
		t.logicalUsed = event.LogicalUsed
		t.quotaUsed = event.QuotaUsed
		t.pendingQuota = event.QuotaUsed
		t.pendingUsed = event.LogicalUsed
		t.targetReady = record.Proof
		t.targetBoot = event.Boot
	case rangeCommitted, rangeObserved, rangeRAMCommitLost:
		if record.Kind == rangeRAMCommitLost && (!t.plan.ram() || t.plan.Target != r.node || event.SourceBoot != t.sourceBoot || event.PeerBoot == (client.ID{}) || event.PeerBoot == t.sourceBoot || event.Boot == (client.ID{}) || t.targetBoot == event.Boot) {
			return volume.ErrCorrupt
		}
		if event.Commit == nil || !r.validCommit(t, *event.Commit, record.Kind == rangeObserved) {
			return volume.ErrCorrupt
		}
		copy := *event.Commit
		t.decision = &copy
		t.commitProof = record.Proof
	case rangeTargetDone:
		if t.plan.Target != r.node || t.decision == nil || t.targetDone.RecordSequence != 0 {
			return volume.ErrCorrupt
		}
		t.targetDone = record.Proof
		t.remaining = 0
	case rangeSourceDone:
		if t.plan.Source != r.node || t.decision == nil || event.TargetDone.Node != t.plan.Target || event.TargetDone.RecordSequence == 0 || t.sourceDone.RecordSequence != 0 {
			return volume.ErrCorrupt
		}
		t.sourceDone = record.Proof
	default:
		return volume.ErrCorrupt
	}
	return nil
}
func (r *rangeManager) validCommit(t *rangeTxn, c rangeCommit, observed bool) bool {
	if t.aborted || c.ID != t.plan.id() || c.Source.Node != t.plan.Source || c.Target.Node != t.plan.Target || c.Digest == ([32]byte{}) {
		return false
	}
	if _, err := c.Source.MarshalBinary(); err != nil {
		return false
	}
	if _, err := c.Target.MarshalBinary(); err != nil {
		return false
	}
	if !observed {
		if t.plan.Source == r.node && (t.sourceReady != c.Source || t.digest != c.Digest || t.logicalUsed != c.LogicalUsed || t.quotaUsed != c.QuotaUsed) {
			return false
		}
		if t.plan.Target == r.node && (t.targetReady != c.Target || t.digest != c.Digest || t.logicalUsed != c.LogicalUsed || t.quotaUsed != c.QuotaUsed) {
			return false
		}
	}
	return true
}
func (r *rangeManager) active(t *rangeTxn) bool {
	if t.aborted || t.forceRetired {
		return false
	}
	if t.plan.Source == r.node {
		return t.sourceDone.RecordSequence == 0
	}
	return t.plan.Target == r.node && t.targetDone.RecordSequence == 0
}
func (r *rangeManager) busyLocked() bool {
	for _, t := range r.txns {
		if r.active(t) {
			return true
		}
	}
	return false
}

func (r *rangeManager) reserve(p rangePlan, sourceBoot ...client.ID) error {
	if err := r.ensurePGPrepared(r.server.ctx, p); err != nil {
		return err
	}
	wire, err := p.marshal()
	if len(p.Before.Entries) > 0 && p.Before.Entries[0].State == 4 {
		return fail(503, "range awaits rescued storage")
	}
	if err != nil {
		return err
	}
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	if err := r.server.refreshQuotaProtectionLocked(); err != nil {
		return err
	}
	if b, err := r.server.Bucket(volume.ID(p.Before.Bucket)); err == nil && b.blocksPlacementChange() {
		return fail(409, "bucket is under exclusive maintenance")
	}
	if r.server.clusterPaused() {
		return fail(503, "cluster shutdown is pending")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(nil); err != nil {
		return err
	}
	id := p.id()
	boot := r.server.boot
	if len(sourceBoot) > 0 {
		boot = sourceBoot[0]
	}
	if p.ram() && boot == (client.ID{}) {
		return fail(503, "source process identity is not yet available")
	}
	if prior, ok := r.txns[id]; ok {
		if prior.aborted || prior.forceRetired {
			return errControlConflict
		}
		return nil
	}
	l := r.server.control.ledger
	l.mu.Lock()
	busy := l.slot != nil
	membership := l.current.Membership
	l.mu.Unlock()
	if (busy && r.server.controlBlocksPG(p.Before.Bucket)) || membership.Epoch != p.Before.Membership || !hasControlNode(membershipIDs(membership), p.Source) || !hasControlNode(membershipIDs(membership), p.Target) {
		return errControlConflict
	}
	if p.Source != r.node && p.Target != r.node {
		return errControlConflict
	}
	if (p.ReplicaPromote || p.ReplicaRelocate) && r.server.currentSystem.Load().manifest.ReplicationProtocol < 2 {
		return errControlConflict
	}
	current, known := r.server.routingPatch(volume.ID(p.Before.Bucket), volume.ObjectID(p.Before.Begin))
	if !known || !reflect.DeepEqual(current.Entries, p.Before.Entries) {
		return errControlConflict
	}
	if p.ram() && p.movingRuntime() != p.Before.Entries[0].Runtime {
		if p.movingRuntime() != boot {
			return errControlConflict
		}
		if p.Source == r.node && r.ramAvailableLocked(volume.ID(p.Before.Bucket), volume.ObjectID(p.Before.Begin), p.Before.Entries[0]) {
			return errControlConflict
		}
	}
	for _, prior := range r.txns {
		if r.active(prior) && rangePlansOverlap(prior.plan, p) {
			return errControlConflict
		}
	}
	if p.Target == r.node {
		r.server.mu.RLock()
		managed := r.server.volumes[p.Volume.String()]
		active := managed != nil && (managed.config.State == "active" || p.ReplicaRemove && managed.config.State == "draining") && !managed.config.RecoveryOnly && (!r.server.cfg.Node.Draining || p.Source == p.Target || p.ReplicaRemove)
		r.server.mu.RUnlock()
		if !active {
			return volume.ErrFull
		}
		store, err := r.destination(p)
		if err != nil {
			return err
		}
		if p.ram() {
			pin := volume.ID(p.Before.Entries[0].Locations[0].Volume)
			if p.Source != p.Target || pin != (volume.ID{}) && pin != p.Volume || p.movingRuntime() != p.Before.Entries[0].Runtime {
				if err := r.clearRAMStagingLocked(store, p); err != nil {
					return err
				}
			}
		}
		stats := store.Stats()
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		u, err := h.StorageUsage()
		h.Close()
		if err != nil {
			return err
		}
		owned := r.server.bucketVolumeGuarantee(p.Before.Bucket, client.ID(p.Volume), u)
		owned = subtractCapacity(owned, r.server.importReserved(p.Before.Bucket, client.ID(p.Volume)))
		if p.Physical > addCapacity(stats.AdmissionFreePhysical, owned) {
			return volume.ErrFull
		}
		discount := min(owned, p.Physical)
		if err := store.SetGuaranteedReserve(subtractCapacity(stats.GuaranteedRemainingPhysical, discount)); err != nil {
			return err
		}
		if err := store.SetInFlightReserve(addCapacity(stats.InFlightReservedPhysical, p.Physical)); err != nil {
			_ = store.SetGuaranteedReserve(stats.GuaranteedRemainingPhysical)
			return err
		}
		_, err = r.persist(rangeReserved, rangeEvent{ID: id, Plan: wire, SourceBoot: boot})
		if err != nil {
			_ = store.SetInFlightReserve(stats.InFlightReservedPhysical)
			_ = store.SetGuaranteedReserve(stats.GuaranteedRemainingPhysical)
		}
		return err
	}
	_, err = r.persist(rangeReserved, rangeEvent{ID: id, Plan: wire, SourceBoot: boot})
	return err
}
func mustRangeWire(p rangePlan) []byte { wire, _ := p.marshal(); return wire }
func rangePlansOverlap(a, b rangePlan) bool {
	if a.Before.Bucket != b.Before.Bucket {
		return false
	}
	return (b.Before.ToEnd || bytes.Compare(a.Before.Begin[:], b.Before.End[:]) < 0) && (a.Before.ToEnd || bytes.Compare(b.Before.Begin[:], a.Before.End[:]) < 0)
}
func (r *rangeManager) destination(p rangePlan) (*volume.Volume, error) {
	s := r.server
	s.mu.RLock()
	managed := s.volumes[p.Volume.String()]
	node := s.cfg.Node.ID
	var config VolumeConfig
	if managed != nil {
		config = managed.config
	}
	s.mu.RUnlock()
	if managed == nil || len(config.PlacementGroupsPending) > 0 || config.System || config.State != "active" && config.State != "draining" {
		return nil, volume.ErrFull
	}
	b, err := s.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return nil, err
	}
	d := b.Describe()
	policy := d.Policy
	if !p.ReplicaRemove && d.DesiredPolicy != nil && p.movingPolicyVersion() == d.DesiredPolicy.Version {
		policy = *d.DesiredPolicy
	}
	if !p.ReplicaRemove && p.movingPolicyVersion() != policy.Version {
		return nil, errControlConflict
	}
	for _, entry := range p.After.Entries {
		if entry.Start == p.Moving.Start && entry.PrefixBits == p.Moving.PrefixBits {
			for _, loc := range entry.Locations {
				if loc.Node == p.Target && loc.Volume == client.ID(p.Volume) && loc.Class != uint8(managed.store.Identity().Class) {
					return nil, errControlConflict
				}
			}
		}
	}
	if p.Compression != bucketCodec(policy) {
		return nil, errControlConflict
	}
	placementPolicy := policy
	if !p.ReplicaRemove && d.DesiredPolicy != nil && (p.ReplicaAdd || p.ReplicaRelocate || p.ReplicaPromote) {
		placementPolicy = *d.DesiredPolicy
	}
	if !bucketClass(placementPolicy).Allows(managed.store.Identity().Class) || !s.placementGroups.Load().allowsVersion(placementPolicy.PlacementGroup, placementPolicy.PlacementMaskVersion, node, p.Volume.String()) {
		return nil, volume.ErrFull
	}
	return managed.store, nil
}
func (r *rangeManager) fence(id [32]byte) error {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	t := r.txns[id]
	r.mu.Unlock()
	if t == nil {
		return errControlConflict
	}
	if err := r.server.checkPendingReplicas(func(k volume.Key) bool {
		return client.ID(k.Bucket) == t.plan.Before.Bucket && t.plan.contains(k.Object)
	}); err != nil {
		return err
	}
	t.lease.Lock()
	defer t.lease.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(nil); err != nil {
		return err
	}
	if t.aborted || t.forceRetired {
		return errControlConflict
	}
	if t.fenced {
		return nil
	}
	if t.plan.Source != r.node {
		return errControlConflict
	}
	if err := r.server.invalidateVolumeAuthorityLocked(t.plan); err != nil {
		return err
	}
	_, err := r.persist(rangeFenced, rangeEvent{ID: id})
	if err == nil {
		r.server.objectSubscriptions.resyncWhere(func(key volume.Key) bool {
			return client.ID(key.Bucket) == t.plan.Before.Bucket && t.plan.contains(key.Object)
		})
	}
	return err
}
func (r *rangeManager) ready(id, digest [32]byte, source bool, logicalUsed ...uint64) (client.Proof, error) {
	return r.readyWithForce(id, digest, source, nil, logicalUsed...)
}
func (r *rangeManager) readyWithForce(id, digest [32]byte, source bool, force *controlClaim, logicalUsed ...uint64) (client.Proof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(force); err != nil {
		return client.Proof{}, err
	}
	t := r.txns[id]
	if t == nil {
		return client.Proof{}, errControlConflict
	}
	if t.aborted || t.forceRetired || digest == ([32]byte{}) || source && (t.plan.Source != r.node || !t.fenced) || !source && t.plan.Target != r.node || t.digest != ([32]byte{}) && t.digest != digest {
		return client.Proof{}, errControlConflict
	}
	used := t.pendingUsed
	if len(logicalUsed) > 0 {
		used = logicalUsed[0]
	}
	kind := rangeTargetReady
	proof := t.targetReady
	if source {
		kind = rangeSourceReady
		proof = t.sourceReady
	}
	if proof.RecordSequence != 0 {
		if t.digest != digest || t.logicalUsed != used || t.quotaUsed != func() uint64 {
			if len(logicalUsed) > 1 {
				return logicalUsed[1]
			}
			return t.pendingQuota
		}() {
			return client.Proof{}, errControlConflict
		}
		if !source && t.plan.ram() && t.targetBoot != r.server.boot {
			if _, err := r.persist(rangeTargetRestored, rangeEvent{ID: id, Digest: digest, Boot: r.server.boot}); err != nil {
				return client.Proof{}, err
			}
		}
		return proof, nil
	}
	quotaUsed := t.pendingQuota
	if len(logicalUsed) > 1 {
		quotaUsed = logicalUsed[1]
	}
	event := rangeEvent{ID: id, Digest: digest, LogicalUsed: used, QuotaUsed: quotaUsed}
	if !source && t.plan.ram() {
		event.Boot = r.server.boot
	}
	return r.persist(kind, event)
}
func (r *rangeManager) commit(c rangeCommit) (client.Proof, error) {
	return r.commitWithForce(c, nil)
}
func (r *rangeManager) commitWithForce(c rangeCommit, force *controlClaim) (proof client.Proof, err error) {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	activation := r.txns[c.ID]
	needsActivation := activation != nil && activation.plan.Target == r.node && activation.decision == nil && !activation.plan.ReplicaRemove && !activation.plan.ReplicaPromote
	needsSource := activation != nil && activation.plan.Source == r.node && activation.decision == nil
	r.mu.Unlock()
	if needsSource {
		if err := r.checkPhysicalSourceLocked(activation, c.QuotaUsed); err != nil {
			return client.Proof{}, err
		}
	}
	if needsActivation {
		if err := r.checkPhysicalActivationLocked(activation, c.QuotaUsed); err != nil {
			return client.Proof{}, err
		}
	}
	r.mu.Lock()
	defer func() {
		txn := r.txns[c.ID]
		var store *volume.Volume
		var before, budget uint64
		if txn != nil && txn.plan.Target == r.node {
			store, _ = r.destination(txn.plan)
			budget = txn.remaining
			if store != nil {
				before = store.Stats().InFlightReservedPhysical
			}
		}
		r.mu.Unlock()
		if err == nil {
			if err = r.server.refreshQuotaProtectionLocked(); err != nil {
				return
			}
			if store != nil {
				err = r.server.ensureVolumeAuthorityLocked(map[volume.ID]uint64{store.Identity().VolumeID: budget})
				r.consumed(txn, store, before)
			} else {
				err = r.server.ensureVolumeAuthorityLocked()
			}
		}
	}()
	if err := r.server.scopedTransition(force); err != nil {
		return client.Proof{}, err
	}
	t := r.txns[c.ID]
	if t == nil || !r.validCommit(t, c, false) {
		return client.Proof{}, errControlConflict
	}
	if t.decision != nil {
		if *t.decision != c {
			return client.Proof{}, errControlConflict
		}
		return t.commitProof, nil
	}
	if t.plan.Target == r.node && t.plan.ram() && t.targetBoot != r.server.boot {
		return client.Proof{}, fail(503, "RAM READY belongs to a previous process")
	}
	proof, err = r.persist(rangeCommitted, rangeEvent{ID: c.ID, Commit: &c})
	if err == nil {
		err = r.rebuildLocked()
	}
	return proof, err
}
func (r *rangeManager) observe(p rangePlan, c rangeCommit) error {
	if _, err := p.marshal(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.txns[c.ID]; t != nil {
		if t.decision != nil && *t.decision == c {
			return nil
		}
		return errControlConflict
	}
	if c.ID != p.id() {
		return errControlConflict
	}
	if p.Source == r.node || p.Target == r.node || !r.validCommit(&rangeTxn{plan: p}, c, true) {
		return errControlConflict
	}
	if _, err := r.persist(rangeObserved, rangeEvent{ID: c.ID, Plan: mustRangeWire(p), Commit: &c}); err != nil {
		return err
	}
	return r.rebuildLocked()
}
func (r *rangeManager) done(id [32]byte, targetProof *client.Proof) (client.Proof, error) {
	r.server.opMu.Lock()
	defer r.server.opMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(nil); err != nil {
		return client.Proof{}, err
	}
	t := r.txns[id]
	if t == nil || t.decision == nil || t.forceRetired {
		return client.Proof{}, errControlConflict
	}
	if targetProof == nil {
		if t.plan.Target != r.node {
			return client.Proof{}, errControlConflict
		}
		if t.targetDone.RecordSequence != 0 {
			return t.targetDone, r.releaseReserveLocked(t)
		}
		proof, err := r.persist(rangeTargetDone, rangeEvent{ID: id})
		if err == nil {
			err = r.releaseReserveLocked(t)
		}
		return proof, err
	}
	if t.plan.Source != r.node || targetProof.Node != t.plan.Target {
		return client.Proof{}, errControlConflict
	}
	if _, err := targetProof.MarshalBinary(); err != nil {
		return client.Proof{}, err
	}
	if t.sourceDone.RecordSequence != 0 {
		return t.sourceDone, nil
	}
	return r.persist(rangeSourceDone, rangeEvent{ID: id, TargetDone: *targetProof})
}

func (r *rangeManager) guard(bucket volume.ID, object volume.ObjectID) (func(), error) {
	r.mu.Lock()
	var active []*rangeTxn
	for _, t := range r.txns {
		if r.active(t) && volume.ID(t.plan.Before.Bucket) == bucket && t.plan.contains(object) {
			active = append(active, t)
		}
	}
	r.mu.Unlock()
	for _, t := range active {
		t.lease.RLock()
	}
	release := func() {
		for _, t := range active {
			t.lease.RUnlock()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range active {
		if t.plan.Source == r.node && t.fenced && !(t.plan.Target == r.node && t.decision != nil) {
			release()
			return nil, fail(503, "range is fenced for durable handoff")
		}
	}
	return release, nil
}
func (r *rangeManager) incoming(bucket volume.ID, object volume.ObjectID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.txns {
		if !t.plan.ReplicaRemove && !t.plan.ReplicaPromote && !t.plan.ReplicaRelocate && r.active(t) && t.plan.Target == r.node && t.plan.Source != r.node && t.decision == nil && volume.ID(t.plan.Before.Bucket) == bucket && t.plan.contains(object) {
			return true
		}
	}
	return false
}

// A global checkpoint may already contain older handoffs. Replay never rolls
// their descendants back. Missing lineage is retained for later peer catchup.
func (r *rangeManager) rebuildLocked() error {
	view := r.server.currentSystem.Load()
	if view == nil {
		return nil
	}
	patches := append([]client.MapPatch{}, view.patches...)
	// Journal arrival order is not causal order after a partition. Index
	// transitions by their complete BEFORE binding and replay descendants as
	// their parents become available. Disjoint scopes commute naturally.
	type binding struct {
		bucket client.ID
		digest [32]byte
	}
	key := func(bucket client.ID, entry client.RangeEntry) binding {
		raw, _ := json.Marshal(entry)
		return binding{bucket, sha256.Sum256(raw)}
	}
	waiting := make(map[binding]*rangeTxn)
	for _, id := range r.order {
		t := r.txns[id]
		if t.decision == nil {
			continue
		}
		k := key(t.plan.Before.Bucket, t.plan.Before.Entries[0])
		if previous := waiting[k]; previous != nil && !reflect.DeepEqual(previous.plan.After.Entries, t.plan.After.Entries) {
			return volume.ErrCorrupt
		}
		waiting[k] = t
	}
	for i, patch := range patches {
		queue := append([]client.RangeEntry(nil), patch.Entries...)
		var entries []client.RangeEntry
		for len(queue) > 0 {
			entry := queue[0]
			queue = queue[1:]
			k := key(patch.Bucket, entry)
			if t := waiting[k]; t != nil {
				delete(waiting, k)
				queue = append(queue, t.plan.After.Entries...)
			} else {
				entries = append(entries, entry)
			}
		}
		sort.Slice(entries, func(a, b int) bool { return bytes.Compare(entries[a].Start[:], entries[b].Start[:]) < 0 })
		patches[i].Entries = entries
	}
	image, err := buildPlacement(patches)
	if err != nil {
		return err
	}
	r.server.resyncMovedSubscriptions(image)
	r.server.placement.Store(image)
	r.server.dirtyVolumeAuthority()
	return nil
}
