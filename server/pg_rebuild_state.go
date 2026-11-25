// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	peerPGRebuildRequest uint16 = 0x10a0
	peerPGRebuildState   uint16 = 0x10a1
	peerPGRebuildArchive uint16 = 0x10a2
	peerPGRebuildRanges  uint16 = 0x10a4
)

// A PG generation is a placement round, never an object content version. The
// immutable binding includes the PG, prior round, membership and policy mask.
// The lowest admitted PG member coordinates; transport loss never elects a new
// owner. Other PGs have independent gates, journals and execution workers.
type pgRebuildRound struct {
	Cancelled                    bool `json:",omitempty"`
	Group                        client.ID
	ID, Parent                   [32]byte
	Generation, Membership, Mask uint64
	Members                      []client.ID
	Plan                         []byte
	Done                         client.Proof
	Commit                       *rangeCommit `json:",omitempty"`
}
type pgRebuildWork struct {
	Round   pgRebuildRound
	Started bool
}
type pgRebuildState struct {
	Generation uint64
	Head       [32]byte
	Active     *pgRebuildWork
}
type pgRebuildRequest struct {
	Commit *rangeCommit `json:",omitempty"`
	Action uint8
	Plan   []byte
	Done   client.Proof
}
type pgRebuildStateRequest struct {
	Action uint8
	Round  pgRebuildRound
}
type pgRebuildRuntime struct {
	rpcOnce    sync.Once
	rpcSlots   chan struct{}
	gates      sync.Map // PG ID -> bounded coordinator gate
	leaseGates sync.Map // bucket ID -> independent maintenance coordinator gate
	workersMu  sync.Mutex
	workers    map[string]pgRebuildWorker
}

type pgRebuildInfo struct {
	Generation   string      `json:"generation"`
	State        string      `json:"state"`
	Round        string      `json:"round,omitempty"`
	Coordinator  string      `json:"coordinator,omitempty"`
	Participants []client.ID `json:"participants,omitempty"`
}

func (r *rangeManager) pgInfo(group string) *pgRebuildInfo {
	id, err := bucketID(group)
	if err != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.pgStates[client.ID(id)]
	out := &pgRebuildInfo{Generation: "0", State: "idle"}
	if state != nil {
		out.Generation = strconv.FormatUint(state.Generation, 10)
		if state.Active != nil {
			a := state.Active.Round
			out.State = "preparing"
			if state.Active.Started {
				out.State = "rebuilding"
			}
			out.Round = hex.EncodeToString(a.ID[:])
			out.Coordinator = a.Members[0].String()
			out.Participants = append([]client.ID(nil), a.Members...)
		}
	}
	return out
}
func (s *Server) pgMembers(d BucketDescription) ([]client.ID, uint64, error) {
	return pgMembersFrom(d, s.currentSystem.Load(), s.placementGroups.Load())
}
func pgMembersFrom(d BucketDescription, view *systemView, groups *placementGroupImage) ([]client.ID, uint64, error) {
	if view == nil || groups == nil {
		return nil, 0, fail(503, "PG membership unavailable")
	}
	policy := d.Policy
	mask, ok := groups.mask(policy.PlacementGroup, policy.PlacementMaskVersion)
	if !ok {
		return nil, 0, fail(503, "PG mask unavailable")
	}
	allowed := func(id client.ID) bool {
		if mask.NodeSelection == "all" {
			return true
		}
		for _, n := range mask.Nodes {
			if n.NodeID == id.String() {
				return n.Mode != VolumesWhitelist || len(n.Volumes) > 0
			}
		}
		return false
	}
	var members []client.ID
	for _, m := range view.manifest.Membership.Members {
		if allowed(m.ID) {
			members = append(members, m.ID)
		}
	}
	// A released PG migration may need both old and new masks until cutover.
	if d.DesiredPolicy != nil {
		next, ok := groups.mask(d.DesiredPolicy.PlacementGroup, d.DesiredPolicy.PlacementMaskVersion)
		if !ok {
			return nil, 0, fail(503, "target PG mask unavailable")
		}
		for _, m := range view.manifest.Membership.Members {
			eligible := next.NodeSelection == "all"
			for _, n := range next.Nodes {
				if n.NodeID == m.ID.String() && (n.Mode != VolumesWhitelist || len(n.Volumes) > 0) {
					eligible = true
				}
			}
			if eligible && !hasControlNode(members, m.ID) {
				members = append(members, m.ID)
			}
		}
	}
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i][:], members[j][:]) < 0 })
	if len(members) == 0 {
		return nil, 0, fail(503, "PG has no admitted participants")
	}
	return members, mask.Version, nil
}
func (s *Server) pgForPlan(p rangePlan) (client.ID, []client.ID, uint64, error) {
	b, err := s.Bucket(volume.ID(p.Before.Bucket))
	if err != nil {
		return client.ID{}, nil, 0, err
	}
	d := b.Describe()
	if d.Policy.PlacementGroup == "" {
		return client.ID{}, nil, 0, nil
	}
	group, err := bucketID(d.Policy.PlacementGroup)
	if err != nil || group == systemPlacementGroup {
		return client.ID{}, nil, 0, errControlConflict
	}
	members, mask, err := s.pgMembers(d)
	return client.ID(group), members, mask, err
}
func samePGRound(a, b pgRebuildRound) bool {
	return a.Group == b.Group && a.ID == b.ID && a.Parent == b.Parent && a.Generation == b.Generation && a.Membership == b.Membership && a.Mask == b.Mask && bytes.Equal(a.Plan, b.Plan) && equalIDs(a.Members, b.Members)
}
func samePGCompletion(a, b pgRebuildRound) bool {
	return samePGRound(a, b) && a.Cancelled == b.Cancelled && a.Done == b.Done && reflect.DeepEqual(a.Commit, b.Commit)
}
func equalIDs(a, b []client.ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func validPGRound(a pgRebuildRound) bool {
	p, err := decodeRangePlan(a.Plan)
	if err != nil || a.Group == (client.ID{}) || a.ID != p.id() || a.Generation == 0 || a.Membership != p.Before.Membership || a.Mask == 0 || len(a.Members) == 0 || !hasControlNode(a.Members, p.Source) || !hasControlNode(a.Members, p.Target) {
		return false
	}
	for i, m := range a.Members {
		if m == (client.ID{}) || i > 0 && bytes.Compare(a.Members[i-1][:], m[:]) >= 0 {
			return false
		}
	}
	return true
}
func validPGCompletion(a pgRebuildRound) bool {
	if !validPGRound(a) {
		return false
	}
	p := mustDecodePGPlan(a.Plan)
	if a.Done.Node != p.Source || a.Done.RecordSequence == 0 {
		return false
	}
	if _, err := a.Done.MarshalBinary(); err != nil {
		return false
	}
	if a.Cancelled {
		return a.Commit == nil
	}
	if a.Commit == nil {
		return false
	}
	return (&rangeManager{}).validCommit(&rangeTxn{plan: p}, *a.Commit, true)
}

func (r *rangeManager) replayPG(kind uint16, a *pgRebuildRound) error {
	if a == nil || !validPGRound(*a) {
		return volume.ErrCorrupt
	}
	if r.pgStates == nil {
		r.pgStates = map[client.ID]*pgRebuildState{}
		r.pgRounds = map[[32]byte]pgRebuildRound{}
	}
	state := r.pgStates[a.Group]
	if state == nil {
		state = &pgRebuildState{}
		r.pgStates[a.Group] = state
	}
	if kind == rangePGSourceCancelled {
		return nil
	}
	if kind == rangePGPrepared {
		if state.Active != nil || a.Generation != state.Generation+1 || a.Parent != state.Head {
			return volume.ErrCorrupt
		}
		state.Active = &pgRebuildWork{Round: *a}
		return nil
	}
	if (kind == rangePGImported || kind == rangePGCompleted) && !validPGCompletion(*a) {
		return volume.ErrCorrupt
	}
	if kind == rangePGImported {
		if state.Active != nil || a.Generation != state.Generation+1 || a.Parent != state.Head || a.Done.RecordSequence == 0 {
			return volume.ErrCorrupt
		}
	} else {
		if state.Active == nil || !samePGRound(state.Active.Round, *a) {
			return volume.ErrCorrupt
		}
		if kind == rangePGStarted {
			state.Active.Started = true
			delete(r.pgOffers, a.Group)
			return nil
		}
		if kind != rangePGCompleted || !state.Active.Started || a.Done.RecordSequence == 0 {
			return volume.ErrCorrupt
		}
	}
	p, _ := decodeRangePlan(a.Plan)
	if a.Done.Node != p.Source {
		return volume.ErrCorrupt
	}
	if _, err := a.Done.MarshalBinary(); err != nil {
		return err
	}
	state.Generation = a.Generation
	state.Head = a.ID
	state.Active = nil
	delete(r.pgOffers, a.Group)
	r.pgRounds[a.ID] = *a
	r.pgOrder = append(r.pgOrder, a.ID)
	return nil
}
func (r *rangeManager) pgCall(ctx context.Context, node client.ID, opcode uint16, request any) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	return (peerControlParticipants{runtime: r.server.control}).call(ctx, node, opcode, body)
}
func (s *Server) pgRebuildPeer(ctx context.Context, m client.Message, actor client.ID) ([]byte, error) {
	if m.Kind != client.Request || s.scopes == nil || len(m.Body) > client.MaxMessage-256 {
		return nil, client.ErrProtocol
	}
	r := s.scopes
	if m.Opcode == peerPGRebuildCodec {
		return s.applyPGCodec(ctx, m.Body, actor)
	}
	if m.Opcode == peerPGRebuildRanges {
		var req pgArchiveRequest
		if json.Unmarshal(m.Body, &req) != nil {
			return nil, client.ErrProtocol
		}
		return r.archivePage(req.After, &req.Group)
	}
	if m.Opcode == peerPGRebuildArchive {
		return r.pgArchive(m.Body)
	}
	if m.Opcode == peerPGRebuildState {
		var req pgRebuildStateRequest
		if json.Unmarshal(m.Body, &req) != nil || !validPGRound(req.Round) {
			return nil, client.ErrProtocol
		}
		a := req.Round
		if req.Action > 2 || req.Action == 2 && !validPGCompletion(a) {
			return nil, client.ErrProtocol
		}
		if actor != a.Members[0] || !hasControlNode(a.Members, r.node) {
			return nil, ErrBucketForbidden
		}
		// A newly admitted participant imports the committed PG chain before READY.
		r.mu.Lock()
		state := r.pgStates[a.Group]
		generation := uint64(0)
		if state != nil {
			generation = state.Generation
		}
		r.mu.Unlock()
		if generation+1 < a.Generation {
			if err := r.syncPGArchive(ctx, actor, a.Group); err != nil {
				return nil, err
			}
		}
		s.opMu.Lock()
		defer s.opMu.Unlock()
		group, members, mask, err := s.pgForPlan(mustDecodePGPlan(a.Plan))
		if err != nil {
			return nil, err
		}
		if s.controlBlocksPG(mustDecodePGPlan(a.Plan).Before.Bucket) {
			return nil, fail(503, "PG configuration change is pending")
		}
		if group != a.Group || !equalIDs(members, a.Members) || mask != a.Mask || s.currentSystem.Load().manifest.Membership.Epoch != a.Membership {
			return nil, errControlConflict
		}
		r.mu.Lock()
		old, completed := r.pgRounds[a.ID]
		r.mu.Unlock()
		if completed {
			if !samePGRound(old, a) || req.Action == 2 && !samePGCompletion(old, a) {
				return nil, errControlConflict
			}
			return nil, nil
		}
		if req.Action == 2 && a.Commit != nil && !a.Cancelled {
			if a.Commit.ID != a.ID {
				return nil, errControlConflict
			}
			if err := r.observe(mustDecodePGPlan(a.Plan), *a.Commit); err != nil {
				return nil, err
			}
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		state = r.pgStates[a.Group]
		if old, ok := r.pgRounds[a.ID]; ok {
			if !samePGRound(old, a) || req.Action == 2 && !samePGCompletion(old, a) {
				return nil, errControlConflict
			}
			return nil, nil
		}
		if req.Action == 0 {
			if state != nil && state.Active != nil {
				if samePGRound(state.Active.Round, a) {
					return nil, nil
				}
				return nil, fail(503, "PG rebuild already reserved")
			}
			generation := uint64(0)
			var head [32]byte
			if state != nil {
				generation = state.Generation
				head = state.Head
			}
			if generation == math.MaxUint64 || a.Generation != generation+1 || a.Parent != head {
				return nil, errControlConflict
			}
			_, err = r.persist(rangePGPrepared, rangeEvent{PG: &a})
			return nil, err
		}
		if state == nil || state.Active == nil || !samePGRound(state.Active.Round, a) {
			return nil, errControlConflict
		}
		kind := uint16(rangePGStarted)
		if req.Action == 2 {
			kind = rangePGCompleted
		} else if req.Action != 1 {
			return nil, client.ErrProtocol
		}
		if req.Action == 1 && state.Active.Started {
			return nil, nil
		}
		if req.Action == 2 && !state.Active.Started {
			return nil, errControlConflict
		}
		_, err = r.persist(kind, rangeEvent{PG: &a})
		return nil, err
	}
	var req pgRebuildRequest
	if m.Opcode != peerPGRebuildRequest || json.Unmarshal(m.Body, &req) != nil || req.Action > 2 {
		return nil, client.ErrProtocol
	}
	p, err := decodeRangePlan(req.Plan)
	if err != nil || p.Source != actor {
		return nil, ErrBucketForbidden
	}
	group, members, mask, err := s.pgForPlan(p)
	if err != nil {
		return nil, err
	}
	if group == (client.ID{}) || members[0] != r.node {
		return nil, ErrBucketForbidden
	}
	gateValue, _ := r.pgRuntime.gates.LoadOrStore(group, make(chan struct{}, 1))
	gate := gateValue.(chan struct{})
	select {
	case gate <- struct{}{}:
		defer func() { <-gate }()
	default:
		return nil, fail(503, "PG coordinator is already processing this group")
	}
	// Import any committed history the current coordinator missed while offline.
	for _, node := range members {
		if node != r.node {
			if err = r.syncPGArchive(ctx, node, group); err != nil {
				return nil, err
			}
		}
	}
	r.mu.Lock()
	state := r.pgStates[group]
	if completed, ok := r.pgRounds[p.id()]; ok {
		r.mu.Unlock()
		// A reply or the final source acknowledgement may have been lost.
		// Completion at the coordinator alone does not release all members.
		if err := r.completePG(ctx, completed); err != nil {
			return nil, err
		}
		return json.Marshal(completed)
	}
	var round pgRebuildRound
	if state != nil && state.Active != nil {
		round = state.Active.Round
		if round.ID != p.id() {
			r.mu.Unlock()
			return nil, fail(503, "another rebuild is active in this PG")
		}
	} else {
		round = pgRebuildRound{Group: group, ID: p.id(), Generation: 1, Membership: p.Before.Membership, Mask: mask, Members: members, Plan: req.Plan}
		if state != nil {
			if state.Generation == math.MaxUint64 {
				r.mu.Unlock()
				return nil, volume.ErrRange
			}
			round.Generation = state.Generation + 1
			round.Parent = state.Head
		}
	}
	r.mu.Unlock()
	if !equalIDs(round.Members, members) || round.Mask != mask {
		return nil, errControlConflict
	}
	// Reserve/activate on the fixed PG participants only. No global slot is held.
	for _, action := range []uint8{0, 1} {
		for _, node := range round.Members {
			if _, err = r.pgCall(ctx, node, peerPGRebuildState, pgRebuildStateRequest{action, round}); err != nil {
				return nil, err
			}
		}
	}
	if req.Action == 1 || req.Action == 2 {
		round.Cancelled = req.Action == 2
		if req.Done.Node != p.Source || req.Done.RecordSequence == 0 {
			return nil, client.ErrProtocol
		}
		round.Done = req.Done
		round.Commit = req.Commit
		if err = r.completePG(ctx, round); err != nil {
			return nil, err
		}
	}
	return json.Marshal(round)
}

// Release the source last, including retries after the coordinator committed.
func (r *rangeManager) completePG(ctx context.Context, round pgRebuildRound) error {
	p := mustDecodePGPlan(round.Plan)
	var order []client.ID
	for _, node := range round.Members {
		if node != p.Source && node != round.Members[0] {
			order = append(order, node)
		}
	}
	if round.Members[0] != p.Source {
		order = append(order, round.Members[0])
	}
	order = append(order, p.Source)
	for _, node := range order {
		if _, err := r.pgCall(ctx, node, peerPGRebuildState, pgRebuildStateRequest{2, round}); err != nil {
			return err
		}
	}
	return nil
}
func mustDecodePGPlan(wire []byte) rangePlan { p, _ := decodeRangePlan(wire); return p }
func (r *rangeManager) beginPG(ctx context.Context, p rangePlan) error {
	group, members, _, err := r.server.pgForPlan(p)
	if err != nil || group == (client.ID{}) {
		return err
	}
	if p.Source == r.node {
		r.mu.Lock()
		_, completed := r.pgRounds[p.id()]
		state := r.pgStates[group]
		active := state != nil && state.Active != nil && state.Active.Round.ID == p.id()
		if !completed && !active {
			if old, ok := r.pgOffers[group]; ok {
				if old.id() != p.id() {
					r.mu.Unlock()
					return fail(503, "a persisted PG proposal must finish first")
				}
			} else {
				_, err = r.persist(rangePGOffered, rangeEvent{PGGroup: group, Plan: mustRangeWire(p)})
				if err != nil {
					r.mu.Unlock()
					return err
				}
			}
		}
		r.mu.Unlock()
	}
	body, err := r.pgCall(ctx, members[0], peerPGRebuildRequest, pgRebuildRequest{Plan: mustRangeWire(p)})
	if err != nil {
		return err
	}
	var round pgRebuildRound
	if json.Unmarshal(body, &round) != nil {
		return client.ErrProtocol
	}
	if round.Cancelled {
		return errControlConflict
	}
	return nil
}
func (r *rangeManager) finishPG(ctx context.Context, p rangePlan) error {
	group, members, _, err := r.server.pgForPlan(p)
	if err != nil || group == (client.ID{}) {
		return err
	}
	r.mu.Lock()
	t := r.txns[p.id()]
	var done client.Proof
	if t != nil {
		done = t.sourceDone
	}
	var commit *rangeCommit
	if t != nil && t.decision != nil {
		copy := *t.decision
		commit = &copy
	}
	r.mu.Unlock()
	_, err = r.pgCall(ctx, members[0], peerPGRebuildRequest, pgRebuildRequest{Action: 1, Plan: mustRangeWire(p), Done: done, Commit: commit})
	return err
}

// Archives contain completed generations, including the complete prior binding.
// Only a contiguous chain can be imported; conflicting equal counters are fatal.
type pgArchiveRequest struct {
	Group client.ID
	After uint64
}

func (r *rangeManager) pgArchive(body []byte) ([]byte, error) {
	var req pgArchiveRequest
	if json.Unmarshal(body, &req) != nil {
		return nil, client.ErrProtocol
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rounds := []pgRebuildRound{}
	for _, id := range r.pgOrder {
		a := r.pgRounds[id]
		if a.Group == req.Group && a.Generation > req.After {
			candidate, err := json.Marshal(append(rounds, a))
			if err != nil {
				return nil, err
			}
			if len(candidate) > 1<<20 && len(rounds) > 0 {
				break
			}
			rounds = append(rounds, a)
			if len(rounds) == 16 {
				break
			}
		}
	}
	return json.Marshal(rounds)
}
func (r *rangeManager) syncPGArchive(ctx context.Context, node, group client.ID) error {
	if node == r.node {
		return nil
	}
	for {
		r.mu.Lock()
		after := uint64(0)
		if state := r.pgStates[group]; state != nil {
			after = state.Generation
		}
		r.mu.Unlock()
		body, err := r.pgCall(ctx, node, peerPGRebuildArchive, pgArchiveRequest{Group: group, After: after})
		if err != nil {
			return err
		}
		var rounds []pgRebuildRound
		if json.Unmarshal(body, &rounds) != nil || len(rounds) > 16 {
			return client.ErrProtocol
		}
		if len(rounds) == 0 {
			return nil
		}
		for _, a := range rounds {
			if !validPGCompletion(a) || a.Group != group || a.Generation <= after {
				err = client.ErrProtocol
				break
			}
			if a.Commit != nil && !a.Cancelled {
				if err = r.observe(mustDecodePGPlan(a.Plan), *a.Commit); err != nil {
					break
				}
			}
			r.mu.Lock()
			if old, ok := r.pgRounds[a.ID]; ok {
				if !samePGCompletion(old, a) {
					r.mu.Unlock()
					err = errControlConflict
					break
				}
				r.mu.Unlock()
				continue
			}
			state := r.pgStates[group]
			if state != nil && state.Active != nil {
				if !samePGRound(state.Active.Round, a) {
					r.mu.Unlock()
					err = errControlConflict
					break
				}
				if !state.Active.Started {
					err = errControlConflict
				} else {
					_, err = r.persist(rangePGCompleted, rangeEvent{PG: &a})
				}
			} else {
				generation := uint64(0)
				var head [32]byte
				if state != nil {
					generation = state.Generation
					head = state.Head
				}
				if generation == math.MaxUint64 || a.Generation != generation+1 || a.Parent != head {
					err = errControlConflict
				} else {
					_, err = r.persist(rangePGImported, rangeEvent{PG: &a})
				}
			}
			r.mu.Unlock()
			if err != nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}
}

// A normal global metadata action must not serialize unrelated PG work.
// Membership/exclusion and explicitly fenced bucket changes still interlock.
func (s *Server) controlBlocksPG(bucket client.ID) bool {
	if s.control == nil {
		return false
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.slot == nil {
		return false
	}
	claim := l.slot.Claim
	return claim.GlobalBarrier || claim.Exclusion != nil || hasControlNode(claim.FrozenBuckets, bucket) || l.slot.Proposal != nil && (l.slot.Proposal.Kind == controlMembership || l.slot.Proposal.Kind == controlShutdown)
}

// Reconnection retries archive catch-up independently of telemetry and other
// PG workers. Each peer has its own bounded attempt; silence changes no owner.
func (s *Server) catchupRangePeer(p *peerConnection) {
	if s.scopes == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
		defer cancel()
		if s.scopes.syncArchiveFrom(ctx, p.Node) != nil {
			return
		}
		for _, g := range s.placementGroups.Load().object.Groups {
			id, err := bucketID(g.ID)
			if err == nil && id != systemPlacementGroup {
				_ = s.scopes.syncPGArchive(ctx, p.Node, client.ID(id))
			}
		}
	}()
}

// Persist cancellation before asking participants to close a generation whose
// range never fenced, or was durably aborted. Late activations cannot revive it.
func (r *rangeManager) abortPG(ctx context.Context, p rangePlan) error {
	group, members, _, err := r.server.pgForPlan(p)
	if err != nil || group == (client.ID{}) {
		return err
	}
	r.mu.Lock()
	state := r.pgStates[group]
	t := r.txns[p.id()]
	if state == nil || state.Active == nil || state.Active.Round.ID != p.id() || t != nil && !t.aborted {
		r.mu.Unlock()
		return nil
	}
	round := state.Active.Round
	proof, err := r.persist(rangePGSourceCancelled, rangeEvent{PG: &round})
	r.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = r.pgCall(ctx, members[0], peerPGRebuildRequest, pgRebuildRequest{Action: 2, Plan: mustRangeWire(p), Done: proof})
	return err
}

func (r *rangeManager) ensurePGPrepared(ctx context.Context, p rangePlan) error {
	group, _, _, err := r.server.pgForPlan(p)
	if err != nil || group == (client.ID{}) {
		return err
	}
	r.mu.Lock()
	state := r.pgStates[group]
	ready := state != nil && state.Active != nil && state.Active.Started && state.Active.Round.ID == p.id()
	if completed, ok := r.pgRounds[p.id()]; ok {
		ready = !completed.Cancelled
	}
	r.mu.Unlock()
	if ready {
		return nil
	}
	if p.Source != r.node {
		return fail(409, "PG generation must be activated before reserving its ranges")
	}
	return r.beginPG(ctx, p)
}

// Only a committed membership change can retire an old participant binding.
// Range ownership is reconciled by the global membership/force protocol first;
// this clears scheduling shells, never decides data authority on a timeout.
func (r *rangeManager) retirePGMembership() error {
	view := r.server.currentSystem.Load()
	if view == nil {
		return nil
	}
	epoch := view.manifest.Membership.Epoch
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.pgStates {
		if state.Active == nil || state.Active.Round.Membership >= epoch {
			continue
		}
		a := state.Active.Round
		if _, err := r.persist(rangePGRetired, rangeEvent{PG: &a, RetiredMembership: epoch}); err != nil {
			return err
		}
	}
	for group, p := range r.pgOffers {
		if p.Before.Membership >= epoch {
			continue
		}
		if _, err := r.persist(rangePGOfferRetired, rangeEvent{PGGroup: group, Plan: mustRangeWire(p)}); err != nil {
			return err
		}
	}
	return nil
}
