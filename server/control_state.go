// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

var errControlConflict = errors.New("control: conflicting or out-of-order transition")

const (
	controlGenesis uint16 = 1 + iota
	controlReserve
	controlPrepare
	controlCommit
	controlApplied
	controlRelease
	controlAbort
	controlRequestBegin
	controlRequestPrepared
	controlRequestFinished
	controlRequestCancelled
	controlForceBuildStarted
	controlForceRequestBegin
	controlForceReserve
	controlForceSlotStarted
)

const (
	controlMembership      uint16 = 1
	controlAdministration  uint16 = 2
	controlShutdown        uint16 = 3
	controlForceDisconnect uint16 = 4
)

// Global membership/admin control is ALL of the bound membership, independently
// of the later range-scoped rebuild scheduler. No connection loss shrinks it.
type controlBase struct {
	Membership              client.Membership
	Sequence                uint64
	Head                    [32]byte
	Generation, MapRevision uint64
	SystemImage             [32]byte
}

type controlClaim struct {
	FrozenBuckets []client.ID
	Exclusion     *controlExclusion `json:",omitempty"`
	Round         client.RoundID
	Base          controlBase
	Required      []client.ID
}

type controlProposal struct {
	Claim          controlClaim
	Kind           uint16
	Image          [32]byte
	NextMembership *client.Membership
}

type controlSlot struct {
	Suspended                 *controlSlot `json:",omitempty"`
	ConstructionStarted       bool
	Claim                     controlClaim
	Proposal                  *controlProposal
	Decision                  client.Decision
	Ready, Committed, Applied client.Proof
	Certificate               client.Certificate
}

type controlLedger struct {
	activity      atomic.Pointer[controlActivity]
	mu            sync.Mutex
	journal       *controlJournal
	cluster, node client.ID
	current       controlBase
	slot          *controlSlot
	// Closed rounds prevent delayed RESERVE/PREPARE packets resurrecting an
	// aborted/released slot after a crash. Attempts are monotonically allocated
	// by their initiators; reclamation needs a later durable watermark.
	closed            map[client.RoundID]bool
	aborted           map[client.RoundID]client.Proof
	abortClaims       map[client.RoundID]controlClaim
	initialized       bool
	lastRelease       *controlCertificateRecord
	completed         map[client.Decision]controlCompletion
	ancestors         map[client.Decision]controlSlot
	lastReleasedProof client.Proof
	request           *controlRequest
	suspendedRequest  *controlRequest
	attempt           uint64
}

// Requester state survives independently of this node's participant slot: the
// first remote grant can precede its own grant, and its own release can precede
// receiving all final acknowledgements. Neither crash window may strand peers.
// Only prepared references and public results are stored, never raw passwords.
type controlRequest struct {
	ConstructionStarted bool
	Claim               controlClaim
	Kind                uint16
	Proposal            *controlProposal
	Result              []byte
	ResultReady         bool
}

type controlRequestResult struct {
	Round  client.RoundID
	Result []byte
}

type controlRequestFinish struct {
	Decision client.Decision
	Released client.Certificate
}

type controlCompletion struct {
	Slot    controlSlot
	Release controlCertificateRecord
	Proof   client.Proof
}

type controlRequestCancel struct {
	Round   client.RoundID
	Aborted client.Certificate
}

func membershipIDs(m client.Membership) []client.ID {
	ids := make([]client.ID, 0, len(m.Members))
	for _, member := range m.Members {
		ids = append(ids, member.ID)
	}
	return ids
}

func hasControlNode(nodes []client.ID, id client.ID) bool {
	for _, node := range nodes {
		if node == id {
			return true
		}
	}
	return false
}

func validateControlMembership(m client.Membership) bool {
	// Reuse the strict canonical identity codec, including duplicate/exclusion
	// checks. Epoch zero is explicitly the non-authoritative joining state.
	ack := client.NodeHelloAck{Membership: m}
	if m.Epoch != 0 {
		ack.Generation = 1
	}
	_, err := ack.MarshalBinary()
	return err == nil
}

func cloneControlBase(b controlBase) controlBase {
	// All persisted values are bounded control metadata. Detach the nested
	// address/tag slices so a caller cannot alter a promise after validation.
	raw, _ := json.Marshal(b)
	var copy controlBase
	_ = json.Unmarshal(raw, &copy)
	return copy
}

// A zero-length wire vector has no distinction between nil and an empty Go
// slice. Compare membership through its canonical codec rather than reflection
// so a network retry and a replayed JSON journal promise mean the same thing.
func equalControlMembership(a, b client.Membership) bool {
	x, err := controlMembershipWire(a)
	if err != nil {
		return false
	}
	y, err := controlMembershipWire(b)
	return err == nil && bytes.Equal(x, y)
}

func equalControlBase(a, b controlBase) bool {
	return a.Sequence == b.Sequence && a.Head == b.Head && a.Generation == b.Generation && a.MapRevision == b.MapRevision && a.SystemImage == b.SystemImage && equalControlMembership(a.Membership, b.Membership)
}

func equalControlClaim(a, b controlClaim) bool {
	return a.Round == b.Round && equalControlBase(a.Base, b.Base) && reflect.DeepEqual(a.Required, b.Required) && reflect.DeepEqual(a.FrozenBuckets, b.FrozenBuckets) && reflect.DeepEqual(a.Exclusion, b.Exclusion)
}

func equalControlProposal(a, b controlProposal) bool {
	if a.Kind != b.Kind || a.Image != b.Image || !equalControlClaim(a.Claim, b.Claim) || (a.NextMembership == nil) != (b.NextMembership == nil) {
		return false
	}
	return a.NextMembership == nil || equalControlMembership(*a.NextMembership, *b.NextMembership)
}

func (p controlProposal) decision() (client.Decision, error) {
	claim := p.Claim
	e := client.Encoder{}
	baseMembership, err := (client.NodeHelloAck{Membership: claim.Base.Membership, Generation: 1}).MarshalBinary()
	if err != nil {
		return client.Decision{}, err
	}
	e.Blob(baseMembership)
	e.U64(claim.Round.Membership)
	e.U64(claim.Round.Generation)
	e.ID(claim.Round.Initiator)
	e.U64(claim.Round.Attempt)
	e.U64(claim.Base.Generation)
	e.U64(claim.Base.MapRevision)
	e.B = append(e.B, claim.Base.SystemImage[:]...)
	e.V(uint64(len(claim.Required)))
	for _, node := range claim.Required {
		e.ID(node)
	}
	e.B = append(e.B, p.Image[:]...)
	e.Bool(p.NextMembership != nil)
	if p.NextMembership != nil {
		wire, err := (client.NodeHelloAck{Membership: *p.NextMembership, Generation: 1}).MarshalBinary()
		if err != nil {
			return client.Decision{}, err
		}
		e.Blob(wire)
	}
	if len(claim.FrozenBuckets) > 0 {
		e.Text("bucket mutation fences v1")
		e.V(uint64(len(claim.FrozenBuckets)))
		for _, id := range claim.FrozenBuckets {
			e.ID(id)
		}
	}
	if claim.Exclusion != nil {
		e.Text("externally fenced exclusion v1")
		encodeControlExclusion(&e, claim.Exclusion)
	}
	return client.NewDecision(claim.Base.Membership.Epoch, claim.Base.Sequence+1, claim.Base.Head, p.Kind, e.B)
}

func openControlLedger(path string, cluster, node volume.ID, initial *controlBase) (_ *controlLedger, err error) {
	l := &controlLedger{cluster: client.ID(cluster), node: client.ID(node), closed: make(map[client.RoundID]bool), aborted: make(map[client.RoundID]client.Proof), abortClaims: make(map[client.RoundID]controlClaim), completed: make(map[client.Decision]controlCompletion), ancestors: make(map[client.Decision]controlSlot)}
	j, err := openControlJournal(path, cluster, node, initial != nil, l.replay)
	if err != nil {
		return nil, err
	}
	l.journal = j
	defer func() {
		if err != nil {
			j.close()
		}
	}()
	if j.sequence == 0 {
		if initial == nil || !l.validBase(*initial) {
			return nil, volume.ErrCorrupt
		}
		if _, err = l.persist(controlGenesis, *initial); err != nil {
			return nil, err
		}
	}
	if !l.initialized || !l.validBase(l.current) {
		return nil, volume.ErrCorrupt
	}
	return l, nil
}

func (l *controlLedger) validBase(base controlBase) bool {
	if base.Membership.Cluster != l.cluster || !validateControlMembership(base.Membership) {
		return false
	}
	if base.Membership.Epoch == 0 {
		return base.Sequence == 0 && base.Head == ([32]byte{}) && base.Generation == 0 && base.MapRevision == 0 && base.SystemImage == ([32]byte{})
	}
	return base.Generation > 0 && (base.Sequence == 0 && base.Head == ([32]byte{}) || base.Sequence != 0 && base.Head != ([32]byte{}) && base.SystemImage != ([32]byte{}))
}

func (l *controlLedger) checkClaimShape(c controlClaim) bool {
	if len(c.Required) > 10000 || !l.validBase(c.Base) || c.Base.Membership.Epoch == 0 || c.Round.Membership != c.Base.Membership.Epoch || c.Round.Generation != c.Base.Generation || c.Round.Attempt == 0 || !hasControlNode(membershipIDs(c.Base.Membership), c.Round.Initiator) || !hasControlNode(c.Required, l.node) {
		return false
	}
	for i, id := range c.Required {
		if id == (client.ID{}) || i > 0 && bytes.Compare(c.Required[i-1][:], id[:]) >= 0 {
			return false
		}
	}
	if c.Exclusion != nil {
		if !validExclusionClaim(c) {
			return false
		}
	} else {
		for _, id := range membershipIDs(c.Base.Membership) {
			if !hasControlNode(c.Required, id) {
				return false
			}
		}
	}
	if len(c.FrozenBuckets) > 64 {
		return false
	}
	for i, id := range c.FrozenBuckets {
		if id == (client.ID{}) || id == client.ID(systemBucket) || i > 0 && bytes.Compare(c.FrozenBuckets[i-1][:], id[:]) >= 0 {
			return false
		}
	}
	return true
}

func (l *controlLedger) checkClaim(c controlClaim) bool {
	if !l.checkClaimShape(c) || l.closed[c.Round] {
		return false
	}
	if c.Exclusion != nil && c.Exclusion.ExpectedMembership != 0 && c.Exclusion.ExpectedMembership != c.Base.Membership.Epoch && !l.knownObservedMembership(c.Exclusion.ExpectedMembership, c.Base.Membership) {
		return false
	}
	if c.Exclusion != nil && len(c.Exclusion.AlreadyRetired) > 0 && !reflect.DeepEqual(c.Exclusion.AlreadyRetired, l.retiredVictims(c.Exclusion.ExpectedMembership, c.Exclusion.Nodes, c.Base.Membership)) {
		return false
	}
	if l.current.Membership.Epoch == 0 {
		return !hasControlNode(membershipIDs(c.Base.Membership), l.node)
	}
	return equalControlBase(l.current, c.Base)
}

func (l *controlLedger) checkProposal(p controlProposal) bool {
	if !l.checkClaim(p.Claim) || !validControlKind(p.Kind, p.Claim) || p.Image == ([32]byte{}) || p.Claim.Base.Sequence == ^uint64(0) {
		return false
	}
	switch p.Kind {
	case controlForceDisconnect:
		next, err := exclusionMembership(p.Claim.Base.Membership, p.Claim.Exclusion)
		if err != nil || p.NextMembership == nil || !equalControlMembership(next, *p.NextMembership) {
			return false
		}
	case controlAdministration, controlShutdown:
		if p.NextMembership != nil || len(p.Claim.Required) != len(p.Claim.Base.Membership.Members) {
			return false
		}
	case controlMembership:
		if p.NextMembership == nil || !validateControlMembership(*p.NextMembership) || p.NextMembership.Cluster != l.cluster || p.Claim.Base.Membership.Epoch == ^uint64(0) || p.NextMembership.Epoch != p.Claim.Base.Membership.Epoch+1 {
			return false
		}
		// Normal join/leave requires the union of old and new members. Forceful
		// disconnect has a distinct externally fenced contract, not this path.
		ids := append(membershipIDs(p.Claim.Base.Membership), membershipIDs(*p.NextMembership)...)
		for _, id := range ids {
			if !hasControlNode(p.Claim.Required, id) {
				return false
			}
		}
		for _, id := range p.Claim.Required {
			if !hasControlNode(ids, id) {
				return false
			}
		}
		if len(p.NextMembership.Excluded) != len(p.Claim.Base.Membership.Excluded) {
			return false
		}
		for _, id := range p.Claim.Base.Membership.Excluded {
			if !hasControlNode(p.NextMembership.Excluded, id) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func (l *controlLedger) reserve(claim controlClaim) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.slot != nil {
		if equalControlClaim(l.slot.Claim, claim) {
			return nil
		}
		if claim.Exclusion != nil && l.canSuspendSlot() && l.checkClaim(claim) {
			_, err := l.persist(controlForceReserve, claim)
			return err
		}
		return errControlConflict
	}
	if l.request != nil && l.request.Claim.Round != claim.Round && l.lastRelease != nil && l.request.Proposal != nil {
		decision, _ := l.request.Proposal.decision()
		if l.lastRelease.Decision == decision {
			if claim.Exclusion == nil || !l.canSuspendRequest() {
				return errControlConflict
			}
		}
	}
	if !l.checkClaim(claim) {
		return errControlConflict
	}
	_, err := l.persist(controlReserve, claim)
	return err
}

func (l *controlLedger) prepare(proposal controlProposal) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if decision, err := proposal.decision(); err == nil {
		if prior, ok := l.completed[decision]; ok && equalControlProposal(*prior.Slot.Proposal, proposal) {
			return prior.Slot.Ready, nil
		}
	}
	if l.slot == nil || !equalControlClaim(l.slot.Claim, proposal.Claim) {
		return client.Proof{}, errControlConflict
	}
	if l.slot.Proposal != nil {
		if equalControlProposal(*l.slot.Proposal, proposal) {
			return l.slot.Ready, nil
		}
		return client.Proof{}, errControlConflict
	}
	if !l.checkProposal(proposal) {
		return client.Proof{}, errControlConflict
	}
	return l.persist(controlPrepare, proposal)
}

type controlCertificateRecord struct {
	Decision    client.Decision
	Certificate client.Certificate
}

func containsOwnProof(cert client.Certificate, proof client.Proof) bool {
	for _, p := range cert {
		if p.Node == proof.Node {
			return p == proof
		}
	}
	return false
}

func (l *controlLedger) commit(decision client.Decision, certificate client.Certificate) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.completed[decision]; ok && reflect.DeepEqual(prior.Slot.Certificate, certificate) {
		return prior.Slot.Committed, nil
	}
	if l.slot == nil || l.slot.Proposal == nil || l.slot.Decision != decision || !certificate.Covers(l.slot.Claim.Required) || !containsOwnProof(certificate, l.slot.Ready) {
		return client.Proof{}, errControlConflict
	}
	if l.slot.Committed.RecordSequence != 0 {
		if reflect.DeepEqual(certificate, l.slot.Certificate) {
			return l.slot.Committed, nil
		}
		return client.Proof{}, errControlConflict
	}
	return l.persist(controlCommit, controlCertificateRecord{decision, certificate})
}

// markApplied may be called only after publishing the referenced system image.
// The controller must replay that publication idempotently if it crashed after
// publication but before this durable APPLIED record. No callback runs in replay.
func (l *controlLedger) markApplied(decision client.Decision) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.completed[decision]; ok {
		return prior.Slot.Applied, nil
	}
	if l.slot == nil || l.slot.Committed.RecordSequence == 0 || l.slot.Decision != decision {
		return client.Proof{}, errControlConflict
	}
	if l.slot.Applied.RecordSequence != 0 {
		return l.slot.Applied, nil
	}
	return l.persist(controlApplied, decision)
}

func (l *controlLedger) release(decision client.Decision, applied client.Certificate) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.completed[decision]; ok && reflect.DeepEqual(prior.Release.Certificate, applied) {
		return nil
	}
	if l.slot == nil || l.slot.Decision != decision || l.slot.Applied.RecordSequence == 0 || !applied.Covers(l.slot.Claim.Required) || !containsOwnProof(applied, l.slot.Applied) {
		return errControlConflict
	}
	_, err := l.persist(controlRelease, controlCertificateRecord{decision, applied})
	return err
}

func (l *controlLedger) releasedProof(decision client.Decision) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	prior, ok := l.completed[decision]
	if !ok {
		return client.Proof{}, errControlConflict
	}
	return prior.Proof, nil
}

func (l *controlLedger) beginRequest(kind uint16, required []client.ID, frozen ...client.ID) (controlClaim, error) {
	return l.beginRequestWithExclusion(kind, required, nil, frozen...)
}
func (l *controlLedger) beginRequestWithExclusion(kind uint16, required []client.ID, exclusion *controlExclusion, frozen ...client.ID) (controlClaim, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.request != nil && (exclusion == nil || !l.canSuspendRequest()) || l.attempt == ^uint64(0) || l.current.Membership.Epoch == 0 || !hasControlNode(membershipIDs(l.current.Membership), l.node) {
		return controlClaim{}, errControlConflict
	}
	claim := controlClaim{Round: client.RoundID{Membership: l.current.Membership.Epoch, Generation: l.current.Generation, Initiator: l.node, Attempt: l.attempt + 1}, Base: cloneControlBase(l.current), Required: append([]client.ID{}, required...), Exclusion: exclusion, FrozenBuckets: append([]client.ID(nil), frozen...)}
	if !l.checkClaim(claim) || !validControlKind(kind, claim) {
		return controlClaim{}, errControlConflict
	}
	record := uint16(controlRequestBegin)
	if l.request != nil {
		record = controlForceRequestBegin
	}
	if _, err := l.persist(record, controlRequest{Claim: claim, Kind: kind}); err != nil {
		return controlClaim{}, err
	}
	return claim, nil
}

func (l *controlLedger) saveRequestResult(round client.RoundID, result []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.request == nil || l.request.Claim.Round != round || l.request.Proposal == nil {
		return errControlConflict
	}
	if l.request.ResultReady {
		if bytes.Equal(l.request.Result, result) {
			return nil
		}
		return errControlConflict
	}
	_, err := l.persist(controlRequestPrepared, controlRequestResult{round, result})
	return err
}

func (l *controlLedger) finishRequest(decision client.Decision, released client.Certificate) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.request == nil || l.request.Proposal == nil || l.lastRelease == nil || l.lastRelease.Decision != decision || !released.Covers(l.request.Claim.Required) || !containsOwnProof(released, l.lastReleasedProof) {
		return errControlConflict
	}
	expected, err := l.request.Proposal.decision()
	if err != nil || expected != decision || !l.request.ResultReady {
		return errControlConflict
	}
	_, err = l.persist(controlRequestFinished, controlRequestFinish{decision, released})
	return err
}

func (l *controlLedger) abort(round client.RoundID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed[round] {
		if _, ok := l.aborted[round]; ok {
			return nil
		}
		return errControlConflict
	}
	if l.slot == nil || l.slot.Claim.Round != round || l.slot.Committed.RecordSequence != 0 || l.slot.ConstructionStarted {
		return errControlConflict
	}
	_, err := l.persist(controlAbort, l.slot.Claim)
	return err
}

// abortClaim also records an ungranted round. A requester which crashed after
// its first remote grant must fence late RESERVEs on every required participant
// before forgetting its request. A committed round can never take this path.
func (l *controlLedger) abortClaim(claim controlClaim) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if proof, ok := l.aborted[claim.Round]; ok {
		if !equalControlClaim(l.abortClaims[claim.Round], claim) {
			return client.Proof{}, errControlConflict
		}
		return proof, nil
	}
	if l.closed[claim.Round] || !l.checkClaimShape(claim) {
		return client.Proof{}, errControlConflict
	}
	if l.slot != nil && l.slot.Claim.Round == claim.Round && (!equalControlClaim(l.slot.Claim, claim) || l.slot.Committed.RecordSequence != 0 || l.slot.ConstructionStarted) {
		return client.Proof{}, errControlConflict
	}
	if l.slot != nil && l.slot.Suspended != nil && l.slot.Suspended.Claim.Round == claim.Round && (!equalControlClaim(l.slot.Suspended.Claim, claim) || l.slot.Suspended.Committed.RecordSequence != 0 || l.slot.Suspended.ConstructionStarted) {
		return client.Proof{}, errControlConflict
	}
	return l.persist(controlAbort, claim)
}

func (l *controlLedger) cancelRequest(round client.RoundID, aborted client.Certificate) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.request == nil || l.request.Claim.Round != round || !aborted.Covers(l.request.Claim.Required) || !containsOwnProof(aborted, l.aborted[round]) {
		return errControlConflict
	}
	_, err := l.persist(controlRequestCancelled, controlRequestCancel{round, aborted})
	return err
}

func (l *controlLedger) persist(kind uint16, value any) (client.Proof, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return client.Proof{}, err
	}
	proof, err := l.journal.append(kind, raw)
	if err != nil {
		return client.Proof{}, err
	}
	if err := l.replay(controlRecord{Kind: kind, Proof: proof, Data: raw}); err != nil {
		l.journal.mu.Lock()
		l.journal.failed = volume.ErrCorrupt
		l.journal.mu.Unlock()
		return client.Proof{}, err
	}
	return proof, nil
}

func decodeControlRecord(data []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return volume.ErrCorrupt
	}
	return nil
}

func (l *controlLedger) replay(record controlRecord) error {
	bad := func() error { return volume.ErrCorrupt }
	if record.Kind != controlGenesis && !l.initialized {
		return bad()
	}
	switch record.Kind {
	case controlGenesis:
		var base controlBase
		if l.initialized || record.Proof.RecordSequence != 1 || decodeControlRecord(record.Data, &base) != nil || !l.validBase(base) {
			return bad()
		}
		l.current = base
		l.initialized = true
	case controlReserve:
		var claim controlClaim
		if decodeControlRecord(record.Data, &claim) != nil || l.slot != nil || !l.checkClaim(claim) {
			return bad()
		}
		l.slot = &controlSlot{Claim: claim}
	case controlForceReserve:
		var claim controlClaim
		if decodeControlRecord(record.Data, &claim) != nil || claim.Exclusion == nil || !l.canSuspendSlot() || !l.checkClaim(claim) {
			return bad()
		}
		l.slot = &controlSlot{Claim: claim, Suspended: l.slot}
	case controlForceSlotStarted:
		var claim controlClaim
		if decodeControlRecord(record.Data, &claim) != nil || claim.Exclusion == nil || l.slot == nil || !equalControlClaim(l.slot.Claim, claim) || l.slot.ConstructionStarted {
			return bad()
		}
		l.slot.ConstructionStarted = true
	case controlPrepare:
		var proposal controlProposal
		if decodeControlRecord(record.Data, &proposal) != nil || l.slot == nil || l.slot.Proposal != nil || !equalControlClaim(l.slot.Claim, proposal.Claim) || !l.checkProposal(proposal) {
			return bad()
		}
		decision, err := proposal.decision()
		if err != nil {
			return bad()
		}
		l.slot.Proposal, l.slot.Decision, l.slot.Ready = &proposal, decision, record.Proof
		if l.request != nil && l.request.Claim.Round == proposal.Claim.Round {
			l.request.Proposal = &proposal
		}
	case controlCommit:
		var r controlCertificateRecord
		if decodeControlRecord(record.Data, &r) != nil || l.slot == nil || l.slot.Proposal == nil || l.slot.Decision != r.Decision || l.slot.Committed.RecordSequence != 0 || !r.Certificate.Covers(l.slot.Claim.Required) || !containsOwnProof(r.Certificate, l.slot.Ready) {
			return bad()
		}
		l.slot.Committed, l.slot.Certificate = record.Proof, r.Certificate
		if l.slot.Claim.Exclusion != nil {
			if old := l.slot.Suspended; old != nil {
				if old.Committed.RecordSequence != 0 && old.Applied.RecordSequence != 0 {
					l.ancestors[old.Decision] = *old
				}
				l.closed[old.Claim.Round] = true
				l.slot.Suspended = nil
			}
			if l.request != nil && l.request.Claim.Round != l.slot.Claim.Round {
				l.closed[l.request.Claim.Round] = true
				l.request = nil
			}
			if l.suspendedRequest != nil {
				l.closed[l.suspendedRequest.Claim.Round] = true
				l.suspendedRequest = nil
			}
		}
	case controlApplied:
		var d client.Decision
		if decodeControlRecord(record.Data, &d) != nil || l.slot == nil || l.slot.Committed.RecordSequence == 0 || l.slot.Applied.RecordSequence != 0 || l.slot.Decision != d {
			return bad()
		}
		p := l.slot.Proposal
		l.current = cloneControlBase(p.Claim.Base)
		l.current.Sequence, l.current.Head, l.current.SystemImage = d.Sequence, d.Digest, p.Image
		if p.NextMembership != nil {
			l.current.Membership = *p.NextMembership
		}
		l.slot.Applied = record.Proof
	case controlRelease:
		var r controlCertificateRecord
		if decodeControlRecord(record.Data, &r) != nil || l.slot == nil || l.slot.Applied.RecordSequence == 0 || l.slot.Decision != r.Decision || !r.Certificate.Covers(l.slot.Claim.Required) || !containsOwnProof(r.Certificate, l.slot.Applied) {
			return bad()
		}
		l.closed[l.slot.Claim.Round] = true
		l.lastRelease = &r
		l.lastReleasedProof = record.Proof
		l.completed[r.Decision] = controlCompletion{Slot: *l.slot, Release: r, Proof: record.Proof}
		l.slot = nil
	case controlAbort:
		var claim controlClaim
		if decodeControlRecord(record.Data, &claim) != nil || !l.checkClaimShape(claim) || l.closed[claim.Round] {
			return bad()
		}
		if l.slot != nil && l.slot.Claim.Round == claim.Round {
			if !equalControlClaim(l.slot.Claim, claim) || l.slot.Committed.RecordSequence != 0 || l.slot.ConstructionStarted {
				return bad()
			}
			l.slot = l.slot.Suspended
		}
		if l.slot != nil && l.slot.Suspended != nil && l.slot.Suspended.Claim.Round == claim.Round {
			if !equalControlClaim(l.slot.Suspended.Claim, claim) || l.slot.Suspended.Committed.RecordSequence != 0 {
				return bad()
			}
			l.slot.Suspended = nil
		}
		l.closed[claim.Round] = true
		l.aborted[claim.Round] = record.Proof
		l.abortClaims[claim.Round] = claim
	case controlRequestBegin, controlForceRequestBegin:
		var request controlRequest
		if decodeControlRecord(record.Data, &request) != nil || request.ConstructionStarted || request.Proposal != nil || request.ResultReady || len(request.Result) != 0 || request.Claim.Round.Initiator != l.node || request.Claim.Round.Attempt != l.attempt+1 || !l.checkClaim(request.Claim) || !validControlKind(request.Kind, request.Claim) {
			return bad()
		}
		if record.Kind == controlForceRequestBegin {
			if request.Claim.Exclusion == nil || !l.canSuspendRequest() {
				return bad()
			}
			l.suspendedRequest = l.request
		} else if l.request != nil {
			return bad()
		}
		l.attempt = request.Claim.Round.Attempt
		l.request = &request
	case controlForceBuildStarted:
		var claim controlClaim
		if decodeControlRecord(record.Data, &claim) != nil || claim.Exclusion == nil || l.request == nil || l.request.ConstructionStarted || !equalControlClaim(l.request.Claim, claim) || l.request.ResultReady || l.slot == nil || !equalControlClaim(l.slot.Claim, claim) {
			return bad()
		}
		l.request.ConstructionStarted = true
	case controlRequestPrepared:
		var r controlRequestResult
		if decodeControlRecord(record.Data, &r) != nil || l.request == nil || l.request.Claim.Round != r.Round || l.request.Proposal == nil || l.request.ResultReady {
			return bad()
		}
		l.request.Result = r.Result
		l.request.ResultReady = true
	case controlRequestFinished:
		var r controlRequestFinish
		if decodeControlRecord(record.Data, &r) != nil || l.request == nil || l.request.Proposal == nil || l.lastRelease == nil || l.lastRelease.Decision != r.Decision || !r.Released.Covers(l.request.Claim.Required) || !containsOwnProof(r.Released, l.lastReleasedProof) {
			return bad()
		}
		expected, err := l.request.Proposal.decision()
		if err != nil || expected != r.Decision || !l.request.ResultReady {
			return bad()
		}
		l.request = nil
	case controlRequestCancelled:
		var r controlRequestCancel
		if decodeControlRecord(record.Data, &r) != nil || l.request == nil || l.request.Claim.Round != r.Round || !r.Aborted.Covers(l.request.Claim.Required) || !containsOwnProof(r.Aborted, l.aborted[r.Round]) {
			return bad()
		}
		l.request, l.suspendedRequest = l.suspendedRequest, nil
	default:
		return bad()
	}
	l.publishControlActivity()
	return nil
}
