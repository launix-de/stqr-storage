// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"sort"

	"github.com/launix-de/stqr-storage/client"
)

// Exclusion is an explicit administrative exception to old-membership ALL.
// It never represents a timeout or an inferred failure. The removed instances
// must already be externally fenced; no packet can disable an isolated writer.
type controlExclusion struct {
	ExpectedMembership                       uint64      `json:",omitempty"`
	AlreadyRetired                           []client.ID `json:",omitempty"`
	Operation                                client.ID
	Nodes                                    []client.ID
	Administrator                            client.ID
	Username                                 string
	AcceptDataLoss, ExternalFencingConfirmed bool
}

func exclusionMembership(base client.Membership, exclusion *controlExclusion) (client.Membership, error) {
	if exclusion == nil || exclusion.ExpectedMembership > base.Epoch || !exclusion.AcceptDataLoss || !exclusion.ExternalFencingConfirmed || exclusion.Operation == (client.ID{}) || exclusion.Administrator == (client.ID{}) || !validUsername(exclusion.Username) || len(exclusion.Nodes) == 0 || base.Epoch == 0 || base.Epoch == ^uint64(0) {
		return client.Membership{}, client.ErrProtocol
	}
	old := membershipIDs(base)
	for i, node := range exclusion.AlreadyRetired {
		if exclusion.ExpectedMembership == 0 || hasControlNode(old, node) || hasControlNode(base.Excluded, node) || !hasControlNode(exclusion.Nodes, node) || i > 0 && bytes.Compare(exclusion.AlreadyRetired[i-1][:], node[:]) >= 0 {
			return client.Membership{}, client.ErrProtocol
		}
	}
	for i, node := range exclusion.Nodes {
		if !hasControlNode(old, node) && !hasControlNode(exclusion.AlreadyRetired, node) || i > 0 && bytes.Compare(exclusion.Nodes[i-1][:], node[:]) >= 0 {
			return client.Membership{}, client.ErrProtocol
		}
	}
	next := base
	next.Epoch++
	next.Members = nil
	for _, member := range base.Members {
		if !hasControlNode(exclusion.Nodes, member.ID) {
			next.Members = append(next.Members, member)
		}
	}
	if len(next.Members) == 0 {
		return client.Membership{}, client.ErrProtocol
	}
	next.Excluded = append(append([]client.ID{}, base.Excluded...), exclusion.Nodes...)
	sort.Slice(next.Excluded, func(i, j int) bool { return bytes.Compare(next.Excluded[i][:], next.Excluded[j][:]) < 0 })
	if !validateControlMembership(next) {
		return client.Membership{}, client.ErrProtocol
	}
	return next, nil
}

func validExclusionClaim(c controlClaim) bool {
	next, err := exclusionMembership(c.Base.Membership, c.Exclusion)
	if err != nil || len(c.Required) != len(next.Members) || c.Round.Initiator != next.Members[0].ID {
		return false
	}
	for i, member := range next.Members {
		if c.Required[i] != member.ID {
			return false
		}
	}
	return true
}

func validControlKind(kind uint16, claim controlClaim) bool {
	if kind == controlForceDisconnect {
		return claim.Exclusion != nil
	}
	return claim.Exclusion == nil && (kind == controlMembership || kind == controlAdministration || kind == controlShutdown)
}

func encodeControlExclusion(e *client.Encoder, x *controlExclusion) {
	e.ID(x.Operation)
	e.ID(x.Administrator)
	e.Text(x.Username)
	e.Bool(x.AcceptDataLoss)
	e.Bool(x.ExternalFencingConfirmed)
	e.V(uint64(len(x.Nodes)))
	for _, node := range x.Nodes {
		e.ID(node)
	}
	if x.ExpectedMembership != 0 {
		e.U64(x.ExpectedMembership)
	}
	if len(x.AlreadyRetired) > 0 {
		e.V(uint64(len(x.AlreadyRetired)))
		for _, node := range x.AlreadyRetired {
			e.ID(node)
		}
	}
}

func exclusionObservedEpoch(claim controlClaim) uint64 {
	if claim.Exclusion != nil && claim.Exclusion.ExpectedMembership != 0 {
		return claim.Exclusion.ExpectedMembership
	}
	return claim.Base.Membership.Epoch
}

func decodeControlExclusion(d *client.Decoder) *controlExclusion {
	x := &controlExclusion{Operation: d.ID(), Administrator: d.ID(), Username: d.Text(), AcceptDataLoss: d.Bool(), ExternalFencingConfirmed: d.Bool()}
	n := d.Count(10000)
	if n == 0 || n > len(d.B)/16 {
		d.Err = client.ErrProtocol
		return x
	}
	x.Nodes = make([]client.ID, n)
	for i := range x.Nodes {
		x.Nodes[i] = d.ID()
	}
	return x
}

// Persist before the first forced range cancellation/promotion. A crash before
// this marker can safely close a partially acquired ALL promise; afterward its
// private recovery graph may hold the only sealed copy and must be resumed.
func (l *controlLedger) startForceConstruction(claim controlClaim) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if claim.Exclusion == nil || l.request == nil || !equalControlClaim(l.request.Claim, claim) || l.slot == nil || !equalControlClaim(l.slot.Claim, claim) {
		return errControlConflict
	}
	if l.request.ConstructionStarted {
		return nil
	}
	_, err := l.persist(controlForceBuildStarted, claim)
	return err
}

// Keep an uncommitted requester, or an already applied known parent, until the
// explicit exclusion commits. A failed force restores its original slot/result
// rather than erasing READY or pretending an old COMMIT was aborted.
func (l *controlLedger) canSuspendRequest() bool {
	if l.request == nil || l.suspendedRequest != nil {
		return false
	}
	if l.slot != nil && l.slot.Committed.RecordSequence != 0 && !l.canSuspendSlot() {
		return false
	}
	if l.request.Proposal != nil {
		decision, err := l.request.Proposal.decision()
		_, completed := l.completed[decision]
		applied := completed || l.slot != nil && l.slot.Decision == decision && l.slot.Applied.RecordSequence != 0
		if err != nil || decision.Sequence < l.current.Sequence || decision.Sequence == l.current.Sequence && !applied {
			return false
		}
		if l.request.Claim.Exclusion != nil && !applied {
			return false
		}
	} else if l.request.Claim.Exclusion != nil {
		return false
	}
	return true
}

func (l *controlLedger) canSuspendSlot() bool {
	if l.slot == nil {
		return false
	}
	return l.slot.Committed.RecordSequence == 0 && l.slot.Claim.Exclusion == nil || l.slot.Applied.RecordSequence != 0 && l.current.Sequence == l.slot.Decision.Sequence && l.current.Head == l.slot.Decision.Digest
}

const peerForceConstruction uint16 = 0x1069

func (s *Server) receiveForceConstruction(body []byte, actor client.ID) error {
	claim, err := decodeControlClaim(body)
	if err != nil || claim.Exclusion == nil || claim.Round.Initiator != actor || s.control == nil {
		return client.ErrProtocol
	}
	return s.control.ledger.startForceSlot(claim)
}

func (l *controlLedger) startForceSlot(claim controlClaim) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if claim.Exclusion == nil || l.slot == nil || !equalControlClaim(l.slot.Claim, claim) {
		return errControlConflict
	}
	if l.slot.ConstructionStarted {
		return nil
	}
	_, err := l.persist(controlForceSlotStarted, claim)
	return err
}

func (s *Server) acceptsObservedMembership(epoch uint64) bool {
	if s.control == nil {
		return false
	}
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	return epoch == l.current.Membership.Epoch || l.knownObservedMembership(epoch, l.current.Membership)
}

// A forced recovery may first have to publish an already committed join. Keep
// the caller's original epoch bound to its request while adopting that exact
// parent; this is not permission to accept arbitrary stale membership data.
func (l *controlLedger) knownObservedMembership(epoch uint64, current client.Membership) bool {
	check := func(slot *controlSlot) bool {
		if slot == nil || slot.Proposal == nil || slot.Committed.RecordSequence == 0 || slot.Applied.RecordSequence == 0 {
			return false
		}
		p := slot.Proposal
		return p.Claim.Base.Membership.Epoch == epoch && committedControlMembership(*p) && equalControlMembership(*p.NextMembership, current)
	}
	if check(l.slot) || l.slot != nil && check(l.slot.Suspended) {
		return true
	}
	for _, completed := range l.completed {
		copy := completed.Slot
		if check(&copy) {
			return true
		}
	}
	for _, ancestor := range l.ancestors {
		copy := ancestor
		if check(&copy) {
			return true
		}
	}
	return false
}

func monotoneControlJoin(p controlProposal) bool {
	if !normalControlMembership(p) || len(p.NextMembership.Members) <= len(p.Claim.Base.Membership.Members) {
		return false
	}
	for _, member := range p.Claim.Base.Membership.Members {
		if !hasControlNode(membershipIDs(*p.NextMembership), member.ID) {
			return false
		}
	}
	return true
}

func normalControlMembership(p controlProposal) bool {
	return p.Kind == controlMembership && p.NextMembership != nil && p.NextMembership.Epoch == p.Claim.Base.Membership.Epoch+1 && p.NextMembership.Cluster == p.Claim.Base.Membership.Cluster
}

func committedControlMembership(p controlProposal) bool {
	return (p.Kind == controlMembership || p.Kind == controlForceDisconnect) && p.NextMembership != nil && p.NextMembership.Epoch == p.Claim.Base.Membership.Epoch+1 && p.NextMembership.Cluster == p.Claim.Base.Membership.Cluster
}

func (s *Server) knownRetiredVictims(epoch uint64, nodes []client.ID) []client.ID {
	l := s.control.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.retiredVictims(epoch, nodes, l.current.Membership)
}

func (l *controlLedger) retiredVictims(epoch uint64, nodes []client.ID, current client.Membership) []client.ID {
	var retired []client.ID
	for _, node := range nodes {
		if hasControlNode(membershipIDs(current), node) {
			continue
		}
		check := func(slot *controlSlot) bool {
			if slot == nil || slot.Proposal == nil || slot.Committed.RecordSequence == 0 || slot.Applied.RecordSequence == 0 {
				return false
			}
			p := slot.Proposal
			return normalControlMembership(*p) && (p.Claim.Base.Membership.Epoch == epoch || current.Epoch == epoch) && equalControlMembership(*p.NextMembership, current) && hasControlNode(membershipIDs(p.Claim.Base.Membership), node) && !hasControlNode(membershipIDs(current), node)
		}
		known := check(l.slot) || l.slot != nil && check(l.slot.Suspended)
		for _, completed := range l.completed {
			copy := completed.Slot
			known = known || check(&copy)
		}
		if known {
			retired = append(retired, node)
		}
	}
	return retired
}
