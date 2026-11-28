// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// This is a read-only administrative proof, never an instruction to free data.
// It inspects catalog metadata only. An absent namespace is not a tombstone, and
// missing historical clone evidence is not evidence of an unshared lifetime.
// Positive proofs are monotonic: every member of the complete receipt component
// is already durably DELETING, whose state cannot be reversed or cloned again.
func deletedBucketDependencyProof(descriptions []BucketDescription, subject volume.ID) bool {
	byID := make(map[string]BucketDescription, len(descriptions))
	links := make(map[string][]string)
	receipts := make(map[string]BucketReceipt)
	bad := make(map[string]bool)
	linked := make(map[string]bool)
	for _, d := range descriptions {
		if _, duplicate := byID[d.ID]; duplicate {
			return false
		}
		byID[d.ID] = d
		for _, edge := range d.ClonePeers {
			linked[d.ID] = true
			linked[edge.Bucket] = true
		}
		for _, r := range d.Receipts {
			if r.Opcode != client.BucketClone {
				continue
			}
			if prior, ok := receipts[r.ID]; ok && prior != r {
				bad[r.ID] = true
			}
			receipts[r.ID] = r
			links[r.ID] = append(links[r.ID], d.ID)
		}
	}
	if _, ok := byID[subject.String()]; !ok {
		return false
	}
	seen := make(map[string]bool)
	todo := []string{subject.String()}
	for len(todo) != 0 {
		id := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		d, ok := byID[id]
		if !ok || !d.ReferenceLineageComplete || linked[id] || d.State != BucketDeleting || len(d.ClonePeers) != 0 || d.CloneDetach || d.DesiredPolicy != nil {
			return false
		}
		deleted := false
		for _, r := range d.Receipts {
			if r.Opcode == client.BucketDelete && (r.Status == 202 && r.Running || r.Status == 200 && !r.Running) {
				deleted = true
			}
			if r.Opcode != client.BucketClone {
				continue
			}
			ends := links[r.ID]
			if bad[r.ID] || len(ends) != 2 || ends[0] == ends[1] || r.ID == "" || r.Digest == "" || r.Running || r.Status != 200 {
				return false
			}
			todo = append(todo, ends...)
		}
		if !deleted {
			return false
		}
	}
	return true
}

func (s *Server) canRetireDeletedBucketDependencies(ctx context.Context, subject volume.ID) (bool, error) {
	if subject == systemBucket {
		return false, nil
	}
	// Capture one authoritative immutable cut without holding a mutation or ledger
	// mutex during peer I/O. A joining, busy or preview node cannot certify it.
	s.opMu.Lock()
	if !s.referenceAccountsStable() {
		s.opMu.Unlock()
		return false, fail(503, "dependency proof awaits stable catalog authority")
	}
	descriptions := s.bucketSnapshots()
	view := s.currentSystem.Load()
	var base controlBase
	if s.controller != nil {
		if s.control == nil || view == nil {
			s.opMu.Unlock()
			return false, fail(503, "dependency proof catalog authority unavailable")
		}
		l := s.control.ledger
		l.mu.Lock()
		base = cloneControlBase(l.current)
		busy := l.slot != nil || l.request != nil
		l.mu.Unlock()
		if busy || base.SystemImage != [32]byte(view.root) || len(base.Membership.Members) == 0 {
			s.opMu.Unlock()
			return false, fail(503, "dependency proof catalog is not committed")
		}
		descriptions = view.descriptions
	}
	safe := deletedBucketDependencyProof(descriptions, subject)
	s.opMu.Unlock()
	if !safe || s.controller == nil {
		return safe, ctx.Err()
	}
	// Every required member must confirm the same committed image. Offline nodes
	// and transport/semantics errors stay errors; they never shrink membership.
	for _, node := range membershipIDs(base.Membership) {
		info, err := s.controller.info(ctx, node)
		if err != nil {
			return false, fail(503, "dependency proof requires every catalog member")
		}
		if !equalControlBase(base, info.base) {
			return false, fail(503, "dependency proof catalog members disagree")
		}
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if !s.referenceAccountsStable() || s.currentSystem.Load() != view {
		return false, fail(503, "dependency proof catalog changed")
	}
	l := s.control.ledger
	l.mu.Lock()
	unchanged := equalControlBase(base, l.current) && l.slot == nil && l.request == nil
	l.mu.Unlock()
	if !unchanged {
		return false, fail(503, "dependency proof catalog changed")
	}
	return true, ctx.Err()
}

// The catalog parser rejects unknown fields, so clone-receipt support alone is
// insufficient. Only fresh creation may issue provenance, after every member
// advertises the additive creation/catalog decoder contract as well.
func (s *Server) referenceLineageCreationAllowed() bool {
	if s.metadataPreview {
		return s.lineageCreationProof
	}
	if s.requireDirectedCOWCompatibility(s.ctx) != nil {
		return false
	}
	local := s.protocolVersion()
	if local.semantics(peerControlInfo).Max < 4 {
		return false
	}

	if s.controller == nil {
		return true
	}
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	for _, member := range view.manifest.Membership.Members {
		if member.ID.String() == s.running.Node.ID {
			continue
		}
		remote := s.issuerProfile(member.ID)
		if remote.semantics(peerControlInfo).Max < 4 || checkPeerOperation(local, remote, peerControlInfo) != nil {
			return false
		}
		// Recheck clone semantics against this same immutable membership cut.
		// The earlier general gate may have observed a preceding membership.
		for op := range directedCOWRequiredSemantics() {
			if checkPeerOperation(local, remote, op) != nil {
				return false
			}
		}
	}
	return s.currentSystem.Load() == view
}

// Receipt history is lifetime evidence. Keeping the marker while deleting or
// changing an old link would falsely certify a smaller component. Dropping the
// marker is conservative; restoring it on an existing row needs a separate
// explicit offline provenance reconstruction, which normal edits do not offer.
func validateReferenceLineageEdit(before, after BucketDescription) error {
	if !before.ReferenceLineageComplete && after.ReferenceLineageComplete {
		return ErrBucketInvalid
	}
	if !before.ReferenceLineageComplete || !after.ReferenceLineageComplete {
		return nil
	}
	kept := make(map[BucketReceipt]int, len(after.Receipts))
	for _, r := range after.Receipts {
		if r.Opcode == client.BucketClone {
			kept[r]++
		}
	}
	for _, r := range before.Receipts {
		if r.Opcode != client.BucketClone {
			continue
		}
		if kept[r] == 0 {
			return ErrBucketInvalid
		}
		kept[r]--
	}
	return nil
}
