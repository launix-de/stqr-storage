// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
)

func supportsRecoveryOrigins(view *systemView, capabilities uint64) bool {
	if capabilities&client.CapabilityRecoveryFenceClearance != 0 || view == nil {
		return true
	}
	for _, origin := range view.manifest.VolumeRecoveries {
		if len(origin.CancelledMigrations) > 0 {
			return false
		}
	}
	return true
}

// The target's durable cancellation proves that this particular source fence
// cannot hide an acknowledged handoff. Collect only after every survivor has
// reserved the exclusion and reconciliation has closed unsealed transfers.
func (c *clusterController) collectRecoveryCancellations(ctx context.Context, claim controlClaim, origins []volumeRecoveryOrigin) error {
	wire, err := marshalControlClaim(claim)
	if err != nil {
		return err
	}
	transport := peerControlParticipants{runtime: c.server.control}
	for _, node := range claim.Required {
		var offset uint64
		for {
			e := client.Encoder{}
			e.U16(1)
			e.Blob(wire)
			e.U8(0)
			e.U64(offset)
			raw, err := transport.call(ctx, node, peerForceScopes, e.B)
			if err != nil {
				return err
			}
			var page forceScopePage
			if decodeControlRecord(raw, &page) != nil || page.Next < offset || !page.Done && page.Next == offset || len(page.Reports) > 24 {
				return client.ErrProtocol
			}
			for _, report := range page.Reports {
				if report.Cancellation == nil {
					continue
				}
				plan, err := decodeRangePlan(report.Plan)
				if err != nil || report.Cancellation.Node != node {
					return client.ErrProtocol
				}
				if plan.Target != node || plan.ram() || !hasControlNode(claim.Exclusion.Nodes, plan.Source) || report.Ready || report.Decision != nil || report.Promoted {
					continue
				}
				if _, err := report.Cancellation.MarshalBinary(); err != nil {
					return err
				}
				for i := range origins {
					o := &origins[i]
					if o.Complete || o.Before.Bucket != plan.Before.Bucket || !sameRangeAuthority(o.Before.Entries[0], plan.Before.Entries[0]) {
						continue
					}
					// Existing origins retain their original witnesses. A later
					// exclusion cannot manufacture a clearance retroactively.
					if !hasControlNode(claim.Exclusion.Nodes, o.Before.Entries[0].Locations[0].Node) {
						continue
					}
					found := false
					for _, prior := range o.CancelledMigrations {
						old, _ := decodeRangePlan(prior.Plan)
						found = found || old.id() == plan.id()
					}
					if !found {
						o.CancelledMigrations = append(o.CancelledMigrations, volumeRecoveryCancellation{Plan: append([]byte(nil), report.Plan...), Proof: *report.Cancellation})
					}
				}
			}
			if page.Done {
				break
			}
			offset = page.Next
		}
	}
	for i := range origins {
		sort.Slice(origins[i].CancelledMigrations, func(a, b int) bool {
			x, _ := decodeRangePlan(origins[i].CancelledMigrations[a].Plan)
			y, _ := decodeRangePlan(origins[i].CancelledMigrations[b].Plan)
			xid, yid := x.id(), y.id()
			return bytes.Compare(xid[:], yid[:]) < 0
		})
	}
	return nil
}

func validateRecoveryCancellations(origin volumeRecoveryOrigin) error {
	var previous [32]byte
	for i, cancellation := range origin.CancelledMigrations {
		plan, err := decodeRangePlan(cancellation.Plan)
		if err != nil || plan.ram() || plan.Source == plan.Target || plan.Before.Bucket != origin.Before.Bucket || !sameRangeAuthority(plan.Before.Entries[0], origin.Before.Entries[0]) || plan.Target != cancellation.Proof.Node {
			return client.ErrProtocol
		}
		if _, err := cancellation.Proof.MarshalBinary(); err != nil {
			return err
		}
		id := plan.id()
		if i > 0 && bytes.Compare(previous[:], id[:]) >= 0 {
			return client.ErrProtocol
		}
		previous = id
	}
	return nil
}

// Every target validates its own journal entry before voting READY. The ALL
// certificate therefore binds all cancellation witnesses without synchronous
// cross-calls between simultaneously preparing participants.
// Called under opMu, before any candidate map can become active.
func (s *Server) prepareRecoveryCancellations(view *systemView, claim controlClaim) error {
	current := s.currentSystem.Load()
	if current == nil {
		return errControlConflict
	}
	known := make(map[[32]byte]volumeRecoveryOrigin, len(current.manifest.VolumeRecoveries))
	for _, old := range current.manifest.VolumeRecoveries {
		known[old.ID] = old
	}
	for _, origin := range view.manifest.VolumeRecoveries {
		if old, ok := known[origin.ID]; ok {
			if !reflect.DeepEqual(old.CancelledMigrations, origin.CancelledMigrations) {
				return errControlConflict
			}
			continue
		}
		for _, cancellation := range origin.CancelledMigrations {
			if claim.Exclusion == nil || !claim.Exclusion.RecoverOnly || !hasControlNode(claim.Required, cancellation.Proof.Node) {
				return errControlConflict
			}
			if cancellation.Proof.Node != s.control.ledger.node {
				continue
			}
			plan, err := decodeRangePlan(cancellation.Plan)
			if err != nil || s.scopes == nil {
				return client.ErrProtocol
			}
			r := s.scopes
			r.mu.Lock()
			t := r.txns[plan.id()]
			valid := t != nil && t.aborted && t.forceCancellation != nil && equalControlClaim(*t.forceCancellation, claim) && t.cancellationProof == cancellation.Proof && t.targetReady.RecordSequence == 0 && t.decision == nil
			r.mu.Unlock()
			if !valid {
				return fail(409, "recovery clearance lacks the target's durable cancellation")
			}
		}
	}
	return nil
}
