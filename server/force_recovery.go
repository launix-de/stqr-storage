// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
)

func (c *clusterController) resumeExclusionBuild(ctx context.Context, r *controlRequest) ([]byte, error) {
	if r.Claim.Exclusion == nil {
		return nil, errControlConflict
	}
	if err := c.coordinator.rebindBuilding(r.Claim); err != nil {
		return nil, err
	}
	defer c.coordinator.suspendBuilding()
	if r.Proposal != nil {
		// Source READY already fixed the complete graph. Never rebuild a
		// different candidate after that durable promise, including RAM loss.
		e := client.Encoder{}
		e.V(200)
		e.Blob(nil)
		if err := c.coordinator.prepared(ctx, *r.Proposal, e.B); err != nil {
			return nil, err
		}
		return c.resume(ctx)
	}
	for _, node := range r.Claim.Required {
		if err := c.coordinator.participants.Reserve(ctx, node, r.Claim); err != nil {
			if !r.ConstructionStarted {
				if cancelErr := c.coordinator.cancelBuilding(ctx); cancelErr == nil {
					s := c.server
					s.opMu.Lock()
					if op, loadErr := s.operation(r.Claim.Exclusion.Operation.String()); loadErr == nil {
						_, _ = s.finishOperation(op, err)
					}
					s.opMu.Unlock()
				}
			}
			return nil, err
		}
	}
	next, err := exclusionMembership(r.Claim.Base.Membership, r.Claim.Exclusion)
	if err != nil {
		return nil, err
	}
	body, status, err := c.finishReservedChange(ctx, r.Claim, next, controlForceDisconnect, &next, nil, nil, r.Claim.Exclusion)
	if err != nil {
		return nil, err
	}
	e := client.Encoder{}
	e.V(uint64(status))
	e.Blob(body)
	return e.B, nil
}
