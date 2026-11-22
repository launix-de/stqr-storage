// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/launix-de/stqr-storage/client"
)

// controlParticipants is the authenticated participant transport, including the
// local participant. Prepare must validate flushed system content; Apply must
// publish it before returning its durable APPLIED proof. No method infers an
// acknowledgement from a connection or a timeout.
type controlParticipants interface {
	Reserve(context.Context, client.ID, controlClaim) error
	Prepare(context.Context, client.ID, controlProposal) (client.Proof, error)
	Commit(context.Context, client.ID, client.Decision, client.Certificate) (client.Proof, error)
	Apply(context.Context, client.ID, client.Decision) (client.Proof, error)
	Release(context.Context, client.ID, client.Decision, client.Certificate) (client.Proof, error)
	Abort(context.Context, client.ID, controlClaim) (client.Proof, error)
}

// A coordinator exists only for a persisted request; any admitted member may
// instantiate one. Range-scoped rebuilds use separate scope claims, not this
// global membership/admin slot. The mutex protects this local requester, never
// serializes unrelated object requests, and is not held by ledger operations.
type controlCoordinator struct {
	mu           sync.Mutex
	ledger       *controlLedger
	participants controlParticipants
	building     bool // volatile: a restarted requester has no raw request to build
}

func (c *controlCoordinator) requestSnapshot() *controlRequest {
	c.ledger.mu.Lock()
	defer c.ledger.mu.Unlock()
	if c.ledger.request == nil {
		return nil
	}
	data, _ := json.Marshal(c.ledger.request)
	var request controlRequest
	_ = json.Unmarshal(data, &request)
	return &request
}

// begin persists the requester before contacting even the first participant.
// Claim.Required is canonical/sorted and remains fixed throughout the request.
// Partial reservation failure leaves a recoverable cancellation, not an
// implicit timeout-based release at any participant.
func (c *controlCoordinator) begin(ctx context.Context, kind uint16, required []client.ID, frozen ...client.ID) (controlClaim, error) {
	return c.beginWithExclusion(ctx, kind, required, nil, frozen...)
}
func (c *controlCoordinator) beginWithExclusion(ctx context.Context, kind uint16, required []client.ID, exclusion *controlExclusion, frozen ...client.ID) (controlClaim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	claim, err := c.ledger.beginRequestWithExclusion(kind, required, exclusion, frozen...)
	if err != nil {
		return controlClaim{}, err
	}
	c.building = true
	for _, node := range claim.Required {
		if err := c.participants.Reserve(ctx, node, claim); err != nil {
			c.building = false
			return claim, err
		}
	}
	return claim, nil
}

// prepared persists only the public response, never raw request credentials.
// It must run before PREPARE is sent to any other participant. A crash before
// ResultReady is durable therefore always has a safe abort path; afterward the
// requester consistently completes this decision instead of abandoning peers.
func (c *controlCoordinator) prepared(ctx context.Context, proposal controlProposal, result []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.building {
		return errControlConflict
	}
	defer func() { c.building = false }()
	r := c.requestSnapshot()
	if r == nil || r.Claim.Round != proposal.Claim.Round || r.Kind != proposal.Kind {
		return errControlConflict
	}
	if _, err := c.participants.Prepare(ctx, c.ledger.node, proposal); err != nil {
		return err
	}
	return c.ledger.saveRequestResult(proposal.Claim.Round, result)
}

// resume is used after reconnect/restart as well as normal completion. It has
// no election or failure timeout: a missing required participant leaves the
// persisted request pending until that participant returns.
func (c *controlCoordinator) resume(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.building {
		return nil, errControlConflict
	}
	return c.resumeLocked(ctx)
}

// cancelBuilding is called when validation/building failed before prepared.
// A background recovery worker must never mistake the live builder's window
// between begin and prepared for a requester lost during a process restart.
func (c *controlCoordinator) cancelBuilding(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.requestSnapshot()
	if r != nil && (r.ResultReady || r.Claim.Exclusion != nil && r.ConstructionStarted && (r.Claim.Exclusion.Restore == nil || r.Proposal != nil)) {
		return errControlConflict
	}
	c.building = false
	_, err := c.resumeLocked(ctx)
	return err
}

func (c *controlCoordinator) resumeLocked(ctx context.Context) ([]byte, error) {
	r := c.requestSnapshot()
	if r == nil {
		return nil, nil
	}
	if !r.ResultReady {
		if r.Claim.Exclusion != nil && r.ConstructionStarted && (r.Claim.Exclusion.Restore == nil || r.Proposal != nil) {
			return nil, errControlConflict // only the cluster builder can resume it
		}
		aborted, err := collectControlProofs(ctx, r.Claim.Required, func(ctx context.Context, node client.ID) (client.Proof, error) {
			return c.participants.Abort(ctx, node, r.Claim)
		})
		if err != nil {
			return nil, err
		}
		return nil, c.ledger.cancelRequest(r.Claim.Round, aborted)
	}
	if r.Proposal == nil {
		return nil, errControlConflict
	}
	p := *r.Proposal
	decision, err := p.decision()
	if err != nil {
		return nil, err
	}
	ready, err := collectControlProofs(ctx, r.Claim.Required, func(ctx context.Context, node client.ID) (client.Proof, error) {
		return c.participants.Prepare(ctx, node, p)
	})
	if err != nil {
		return nil, err
	}
	if _, err = collectControlProofs(ctx, r.Claim.Required, func(ctx context.Context, node client.ID) (client.Proof, error) {
		return c.participants.Commit(ctx, node, decision, ready)
	}); err != nil {
		return nil, err
	}
	applied, err := collectControlProofs(ctx, r.Claim.Required, func(ctx context.Context, node client.ID) (client.Proof, error) {
		return c.participants.Apply(ctx, node, decision)
	})
	if err != nil {
		return nil, err
	}
	// Release others first. The local slot/request prevents a newer global
	// round from overtaking unfinished acknowledgements. Completed participants
	// retain their proofs so a leaving coordinator can resume even if subsequent
	// rounds no longer need that node.
	others := make([]client.ID, 0, len(r.Claim.Required)-1)
	for _, node := range r.Claim.Required {
		if node != c.ledger.node {
			others = append(others, node)
		}
	}
	var released client.Certificate
	if len(others) != 0 {
		released, err = collectControlProofs(ctx, others, func(ctx context.Context, node client.ID) (client.Proof, error) {
			return c.participants.Release(ctx, node, decision, applied)
		})
		if err != nil {
			return nil, err
		}
	}
	own, err := c.participants.Release(ctx, c.ledger.node, decision, applied)
	if err != nil {
		return nil, err
	}
	released, err = client.NewCertificate(append(released, own))
	if err != nil {
		return nil, err
	}
	if err = c.ledger.finishRequest(decision, released); err != nil {
		return nil, err
	}
	return append([]byte(nil), r.Result...), nil
}

func collectControlProofs(ctx context.Context, nodes []client.ID, run func(context.Context, client.ID) (client.Proof, error)) (client.Certificate, error) {
	proofs := make([]client.Proof, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	// Bound concurrent network requests for large memberships without changing
	// ALL. Workers continue collecting independent acknowledgements on errors.
	workers := len(nodes)
	if workers > 32 {
		workers = 32
	}
	jobs := make(chan int)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := ctx.Err(); err != nil {
					errs[index] = err
					continue
				}
				proofs[index], errs[index] = run(ctx, nodes[index])
				if errs[index] == nil && proofs[index].Node != nodes[index] {
					errs[index] = client.ErrProtocol
				}
			}
		}()
	}
	for i := range nodes {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return client.NewCertificate(proofs)
}

// Force requests retain only node IDs and explicit administrative loss consent,
// so their unprepared construction can resume without raw credentials.
func (c *controlCoordinator) rebindBuilding(claim controlClaim) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.building {
		return errControlConflict
	}
	r := c.requestSnapshot()
	if r == nil || r.ResultReady || !equalControlClaim(r.Claim, claim) {
		return errControlConflict
	}
	c.building = true
	return nil
}
func (c *controlCoordinator) suspendBuilding() { c.mu.Lock(); c.building = false; c.mu.Unlock() }
