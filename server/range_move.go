// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (r *rangeManager) move(ctx context.Context, p rangePlan) (retErr error) {
	stage := "source reservation"
	defer func() {
		if retErr != nil {
			retErr = fmt.Errorf("range handoff %s: %w", stage, retErr)
		}
	}()
	if p.Source != r.node {
		return errControlConflict
	}
	// Persist the source's requester role before asking even the first target.
	if err := r.reserve(p); err != nil {
		return err
	}
	id := p.id()
	r.mu.Lock()
	t := r.txns[id]
	finished := t.sourceDone.RecordSequence != 0
	r.mu.Unlock()
	if finished {
		return nil
	}
	stage = "history transfer"
	if err := r.sendHistory(ctx, p.Target, id); err != nil {
		return r.cancelBeforeFence(ctx, p, err)
	}
	stage = "target reservation"
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(p)); err != nil {
		return r.cancelBeforeFence(ctx, p, err)
	}
	r.mu.Lock()
	committed := t.decision != nil
	fenced := t.fenced
	r.mu.Unlock()
	if !committed && (p.ReplicaRemove || p.ReplicaPromote) {
		stage = "replica topology agreement"
		if err := r.fence(id); err != nil {
			return err
		}
		used := uint64(0)
		if p.ReplicaPromote {
			for _, store := range r.sourceVolumes(p) {
				n, err := r.replicaLogicalUsage(p, store)
				if err != nil {
					return err
				}
				used = addCapacity(used, n)
			}
		}
		if err := r.sendReplicaAbortHistory(ctx, p); err != nil {
			return err
		}
		digest := replicaTopologyDigest(p, used)
		e := rangeMessage(peerReplicaTopologyReady, id)
		body, err := r.call(ctx, p.Target, peerReplicaTopologyReady, e.B)
		if err != nil {
			return err
		}
		if p.ReplicaPromote {
			d := client.Decoder{B: body}
			body = d.Take(56)
			if d.U64() != used || d.Done() != nil {
				return errControlConflict
			}
		}
		target, err := client.DecodeProof(body)
		if err != nil {
			return err
		}
		source, err := r.ready(id, digest, true, used, 0)
		if err != nil {
			return err
		}
		if _, err = r.commit(rangeCommit{ID: id, Digest: digest, LogicalUsed: used, Source: source, Target: target}); err != nil {
			return err
		}
		committed = true
	}
	stage = "RAM handoff recovery"
	ramHandled, err := r.prepareRAMRecovery(ctx, p, t, committed)
	if err != nil {
		return err
	}
	if !committed && !ramHandled {
		stage = "local destination codec"
		if err := r.reencodeLocalDestination(ctx, p); err != nil {
			return r.cancelBeforeFence(ctx, p, err)
		}
		stage = "bulk copy"
		if !fenced {
			if _, _, err := r.sendSnapshot(ctx, p, false); err != nil {
				if statusOf(err) == 507 {
					return r.cancelBeforeFence(ctx, p, err)
				}
				return err
			}
		}
		stage = "source fence"
		if err := r.fence(id); err != nil {
			return err
		}
		stage = "final local destination codec"
		if err := r.reencodeLocalDestination(ctx, p); err != nil {
			return err
		}
		stage = "abort history"
		if err := r.sendReplicaAbortHistory(ctx, p); err != nil {
			return err
		}
		stage = "final snapshot"
		digest, target, err := r.sendSnapshot(ctx, p, true)
		if err != nil {
			return err
		}
		if target.Node != p.Target {
			return client.ErrProtocol
		}
		stage = "source READY"
		source, err := r.ready(id, digest, true)
		if err != nil {
			return err
		}
		stage = "source commit"
		if _, err := r.commit(rangeCommit{ID: id, Digest: digest, LogicalUsed: t.logicalUsed, QuotaUsed: t.quotaUsed, Source: source, Target: target}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	decision := *t.decision
	r.mu.Unlock()
	wire, err := decision.marshal()
	if err != nil {
		return err
	}
	stage = "target commit"
	body, err := r.call(ctx, p.Target, peerRangeCommit, wire)
	if err != nil {
		return err
	}
	proof, err := client.DecodeProof(body)
	if err != nil || proof.Node != p.Target {
		return client.ErrProtocol
	}
	stage = "source cleanup"
	if p.ReplicaAdd || p.ReplicaRemove || p.ReplicaPromote || p.ReplicaRelocate {
		for _, loc := range p.Before.Entries[0].Locations {
			if loc.Node != p.Source && loc.Node != p.Target {
				if err := r.sendHistory(ctx, loc.Node, [32]byte{}); err != nil {
					return err
				}
			}
		}
	}
	if err := r.cleanup(ctx, p); err != nil {
		return err
	}
	if p.ReplicaRemove || p.ReplicaRelocate {
		e := rangeMessage(peerReplicaTopologyCleanup, id)
		if _, err = r.call(ctx, p.Target, peerReplicaTopologyCleanup, e.B); err != nil {
			return err
		}
	}
	stage = "target DONE"
	e := rangeMessage(peerRangeDone, id)
	body, err = r.call(ctx, p.Target, peerRangeDone, e.B)
	if err != nil {
		return err
	}
	targetDone, err := client.DecodeProof(body)
	if err != nil || targetDone.Node != p.Target {
		return client.ErrProtocol
	}

	stage = "source DONE"
	if _, err := r.done(id, &targetDone); err != nil {
		return err
	}
	r.announce(p, decision)
	return nil
}

func (r *rangeManager) sendHistory(ctx context.Context, node client.ID, skip [32]byte) error {
	if node == r.node {
		return nil
	}
	r.mu.Lock()
	type record struct {
		p rangePlan
		c rangeCommit
	}
	var records []record
	for _, id := range r.order {
		if id == skip {
			continue
		}
		t := r.txns[id]
		if t.decision != nil {
			records = append(records, record{t.plan, *t.decision})
		}
	}
	r.mu.Unlock()
	for _, record := range records {
		e := client.Encoder{}
		e.Blob(mustRangeWire(record.p))
		wire, err := record.c.marshal()
		if err != nil {
			return err
		}
		e.Blob(wire)
		if _, err := r.call(ctx, node, peerRangeObserve, e.B); err != nil {
			return err
		}
	}
	return nil
}

func (r *rangeManager) announce(p rangePlan, c rangeCommit) {
	e := client.Encoder{}
	e.Blob(mustRangeWire(p))
	wire, err := c.marshal()
	if err != nil {
		return
	}
	e.Blob(wire)
	s := r.server
	s.peerStatsMu.RLock()
	defer s.peerStatsMu.RUnlock()
	for node, observation := range s.peerStats {
		if node == p.Target || node == p.Source || !observation.online {
			continue
		}
		peer := observation.connection
		select {
		case peer.outgoing <- client.Message{Kind: client.FireAndForget, Opcode: peerRangeObserve, Body: e.B}:
		default:
		}
	}
}

func (r *rangeManager) cleanup(ctx context.Context, p rangePlan) error {
	if p.ReplicaAdd || p.ReplicaRemove || p.ReplicaPromote || p.ReplicaRelocate {
		return nil
	}
	for _, v := range r.sourceVolumes(p) {
		var after *volume.ObjectID
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			h, err := v.Open(false)
			if err != nil {
				return err
			}
			records, err := h.ScanBucketObjects(volume.ID(p.Before.Bucket), after, 128)
			h.Close()
			if err != nil {
				return err
			}
			if len(records) == 0 {
				break
			}
			for _, record := range records {
				if !p.contains(record.Key.Object) {
					continue
				}
				r.server.opMu.Lock()
				if err := r.cleanupAllowed(p); err != nil {
					r.server.opMu.Unlock()
					return err
				}
				h, err := v.Open(true)
				if err == nil {
					err = h.Purge(record.Key, record.Info.Version)
					h.Close()
				}
				r.server.opMu.Unlock()
				if err != nil {
					return err
				}
			}
			last := records[len(records)-1].Key.Object
			after = &last
		}
		file, err := os.CreateTemp("", "stqr-range-cleanup-*")
		if err != nil {
			return err
		}
		err = func() error {
			defer func() { file.Close(); os.Remove(file.Name()) }()
			h, err := v.Open(false)
			if err != nil {
				return err
			}
			err = h.ScanRequests(func(record volume.RequestRecord) error {
				if record.Key.Bucket != volume.ID(p.Before.Bucket) || !p.contains(record.Key.Object) {
					return nil
				}
				_, err := file.Write(record.ID[:])
				return err
			})
			h.Close()
			if err != nil {
				return err
			}
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			for {
				var id volume.ID
				_, err := io.ReadFull(file, id[:])
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				r.server.opMu.Lock()
				if err := r.cleanupAllowed(p); err != nil {
					r.server.opMu.Unlock()
					return err
				}
				h, err := v.Open(true)
				if err == nil {
					err = h.ForgetRequest(id)
					h.Close()
				}
				r.server.opMu.Unlock()
				if err != nil {
					return err
				}
			}
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

// The scan is advisory. Recheck authority under opMu immediately before each
// destructive write: an exclusion may have cancelled this worker meanwhile.
func (r *rangeManager) cleanupAllowed(p rangePlan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.server.scopedTransition(nil); err != nil {
		return err
	}
	t := r.txns[p.id()]
	if t == nil || t.aborted || t.forceRetired || t.decision == nil {
		return errControlConflict
	}
	return nil
}

func (r *rangeManager) close() error {
	r.mu.Lock()
	var snapshots []*rangeSnapshot
	for _, t := range r.txns {
		if t.snapshot != nil {
			snapshots = append(snapshots, t.snapshot)
		}
	}
	r.mu.Unlock()
	for _, snapshot := range snapshots {
		snapshot.mu.Lock()
		if snapshot.object != nil {
			snapshot.object.close()
			snapshot.object = nil
		}
		snapshot.mu.Unlock()
	}
	return r.journal.close()
}
