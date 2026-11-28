// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestAbortedRangeRetriesPGCancellationWithoutNewProofOrMove(t *testing.T) {
	testAbortedPGCancellation(t, false)
}
func TestAbortedRangeCancellationSurvivesBucketDeletion(t *testing.T) {
	testAbortedPGCancellation(t, true)
}
func testAbortedPGCancellation(t *testing.T, deleteBucket bool) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	addReplicaDataVolume(t, s, "source", "hdd", 8<<20)
	target := addReplicaDataVolume(t, s, "target", "hdd", 8<<20)
	bucket, _ := createProtocolBucket(t, s, "cancel-retry")
	before, ok := s.routingPatch(bucket.ID(), volume.ObjectID{})
	if !ok {
		t.Fatal("missing placement")
	}
	p, err := planRangeMove(before, before.Entries[0], s.scopes.node, target.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	r := s.scopes
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = r.beginPG(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = r.reserve(p); err != nil {
		t.Fatal(err)
	}
	if err = r.abort(p); err != nil {
		t.Fatal(err)
	}
	group, _, _, err := s.pgForPlan(p)
	if err != nil {
		t.Fatal(err)
	}
	// Reject the coordinator request after source cancellation was durably logged.
	gateValue, _ := r.pgRuntime.gates.LoadOrStore(group, make(chan struct{}, 1))
	gate := gateValue.(chan struct{})
	gate <- struct{}{}
	err = r.abortPG(ctx, p)
	<-gate
	if err == nil {
		t.Fatal("blocked coordinator unexpectedly completed cancellation")
	}
	r.mu.Lock()
	proof := r.pgStates[group].Active.Cancellation
	sequence := r.journal.sequence
	r.mu.Unlock()
	if proof.RecordSequence == 0 {
		t.Fatal("source cancellation proof was not retained")
	}
	r.mu.Lock()
	round := r.pgStates[group].Active.Round
	r.mu.Unlock()
	replayed := &rangeManager{}
	if err = replayed.replayPG(rangePGPrepared, &round, proof); err != nil {
		t.Fatal(err)
	}
	if err = replayed.replayPG(rangePGStarted, &round, proof); err != nil {
		t.Fatal(err)
	}
	if err = replayed.replayPG(rangePGSourceCancelled, &round, proof); err != nil {
		t.Fatal(err)
	}
	duplicate := proof
	duplicate.RecordSequence++
	if err = replayed.replayPG(rangePGSourceCancelled, &round, duplicate); err != nil {
		t.Fatal(err)
	}
	if replayed.pgStates[group].Active.Cancellation != proof {
		t.Fatal("replay replaced original cancellation proof")
	}

	if deleteBucket {
		body := client.Encoder{}
		body.ID(client.ID(bucket.ID()))
		if _, status, reply := adminBucketMessage(t, s, client.BucketDelete, body.B); status != 202 {
			t.Fatalf("delete bucket while cancellation is pending: %d %+v", status, reply)
		}
		for {
			if _, err = s.Bucket(bucket.ID()); errors.Is(err, ErrBucketNotFound) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("bucket was not purged", ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		if _, _, _, err = s.pgForPlan(p); !errors.Is(err, ErrBucketNotFound) {
			t.Fatal("deleted bucket lookup unexpectedly usable", err)
		}
	}
	forged := round
	forged.Cancelled = true
	forged.Done = proof
	forged.Done.RecordSequence += 100
	if err = r.acceptPGCancellation(forged); !errors.Is(err, errControlConflict) {
		t.Fatal("source accepted unjournalled cancellation proof", err)
	}
	unknown := p
	unknown.Attempt[0] ^= 0xff
	if _, err = r.requestPGCancellation(ctx, unknown, proof); !errors.Is(err, errControlConflict) {
		t.Fatal("cancellation created an unaccepted generation", err)
	}
	if err = r.executeMove(ctx, p, false); !errors.Is(err, errControlConflict) {
		t.Fatal("aborted range retry", err)
	}
	r.mu.Lock()
	state := r.pgStates[group]
	completed, found := r.pgRounds[p.id()]
	txn := r.txns[p.id()]
	after := r.journal.sequence
	r.mu.Unlock()
	if state.Active != nil || !found || !completed.Cancelled || completed.Done != proof {
		t.Fatal("PG cancellation remained pending or changed proof", state, completed)
	}
	if !txn.aborted || txn.decision != nil || txn.sourceDone.RecordSequence != 0 || txn.snapshot != nil {
		t.Fatal("aborted handoff was restarted")
	}
	// Exactly the normal completion record may follow the retained cancellation.
	if after != sequence+1 {
		t.Fatal("retry manufactured another cancellation proof", sequence, after)
	}
	if err = r.executeMove(ctx, p, false); !errors.Is(err, errControlConflict) {
		t.Fatal(err)
	}
	r.mu.Lock()
	final := r.journal.sequence
	r.mu.Unlock()
	if final != after {
		t.Fatal("completed retry appended new state")
	}
}
