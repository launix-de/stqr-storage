// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"testing"
)

func TestAcknowledgedReceiptReconnectExplicitOnly(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	p, host := newDeferredFSPeer(t, caps)
	p.recoverReceipts.Store(true)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	id := NewRequestID()
	fs := filesystemDeferredStore(c).(*filesystemNativeStore)
	if err := fs.PinObject(ctx, ObjectID{3}, id); err != nil {
		t.Fatal(err)
	}
	old := c.transport
	old.mu.Lock()
	acknowledged := false
	for _, work := range old.connectionWrites {
		acknowledged = acknowledged || work.acknowledged
	}
	old.mu.Unlock()
	if !acknowledged {
		t.Fatal("ACK was not recorded before result delivery")
	}
	old.fail(errors.New("EOF after accepted reply"))
	if p.fences.Load() != 0 || p.receiptFences.Load() != 0 {
		t.Fatal("acceptance or disconnect synced implicitly")
	}
	if err := c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if p.receiptFences.Load() != 1 || p.fences.Load() != 0 {
		t.Fatal("new connection fence substituted for old prefix proof")
	}
	if old.hasConnectionWork() {
		t.Fatal("proven old work retained")
	}
	if err := c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if p.receiptFences.Load() != 1 {
		t.Fatal("already proven receipt requested again")
	}
}

func TestReceiptDoesNotCertifyUnansweredOrNoACKMutation(t *testing.T) {
	for _, noAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "unanswered", true: "noACK"}[noAck], func(t *testing.T) {
			const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
			p, host := newDeferredFSPeer(t, caps)
			p.recoverReceipts.Store(true)
			c := dialConnectionTest(t, host)
			ctx := context.Background()
			fs := filesystemDeferredStore(c).(*filesystemNativeStore)
			if err := fs.PinObject(ctx, ObjectID{3}, NewRequestID()); err != nil {
				t.Fatal(err)
			}
			old := c.transport
			unknownID := NewRequestID()
			old.mu.Lock()
			old.connectionSequence++
			old.connectionWrites[unknownID] = &connectionSubmission{sequence: old.connectionSequence, accepted: true, noAck: noAck}
			old.mu.Unlock()
			old.fail(errors.New("EOF before independent result"))
			var unknown *UnknownOutcomeError
			if err := c.Sync(ctx); !errors.As(err, &unknown) || unknown.RequestID != unknownID {
				t.Fatal("unanswered write uncertainty lost", err)
			}
			if p.receiptFences.Load() != 0 {
				t.Fatal("known receipt incorrectly covered an unanswered mutation")
			}
		})
	}
}

func TestReceiptRecordedBeforeCallerObservation(t *testing.T) {
	id, boot := NewRequestID(), NewRequestID()
	tpt := &objectTransport{capabilities: CapabilityConnectionSync, connectionSequence: 1, connectionWrites: map[ID]*connectionSubmission{id: {sequence: 1, accepted: true}}}
	mark := CommitMark{Boot: boot, Sequence: 91}
	tpt.finishConnectionRequest(id, wireReply{headers: map[uint64][]byte{DurabilityHeader: mark.MarshalBinary()}}, nil)
	if !tpt.connectionWrites[id].acknowledged || tpt.connectionReceipts[boot].mark != mark {
		t.Fatal("ACK/EOF window lacks exact receipt")
	}
}
