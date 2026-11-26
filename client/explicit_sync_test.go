// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestAcceptedStreamingRetainsExplicitSync(t *testing.T) {
	p, host := newWirePeer(t)
	p.helloCaps.Store(CapabilityDurability | CapabilityAcceptedStreams)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	payload := bytes.Repeat([]byte("streamed"), transferChunk/4)
	if _, err := c.PutFrom(ctx, ObjectID{41}, bytes.NewReader(payload), uint64(len(payload)), PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if p.acceptedHeaders.Load() != 1 || p.nativeHeaders.Load() != 0 || p.fences.Load() != 0 {
		t.Fatal("stream escaped acceptance or implicitly fenced", p.acceptedHeaders.Load(), p.nativeHeaders.Load(), p.fences.Load())
	}
	if err := c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if p.fences.Load() != 1 {
		t.Fatal("explicit Sync did not fence", p.fences.Load())
	}
}
func TestAcceptedStreamingUnsupportedNeverFallsBack(t *testing.T) {
	p, host := newWirePeer(t)
	p.helloCaps.Store(CapabilityDurability)
	c := dialConnectionTest(t, host)
	var status *StatusError
	data := bytes.Repeat([]byte("x"), transferChunk+1)
	_, err := c.PutFrom(context.Background(), ObjectID{42}, bytes.NewReader(data), uint64(len(data)), PutOptions{Accepted: true})
	if !errors.As(err, &status) || status.Status != 426 {
		t.Fatal("missing explicit capability error", err)
	}
	if p.acceptedHeaders.Load() != 0 || p.nativeHeaders.Load() != 0 || p.commits.Load() != 0 || p.fences.Load() != 0 {
		t.Fatal("unsupported stream performed native durable write")
	}
}
func TestAcceptedWriteDoesNotSyncOtherOwners(t *testing.T) {
	p, host := newDeferredFSPeer(t, CapabilityDurability)
	c := dialConnectionTest(t, host)
	c.mu.Lock()
	c.durability = map[ID]pendingDurability{ID{88}: {mark: CommitMark{Boot: ID{88}, Sequence: 1}, endpoint: "ws://127.0.0.1:1/api/ws", node: ID{77}}}
	c.mu.Unlock()
	ctx := context.Background()
	if _, err := c.Put(ctx, ObjectID{40}, []byte("accepted"), PutOptions{Accepted: true}); err != nil {
		t.Fatal("ordinary write tried to fence unrelated owner", err)
	}
	if p.accepted.Load() != 1 || p.durable.Load() != 0 || p.fences.Load() != 0 {
		t.Fatal("ordinary accepted write fenced")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if p.fences.Load() != 0 {
		t.Fatal("close requested durability")
	}
}

func TestAcceptedDisconnectedOwnerRemainsUnknownAfterAnotherOwnerWrite(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync
	first, host := newDeferredFSPeer(t, caps)
	second, otherHost := newDeferredFSPeer(t, caps)
	c := dialConnectionTest(t, host)
	other := dialConnectionTest(t, otherHost)
	ctx := context.Background()
	if _, err := other.Put(ctx, ObjectID{1}, []byte("owner two"), PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	// This is the same separate owner registry used by routed connections: it
	// must outlive replacement/closure of the active routing socket.
	c.rememberConnection(other.transport)
	other.transport.fail(errors.New("owner two disconnected before explicit fence"))
	if _, err := c.Put(ctx, ObjectID{2}, []byte("owner one"), PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if first.fences.Load() != 0 || second.fences.Load() != 0 {
		t.Fatal("owner change implicitly fenced")
	}
	var unknown *UnknownOutcomeError
	if err := c.Sync(ctx); !errors.As(err, &unknown) {
		t.Fatal("disconnected owner dependency silently vanished", err)
	}
	if first.fences.Load() != 1 || second.fences.Load() != 0 {
		t.Fatal("explicit Sync did not retain owner cut", first.fences.Load(), second.fences.Load())
	}
}
