// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
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

func TestAcceptedDisconnectedOwnerRequiresProofAfterAnotherOwnerWrite(t *testing.T) {
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
	var status *StatusError
	if err := c.Sync(ctx); !errors.As(err, &status) || status.Status != 410 {
		t.Fatal("disconnected owner proof requirement silently vanished", err)
	}
	if second.receiptFences.Load() != 1 {
		t.Fatal("old owner prefix was not checked")
	}
	if first.fences.Load() != 1 || second.fences.Load() != 0 {
		t.Fatal("explicit Sync did not retain owner cut", first.fences.Load(), second.fences.Load())
	}
}

func TestS3WireStreamUsesAcceptedMutationAndExplicitFence(t *testing.T) {
	for _, supported := range []bool{true, false} {
		name := "supported"
		if !supported {
			name = "unsupported"
		}
		t.Run(name, func(t *testing.T) {
			peer, host := newWirePeer(t)
			caps := CapabilityDurability
			if supported {
				caps |= CapabilityAcceptedStreams
			}
			peer.helloCaps.Store(caps)
			native := dialConnectionTest(t, host)
			api := NewS3(native)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			payload := bytes.Repeat([]byte("wire-s3-payload"), transferChunk/8)
			object, err := api.Put(ctx, "streamed-publication", bytes.NewReader(payload), uint64(len(payload)), S3PutOptions{})
			if !supported {
				var status *StatusError
				if !errors.As(err, &status) || status.Status != 426 {
					t.Fatal("S3 silently strengthened unsupported stream acceptance", err)
				}
				if peer.acceptedHeaders.Load() != 0 || peer.nativeHeaders.Load() != 0 || peer.commits.Load() != 0 || peer.fences.Load() != 0 {
					t.Fatal("unsupported S3 stream wrote or fenced storage", peer.acceptedHeaders.Load(), peer.nativeHeaders.Load(), peer.commits.Load(), peer.fences.Load())
				}
				return
			}
			if err != nil || object.Size != uint64(len(payload)) {
				t.Fatal("S3 accepted stream failed", object.Size, err)
			}
			if peer.acceptedHeaders.Load() != 1 || peer.nativeHeaders.Load() != 0 || peer.commits.Load() != 1 || peer.fences.Load() != 1 {
				t.Fatal("S3 wire publication did not use acceptance followed by an explicit fence", peer.acceptedHeaders.Load(), peer.nativeHeaders.Load(), peer.commits.Load(), peer.fences.Load())
			}
			peer.mu.Lock()
			stored := bytes.Clone(peer.data[S3MetadataID("streamed-publication")])
			peer.mu.Unlock()
			if !bytes.HasSuffix(stored, payload) {
				t.Fatal("wire stream lost S3 payload bytes")
			}
			if err := native.Close(); err != nil {
				t.Fatal(err)
			}
			if peer.fences.Load() != 1 {
				t.Fatal("closing S3 transport introduced another fence", peer.fences.Load())
			}
		})
	}
}
