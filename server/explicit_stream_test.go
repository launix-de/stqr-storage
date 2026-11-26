// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func acceptedStreamHeader(b *Bucket, id client.ObjectID, payload []byte) client.Message {
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.B = append(e.B, id[:]...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	e.U16(0x0102)
	e.Bool(false)
	e.Bool(false)
	e.Bool(false)
	e.U8(1)
	e.U64(uint64(len(payload)))
	sum := sha256.Sum256(payload)
	e.B = append(e.B, sum[:]...)
	e.Blob(nil)
	return client.Message{Kind: client.Request, Opcode: client.ObjectAccepted, ID: client.NewRequestID(), Body: e.B}
}

func TestAcceptedStreamRequiresNegotiation(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "stream", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "stream")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
	ws.durabilityCap.Store(true)
	m := acceptedStreamHeader(b, client.ObjectID{1}, []byte("stream"))
	if !objectUploadHeader(m) {
		t.Fatal("accepted stream header was not classified")
	}
	_, _, err := s.executeObjects(m, ws, quotaPrincipal(t, s))
	if statusOf(err) != 501 {
		t.Fatalf("unnegotiated stream: %v", err)
	}
	ws.uploadMu.Lock()
	n := len(ws.uploads)
	ws.uploadMu.Unlock()
	if n != 0 {
		t.Fatal("unnegotiated upload created staging")
	}
}

func TestAcceptedStreamWaitsForExplicitFence(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "stream", "hdd", 16<<20)
	var v *volume.Volume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "stream" {
			v = m.store
		}
	}
	s.mu.RUnlock()
	b, _ := createProtocolBucket(t, s, "stream")
	p := quotaPrincipal(t, s)
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
	ws.durabilityCap.Store(true)
	ws.acceptedStreamsCap.Store(true)
	payload := bytes.Repeat([]byte("accepted-stream"), 2048)
	id := client.ObjectID{9}
	m := acceptedStreamHeader(b, id, payload)
	before := v.IOStats().SyncOps
	if !ws.admitConnectionMessage(m) {
		t.Fatal("admission")
	}
	_, _, err := s.executeObjects(m, ws, p)
	if !errors.Is(err, errObjectStaged) {
		t.Fatalf("stream header: %v", err)
	}
	chunk := client.Encoder{}
	chunk.ID(m.ID)
	chunk.U64(0)
	chunk.U64(0)
	chunk.Blob(payload)
	run := func(fn func()) bool { fn(); return true }
	if err := ws.objectTransfer(client.Message{Opcode: 5, Body: chunk.B}, p, run); err != nil {
		t.Fatal(err)
	}
	end := client.Encoder{}
	end.ID(m.ID)
	end.U64(uint64(len(payload)))
	sum := sha256.Sum256(payload)
	end.B = append(end.B, sum[:]...)
	if err := ws.objectTransfer(client.Message{Opcode: 6, Body: end.B}, p, run); err != nil {
		t.Fatal(err)
	}
	reply, err := client.DecodeMessage(<-ws.responses)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Opcode != client.ObjectAccepted || reply.ID != m.ID || connectionTestStatus(reply.Body) >= 300 {
		t.Fatal("uncorrelated streamed accepted reply", reply)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("streamed accepted write started a checkpoint")
	}
	data, _, err := b.Get(context.Background(), p, volume.ObjectID(id))
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal("accepted data not visible", err)
	}
	fence := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(fence)
	_, status := ws.executeConnectionSync(context.Background(), fence)
	if status != 200 {
		t.Fatal("explicit fence failed", status)
	}
	if v.IOStats().SyncOps == before {
		t.Fatal("explicit fence did not persist streamed payload")
	}
}

func TestS3ExplicitFence(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "s3fence", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "s3fence")
	p := quotaPrincipal(t, s)
	p.frontendPermission = BucketWrite
	store := s3Store{b: b, p: p}
	id := client.ObjectID{9}
	var v *volume.Volume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "s3fence" {
			v = m.store
		}
	}
	s.mu.RUnlock()
	before := v.IOStats().SyncOps
	if _, err := store.PutFrom(context.Background(), id, bytes.NewReader([]byte("s3")), 2, client.PutOptions{RequestID: client.NewRequestID(), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("direct S3 store ignored accepted mode")
	}
	if err := store.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps == before {
		t.Fatal("S3 explicit fence did not persist data")
	}
}

func TestExplicitOnlyReferenceBurst(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "references", "hdd", 16<<20)
	var v *volume.Volume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "references" {
			v = m.store
		}
	}
	s.mu.RUnlock()
	b, _ := createProtocolBucket(t, s, "references")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	p := quotaPrincipal(t, s)
	accepted := true
	before := v.IOStats().SyncOps
	for i := 0; i < 64; i++ {
		m := referenceRequest(b, client.ObjectID{byte(i + 1)}, client.NewRequestID(), &accepted)
		ws.admitConnectionMessage(m)
		body, status := s.dispatchAs(m, ws, p)
		ws.completeConnectionMessage(m, status, body)
		if status != 200 {
			t.Fatalf("reference %d status %d", i, status)
		}
	}
	if got := v.IOStats().SyncOps; got != before {
		t.Fatalf("reference burst implicitly synced: before=%d after=%d", before, got)
	}
	fence := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(fence)
	_, status := ws.executeConnectionSync(context.Background(), fence)
	if status != 200 || v.IOStats().SyncOps == before {
		t.Fatal("explicit reference fence did not persist", status)
	}
	checkpointed := v.IOStats().SyncOps
	ws.close()
	if got := v.IOStats().SyncOps; got != checkpointed {
		t.Fatal("connection close started a checkpoint")
	}
}

func TestExplicitOnlyShortShutdownDoesNotFence(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "short-shutdown", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "short-shutdown")
	p := quotaPrincipal(t, s)
	var v *volume.Volume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == "short-shutdown" {
			v = m.store
		}
	}
	s.mu.RUnlock()
	before := v.IOStats().SyncOps
	if _, err := b.Put(context.Background(), p, volume.ObjectID{8}, []byte("unconfirmed"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.prepareShortShutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("SHORT shutdown persisted unrequested data")
	}
	if _, pending := s.acceptedMark(b.ID()); !pending {
		t.Fatal("shutdown falsely confirmed accepted mark")
	}
}
