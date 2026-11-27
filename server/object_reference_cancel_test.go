// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestObjectReferenceWebSocketCancelBeforeGate(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		for _, action := range []byte{0, 1} {
			t.Run(fmt.Sprintf("accepted=%t/action=%d", accepted, action), func(t *testing.T) {
				s := testServer(t)
				addTestVolume(t, s, "reference-cancel", "hdd", 8<<20)
				b, _ := createProtocolBucket(t, s, "reference-cancel")
				p := quotaPrincipal(t, s)
				token := client.NewRequestID()
				target := client.ObjectID{42}
				uid, err := bucketID(p.User.ID)
				if err != nil {
					t.Fatal(err)
				}
				entries := []objectReferenceEntry{{object: volume.ObjectID{77}, proof: [32]byte{8}, account: uid}}
				if action == 1 {
					entries = append(entries, objectReferenceEntry{object: volume.ObjectID(target), proof: sha256.Sum256(token[:]), account: uid})
				}
				root := volume.ObjectID(client.ObjectReferenceRootID())
				original := encodeObjectReferences(entries, b.ID())
				if _, err = b.Put(context.Background(), p, root, original, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
					t.Fatal(err)
				}
				host := httptest.NewServer(s.Handler())
				defer host.Close()
				admin := s.cfg.Auth.BootstrapAdmin
				conn, _, err := dialTestSocket(t, host.URL, admin.Username, admin.Password, b.ID().String())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				readResponse := func(id client.ID) client.Message {
					for {
						got := connectionTestRead(t, conn)
						if got.ID == id {
							return got
						}
					}
				}

				hello := connectionTestMessage(client.Hello, client.Request)
				e := client.Encoder{}
				e.U16(2)
				e.U16(2)
				e.U64(client.CapabilityDurability | client.CapabilityConnectionSync | client.CapabilityObjectReferences | client.CapabilityAcceptedReferences)
				e.Text("reference-cancel-test")
				hello.Body = e.B
				connectionTestSend(t, conn, hello)
				reply := connectionTestRead(t, conn)
				if reply.ID != hello.ID || connectionTestStatus(reply.Body) != 200 {
					t.Fatal("hello", reply)
				}
				var ws *wsSession
				s.wsMu.Lock()
				for candidate := range s.sockets {
					ws = candidate
				}
				s.wsMu.Unlock()
				if ws == nil {
					t.Fatal("socket not registered")
				}
				release, err := s.referenceCoordination.acquire(context.Background(), b.ID())
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if release != nil {
						release()
					}
				}()
				var trailer *bool
				if accepted {
					trailer = &accepted
				}
				m := referenceRequest(b, target, token, trailer)
				m.Body[112] = action
				backend := b.allVolumes()[0]
				before := backend.IOStats()
				connectionTestSend(t, conn, m)
				referenceGateWaiters(t, &s.referenceCoordination, b.ID(), 2)
				cancel := client.Encoder{}
				cancel.ID(m.ID)
				connectionTestSend(t, conn, client.Message{Kind: client.FireAndForget, Opcode: 7, ID: client.NewRequestID(), Body: cancel.B})
				if err = conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				reply = readResponse(m.ID)
				if reply.ID != m.ID || connectionTestStatus(reply.Body) != 499 {
					t.Fatal("queued reference was not cancelled", reply)
				}
				after := backend.IOStats()
				if testStorageReadOps(after) != testStorageReadOps(before) || after.WriteOps != before.WriteOps || after.SyncOps != before.SyncOps {
					t.Fatal("cancelled queued reference reached storage", before, after)
				}
				ws.objectMu.Lock()
				_, registered := ws.objectContexts[m.ID]
				remaining := len(ws.objectContexts)
				ws.objectMu.Unlock()
				if registered || remaining != 0 {
					t.Fatal("reference context leaked", registered, remaining)
				}
				got, _, err := b.Get(context.Background(), p, root)
				if err != nil || !bytes.Equal(got, original) {
					t.Fatal("cancel changed root", err)
				}
				release()
				release = nil
				// Retirement completes independently of the response writer. Await it before
				// deliberately reusing this ID, so retry must obtain a fresh request context.
				deadline := time.Now().Add(time.Second)
				for {
					ws.fenceMu.Lock()
					_, pending := ws.connectionJobs[m.ID]
					ws.fenceMu.Unlock()
					if !pending {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("request work leaked")
					}
					time.Sleep(time.Millisecond)
				}
				conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				ping := connectionTestMessage(client.Ping, client.Request)
				ping.Body = make([]byte, 8)
				connectionTestSend(t, conn, ping)
				reply = readResponse(ping.ID)
				if reply.ID != ping.ID || connectionTestStatus(reply.Body) != 200 {
					t.Fatal("socket unusable after CANCEL")
				}
				connectionTestSend(t, conn, m)
				reply = readResponse(m.ID)
				if reply.ID != m.ID || connectionTestStatus(reply.Body) != 200 {
					t.Fatal("same-ID retry inherited cancellation", reply)
				}
				got, _, err = b.Get(context.Background(), p, root)
				if err != nil {
					t.Fatal(err)
				}
				changed, err := decodeObjectReferences(got, b.ID())
				if err != nil {
					t.Fatal(err)
				}
				want := len(entries) + 1
				if action == 1 {
					want = len(entries) - 1
				}
				if len(changed) != want {
					t.Fatal("retry did not execute", len(changed), want)
				}
				ws.objectMu.Lock()
				remaining = len(ws.objectContexts)
				ws.objectMu.Unlock()
				if remaining != 0 {
					t.Fatal("successful reference context leaked", remaining)
				}
				// A late CANCEL is not a rollback of an already confirmed mutation.
				connectionTestSend(t, conn, client.Message{Kind: client.FireAndForget, Opcode: 7, ID: client.NewRequestID(), Body: cancel.B})
				connectionTestSend(t, conn, ping)
				reply = readResponse(ping.ID)
				if reply.ID != ping.ID || connectionTestStatus(reply.Body) != 200 {
					t.Fatal("late CANCEL damaged connection")
				}
				stable, _, err := b.Get(context.Background(), p, root)
				if err != nil || !bytes.Equal(stable, got) {
					t.Fatal("late CANCEL rolled back confirmation", err)
				}
			})
		}
	}
}

func TestObjectRequestContextStreamHeaderLifetime(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "context-stream", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "context-stream")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
	ws.durabilityCap.Store(true)
	ws.connectionFenceCap.Store(true)
	ws.acceptedStreamsCap.Store(true)
	m := acceptedStreamHeader(b, client.ObjectID{7}, bytes.Repeat([]byte("x"), 4096))
	if !usesObjectRequestContext(m.Opcode) || !ws.admitConnectionMessage(m) || !ws.registerObjectContext(m.ID) {
		t.Fatal("stream context admission")
	}
	ws.executeRequest(m, quotaPrincipal(t, s))
	ws.objectMu.Lock()
	_, present := ws.objectContexts[m.ID]
	ws.objectMu.Unlock()
	if !present {
		t.Fatal("status-zero upload header retired context")
	}
	if !ws.discardUpload(m.ID) {
		t.Fatal("stream header was not staged")
	}
	ws.objectMu.Lock()
	_, present = ws.objectContexts[m.ID]
	ws.objectMu.Unlock()
	if present {
		t.Fatal("discarded upload retained context")
	}
}
