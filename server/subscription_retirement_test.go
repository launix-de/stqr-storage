// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func publicationFixture(t *testing.T) (*Server, *wsSession, *objectSubscription) {
	t.Helper()
	s := &Server{}
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: &Bucket{}}
	sub := &objectSubscription{ws: ws, id: client.ID{1}, key: volume.Key{Bucket: volume.ID{2}, Object: volume.ObjectID{3}}, cursor: 7, epoch: 4, runtime: client.ID{5}}
	if err := s.objectSubscriptions.add(sub); err != nil {
		t.Fatal(err)
	}
	return s, ws, sub
}
func publicationControl(t *testing.T, s *Server, ws *wsSession, sub *objectSubscription, op uint16) {
	t.Helper()
	e := client.Encoder{}
	e.ID(sub.id)
	if op == client.SubscriptionAck {
		e.U64(7)
	}
	_, status, err := s.executeObjectSubscription(client.Message{Kind: client.FireAndForget, Opcode: op, Body: e.B}, ws, principal{})
	if err != nil || status != 200 {
		t.Errorf("RAM control: status%d %v", status, err)
	}
}
func publicationUpdate(t *testing.T, ws *wsSession) client.ObjectUpdate {
	t.Helper()
	select {
	case raw := <-ws.responses:
		message, err := client.DecodeMessage(raw)
		if err != nil || message.Opcode != client.ObjectUpdateMessage {
			t.Fatal(message, err)
		}
		update, err := client.DecodeObjectUpdate(message.Body)
		if err != nil {
			t.Fatal(err)
		}
		return update
	default:
		t.Fatal("missing publication")
		return client.ObjectUpdate{}
	}
}
func startBlockedPublication(t *testing.T, s *Server, sub *objectSubscription) (chan struct{}, chan struct{}) {
	t.Helper()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		s.objectSubscriptions.publishCaptured(sub.key, volume.Info{Length: 3}, 0, func(off, n uint64) ([]byte, error) { close(entered); <-release; return []byte("old"), nil })
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("payload capture did not start")
	}
	return release, done
}
func awaitPublication(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publication did not finish")
	}
}

func TestSubscriptionRetirementDuringCapture(t *testing.T) {
	for _, mode := range []string{"ack", "unsubscribe-resubscribe", "migration", "epoch-change", "credit-pressure"} {
		t.Run(mode, func(t *testing.T) {
			s, ws, sub := publicationFixture(t)
			r := &s.objectSubscriptions
			if mode == "ack" {
				sub.bytes = 131
				sub.journal = []subscriptionJournalEntry{{cursor: 7, bytes: 131}}
				r.bytes = 131
			}
			release, done := startBlockedPublication(t, s, sub)
			progressed := make(chan struct{})
			go func() {
				defer close(progressed)
				switch mode {
				case "ack":
					publicationControl(t, s, ws, sub, client.SubscriptionAck)
				case "unsubscribe-resubscribe":
					publicationControl(t, s, ws, sub, client.ObjectUnsubscribe)
					replacement := *sub
					replacement.journal = nil
					replacement.bytes = 0
					if err := r.add(&replacement); err != nil {
						t.Error(err)
					}
				case "migration":
					r.resyncWhere(func(k volume.Key) bool { return k == sub.key })
				case "epoch-change":
					r.mu.Lock()
					sub.epoch++
					r.mu.Unlock()
				case "credit-pressure":
					r.mu.Lock()
					r.bytes = maxSubscriptionJournalBytes
					r.mu.Unlock()
				}
			}()
			select {
			case <-progressed:
			case <-time.After(time.Second):
				close(release)
				awaitPublication(t, done)
				t.Fatal("RAM control waited for payload storage")
			}
			if mode == "ack" {
				r.mu.Lock()
				credits := r.bytes
				r.mu.Unlock()
				if credits != 0 {
					t.Fatal("ACK credits retained", credits)
				}
			}
			close(release)
			awaitPublication(t, done)
			r.mu.Lock()
			remaining, reserved := r.count, r.snapshotBytes
			r.mu.Unlock()
			if reserved != 0 {
				t.Fatal("snapshot reservation leaked", reserved)
			}
			switch mode {
			case "unsubscribe-resubscribe":
				if len(ws.responses) != 0 || remaining != 1 {
					t.Fatal("old capture reached replacement subscription", len(ws.responses), remaining)
				}
			case "ack":
				u := publicationUpdate(t, ws)
				if u.Cursor != 8 || u.Change != client.UpdateReplace || string(u.Data) != "old" {
					t.Fatal(u)
				}
			default:
				u := publicationUpdate(t, ws)
				if u.Change != client.UpdateResyncRequired || remaining != 0 || len(ws.responses) != 0 {
					t.Fatal("stale publication survived", u, remaining, len(ws.responses))
				}
			}
		})
	}
}
func TestSubscriptionConcurrentCaptureRequiresResnapshot(t *testing.T) {
	s, ws, sub := publicationFixture(t)
	r := &s.objectSubscriptions
	release, done := startBlockedPublication(t, s, sub)
	r.publishCaptured(sub.key, volume.Info{Length: 3}, 0, func(off, n uint64) ([]byte, error) { return []byte("new"), nil })
	newest := publicationUpdate(t, ws)
	if newest.Cursor != 8 || string(newest.Data) != "new" {
		t.Fatal(newest)
	}
	close(release)
	awaitPublication(t, done)
	resync := publicationUpdate(t, ws)
	if resync.Cursor != 9 || resync.Change != client.UpdateResyncRequired || len(resync.Data) != 0 {
		t.Fatal("older replacement followed newer cursor", resync)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count != 0 || r.bytes != 0 || r.snapshotBytes != 0 {
		t.Fatal("concurrent capture credits leaked", r.count, r.bytes, r.snapshotBytes)
	}
}
func TestSubscriptionCapturePressureAvoidsPayloadIO(t *testing.T) {
	s, ws, sub := publicationFixture(t)
	r := &s.objectSubscriptions
	r.snapshotBytes = maxObjectStagingBytes
	r.publishCaptured(sub.key, volume.Info{Length: 3}, 0, func(off, n uint64) ([]byte, error) { t.Fatal("read under exhausted capture budget"); return nil, nil })
	u := publicationUpdate(t, ws)
	if u.Change != client.UpdateResyncRequired {
		t.Fatal(u)
	}
	if r.snapshotBytes != maxObjectStagingBytes || r.count != 0 {
		t.Fatal("capture bounds changed", r.snapshotBytes, r.count)
	}
}
