// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func resyncFixture(t *testing.T) (*Server, *wsSession, *objectSubscription) {
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
func resyncControl(t *testing.T, s *Server, ws *wsSession, sub *objectSubscription, op uint16) {
	t.Helper()
	e := client.Encoder{}
	e.ID(sub.id)
	if op == client.SubscriptionAck {
		e.U64(sub.cursor)
	}
	_, status, err := s.executeObjectSubscription(client.Message{Kind: client.FireAndForget, Opcode: op, Body: e.B}, ws, principal{})
	if err != nil || status != 200 {
		t.Fatal("RAM subscription control failed", status, err)
	}
}

func fillSubscriptionResponseQueue(ws *wsSession) []byte {
	wire := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: client.Ping, ID: client.NewRequestID(), Body: client.ResponseBody(200, nil, "")})
	for len(ws.responses) < cap(ws.responses) {
		ws.responses <- wire
	}
	return wire
}

func deferSubscriptionResync(t *testing.T) (*Server, *wsSession, *objectSubscription) {
	t.Helper()
	s, ws, sub := resyncFixture(t)
	fillSubscriptionResponseQueue(ws)
	s.objectSubscriptions.publish(sub.key, volume.Info{Deleted: true}, nil)
	select {
	case <-ws.done:
		t.Fatal("bounded cache invalidation closed unrelated object transport")
	default:
	}
	if s.objectSubscriptions.count != 0 || ws.subscriptionResync.count() != 1 || s.objectSubscriptions.resyncCount.Load() != 1 {
		t.Fatal("overflow must replace the descriptor with one accounted resnapshot notice")
	}
	t.Cleanup(ws.close)
	return s, ws, sub
}

func TestSubscriptionResyncRequiresUnsubscribeBeforeIDReuse(t *testing.T) {
	s, ws, sub := deferSubscriptionResync(t)
	replacement := *sub
	replacement.cursor = 100
	if err := s.objectSubscriptions.add(&replacement); statusOf(err) != 409 {
		t.Fatal("pending old invalidation permits ambiguous subscription ID reuse", err)
	}
	resyncControl(t, s, ws, sub, client.ObjectUnsubscribe)
	if ws.subscriptionResync.count() != 0 || s.objectSubscriptions.resyncCount.Load() != 0 {
		t.Fatal("unsubscribe retained an unsent invalidation")
	}
	if err := s.objectSubscriptions.add(&replacement); err != nil {
		t.Fatal("unsubscribe barrier did not permit fresh ID registration", err)
	}
	if !ws.writeSubscriptionResync(func(wire []byte) bool {
		m, err := client.DecodeMessage(wire)
		if err != nil || m.Opcode == client.ObjectUpdateMessage {
			t.Error("canceled notice emitted", err)
		}
		return true
	}) {
		t.Fatal("empty mailbox output failed")
	}
}

func TestSubscriptionResyncCancellationAfterWriterSelection(t *testing.T) {
	s, ws, sub := deferSubscriptionResync(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	var firstBatch [][]byte
	go func() {
		done <- ws.writeSubscriptionResync(func(wire []byte) bool {
			if len(firstBatch) == 0 {
				close(entered)
				<-release
			}
			firstBatch = append(firstBatch, wire)
			return true
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not select the pending invalidation")
	}
	// Retire the selected old notice before ACK admission, as the receiver's
	// inline UNSUBSCRIBE control does. A fresh descriptor may reuse the SID.
	resyncControl(t, s, ws, sub, client.ObjectUnsubscribe)
	ack := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: client.ObjectUnsubscribe, ID: client.NewRequestID(), Body: client.ResponseBody(200, nil, "")})
	ws.responses <- ack
	replacement := *sub
	replacement.cursor = 100
	if err := s.objectSubscriptions.add(&replacement); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	// The newer descriptor may itself need resnapshot while the old writer is
	// blocked. Finishing that old selection must not erase the new notice.
	s.objectSubscriptions.publish(sub.key, volume.Info{Deleted: true}, nil)
	if s.objectSubscriptions.resyncCount.Load() != 2 {
		close(release)
		<-done
		t.Fatal("selected canceled bytes escaped retention accounting")
	}
	close(release)
	if !<-done {
		t.Fatal("writer could not retire its canceled selection")
	}
	for _, wire := range firstBatch {
		m, err := client.DecodeMessage(wire)
		if err != nil || m.Opcode == client.ObjectUpdateMessage {
			t.Fatal("old invalidation survived its cancellation barrier", m, err)
		}
	}
	if ws.subscriptionResync.count() != 1 || s.objectSubscriptions.resyncCount.Load() != 1 {
		t.Fatal("finishing an old selection retired the reused SID's new notice")
	}
	var final [][]byte
	if !ws.writeSubscriptionResync(func(wire []byte) bool { final = append(final, wire); return true }) {
		t.Fatal("new invalidation could not be emitted")
	}
	if len(final) != 2 || !bytes.Equal(final[0], ack) {
		t.Fatal("new invalidation overtook UNSUBSCRIBE acknowledgement", len(final))
	}
	m, err := client.DecodeMessage(final[1])
	if err != nil {
		t.Fatal(err)
	}
	u, err := client.DecodeObjectUpdate(m.Body)
	if err != nil || u.Subscription != sub.id || u.Cursor != 101 || u.Change != client.UpdateResyncRequired {
		t.Fatal("wrong subscription incarnation invalidated", u, err)
	}
	if ws.subscriptionResync.count() != 0 || s.objectSubscriptions.resyncCount.Load() != 0 {
		t.Fatal("successful output retained credits")
	}
}

func TestSubscriptionResyncRevocationClosesPendingOnlySession(t *testing.T) {
	s, ws, _ := deferSubscriptionResync(t)
	s.endRevokedSubscriptions(ws)
	select {
	case <-ws.done:
	default:
		t.Fatal("revocation ignored a session with only pending cache coverage")
	}
	if ws.subscriptionResync.count() != 0 || s.objectSubscriptions.resyncCount.Load() != 0 {
		t.Fatal("revocation retained pending invalidations")
	}
}

func TestSubscriptionResyncOutputFailureReleasesSelectedCredits(t *testing.T) {
	s, ws, _ := deferSubscriptionResync(t)
	if ws.writeSubscriptionResync(func([]byte) bool { return false }) {
		t.Fatal("actual output failure was suppressed")
	}
	if ws.subscriptionResync.count() != 0 || s.objectSubscriptions.resyncCount.Load() != 0 {
		t.Fatal("failed output leaked selected invalidation credits")
	}
}

func TestSubscriptionResyncClosedSessionRejectsLatePublisher(t *testing.T) {
	s, ws, sub := deferSubscriptionResync(t)
	r := &s.objectSubscriptions
	// requestLoop cleanup may precede the final ws.close. A captured publisher
	// and a late registration must not repopulate the already retired session.
	r.closeSession(ws)
	if err := r.add(sub); !errors.Is(err, context.Canceled) {
		t.Fatal("registration repopulated a retired session", err)
	}
	r.mu.Lock()
	r.deferResyncLocked(sub, client.ObjectUpdate{Subscription: sub.id, Cursor: 9, Parts: 1})
	r.mu.Unlock()
	if ws.subscriptionResync.count() != 0 || r.resyncCount.Load() != 0 || r.count != 0 {
		t.Fatal("late publisher leaked invalidation credits after session retirement")
	}
}

func TestSubscriptionResyncRetentionSharesSubscriptionBudgets(t *testing.T) {
	s := &Server{}
	r := &s.objectSubscriptions
	var sessions []*wsSession
	defer func() {
		for _, ws := range sessions {
			ws.close()
		}
		if r.resyncCount.Load() != 0 {
			t.Error("session teardown leaked global invalidation credits")
		}
	}()
	for n := 0; n < maxObjectSubscriptions/maxSessionObjectSubscriptions; n++ {
		ws := connectionTestSession(s)
		ws.authorization = &connectionAuthorization{bucket: &Bucket{}}
		sessions = append(sessions, ws)
		fillSubscriptionResponseQueue(ws)
		for i := 0; i < maxSessionObjectSubscriptions; i++ {
			key := volume.Key{Object: volume.ObjectID{byte(i), byte(i >> 8), byte(n)}}
			sub := &objectSubscription{ws: ws, id: client.NewRequestID(), key: key, cursor: 1}
			if err := r.add(sub); err != nil {
				t.Fatal("valid retention admission failed", n, i, err)
			}
			r.publish(key, volume.Info{Deleted: true}, nil)
		}
		if ws.subscriptionResync.count() != maxSessionObjectSubscriptions {
			t.Fatal("pending notices did not replace active descriptors")
		}
		extra := &objectSubscription{ws: ws, id: client.NewRequestID()}
		if err := r.add(extra); statusOf(err) != 507 {
			t.Fatal("retired descriptors escaped the session budget", err)
		}
	}
	if r.count != 0 || r.resyncCount.Load() != maxObjectSubscriptions {
		t.Fatal("global active/pending accounting is inconsistent", r.count, r.resyncCount.Load())
	}
	other := connectionTestSession(s)
	other.authorization = &connectionAuthorization{bucket: &Bucket{}}
	defer other.close()
	if err := r.add(&objectSubscription{ws: other, id: client.NewRequestID()}); statusOf(err) != 507 {
		t.Fatal("pending notices escaped the global subscription budget", err)
	}
}

func TestSubscriptionResyncCanceledSelectionRetainsSessionCredit(t *testing.T) {
	s, ws, sub := deferSubscriptionResync(t)
	selected := ws.subscriptionResync.take()
	resyncControl(t, s, ws, sub, client.ObjectUnsubscribe)
	for i := 0; i < maxSessionObjectSubscriptions-1; i++ {
		if err := s.objectSubscriptions.add(&objectSubscription{ws: ws, id: client.NewRequestID()}); err != nil {
			t.Fatal("valid remaining session credit rejected", i, err)
		}
	}
	extra := &objectSubscription{ws: ws, id: client.NewRequestID()}
	if err := s.objectSubscriptions.add(extra); statusOf(err) != 507 {
		t.Fatal("canceled selected bytes escaped the session retention budget", err)
	}
	for _, notice := range selected {
		ws.subscriptionResync.finish(notice)
	}
	if err := s.objectSubscriptions.add(extra); err != nil {
		t.Fatal("writer retirement did not return the retained session credit", err)
	}
}
