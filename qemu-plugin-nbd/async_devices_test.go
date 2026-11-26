// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type queuedBlockWrite struct {
	id    client.ObjectID
	off   uint64
	data  []byte
	whole bool
}
type queuedBlockStore struct {
	*testStore
	queueMu sync.Mutex
	queued  []queuedBlockWrite
	fail    error
	fences  int
}

func (s *queuedBlockStore) PutAsync(_ context.Context, id client.ObjectID, data []byte, _ client.PutOptions) error {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	s.queued = append(s.queued, queuedBlockWrite{id: id, data: bytes.Clone(data), whole: true})
	return nil
}
func (s *queuedBlockStore) PutRangeAsync(_ context.Context, id client.ObjectID, off uint64, data []byte, _ client.PutOptions) error {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	s.queued = append(s.queued, queuedBlockWrite{id: id, off: off, data: bytes.Clone(data)})
	return nil
}
func (s *queuedBlockStore) Sync(ctx context.Context) error {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	s.fences++
	if s.fail != nil {
		return s.fail
	}
	// Reverse completion deliberately: callers must fence only genuine dependencies.
	for i := len(s.queued) - 1; i >= 0; i-- {
		w := s.queued[i]
		var err error
		if w.whole {
			_, err = s.testStore.Put(ctx, w.id, w.data, client.PutOptions{})
		} else {
			_, err = s.testStore.PutRange(ctx, w.id, w.off, w.data, client.PutOptions{})
		}
		if err != nil {
			return err
		}
	}
	s.queued = nil
	return nil
}
func TestAsyncBlockWritesPipelineAndFenceOverlaps(t *testing.T) {
	ctx := context.Background()
	s := &queuedBlockStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "async", Size: 3 * StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	baseline := s.fences
	if err = c.WriteAt(ctx, m, []byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteAt(ctx, m, []byte("independent"), StripeSize); err != nil {
		t.Fatal(err)
	}
	if s.fences != baseline || len(s.queued) != 2 {
		t.Fatal("independent ranges did not pipeline", s.fences, len(s.queued))
	}
	if err = c.WriteAt(ctx, m, []byte("last!"), 0); err != nil {
		t.Fatal(err)
	}
	if s.fences != baseline+1 || len(s.queued) != 1 {
		t.Fatal("overlap was not fenced", s.fences, len(s.queued))
	}
	got := make([]byte, 5)
	if err = c.ReadAt(ctx, m, got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "last!" || s.fences != baseline+2 {
		t.Fatal("read overtook queued write", string(got), s.fences)
	}
	if err = c.WriteAt(ctx, m, []byte("trim"), 2*StripeSize); err != nil {
		t.Fatal(err)
	}
	if err = c.Trim(ctx, m, 2*StripeSize, StripeSize); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Get(ctx, StripeID(m.ID, 2)); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("late write resurrected trimmed stripe", err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got = make([]byte, 11)
	if err = c.ReadAt(ctx, m, got, StripeSize); err != nil || string(got) != "independent" {
		t.Fatal("independent stripe lost", string(got), err)
	}
}
func TestAsyncBlockFenceFailureRetainsPending(t *testing.T) {
	ctx := context.Background()
	s := &queuedBlockStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "failure", Size: StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.WriteAt(ctx, m, []byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("checkpoint failed")
	s.fail = failure
	if err = c.Sync(ctx); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err = c.WriteAt(ctx, m, []byte("second"), 0); !errors.Is(err, failure) {
		t.Fatal("overlap ignored failed fence", err)
	}
	if len(s.queued) != 1 {
		t.Fatal("dependent write admitted after failed fence")
	}
	s.fail = nil
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if err = c.ReadAt(ctx, m, got, 0); err != nil || string(got) != "first" {
		t.Fatal(string(got), err)
	}
}

func TestBucketCloseDoesNotFlushConnection(t *testing.T) {
	admitted := make(chan struct{})
	observed := make(chan uint16, 8)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			m, err := client.DecodeMessage(raw)
			if err != nil {
				return
			}
			if m.Opcode == client.Hello {
				e := client.Encoder{}
				e.U16(2)
				e.U64(client.CapabilityDurability | client.CapabilityConnectionSync)
				e.ID(client.ID{1})
				e.ID(client.ID{42})
				e.U64(1)
				e.U64(1)
				ws.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(200, e.B, "")}))
				continue
			}
			observed <- m.Opcode
			if m.Opcode == client.ObjectAccepted {
				close(admitted)
			}
		}
	}))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, "user", "password", client.ID{7}, "bd")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.PutAsync(ctx, client.ObjectID{1}, []byte("pending"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-admitted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	b := &Bucket{Client: c}
	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("bucket Close waited for an unsolicited fence")
	}
	for len(observed) > 0 {
		if op := <-observed; op == client.ConnectionSync || op == client.BucketSync {
			t.Fatal("Close requested durability", op)
		}
	}
}
