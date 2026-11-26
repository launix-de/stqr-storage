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

type visibleBlockStore struct {
	*testStore
	gate    func([]byte) <-chan struct{}
	started chan string
	fenceMu sync.Mutex
	fail    error
	fences  int
}

func (s *visibleBlockStore) ConcurrentObjectRequests() bool { return true }
func (s *visibleBlockStore) await(ctx context.Context, data []byte, opt client.PutOptions) error {
	if !opt.Accepted {
		return errors.New("block write requested physical durability")
	}
	s.started <- string(data[:min(len(data), 16)])
	if s.gate != nil {
		select {
		case <-s.gate(data):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (s *visibleBlockStore) Put(ctx context.Context, id client.ObjectID, data []byte, opt client.PutOptions) (client.ObjectInfo, error) {
	if err := s.await(ctx, data, opt); err != nil {
		return client.ObjectInfo{}, err
	}
	return s.testStore.Put(ctx, id, data, opt)
}
func (s *visibleBlockStore) PutRange(ctx context.Context, id client.ObjectID, off uint64, data []byte, opt client.PutOptions) (client.ObjectInfo, error) {
	if err := s.await(ctx, data, opt); err != nil {
		return client.ObjectInfo{}, err
	}
	return s.testStore.PutRange(ctx, id, off, data, opt)
}
func (s *visibleBlockStore) Sync(context.Context) error {
	s.fenceMu.Lock()
	defer s.fenceMu.Unlock()
	s.fences++
	return s.fail
}
func waitBlockOperations(t *testing.T, c *Catalog, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		c.writeMu.Lock()
		count := len(c.operations)
		c.writeMu.Unlock()
		if count == n {
			return
		}
		select {
		case <-deadline:
			t.Fatal("range dependency was not admitted", n, count)
		case <-ticker.C:
		}
	}
}
func TestAsyncBlockWritesUseVisibilityWithoutImplicitFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate := make(chan struct{})
	ready := make(chan struct{})
	close(ready)
	s := &visibleBlockStore{testStore: newTestStore(), started: make(chan string, 16)}
	s.gate = func(data []byte) <-chan struct{} {
		if string(data) == "first" {
			return gate
		}
		return ready
	}
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "async", Size: 3 * StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- c.WriteAt(ctx, m, []byte("first"), 0) }()
	select {
	case <-s.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = c.WriteAt(ctx, m, []byte("independent"), StripeSize); err != nil {
		t.Fatal("independent write blocked", err)
	}
	<-s.started
	last := make(chan error, 1)
	go func() { last <- c.WriteAt(ctx, m, []byte("last!"), 0) }()
	waitBlockOperations(t, c, 2)
	read := make(chan error, 1)
	got := make([]byte, 5)
	go func() { read <- c.ReadAt(ctx, m, got, 0) }()
	waitBlockOperations(t, c, 3)
	select {
	case <-s.started:
		t.Fatal("overlapping write overtook pending result")
	default:
	}
	close(gate)
	if err = <-first; err != nil {
		t.Fatal(err)
	}
	if err = <-last; err != nil {
		t.Fatal(err)
	}
	if err = <-read; err != nil || string(got) != "last!" {
		t.Fatal("read overtook write visibility", string(got), err)
	}
	if err = c.Trim(ctx, m, 0, StripeSize); err != nil {
		t.Fatal(err)
	}
	if s.fences != 0 {
		t.Fatal("overlap/read/trim requested physical Sync", s.fences)
	}
	if err = c.Sync(ctx); err != nil || s.fences != 1 {
		t.Fatal("explicit flush missing", s.fences, err)
	}
}
func TestAsyncBlockIndependentStripesOverlap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gate := make(chan struct{})
	s := &visibleBlockStore{testStore: newTestStore(), started: make(chan string, 8), gate: func([]byte) <-chan struct{} { return gate }}
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "stripes", Size: 2 * StripeSize})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{1}, int(2*StripeSize))
	done := make(chan error, 1)
	go func() { done <- c.WriteAt(ctx, m, data, 0) }()
	for i := 0; i < 2; i++ {
		select {
		case <-s.started:
		case <-ctx.Done():
			t.Fatal("independent stripes serialized", ctx.Err())
		}
	}
	close(gate)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if err = c.ReadAt(ctx, m, got, 0); err != nil || !bytes.Equal(data, got) {
		t.Fatal("stripe visibility mismatch", err)
	}
	if s.fences != 0 {
		t.Fatal("striping requested physical durability")
	}
}
func TestAsyncBlockExplicitFenceFailureDoesNotBlockVisibility(t *testing.T) {
	ctx := context.Background()
	s := &visibleBlockStore{testStore: newTestStore(), started: make(chan string, 8)}
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
	if err = c.WriteAt(ctx, m, []byte("last!"), 0); err != nil {
		t.Fatal("normal write requested another physical fence", err)
	}
	got := make([]byte, 5)
	if err = c.ReadAt(ctx, m, got, 0); err != nil || string(got) != "last!" {
		t.Fatal(string(got), err)
	}
	s.fail = nil
	if err = c.Sync(ctx); err != nil || s.fences != 2 {
		t.Fatal("explicit retry missing", s.fences, err)
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
