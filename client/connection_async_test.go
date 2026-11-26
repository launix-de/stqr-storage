// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type connectionTestWrite struct {
	id     ID
	object ObjectID
	data   []byte
}
type connectionTestJob struct {
	done   chan struct{}
	status int
}
type connectionTestPeer struct {
	t           *testing.T
	node        ID
	boot        ID
	gate        func(ObjectID) <-chan struct{}
	status      func(ObjectID) int
	started     chan struct{}
	received    chan connectionTestWrite
	fences      chan struct{}
	connections atomic.Int64
	puts        atomic.Int64
	closeSocket chan struct{}
}

func newConnectionTestPeer(t *testing.T, node byte) (*connectionTestPeer, *httptest.Server) {
	p := &connectionTestPeer{t: t, node: ID{node}, boot: ID{node, 99}, started: make(chan struct{}, 512), received: make(chan connectionTestWrite, 512), fences: make(chan struct{}, 32), closeSocket: make(chan struct{})}
	host := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(host.Close)
	return p, host
}
func (p *connectionTestPeer) serve(w http.ResponseWriter, r *http.Request) {
	ws, e := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer ws.Close()
	p.connections.Add(1)
	var sender sync.Mutex
	send := func(m Message, status int, body []byte) {
		sender.Lock()
		defer sender.Unlock()
		_ = ws.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(status, body, "")}))
	}
	go func() {
		select {
		case <-p.closeSocket:
			ws.Close()
		case <-r.Context().Done():
		}
	}()
	var jobs []*connectionTestJob
	for {
		_, raw, e := ws.ReadMessage()
		if e != nil {
			return
		}
		m, e := DecodeMessage(raw)
		if e != nil {
			p.t.Error(e)
			return
		}
		switch m.Opcode {
		case Hello:
			result := Encoder{}
			result.U16(2)
			result.U64(CapabilityConnectionSync | CapabilityDurability)
			result.ID(p.node)
			result.ID(ID{42})
			result.U64(1)
			result.U64(1)
			send(m, 200, result.B)
		case ObjectAccepted:
			if m.Kind != FireAndForget {
				p.t.Error("write requested an individual ACK")
				return
			}
			d := Decoder{B: m.Body}
			d.ID()
			var object ObjectID
			copy(object[:], d.Take(32))
			d.Take(32)
			op := d.U16()
			if op == objectPutRange {
				d.U64()
			}
			if d.Bool() {
				d.U64()
			}
			d.Bool()
			if d.Bool() {
				d.U64()
				d.Blob()
			}
			mode, size, _, data := decodeTransfer(&d)
			if d.Done() != nil || mode != 0 || size != uint64(len(data)) {
				p.t.Error("bad no-ACK payload")
				return
			}
			job := &connectionTestJob{done: make(chan struct{})}
			jobs = append(jobs, job)
			p.started <- struct{}{}
			payload := bytes.Clone(data)
			go func(m Message, object ObjectID, job *connectionTestJob) {
				if p.gate != nil {
					select {
					case <-p.gate(object):
					case <-r.Context().Done():
						return
					}
				}
				if p.status != nil {
					job.status = p.status(object)
				}
				if job.status >= 300 {
					send(m, job.status, nil)
				} else {
					p.puts.Add(1)
				}
				p.received <- connectionTestWrite{m.ID, object, payload}
				close(job.done)
			}(m, object, job)
		case ConnectionSync:
			if len(m.Body) != 0 {
				p.t.Error("fence body is not empty")
				return
			}
			capture := append([]*connectionTestJob(nil), jobs...)
			jobs = nil
			p.fences <- struct{}{}
			go func(m Message) {
				status := 200
				for _, job := range capture {
					select {
					case <-job.done:
					case <-r.Context().Done():
						return
					}
					if status == 200 && job.status >= 300 {
						status = job.status
					}
				}
				result := Encoder{}
				result.ID(p.boot)
				result.U64(uint64(len(capture)))
				send(m, status, result.B)
			}(m)
		case objectStat:
			result := Encoder{}
			fixtureInfo(&result, 0, 0)
			send(m, 200, result.B)
		default:
			p.t.Errorf("unexpected opcode %x", m.Opcode)
			return
		}
	}
}
func dialConnectionTest(t *testing.T, host *httptest.Server) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, e := Dial(ctx, host.URL, "user", "credential", ID{7}, "fs")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func waitConnectionSignal(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not arrive")
	}
}

func TestConnectionAsyncOneSocketAndFenceCut(t *testing.T) {
	first, late := make(chan struct{}), make(chan struct{})
	p, host := newConnectionTestPeer(t, 1)
	p.gate = func(id ObjectID) <-chan struct{} {
		if id[0] == 1 {
			return first
		}
		return late
	}
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	payload := []byte("original")
	if e := c.PutAsync(ctx, ObjectID{1}, payload, PutOptions{}); e != nil {
		t.Fatal(e)
	}
	copy(payload, []byte("modified"))
	synced := make(chan error, 1)
	go func() { synced <- c.Sync(ctx) }()
	waitConnectionSignal(t, p.fences)
	if _, e := c.Stat(ctx, ObjectID{9}); e != nil {
		t.Fatal("blocked write/fence pinned independent request", e)
	}
	if e := c.PutAsync(ctx, ObjectID{2}, []byte("later"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	close(first)
	select {
	case e := <-synced:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("later write postponed earlier fence")
	}
	got := <-p.received
	if string(got.data) != "original" {
		t.Fatal("caller buffer retained after submission", string(got.data))
	}
	if !c.transport.hasConnectionWork() {
		t.Fatal("earlier fence consumed a later write")
	}
	close(late)
	if e := c.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	if p.connections.Load() != 1 {
		t.Fatal("parallel work opened extra connections", p.connections.Load())
	}
}
func TestConnectionAsyncErrorsAndLostSocket(t *testing.T) {
	t.Run("quota", func(t *testing.T) {
		p, host := newConnectionTestPeer(t, 1)
		p.status = func(ObjectID) int { return 507 }
		c := dialConnectionTest(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := NewRequestID()
		if e := c.PutAsync(ctx, ObjectID{1}, []byte("quota"), PutOptions{RequestID: request}); e != nil {
			t.Fatal(e)
		}
		e := c.Sync(ctx)
		var status *StatusError
		if !errors.As(e, &status) || status.Status != 507 || status.RequestID != request {
			t.Fatal("deferred failure lost its identity", e)
		}
		if e = c.Sync(ctx); e != nil {
			t.Fatal("consumed error poisoned later fences", e)
		}
	})
	t.Run("disconnect", func(t *testing.T) {
		gate := make(chan struct{})
		p, host := newConnectionTestPeer(t, 1)
		p.gate = func(ObjectID) <-chan struct{} { return gate }
		c := dialConnectionTest(t, host)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		request := NewRequestID()
		if e := c.PutAsync(ctx, ObjectID{1}, []byte("unknown"), PutOptions{RequestID: request}); e != nil {
			t.Fatal(e)
		}
		close(p.closeSocket)
		select {
		case <-c.transport.done:
		case <-ctx.Done():
			t.Fatal("socket did not close")
		}
		var unknown *UnknownOutcomeError
		if e := c.Sync(ctx); !errors.As(e, &unknown) || unknown.RequestID != request {
			t.Fatal("replaced transport silently confirmed write", e)
		}
		close(gate)
	})
}
func TestConnectionAsyncOwnerFencesRunConcurrently(t *testing.T) {
	a, ha := newConnectionTestPeer(t, 1)
	b, hb := newConnectionTestPeer(t, 2)
	ga, gb := make(chan struct{}), make(chan struct{})
	a.gate = func(ObjectID) <-chan struct{} { return ga }
	b.gate = func(ObjectID) <-chan struct{} { return gb }
	c := dialConnectionTest(t, ha)
	patch := testMapPatch()
	patch.Bucket = c.bucket
	patch.Policies[0].Bucket = c.bucket
	left, right, e := SplitRange(patch.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	left.Locations = []Location{{Node: a.node}}
	right.Locations = []Location{{Node: b.node}}
	patch.Entries = []RangeEntry{left, right}
	patch.Addresses = []NodeAddress{{Node: a.node, Addresses: []string{ha.URL}}, {Node: b.node, Addresses: []string{hb.URL}}}
	if _, e = c.routes.Apply(patch); e != nil {
		t.Fatal(e)
	}
	c.rememberRouters(patch)
	c.mu.Lock()
	c.clusterRouting = true
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if e = c.PutAsync(ctx, left.Start, []byte("left"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	if e = c.PutAsync(ctx, right.Start, []byte("right"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- c.Sync(ctx) }()
	waitConnectionSignal(t, a.fences)
	waitConnectionSignal(t, b.fences)
	close(ga)
	close(gb)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if a.connections.Load() != 1 || b.connections.Load() != 1 {
		t.Fatal("fence broadcast or opened redundant owner connection")
	}
}
func TestConnectionAsyncLegacyFallback(t *testing.T) {
	p, host := newWirePeer(t)
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := c.PutAsync(ctx, ObjectID{1}, []byte("fallback"), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	if p.commits.Load() != 1 {
		t.Fatal("old peer write skipped")
	}
	if e := c.Sync(ctx); e != nil {
		t.Fatal(e)
	}
}

type burstIntentStore struct {
	*MemoryFilesystemStore
	entered    chan ObjectID
	release    chan struct{}
	descriptor ObjectID
	published  atomic.Bool
}

func (s *burstIntentStore) ConcurrentObjectRequests() bool { return true }
func (s *burstIntentStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	if id == s.descriptor {
		s.published.Store(true)
	} else {
		s.entered <- id
		select {
		case <-s.release:
		case <-ctx.Done():
			return ObjectInfo{}, ctx.Err()
		}
	}
	return s.MemoryFilesystemStore.CAS(ctx, id, off, expected, replacement, opt)
}
func TestFilesystemImmutableBurstPreservesPublicationOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := ObjectID{88}
	s := &burstIntentStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), entered: make(chan ObjectID, 4), release: make(chan struct{}), descriptor: id}
	f := &Filesystem{store: s}
	raw := append(bytes.Repeat([]byte{1}, FSObjectSize), bytes.Repeat([]byte{2}, 99)...)
	done := make(chan error, 1)
	go func() { _, err := f.storeIntentAt(ctx, id, raw); done <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-s.entered:
		case <-ctx.Done():
			t.Fatal("immutable writes serialized", ctx.Err())
		}
	}
	if s.published.Load() {
		t.Fatal("descriptor published before durable pieces")
	}
	close(s.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	descriptor, _, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	d := Decoder{B: descriptor}
	d.Take(4)
	d.U16()
	d.U8()
	d.U32()
	count := d.U32()
	var assembled []byte
	for i := uint32(0); i < count; i++ {
		var part ObjectID
		copy(part[:], d.Take(32))
		b, _, err := s.Get(ctx, part)
		if err != nil {
			t.Fatal(err)
		}
		assembled = append(assembled, b...)
	}
	if d.Done() != nil || !bytes.Equal(assembled, raw) {
		t.Fatal("completion order changed intent bytes")
	}
}

func TestConnectionAsyncCloseDoesNotFence(t *testing.T) {
	p, host := newConnectionTestPeer(t, 1)
	gate := make(chan struct{})
	p.gate = func(ObjectID) <-chan struct{} { return gate }
	c := dialConnectionTest(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.PutAsync(ctx, ObjectID{1}, []byte("unfenced"), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.started:
	case <-ctx.Done():
		t.Fatal("server did not admit queued write", ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close waited for storage", ctx.Err())
	}
	select {
	case <-p.fences:
		t.Fatal("Close implicitly requested durability")
	default:
	}
	close(gate)
}

type queuedFSWrite struct {
	id   ObjectID
	data []byte
}
type queuedFSBurstStore struct {
	*MemoryFilesystemStore
	mu            sync.Mutex
	entered       chan struct{}
	releaseWrites chan struct{}
	fenceStarted  chan struct{}
	releaseFence  chan struct{}
	queued        []queuedFSWrite
	fences        int
	failFence     error
	references    bool
}

func (s *queuedFSBurstStore) ObjectReferencesSupported() bool { return s.references }
func (s *queuedFSBurstStore) ConcurrentObjectRequests() bool  { return true }
func (s *queuedFSBurstStore) Put(ctx context.Context, id ObjectID, data []byte, opt PutOptions) (ObjectInfo, error) {
	s.entered <- struct{}{}
	select {
	case <-s.releaseWrites:
	case <-ctx.Done():
		return ObjectInfo{}, ctx.Err()
	}
	before, _, err := s.MemoryFilesystemStore.Get(ctx, id)
	absent := errors.Is(err, ErrNotFound)
	if err != nil && !absent {
		return ObjectInfo{}, err
	}
	info, err := s.MemoryFilesystemStore.CAS(ctx, id, 0, before, data, CASOptions{Scope: CASObject, MutationOptions: MutationOptions{Absent: absent}})
	if err == nil {
		s.mu.Lock()
		s.queued = append(s.queued, queuedFSWrite{id, bytes.Clone(data)})
		s.mu.Unlock()
	}
	return info, err
}
func (s *queuedFSBurstStore) Sync(ctx context.Context) error {
	s.mu.Lock()
	s.fences++
	failure := s.failFence
	s.mu.Unlock()
	s.fenceStarted <- struct{}{}
	select {
	case <-s.releaseFence:
	case <-ctx.Done():
		return ctx.Err()
	}
	if failure != nil {
		return failure
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queued = nil
	return nil
}

func TestFilesystemExternalBurstUsesCompletionWithoutImplicitSync(t *testing.T) {
	for _, references := range []bool{false, true} {
		name := "legacy"
		if references {
			name = "pinned"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := &queuedFSBurstStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), entered: make(chan struct{}, 8), releaseWrites: make(chan struct{}), fenceStarted: make(chan struct{}, 8), releaseFence: make(chan struct{}), references: references}
			f, err := OpenFilesystem(ctx, s, FSOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			snap, err := f.snapshot(ctx, FilesystemRootID())
			if err != nil {
				t.Fatal(err)
			}
			next := snap.node
			next.mode = 0711
			next.edits = []fsBlockWrite{{id: ObjectID{61}, full: true, data: []byte("one")}, {id: ObjectID{62}, full: true, data: []byte("two")}}
			done := make(chan error, 1)
			go func() {
				ok, err := f.transact(ctx, []fsTxMember{fsMember(snap, next)})
				if err == nil && !ok {
					err = errors.New("transaction aborted")
				}
				done <- err
			}()
			for i := 0; i < 2; i++ {
				select {
				case <-s.entered:
				case <-ctx.Done():
					t.Fatal("independent mutable payloads serialized", ctx.Err())
				}
			}
			head, _, err := s.MemoryFilesystemStore.Get(ctx, FilesystemRootID())
			if err != nil {
				t.Fatal(err)
			}
			txID, locked, err := fsLockID(head)
			if err != nil || !locked {
				t.Fatal("inode published before payload completion", err)
			}
			if _, _, err = s.MemoryFilesystemStore.Get(ctx, txID); err != nil {
				t.Fatal("recovery intent absent before publication", err)
			}
			close(s.releaseWrites)
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("normal write requested an implicit physical fence", ctx.Err())
			}
			info, err := f.Stat(ctx, FilesystemRootID())
			if err != nil || info.Mode != 0711 {
				t.Fatal("inode visibility failed", info, err)
			}
			for _, w := range next.edits {
				raw, _, err := s.MemoryFilesystemStore.Get(ctx, w.id)
				if err != nil || !bytes.Equal(raw, w.data) {
					t.Fatal("published inode preceded payload visibility", string(raw), err)
				}
			}
			if s.fences != 0 {
				t.Fatal("normal write requested physical Sync", s.fences)
			}
			go func() { done <- f.Sync(ctx, FilesystemRootID()) }()
			select {
			case <-s.fenceStarted:
			case <-ctx.Done():
				t.Fatal("explicit Sync missing", ctx.Err())
			}
			close(s.releaseFence)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			if s.fences != 1 {
				t.Fatal("explicit Sync count", s.fences)
			}
		})
	}
}
func TestFilesystemExternalBurstExplicitFenceFailureIsReturned(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	close(release)
	failure := errors.New("checkpoint failed")
	s := &queuedFSBurstStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), entered: make(chan struct{}, 16), releaseWrites: release, fenceStarted: make(chan struct{}, 16), releaseFence: release, failFence: failure, references: true}
	f, err := OpenFilesystem(ctx, s, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := f.snapshot(ctx, FilesystemRootID())
	if err != nil {
		t.Fatal(err)
	}
	next := snap.node
	next.mode = 0711
	next.edits = []fsBlockWrite{{id: ObjectID{71}, full: true, data: []byte("recover")}, {id: ObjectID{72}, full: true, data: []byte("also")}}
	if ok, err := f.transact(ctx, []fsTxMember{fsMember(snap, next)}); err != nil || !ok {
		t.Fatal("normal mutation waited for failing physical sync", ok, err)
	}
	if err = f.Sync(ctx, FilesystemRootID()); !errors.Is(err, failure) {
		t.Fatal("explicit checkpoint failure lost", err)
	}
	s.failFence = nil
	if err = f.Sync(ctx, FilesystemRootID()); err != nil {
		t.Fatal("checkpoint retry failed", err)
	}
	for _, w := range next.edits {
		raw, _, err := s.MemoryFilesystemStore.Get(ctx, w.id)
		if err != nil || !bytes.Equal(raw, w.data) {
			t.Fatal("explicit checkpoint lost payload", string(raw), err)
		}
	}
	if s.fences != 2 {
		t.Fatal("normal write introduced extra checkpoint", s.fences)
	}
}
func TestFilesystemExternalBurstRepeatedObjectUsesCausalCompletion(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	close(release)
	s := &queuedFSBurstStore{MemoryFilesystemStore: NewMemoryFilesystemStore(), entered: make(chan struct{}, 8), releaseWrites: release, fenceStarted: make(chan struct{}, 8), releaseFence: release}
	f := &Filesystem{store: s}
	id := ObjectID{73}
	jobs := []fsExternalWrite{{index: 0, write: fsBlockWrite{id: id, full: true, data: []byte("first")}}, {index: 1, write: fsBlockWrite{id: id, full: true, data: []byte("last")}}}
	if err := f.applyExternalWrites(ctx, ObjectID{74}, jobs, true); err != nil {
		t.Fatal(err)
	}
	raw, _, err := s.MemoryFilesystemStore.Get(ctx, id)
	if err != nil || string(raw) != "last" || s.fences != 0 {
		t.Fatal("dependent writes reordered or implicitly synced", string(raw), s.fences, err)
	}
}
