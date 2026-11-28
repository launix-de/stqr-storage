// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type combiningFixture struct {
	s  *Server
	b  *Bucket
	v  *volume.Volume
	ws *wsSession
	p  principal
}

func newCombiningFixture(t testing.TB) *combiningFixture {
	t.Helper()
	s, e := openTransportTestNode(Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml"), values: map[string]string{"system-volume-bytes": "262144"}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if _, e = s.AddVolume(newID(), AddVolumeRequest{Name: "refs", Class: "ram", Source: 2, SizeBytes: 64 << 20}); e != nil {
		t.Fatal(e)
	}
	id, _ := volume.NewID()
	draft, e := NewBucket(id, "combined")
	if e != nil {
		t.Fatal(e)
	}
	draft.description.Policy.StorageStrategy = "ram"
	info, _ := draft.Info(volume.ID{}, true)
	body, _ := info.MarshalBinary()
	account, e := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if e != nil {
		t.Fatal(e)
	}
	p := principal{User: account}
	if _, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.BucketCreate, ID: client.NewRequestID(), Body: body[:len(body)-4]}, nil, p); status != 201 {
		t.Fatal("create", status)
	}
	b, e := s.Bucket(id)
	if e != nil {
		t.Fatal(e)
	}
	v := b.allVolumes()[0]
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: id, permissions: BucketAllPermissions}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	entries := make([]objectReferenceEntry, 3218)
	for i := range entries {
		entries[i] = objectReferenceEntry{object: volume.ObjectID{byte(i), byte(i >> 8)}, proof: sha256.Sum256([]byte(fmt.Sprint(i))), account: volume.ID{0xee}}
	}
	if _, e = b.Put(context.Background(), p, volume.ObjectID(client.ObjectReferenceRootID()), encodeObjectReferences(entries, id), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
		t.Fatal(e)
	}
	return &combiningFixture{s, b, v, ws, p}
}
func (f *combiningFixture) message(action byte, token client.ID, i int) client.Message {
	yes := true
	m := referenceRequest(f.b, client.ObjectID{0xfa, byte(i), byte(i >> 8)}, token, &yes)
	m.Body[len(m.Body)-18] = action
	return m
}
func (f *combiningFixture) mark() uint64 {
	f.s.durability.mu.Lock()
	defer f.s.durability.mu.Unlock()
	return f.s.durability.buckets[f.b.ID()].sequence
}
func (f *combiningFixture) root(t testing.TB) []byte {
	t.Helper()
	r, _, e := f.b.Get(context.Background(), f.p, volume.ObjectID(client.ObjectReferenceRootID()))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func (f *combiningFixture) burst(t testing.TB, ms []client.Message, old bool, contexts []context.Context) []error {
	return f.burstWithSetup(t, ms, old, contexts, nil)
}
func (f *combiningFixture) burstWithSetup(t testing.TB, ms []client.Message, old bool, contexts []context.Context, setup func()) []error {
	t.Helper()
	release, e := f.s.referenceCoordination.acquire(context.Background(), f.b.ID())
	if e != nil {
		t.Fatal(e)
	}
	results := make(chan struct {
		i int
		e error
	}, len(ms))
	for i, m := range ms {
		go func(i int, m client.Message) {
			ctx := context.Background()
			if contexts != nil {
				ctx = contexts[i]
			}
			var e error
			if old {
				var unlock func()
				unlock, e = f.s.referenceCoordination.acquire(ctx, f.b.ID())
				if e == nil {
					_, e = f.s.executeObjectReferenceHeld(ctx, m, f.ws, f.p, true)
					unlock()
				}
			} else {
				_, e = f.s.executeObjectReference(ctx, m, f.ws, f.p)
			}
			results <- struct {
				i int
				e error
			}{i, e}
		}(i, m)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.s.referenceCoordination.mu.Lock()
		n := f.s.referenceCoordination.gates[f.b.ID()].refs
		f.s.referenceCoordination.mu.Unlock()
		if n == len(ms)+1 {
			break
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("requests did not queue", n)
		}
		time.Sleep(time.Millisecond)
	}
	if setup != nil {
		setup()
	}
	release()
	errs := make([]error, len(ms))
	for range ms {
		select {
		case r := <-results:
			errs[r.i] = r.e
		case <-time.After(10 * time.Second):
			t.Fatal("burst stalled")
		}
	}
	return errs
}
func TestReferenceCombiningBurst(t *testing.T) {
	for _, old := range []bool{true, false} {
		t.Run(fmt.Sprint("old=", old), func(t *testing.T) {
			f := newCombiningFixture(t)
			raw := f.root(t)
			before := f.mark()
			syncs := f.v.IOStats().SyncOps
			const n = 32
			tokens := make([]client.ID, n)
			ms := make([]client.Message, n)
			for i := range ms {
				tokens[i] = client.NewRequestID()
				ms[i] = f.message(0, tokens[i], i)
			}
			for _, e := range f.burst(t, ms, old, nil) {
				if e != nil {
					t.Fatal(e)
				}
			}
			writes := f.mark() - before
			want := uint64(1)
			if old {
				want = n
			}
			if writes != want {
				t.Fatal("root publications", writes, "want", want)
			}
			got, e := decodeObjectReferences(f.root(t), f.b.ID())
			if e != nil || len(got) != 3218+n {
				t.Fatal("root", len(got), e)
			}
			for i := range ms {
				ms[i] = f.message(1, tokens[i], i)
			}
			for _, e := range f.burst(t, ms, old, nil) {
				if e != nil {
					t.Fatal(e)
				}
			}
			if !bytes.Equal(raw, f.root(t)) {
				t.Fatal("UNPIN burst did not restore root")
			}
			if f.v.IOStats().SyncOps != syncs {
				t.Fatal("implicit sync")
			}
			t.Logf("requests=%d publications=%d effective_batch=%d root_bytes=%d", n, writes, n/int(writes), len(raw))
		})
	}
}
func TestReferenceCombiningFence(t *testing.T) {
	f := newCombiningFixture(t)
	const n = 8
	ms := make([]client.Message, n)
	contexts := make([]context.Context, n)
	for i := range ms {
		ms[i] = f.message(0, client.NewRequestID(), i)
		if !f.ws.admitConnectionMessage(ms[i]) {
			t.Fatal("admission")
		}
		contexts[i] = f.ws.connectionContext(context.Background(), ms[i].ID)
	}
	for _, e := range f.burst(t, ms, false, contexts) {
		if e != nil {
			t.Fatal(e)
		}
	}
	f.ws.fenceMu.Lock()
	defer f.ws.fenceMu.Unlock()
	var generation uint64
	for _, m := range ms {
		job := f.ws.connectionJobs[m.ID]
		g := job.touches[f.v]
		if g == 0 {
			t.Fatal("follower missing committed generation")
		}
		if generation != 0 && g != generation {
			t.Fatal("different committed generations", g, generation)
		}
		generation = g
	}
}
func TestReferenceCombiningFullAndRecovery(t *testing.T) {
	for _, kind := range []string{"known", "unknown", "recovery", "precondition-recovery"} {
		t.Run(kind, func(t *testing.T) {
			f := newCombiningFixture(t)
			token := client.NewRequestID()
			existing := f.message(0, token, 1)
			if _, e := f.s.executeObjectReference(context.Background(), existing, f.ws, f.p); e != nil {
				t.Fatal(e)
			}
			var attempts atomic.Int32
			if e := f.v.SetAdmission(func(change volume.StorageChange) (uint64, error) {
				if attempts.Add(1) == 1 {
					switch kind {
					case "known":
						return 0, &prepublicationQuotaFull{}
					case "unknown":
						return 0, volume.ErrFull
					case "precondition-recovery":
						return 0, errors.Join(volume.ErrPrecondition, volume.ErrRecovery)
					default:
						return 0, errors.Join(&prepublicationQuotaFull{}, volume.ErrRecovery)
					}
				}
				return 0, nil
			}); e != nil {
				t.Fatal(e)
			}
			ms := []client.Message{f.message(1, token, 1), f.message(0, client.NewRequestID(), 2)}
			contexts := make([]context.Context, len(ms))
			for i, m := range ms {
				f.ws.admitConnectionMessage(m)
				contexts[i] = f.ws.connectionContext(context.Background(), m.ID)
			}
			es := f.burst(t, ms, false, contexts)
			f.ws.fenceMu.Lock()
			for _, m := range ms {
				if f.ws.connectionJobs[m.ID].touches[f.v] == 0 {
					f.ws.fenceMu.Unlock()
					t.Fatal("failed request lost its observed generation")
				}
			}
			f.ws.fenceMu.Unlock()
			if kind == "known" {
				for _, e := range es {
					if e != nil {
						t.Fatal(e)
					}
				}
				if attempts.Load() != 3 {
					t.Fatal("known rejection did not split", attempts.Load())
				}
			} else {
				if attempts.Load() != 1 {
					t.Fatal("ambiguous error replayed", attempts.Load())
				}
				for _, e := range es {
					if !errors.Is(e, volume.ErrFull) && !(kind == "precondition-recovery" && errors.Is(e, volume.ErrRecovery)) {
						t.Fatal(e)
					}
				}
			}
		})
	}
}
func TestReferenceCombiningIdempotentFence(t *testing.T) {
	f := newCombiningFixture(t)
	ms := []client.Message{f.message(1, client.NewRequestID(), 1), f.message(1, client.NewRequestID(), 2)}
	contexts := make([]context.Context, len(ms))
	before := f.mark()
	for i, m := range ms {
		f.ws.admitConnectionMessage(m)
		contexts[i] = f.ws.connectionContext(context.Background(), m.ID)
	}
	for _, e := range f.burst(t, ms, false, contexts) {
		if e != nil {
			t.Fatal(e)
		}
	}
	if f.mark() != before {
		t.Fatal("no-op emitted CAS")
	}
	f.ws.fenceMu.Lock()
	defer f.ws.fenceMu.Unlock()
	for _, m := range ms {
		if f.ws.connectionJobs[m.ID].touches[f.v] == 0 {
			t.Fatal("no-op observation omitted")
		}
	}
}
func BenchmarkReferenceCombining(b *testing.B) {
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprint("old=", old), func(b *testing.B) {
			f := newCombiningFixture(b)
			tokens := make([]client.ID, 32)
			pins, unpins := make([]client.Message, 32), make([]client.Message, 32)
			for i := range tokens {
				tokens[i] = client.NewRequestID()
				pins[i] = f.message(0, tokens[i], i)
				unpins[i] = f.message(1, tokens[i], i)
			}
			before := f.mark()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ms := range [][]client.Message{pins, unpins} {
					for _, e := range f.burst(b, ms, old, nil) {
						if e != nil {
							b.Fatal(e)
						}
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(f.mark()-before)/float64(b.N*64), "CAS/request")
		})
	}
}

func TestReferenceCombiningAuthorizationAndRetry(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint("retry=", retry), func(t *testing.T) {
			f := newCombiningFixture(t)
			f.p.connection = f.ws.authorization
			ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
			before := f.mark()
			var attempts atomic.Int32
			setup := func() { f.ws.authorization.invalid.Store(true) }
			if retry {
				setup = nil
				if e := f.v.SetAdmission(func(volume.StorageChange) (uint64, error) {
					attempts.Add(1)
					f.ws.authorization.invalid.Store(true)
					return 0, volume.ErrPrecondition
				}); e != nil {
					t.Fatal(e)
				}
			}
			for _, e := range f.burstWithSetup(t, ms, false, nil, setup) {
				if !errors.Is(e, ErrBucketForbidden) {
					t.Fatal("revoked request updated root", e)
				}
			}
			if f.mark() != before {
				t.Fatal("revoked batch published")
			}
			if retry && attempts.Load() != 1 {
				t.Fatal("retry did not revalidate", attempts.Load())
			}
		})
	}
}
func TestReferenceCombiningExternalCASAndDynamicAdmission(t *testing.T) {
	f := newCombiningFixture(t)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	raw := f.root(t)
	entries, e := decodeObjectReferences(raw, f.b.ID())
	if e != nil {
		t.Fatal(e)
	}
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	external := objectReferenceEntry{object: volume.ObjectID{0xfa, 1}, proof: [32]byte{99}, account: volume.ID{88}, claim: true}
	es := f.burstWithSetup(t, ms, false, nil, func() {
		if _, e := f.b.CAS(context.Background(), f.p, root, 0, raw, encodeObjectReferences(append(entries, external), f.b.ID()), volume.Options{WholeObject: true, Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
			t.Fatal(e)
		}
	})
	if objectStatus(es[0]) != 409 || es[1] != nil {
		t.Fatal("external claim ignored or unrelated PIN poisoned", es)
	}
	got, e := decodeObjectReferences(f.root(t), f.b.ID())
	if e != nil || len(got) != 3220 {
		t.Fatal("external writer lost", len(got), e)
	}
	uid, _ := bucketID(f.p.User.ID)
	for i := range got {
		got[i].account = uid
	}
	if _, e := f.b.Put(context.Background(), f.p, root, encodeObjectReferences(got, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
		t.Fatal(e)
	}
	before := f.mark()
	ms = []client.Message{f.message(0, client.NewRequestID(), 3), f.message(0, client.NewRequestID(), 4)}
	for _, e := range f.burst(t, ms, false, nil) {
		if e != nil {
			t.Fatal("dynamic admission rejected valid holder", e)
		}
	}
	if f.mark() == before {
		t.Fatal("accepted holders did not update root")
	}
	after, e := decodeObjectReferences(f.root(t), f.b.ID())
	if e != nil || len(after) != len(got)+2 {
		t.Fatal("accepted holders lost", len(after), e)
	}
	if !bytes.Equal(encodeObjectReferences(after[:len(got)], f.b.ID()), encodeObjectReferences(got, f.b.ID())) {
		t.Fatal("existing holders changed during growth")
	}
}
func TestReferenceCombiningSelectedCancellationFence(t *testing.T) {
	f := newCombiningFixture(t)
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	contexts := make([]context.Context, 2)
	cancels := make([]context.CancelFunc, 2)
	for i, m := range ms {
		f.ws.admitConnectionMessage(m)
		base, cancel := context.WithCancel(context.Background())
		contexts[i] = f.ws.connectionContext(base, m.ID)
		cancels[i] = cancel
		defer cancel()
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	if e := f.v.SetAdmission(func(volume.StorageChange) (uint64, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
			<-proceed
		}
		return 0, nil
	}); e != nil {
		t.Fatal(e)
	}
	done := make(chan []error, 1)
	go func() { done <- f.burst(t, ms, false, contexts) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("CAS did not start")
	}
	for _, cancel := range cancels {
		cancel()
	}
	select {
	case <-done:
		t.Fatal("selected canceled job retired before publication")
	case <-time.After(20 * time.Millisecond):
	}
	close(proceed)
	es := <-done
	for _, e := range es {
		if e != nil {
			t.Fatal("known accepted publication was lost", e)
		}
	}
	f.ws.fenceMu.Lock()
	defer f.ws.fenceMu.Unlock()
	for _, m := range ms {
		if f.ws.connectionJobs[m.ID].touches[f.v] == 0 {
			t.Fatal("cancellation discarded committed fence")
		}
	}
}
func TestReferenceCombiningCancellationBeforeSelection(t *testing.T) {
	f := newCombiningFixture(t)
	release, e := f.s.referenceCoordination.acquire(context.Background(), f.b.ID())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := f.message(0, client.NewRequestID(), 1)
	done := make(chan error, 1)
	before := f.mark()
	go func() { _, e := f.s.executeObjectReference(ctx, m, f.ws, f.p); done <- e }()
	referenceGateWaiters(t, &f.s.referenceCoordination, f.b.ID(), 2)
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	referenceGateWaiters(t, &f.s.referenceCoordination, f.b.ID(), 1)
	release()
	if f.mark() != before {
		t.Fatal("canceled queued request mutated root")
	}
}
func TestReferenceCombiningBoundAndBarriers(t *testing.T) {
	f := newCombiningFixture(t)
	const n = 65
	ms := make([]client.Message, n)
	contexts := make([]context.Context, n)
	for i := range ms {
		ms[i] = f.message(0, client.NewRequestID(), i)
		f.ws.admitConnectionMessage(ms[i])
		contexts[i] = f.ws.connectionContext(context.Background(), ms[i].ID)
	}
	before := f.mark()
	for _, e := range f.burst(t, ms, false, contexts) {
		if e != nil {
			t.Fatal(e)
		}
	}
	if f.mark()-before != 3 {
		t.Fatal("batch exceeded cap or did not combine", f.mark()-before)
	}
	histogram := map[uint64]int{}
	f.ws.fenceMu.Lock()
	for _, m := range ms {
		histogram[f.ws.connectionJobs[m.ID].touches[f.v]]++
	}
	f.ws.fenceMu.Unlock()
	for _, size := range histogram {
		if size > maxCombinedReferences {
			t.Fatal("unbounded group", size)
		}
	}
	t.Log("batch histogram", histogram)
	// Exercise exact FIFO stop points independent of scheduler timing.
	for _, barrier := range []string{"other-socket", "other-account", "same-proof", "durable-or-maintenance"} {
		t.Run(barrier, func(t *testing.T) {
			r := referenceCoordination{}
			q := &combinedReferenceRequest{ws: f.ws, principal: f.p, token: client.NewRequestID()}
			other := *q
			other.token = client.NewRequestID()
			w := &referenceWaiter{request: &other, ready: make(chan struct{})}
			switch barrier {
			case "other-socket":
				other.ws = connectionTestSession(f.s)
			case "other-account":
				other.principal.User.ID = "another-account"
			case "same-proof":
				other.token = q.token
			default:
				w.request = nil
			}
			tail := &referenceWaiter{request: q, ready: make(chan struct{})}
			r.gates = map[volume.ID]*referenceGate{f.b.ID(): {held: true, refs: 3, pending: []*referenceWaiter{w, tail}}}
			if got := r.take(f.b.ID(), q); len(got) != 0 {
				t.Fatal("crossed FIFO barrier", barrier)
			}
			if len(r.gates[f.b.ID()].pending) != 2 {
				t.Fatal("barrier waiter lost")
			}
		})
	}
}

// This test context runs the external writer only after the first CAS has
// returned a precondition. It inserts no production hook and holds no Volume or
// node admission lock while the independent writer executes.
type referenceRetryContext struct {
	context.Context
	armed  *atomic.Bool
	update func()
}

func (c referenceRetryContext) Err() error {
	if c.armed.CompareAndSwap(true, false) {
		c.update()
	}
	return nil
}
func TestReferenceCombiningExternalCASRetryRecomputesResults(t *testing.T) {
	f := newCombiningFixture(t)
	var armed, once atomic.Bool
	root := volume.ObjectID(client.ObjectReferenceRootID())
	external := objectReferenceEntry{object: volume.ObjectID{0xfa, 1}, proof: [32]byte{111}, account: volume.ID{77}, claim: true}
	ctx := referenceRetryContext{Context: context.Background(), armed: &armed, update: func() {
		raw := f.root(t)
		es, e := decodeObjectReferences(raw, f.b.ID())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.b.CAS(context.Background(), f.p, root, 0, raw, encodeObjectReferences(append(es, external), f.b.ID()), volume.Options{WholeObject: true, Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
			t.Fatal(e)
		}
	}}
	if e := f.v.SetAdmission(func(volume.StorageChange) (uint64, error) {
		if once.CompareAndSwap(false, true) {
			armed.Store(true)
			return 0, volume.ErrPrecondition
		}
		return 0, nil
	}); e != nil {
		t.Fatal(e)
	}
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	es := f.burst(t, ms, false, []context.Context{ctx, ctx})
	if objectStatus(es[0]) != 409 || es[1] != nil {
		t.Fatal("provisional results survived external CAS retry", es)
	}
	entries, e := decodeObjectReferences(f.root(t), f.b.ID())
	if e != nil || len(entries) != 3220 {
		t.Fatal("retry lost external claim or independent PIN", len(entries), e)
	}
}

func TestReferenceCombiningRetryExhaustionKeepsIndividualErrors(t *testing.T) {
	f := newCombiningFixture(t)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	raw := f.root(t)
	entries, e := decodeObjectReferences(raw, f.b.ID())
	if e != nil {
		t.Fatal(e)
	}
	entries = append(entries, objectReferenceEntry{object: volume.ObjectID{0xfa, 1}, account: volume.ID{9}, proof: [32]byte{8}, claim: true})
	if _, e = f.b.Put(context.Background(), f.p, root, encodeObjectReferences(entries, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
		t.Fatal(e)
	}
	var attempts atomic.Int32
	if e = f.v.SetAdmission(func(volume.StorageChange) (uint64, error) { attempts.Add(1); return 0, volume.ErrPrecondition }); e != nil {
		t.Fatal(e)
	}
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	contexts := make([]context.Context, len(ms))
	for i, m := range ms {
		f.ws.admitConnectionMessage(m)
		contexts[i] = f.ws.connectionContext(context.Background(), m.ID)
	}
	es := f.burst(t, ms, false, contexts)
	f.ws.fenceMu.Lock()
	for _, m := range ms {
		if f.ws.connectionJobs[m.ID].touches[f.v] == 0 {
			f.ws.fenceMu.Unlock()
			t.Fatal("exhausted request lost its observed generation")
		}
	}
	f.ws.fenceMu.Unlock()
	if objectStatus(es[0]) != 409 || objectStatus(es[1]) != 503 {
		t.Fatal("retry exhaustion overwrote independent result", es)
	}
	if attempts.Load() != 128 {
		t.Fatal("retry bound changed", attempts.Load())
	}
}
func TestReferenceCombiningPendingStorageCleared(t *testing.T) {
	r := referenceCoordination{}
	q := &combinedReferenceRequest{token: client.ID{1}}
	a := &referenceWaiter{request: q, ready: make(chan struct{})}
	b := &referenceWaiter{request: &combinedReferenceRequest{token: client.ID{2}}, ready: make(chan struct{})}
	backing := []*referenceWaiter{a, b}
	g := &referenceGate{refs: 2, pending: backing}
	r.grantNext(g)
	if backing[0] != nil || g.pending[0] != b {
		t.Fatal("grant retained request backing slot")
	}
	bucket := volume.ID{1}
	r.gates = map[volume.ID]*referenceGate{bucket: g}
	got := r.take(bucket, q)
	if len(got) != 1 || got[0] != b || backing[1] != nil {
		t.Fatal("selection retained request backing slot")
	}
	r = referenceCoordination{}
	release, e := r.acquire(context.Background(), bucket)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, unlock, e := r.join(ctx, bucket, nil)
		if unlock != nil {
			unlock()
		}
		done <- e
	}()
	referenceGateWaiters(t, &r, bucket, 2)
	r.mu.Lock()
	pending := r.gates[bucket].pending
	r.mu.Unlock()
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if pending[0] != nil {
		t.Fatal("cancellation retained request backing slot")
	}
	release()
}

type referenceCanceledRetryContext struct {
	context.Context
	armed  *atomic.Bool
	cancel context.CancelFunc
}

func (c referenceCanceledRetryContext) Err() error {
	if c.armed.CompareAndSwap(true, false) {
		c.cancel()
	}
	return c.Context.Err()
}
func TestReferenceCombiningCanceledRetryRetainsEarlierObservation(t *testing.T) {
	f := newCombiningFixture(t)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var armed, once atomic.Bool
	conditional := referenceCanceledRetryContext{Context: base, armed: &armed, cancel: cancel}
	if e := f.v.SetAdmission(func(volume.StorageChange) (uint64, error) {
		if once.CompareAndSwap(false, true) {
			armed.Store(true)
			return 0, volume.ErrPrecondition
		}
		return 0, nil
	}); e != nil {
		t.Fatal(e)
	}
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	contexts := make([]context.Context, 2)
	for i, m := range ms {
		f.ws.admitConnectionMessage(m)
		ctx := context.Context(context.Background())
		if i == 0 {
			ctx = conditional
		}
		contexts[i] = f.ws.connectionContext(ctx, m.ID)
	}
	es := f.burst(t, ms, false, contexts)
	if !errors.Is(es[0], context.Canceled) || es[1] != nil {
		t.Fatal("canceled retry damaged independent PIN", es)
	}
	f.ws.fenceMu.Lock()
	defer f.ws.fenceMu.Unlock()
	if f.ws.connectionJobs[ms[0].ID].touches[f.v] == 0 {
		t.Fatal("participant excluded from final retry lost earlier root observation")
	}
}
