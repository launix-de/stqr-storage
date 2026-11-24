// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func subscriptionClients(t *testing.T) (*Server, *client.Client, *client.Client, context.Context) {
	t.Helper()
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "subscribed")
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	admin := s.cfg.Auth.BootstrapAdmin
	reader, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	writer, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	return s, reader, writer, ctx
}
func waitSubscription(t *testing.T, ctx context.Context, sub *client.Subscription, wantVersion uint64) ([]byte, client.ObjectInfo, error) {
	t.Helper()
	for {
		data, info, _, err := sub.Snapshot()
		if info.Version >= wantVersion {
			return data, info, err
		}
		select {
		case <-sub.Changes():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
func TestObjectSubscriptionMutationsAndLifetime(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{1}
	info, err := writer.Put(ctx, id, []byte("abc"), client.PutOptions{RequestID: client.NewRequestID()})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, got, cursor, err := sub.Snapshot()
	if err != nil || string(data) != "abc" || got.Version != info.Version || cursor == 0 {
		t.Fatal(string(data), got, cursor, err)
	}
	req := client.NewRequestID()
	app, err := writer.Append(ctx, id, []byte("d"), client.AppendOptions{RequestID: req})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err = waitSubscription(t, ctx, sub, app.Version)
	if err != nil || string(data) != "abcd" {
		t.Fatal(string(data), err)
	}
	// The original retry receipt must not produce another cursor or update.
	_, err = writer.Append(ctx, id, []byte("d"), client.AppendOptions{RequestID: req})
	if err != nil {
		t.Fatal(err)
	}
	trunc, err := writer.Trunc(ctx, id, 2, client.TruncOptions{RequestID: client.NewRequestID()})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err = waitSubscription(t, ctx, sub, trunc.Version)
	if err != nil || string(data) != "ab" {
		t.Fatal(string(data), err)
	}
	if err = writer.Delete(ctx, id, client.DeleteOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	_, _, err = waitSubscription(t, ctx, sub, trunc.Version+1)
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatal(err)
	}
	info, err = writer.Put(ctx, id, []byte("again"), client.PutOptions{RequestID: client.NewRequestID()})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err = waitSubscription(t, ctx, sub, info.Version)
	if err != nil || string(data) != "again" {
		t.Fatal(string(data), err)
	}
	if err = sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	s.objectSubscriptions.mu.Lock()
	count := s.objectSubscriptions.count
	s.objectSubscriptions.mu.Unlock()
	if count != 0 {
		t.Fatalf("retained %d subscriptions", count)
	}
}
func TestObjectSubscriptionStreamAndRangedUpdates(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{2}
	data := bytes.Repeat([]byte("x"), 2<<20)
	if _, err := writer.PutFrom(ctx, id, bytes.NewReader(data), uint64(len(data)), client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	full, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close(ctx)
	got, _, _, err := full.Snapshot()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal(len(got), err)
	}
	ranged, err := reader.Subscribe(ctx, id, client.SubscribeOptions{Ranged: true, Offset: 1, Length: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer ranged.Close(ctx)
	info, err := writer.PutRange(ctx, id, 1, []byte("abc"), client.PutOptions{RequestID: client.NewRequestID()})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = waitSubscription(t, ctx, ranged, info.Version)
	if err != nil || string(got) != "abc" {
		t.Fatal(string(got), err)
	}
	select {
	case <-full.Changes():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, _, _, err = full.Snapshot(); !errors.Is(err, client.ErrSubscriptionResync) {
		t.Fatal("large update did not invalidate", err)
	}
}
func TestObjectSubscriptionGCCleanup(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{3}
	if _, err := writer.Put(ctx, id, []byte("gc"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	abandoned := func() weak.Pointer[client.Subscription] {
		sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return weak.Make(sub)
	}()
	for {
		runtime.GC()
		s.objectSubscriptions.mu.Lock()
		count := s.objectSubscriptions.count
		s.objectSubscriptions.mu.Unlock()
		if abandoned.Value() == nil && count == 0 {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("GC did not unsubscribe", ctx.Err())
		}
	}
}

func TestSubscriptionStoreBoundedAndInvalidated(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	first, second := client.ObjectID{4}, client.ObjectID{5}
	initial, err := writer.Put(ctx, first, []byte("first"), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Put(ctx, second, []byte("second"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	cache := client.NewSubscriptionStore(reader, 600)
	defer cache.Close()
	for i := 0; i < 100; i++ {
		data, _, err := cache.Get(ctx, first)
		if err != nil || string(data) != "first" {
			t.Fatal(string(data), err)
		}
	}
	s.objectSubscriptions.mu.Lock()
	count := s.objectSubscriptions.count
	s.objectSubscriptions.mu.Unlock()
	if count != 1 {
		t.Fatal("cache retained unexpected subscription count", count)
	}
	updated, err := writer.Put(ctx, first, []byte("updated"), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, info, err := cache.Get(ctx, first)
		if err != nil {
			t.Fatal(err)
		}
		if info.Version == updated.Version {
			if string(data) != "updated" {
				t.Fatal(string(data))
			}
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if _, err = cache.CAS(ctx, first, 0, nil, []byte("wrong"), client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{ExpectedVersion: &initial.Version}}); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal(err)
	}
	if data, _, err := cache.Get(ctx, second); err != nil || string(data) != "second" {
		t.Fatal(string(data), err)
	}
	// A later RPC on the same bucket socket is a barrier for eviction's FF unsubscribe.
	if _, err = reader.Stat(ctx, second); err != nil {
		t.Fatal(err)
	}
	s.objectSubscriptions.mu.Lock()
	count = s.objectSubscriptions.count
	s.objectSubscriptions.mu.Unlock()
	if count != 1 {
		t.Fatal("LRU did not release its subscription", count)
	}
	cache.Close()
	if _, err = reader.Stat(ctx, second); err != nil {
		t.Fatal(err)
	}
	s.objectSubscriptions.mu.Lock()
	count = s.objectSubscriptions.count
	s.objectSubscriptions.mu.Unlock()
	if count != 0 {
		t.Fatal("Close retained subscriptions", count)
	}
}

func TestDaemonSubscriptionResyncsBeforeRangeHandoff(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "subscription-handoff")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{0x90}
	if _, err = c.Put(ctx, oid, []byte("before"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Subscribe(ctx, oid, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	_, _, cursor, err := sub.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID(oid))
	var destination *volume.Volume
	target.node.mu.RLock()
	for _, v := range target.node.volumes {
		if !v.config.System {
			destination = v.store
		}
	}
	target.node.mu.RUnlock()
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, destination.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Changes():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, _, _, err = sub.Snapshot(); !errors.Is(err, client.ErrSubscriptionResync) {
		t.Fatal("old owner left a valid cache", err)
	}
	if err = sub.Close(ctx); err != nil {
		t.Fatal(err)
	}
	resumed, err := c.Subscribe(ctx, oid, client.SubscribeOptions{ID: sub.ID(), Resume: &cursor})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close(ctx)
	data, _, newCursor, err := resumed.Snapshot()
	if err != nil || string(data) != "before" || newCursor <= cursor {
		t.Fatal(string(data), newCursor, err)
	}
	updated, err := c.Put(ctx, oid, []byte("after"), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err = waitSubscription(t, ctx, resumed, updated.Version)
	if err != nil || string(data) != "after" {
		t.Fatal(string(data), err)
	}
}

func TestObjectSubscriptionDisconnectAndRevocation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "disconnect", true: "revoke"}[revoke], func(t *testing.T) {
			s, reader, writer, ctx := subscriptionClients(t)
			id := client.ObjectID{6}
			if _, err := writer.Put(ctx, id, []byte("private"), client.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if revoke {
				s.wsMu.Lock()
				var account string
				for ws := range s.sockets {
					if ws.authorization != nil {
						account = ws.authorization.accountID
						break
					}
				}
				s.wsMu.Unlock()
				s.invalidateUserConnections(account)
			} else {
				reader.Close()
			}
			for {
				if _, _, _, err = sub.Snapshot(); errors.Is(err, client.ErrSubscriptionResync) {
					break
				}
				select {
				case <-sub.Changes():
				case <-ctx.Done():
					t.Fatal("cache survived lost authorization", ctx.Err())
				}
			}
		})
	}
}
func TestObjectSubscriptionSnapshotRegistrationBarrier(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{7}
	if _, err := writer.Put(ctx, id, nil, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 40; i++ {
			if _, err := writer.Append(ctx, id, []byte("x"), client.AppendOptions{}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 20; i++ {
		sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, info, _, err := sub.Snapshot()
		if err == nil && (info.Version != uint64(len(data))+1 || info.Length != uint64(len(data))) {
			t.Fatal("snapshot/listener gap", info, len(data))
		}
		if err != nil && !errors.Is(err, client.ErrSubscriptionResync) {
			t.Fatal(err)
		}
		if err = sub.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestObjectSubscriptionJournalOverflowRequestsResync(t *testing.T) {
	ws := &wsSession{authorization: &connectionAuthorization{}, done: make(chan struct{}), responses: make(chan []byte, 64)}
	var registry objectSubscriptionRegistry
	key := volume.Key{}
	sub := &objectSubscription{ws: ws, id: client.NewRequestID(), key: key, cursor: 1}
	if err := registry.add(sub); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxSubscriptionJournalEvents+1; i++ {
		registry.publish(key, uint64(i+1), volume.Info{Version: uint64(i + 2), Deleted: true}, nil)
	}
	if registry.count != 0 || registry.bytes != 0 {
		t.Fatal("retention budget leaked", registry.count, registry.bytes)
	}
	var last client.ObjectUpdate
	for len(ws.responses) > 0 {
		m, err := client.DecodeMessage(<-ws.responses)
		if err != nil {
			t.Fatal(err)
		}
		last, err = client.DecodeObjectUpdate(m.Body)
		if err != nil {
			t.Fatal(err)
		}
	}
	if last.Change != client.UpdateResyncRequired {
		t.Fatal(last)
	}
}

func TestSubscriptionStoreDisconnectCleanupDoesNotReenterTransportOnce(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{8}
	if _, err := writer.Put(ctx, id, []byte("cached"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	cache := client.NewSubscriptionStore(reader, 1<<20)
	defer cache.Close()
	if _, _, err := cache.Get(ctx, id); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { reader.Close(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("transport cleanup deadlocked while evicting cache entries")
	}
	if _, _, err := cache.Get(ctx, id); !errors.Is(err, client.ErrClosed) {
		t.Fatal("closed connection served cached data", err)
	}
}
func TestObjectSubscriptionCanceledCloseStillReleases(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{9}
	if _, err := writer.Put(ctx, id, []byte("close"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	sub, err := reader.Subscribe(ctx, id, client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_ = sub.Close(canceled)
	if _, err = reader.Stat(ctx, id); err != nil {
		t.Fatal(err)
	}
	s.objectSubscriptions.mu.Lock()
	count := s.objectSubscriptions.count
	s.objectSubscriptions.mu.Unlock()
	if count != 0 {
		t.Fatal("canceled Close leaked its server subscription", count)
	}
}

func TestSubscriptionStoreCoalescesParallelColdReaders(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	const n = 64
	for i := 0; i < n; i++ {
		if _, err := writer.Put(ctx, client.ObjectID{byte(i + 1)}, []byte("parallel"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	cache := client.NewSubscriptionStore(reader, 1<<20)
	defer cache.Close()
	for _, same := range []bool{true, false} {
		start := make(chan struct{})
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			go func(i int) {
				<-start
				id := client.ObjectID{byte(i + 1)}
				if same {
					id = client.ObjectID{1}
				}
				data, _, err := cache.Get(ctx, id)
				if err == nil && string(data) != "parallel" {
					err = errors.New("wrong snapshot")
				}
				errs <- err
			}(i)
		}
		close(start)
		for i := 0; i < n; i++ {
			if err := <-errs; err != nil {
				t.Fatal("cold parallel read", err)
			}
		}
		s.objectSubscriptions.mu.Lock()
		count := s.objectSubscriptions.count
		s.objectSubscriptions.mu.Unlock()
		want := n
		if same {
			want = 1
		}
		if count != want {
			t.Fatal("unexpected active subscriptions for parallel misses", count, want)
		}
	}
}

func TestSubscriptionStorePausedOwnerDoesNotBlockIndependentRange(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, source.node, "subscription-independent")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, second := client.ObjectID{0x10}, client.ObjectID{0x90}
	for _, id := range []client.ObjectID{first, second} {
		if _, err = c.Put(ctx, id, []byte("independent"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID(second))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	var destination *volume.Volume
	target.node.mu.RLock()
	for _, v := range target.node.volumes {
		if !v.config.System {
			destination = v.store
		}
	}
	target.node.mu.RUnlock()
	plan, err := planRangeMove(before, moving, target.node.control.ledger.node, destination.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for _, id := range []client.ObjectID{first, second} {
		if _, _, err = c.Get(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if err = source.node.prepareShortShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	cache := client.NewSubscriptionStore(c, 1<<20)
	defer cache.Close()
	waiting, stopWaiting := context.WithCancel(ctx)
	errs := make(chan error, 16)
	started := make(chan struct{}, 16)
	for i := 0; i < 16; i++ {
		go func() { started <- struct{}{}; _, _, err := cache.Get(waiting, first); errs <- err }()
	}
	for i := 0; i < 16; i++ {
		<-started
	}
	available, stopAvailable := context.WithTimeout(ctx, 2*time.Second)
	defer stopAvailable()
	data, _, readErr := cache.Get(available, second)
	stopWaiting()
	for i := 0; i < 16; i++ {
		<-errs
	}
	if readErr != nil || string(data) != "independent" {
		t.Fatal("paused owner blocked another transport", string(data), readErr)
	}
}
