// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type cowMemoryRecord struct {
	data    []byte
	deleted bool
}
type cowMemory struct {
	mu           sync.Mutex
	objects      map[volume.Key]cowMemoryRecord
	afterCapture func(volume.ID)
}

func (m *cowMemory) capture(ctx context.Context, bucket volume.ID, object volume.ObjectID) (*cowSnapshot, error) {
	m.mu.Lock()
	record, ok := m.objects[volume.Key{Bucket: bucket, Object: object}]
	fn := m.afterCapture
	m.afterCapture = nil
	m.mu.Unlock()
	if fn != nil {
		fn(bucket)
	}
	if !ok {
		return nil, nil
	}
	data := bytes.Clone(record.data)
	return &cowSnapshot{Info: volume.Info{Length: uint64(len(data)), Deleted: record.deleted}, Data: bytes.NewReader(data)}, nil
}
func (m *cowMemory) materialize(ctx context.Context, bucket volume.ID, object volume.ObjectID, s *cowSnapshot) error {
	var data []byte
	if !s.Info.Deleted {
		data = make([]byte, s.Info.Length)
		if _, err := s.Data.ReadAt(data, 0); err != nil && err != io.EOF {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := volume.Key{Bucket: bucket, Object: object}
	if _, ok := m.objects[key]; !ok {
		m.objects[key] = cowMemoryRecord{data, s.Info.Deleted}
	}
	return nil
}
func (m *cowMemory) write(ctx context.Context, g cowGraph, bucket volume.ID, object volume.ObjectID, data []byte, deleted bool) error {
	if err := g.prepareWrite(ctx, m, bucket, object); err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[volume.Key{Bucket: bucket, Object: object}] = cowMemoryRecord{bytes.Clone(data), deleted}
	m.mu.Unlock()
	return nil
}
func cowRead(t *testing.T, g cowGraph, m *cowMemory, bucket volume.ID, object volume.ObjectID) string {
	t.Helper()
	s, err := g.resolve(context.Background(), m, bucket, object, map[volume.ID]bool{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s == nil || s.Info.Deleted {
		return "<absent>"
	}
	data := make([]byte, s.Info.Length)
	if _, err = s.Data.ReadAt(data, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(data)
}
func TestBucketCOWObjectIsolationAndAbsentMarkers(t *testing.T) {
	ctx := context.Background()
	a, b, c := volume.ID{1}, volume.ID{2}, volume.ID{3}
	one, two := volume.ObjectID{1}, volume.ObjectID{2}
	g := cowGraph{a: {b}, b: {a, c}, c: {b}}
	m := &cowMemory{objects: map[volume.Key]cowMemoryRecord{{Bucket: a, Object: one}: {data: []byte("original")}}}
	if got := cowRead(t, g, m, c, one); got != "original" {
		t.Fatal(got)
	}
	if len(m.objects) != 1 {
		t.Fatal("read materialized objects")
	}
	if err := m.write(ctx, g, a, one, []byte("source changed"), false); err != nil {
		t.Fatal(err)
	}
	if got := cowRead(t, g, m, c, one); got != "original" {
		t.Fatal(got)
	}
	if err := m.write(ctx, g, b, one, nil, true); err != nil {
		t.Fatal(err)
	}
	if got := cowRead(t, g, m, b, one); got != "<absent>" {
		t.Fatal(got)
	}
	if got := cowRead(t, g, m, c, one); got != "original" {
		t.Fatal(got)
	}
	if err := m.write(ctx, g, c, two, []byte("new in C"), false); err != nil {
		t.Fatal(err)
	}
	for _, id := range []volume.ID{a, b} {
		if got := cowRead(t, g, m, id, two); got != "<absent>" {
			t.Fatal(got)
		}
	}
	if got := cowRead(t, g, m, a, one); got != "source changed" {
		t.Fatal(got)
	}
}
func TestBucketCOWReadRechecksMaterializedPreimage(t *testing.T) {
	a, b := volume.ID{1}, volume.ID{2}
	id := volume.ObjectID{1}
	g := cowGraph{a: {b}, b: {a}}
	m := &cowMemory{objects: map[volume.Key]cowMemoryRecord{{Bucket: a, Object: id}: {data: []byte("before")}}}
	m.afterCapture = func(bucket volume.ID) {
		if bucket != b {
			t.Fatal(bucket)
		}
		if err := m.write(context.Background(), g, a, id, []byte("after"), false); err != nil {
			t.Fatal(err)
		}
	}
	if got := cowRead(t, g, m, b, id); got != "before" {
		t.Fatal("read-through crossed a concurrent source write", got)
	}
}
func TestBucketCOWConcurrentMaterializationCannotOverwriteUserData(t *testing.T) {
	a, b, c := volume.ID{1}, volume.ID{2}, volume.ID{3}
	id := volume.ObjectID{1}
	g := cowGraph{a: {b}, b: {a, c}, c: {b}}
	m := &cowMemory{objects: map[volume.Key]cowMemoryRecord{{Bucket: a, Object: id}: {data: []byte("base")}}}
	var wg sync.WaitGroup
	for _, bucket := range []volume.ID{a, b, c} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if err := m.write(context.Background(), g, bucket, id, []byte(bucket.String()), false); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, bucket := range []volume.ID{a, b, c} {
		if got := cowRead(t, g, m, bucket, id); got != bucket.String() {
			t.Fatal("late materialization overwrote user data", bucket, got)
		}
	}
}

func TestBucketCOWNativeCloneReadWriteAndSparse(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-data", "hdd", 32<<20)
	a, _ := createProtocolBucket(t, s, "cow-source")
	b, _ := createProtocolBucket(t, s, "cow-copy")
	ctx := context.Background()
	actor := userPrincipal(t, s)
	object := volume.ObjectID{7}
	sparse := volume.ObjectID{8}
	if _, err := a.Put(ctx, actor, object, []byte("original"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Trunc(ctx, actor, sparse, 2<<40, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PutRange(ctx, actor, sparse, (2<<40)-4, []byte("tail"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(a.ID()))
	e.ID(client.ID(b.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
		t.Fatal(err)
	}
	// Clone publication does not create a single destination data record.
	for _, v := range b.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		page, err := h.ScanBucketObjects(b.ID(), nil, 1)
		h.Close()
		if err != nil || len(page) != 0 {
			t.Fatal("clone copied payload up front", page, err)
		}
	}
	if data, _, err := b.Get(ctx, actor, object); err != nil || string(data) != "original" {
		t.Fatal(string(data), err)
	}
	if _, err := a.Put(ctx, actor, object, []byte("changed"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if data, _, err := b.Get(ctx, actor, object); err != nil || string(data) != "original" {
		t.Fatal(string(data), err)
	}
	if _, err := b.PutRange(ctx, actor, sparse, (2<<40)-4, []byte("COPY"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if data, _, err := a.GetRange(ctx, actor, sparse, (2<<40)-4, 4); err != nil || string(data) != "tail" {
		t.Fatal(string(data), err)
	}
	if data, _, err := b.GetRange(ctx, actor, sparse, (2<<40)-4, 4); err != nil || string(data) != "COPY" {
		t.Fatal(string(data), err)
	}
	fresh := volume.ObjectID{9}
	if _, err := a.Put(ctx, actor, fresh, []byte("only-source"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, actor, fresh); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("new source object leaked into clone", err)
	}
}

func TestBucketCOWFilesystemFsckPreservesSource(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-fsck-data", "hdd", 32<<20)
	source, _ := createProtocolBucket(t, s, "cow-fsck-source")
	target, _ := createProtocolBucket(t, s, "cow-fsck-copy")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	dial := func(bucket volume.ID) *client.Client {
		t.Helper()
		c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(bucket), "fs")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := dial(source.ID())
	defer c.Close()
	fs, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create(ctx, client.FilesystemRootID(), "kept", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("shared-data"), 6000)
	if _, err = fs.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	orphan := client.ObjectID{}
	if _, err = c.Put(ctx, orphan, []byte("unreachable"), client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err = s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s)); err != nil {
		t.Fatal(err)
	}
	copyClient := dial(target.ID())
	defer copyClient.Close()
	result, err := copyClient.CheckFilesystem(ctx)
	if err != nil || result.Deleted < 1 || result.DeletedBytes < 11 {
		t.Fatal(result, err)
	}
	if _, _, err = copyClient.Get(ctx, orphan); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("orphan retained in copy", err)
	}
	if data, _, err := c.Get(ctx, orphan); err != nil || string(data) != "unreachable" {
		t.Fatal("copy fsck changed source", string(data), err)
	}
	copyClient.Close()
	waitFsckReleased(t, target)
	copyClient = dial(target.ID())
	defer copyClient.Close()
	copyFS, err := client.OpenFilesystem(ctx, copyClient, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err = copyFS.ReadAt(ctx, file.ID, got, 0); err != nil || !bytes.Equal(got, want) {
		t.Fatal("copied filesystem damaged", err)
	}
}

func TestBucketCOWSubscriptionUsesNoReadMaterializationAndEnforcesOriginalGrant(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-sub-data", "hdd", 16<<20)
	source, _ := createProtocolBucket(t, s, "cow-sub-source")
	target, _ := createProtocolBucket(t, s, "cow-sub-copy")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	actor := userPrincipal(t, s)
	id := volume.ObjectID{21}
	if _, err := source.Put(ctx, actor, id, []byte("before"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
		t.Fatal(err)
	}
	user := bucketProtocolUser(t, s)
	e = client.Encoder{}
	e.ID(client.ID(target.ID()))
	uid, _ := parseID(user.ID)
	e.ID(client.ID(uid))
	e.U32(uint32(BucketAllPermissions))
	if _, status, _ := adminBucketMessage(t, s, client.BucketGrant, e.B); status != 200 {
		t.Fatal(status)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.ID(target.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if forbidden, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.ID(source.ID()), "fs"); err == nil {
		forbidden.Close()
		t.Fatal("copy-only user acquired source grant")
	}
	t.Run("RAM-only inherited snapshot", func(t *testing.T) {
		t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
		sub, err := c.Subscribe(ctx, client.ObjectID(id), client.SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close(ctx)
		data, _, _, err := sub.Snapshot()
		if err != nil || string(data) != "before" {
			t.Fatalf("inherited snapshot %q: %v", data, err)
		}
	})
	sub, err := c.Subscribe(ctx, client.ObjectID(id), client.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	data, _, cursor, err := sub.Snapshot()
	if err != nil || string(data) != "before" {
		t.Fatal(string(data), err)
	}
	for _, v := range target.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.RecordInfo(volume.Key{Bucket: target.ID(), Object: id})
		h.Close()
		if !errors.Is(err, volume.ErrNotFound) {
			t.Fatal("subscribe wrote a materialization", err)
		}
	}
	if _, err := source.Put(ctx, actor, id, []byte("source-after"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	data, _, after, err := sub.Snapshot()
	if err != nil || string(data) != "before" || after != cursor {
		t.Fatal("source write changed cloned subscription", string(data), after, err)
	}
	if _, err := c.Put(ctx, client.ObjectID(id), []byte("copy-after"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	data, _, err = waitSubscription(t, ctx, sub, "copy-after")
	if err != nil || string(data) != "copy-after" {
		t.Fatal(string(data), err)
	}
	if data, _, err := source.Get(ctx, actor, id); err != nil || string(data) != "source-after" {
		t.Fatal(string(data), err)
	}
	// Internal COW messages are unavailable on an ordinary bucket connection.
	request := client.Message{Kind: client.Request, Opcode: peerCowCapture, ID: client.NewRequestID()}
	response, status := s.dispatchAs(request, nil, principal{User: user})
	if status != 403 && status != 501 {
		t.Fatal("client reached cluster-only preservation", status, response)
	}
}

func TestBucketCOWReplicatedSparseMaterialization(t *testing.T) {
	first, second := startAutomaticDaemonPair(t, false)
	addTestVolume(t, first.node, "cow-replica", "hdd", 16<<20)
	addTestVolume(t, second.node, "cow-replica", "hdd", 16<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := first.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer volume inventory unavailable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, first.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	source := replicaTestBucket(t, first.node, group, "cow-replicated-source")
	target := replicaTestBucket(t, first.node, group, "cow-replicated-copy")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := first.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, first.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(source.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{12}
	if _, err = c.Trunc(ctx, id, 2<<40, client.TruncOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutRange(ctx, id, (2<<40)-4, []byte("tail"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err = first.node.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, first.node)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.PutRange(ctx, id, (2<<40)-4, []byte("NEW!"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Source mutation must have committed the old sparse bytes on EVERY required
	// destination replica before acknowledging its own change.
	for _, node := range []*Server{first.node, second.node} {
		b, err := node.Bucket(target.ID())
		if err != nil {
			t.Fatal(err)
		}
		v, _, found, err := b.selectVolume(b.Describe(), volume.Key{Bucket: b.ID(), Object: volume.ObjectID(id)}, volume.ID{}, false)
		if err != nil || !found {
			t.Fatal(found, err)
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		data, info, err := h.GetRange(volume.Key{Bucket: b.ID(), Object: volume.ObjectID(id)}, (2<<40)-4, 4)
		h.Close()
		if err != nil || string(data) != "tail" || info.Length != 2<<40 {
			t.Fatal(string(data), info, err)
		}
		if v.Stats().DataPhysical > 128<<10 {
			t.Fatal("sparse clone allocated its holes", v.Stats().DataPhysical)
		}
	}
}

func TestBucketCOWDetachPreservesBothSidesAndAllowsIndependentWrites(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-detach", "hdd", 16<<20)
	a, _ := createProtocolBucket(t, s, "cow-detach-source")
	b, _ := createProtocolBucket(t, s, "cow-detach-copy")
	ctx := context.Background()
	actor := userPrincipal(t, s)
	for i := byte(1); i < 70; i++ {
		if _, err := a.Put(ctx, actor, volume.ObjectID{i}, []byte{i}, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	e := client.Encoder{}
	e.ID(client.ID(a.ID()))
	e.ID(client.ID(b.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
		t.Fatal(err)
	}
	done, err := s.cowDetachStep(ctx, b.ID())
	if err != nil || done {
		t.Fatal("detach should yield between metadata pages", done, err)
	}
	// A newly created object below the scan cursor must stay private even when
	// created between materialization pages and the final edge removal.
	if _, err := a.Put(ctx, actor, volume.ObjectID{}, []byte("new after first page"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	for attempts := 0; !done && attempts < 10; attempts++ {
		done, err = s.cowDetachStep(ctx, b.ID())
		if err != nil {
			t.Fatal(err)
		}
	}
	if !done || len(a.Describe().ClonePeers) > 0 || len(b.Describe().ClonePeers) > 0 {
		t.Fatal("clone connections not removed")
	}
	for i := byte(1); i < 70; i++ {
		data, _, err := b.Get(ctx, actor, volume.ObjectID{i})
		if err != nil || !bytes.Equal(data, []byte{i}) {
			t.Fatal(i, data, err)
		}
	}
	if _, _, err := b.Get(ctx, actor, volume.ObjectID{}); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("creation during detach leaked into copy", err)
	}
	if _, err := a.Put(ctx, actor, volume.ObjectID{1}, []byte("independent"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	data, _, err := b.Get(ctx, actor, volume.ObjectID{1})
	if err != nil || !bytes.Equal(data, []byte{1}) {
		t.Fatal(data, err)
	}
}

func TestBucketCOWInterruptedMaterializationRecoversAfterPrimaryRestart(t *testing.T) {
	first, second := startAutomaticDaemonPair(t, false)
	addTestVolume(t, first.node, "cow-resume", "hdd", 16<<20)
	addTestVolume(t, second.node, "cow-resume", "hdd", 16<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := first.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer volumes unavailable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, first.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	a := replicaTestBucket(t, first.node, group, "cow-resume-source")
	if err := first.node.openBucketFormat(context.Background(), a.ID().String(), "fs", userPrincipal(t, first.node)); err != nil {
		t.Fatal(err)
	}
	id := volume.ObjectID{31}
	entry, _ := first.node.placement.Load().lookup(client.ID(a.ID()), client.ObjectID(id))
	primary, offline := first, second
	if entry.Locations[0].Node != first.node.control.ledger.node {
		primary, offline = second, first
	}
	var b *Bucket
	for attempts := 0; attempts < 16; attempts++ {
		candidate := replicaTestBucket(t, first.node, group, fmt.Sprintf("cow-resume-copy-%d", attempts))
		placement, _ := first.node.placement.Load().lookup(client.ID(candidate.ID()), client.ObjectID(id))
		if placement.Locations[0].Node == primary.node.control.ledger.node {
			b = candidate
			break
		}
	}
	if b == nil {
		t.Fatal("could not place test buckets on the same primary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	actor := userPrincipal(t, primary.node)
	a, _ = primary.node.Bucket(a.ID())
	if _, err := a.Put(ctx, actor, id, []byte("before"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(a.ID()))
	e.ID(client.ID(b.ID()))
	if _, err := primary.node.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
		t.Fatal(err)
	}
	g, err := primary.node.cowGraph()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := primary.node.cowGraphDigest(g, b.ID())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (cowServerObjects{server: primary.node}).capture(ctx, a.ID(), id)
	if err != nil {
		t.Fatal(err)
	}
	offline.close()
	short, stop := context.WithTimeout(context.WithValue(ctx, cowProofKey{}, proof), 150*time.Millisecond)
	err = (cowServerObjects{server: primary.node}).materialize(short, b.ID(), id, snapshot)
	stop()
	snapshot.close()
	if err == nil {
		t.Fatal("materialization acknowledged without required replica")
	}
	local, _ := primary.node.Bucket(b.ID())
	pendingID, err := local.cowMaterializationRequest(id, proof)
	if err != nil {
		t.Fatal(err)
	}
	foundPrepared := false
	for _, v := range local.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		record, found, err := h.ReplicaStatus(pendingID)
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
		foundPrepared = foundPrepared || found && record.State == volume.ReplicaPrepared
	}
	if !foundPrepared {
		t.Fatal("test did not reach a durable private PREPARE")
	}
	primary = restartDaemonTestNode(t, primary)
	offline = restartDaemonTestNode(t, offline)
	local, _ = primary.node.Bucket(b.ID())
	nextID, err := local.cowMaterializationRequest(id, proof)
	if err != nil || nextID == pendingID {
		t.Fatal("terminal orphan abort did not get a fresh retry identity", err)
	}
	snapshot, err = (cowServerObjects{server: primary.node}).capture(ctx, a.ID(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	deadline = time.Now().Add(10 * time.Second)
	for {
		err = (cowServerObjects{server: primary.node}).materialize(context.WithValue(ctx, cowProofKey{}, proof), b.ID(), id, snapshot)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("preservation did not resume after aborted prepare", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, node := range []*Server{primary.node, offline.node} {
		copy, _ := node.Bucket(b.ID())
		v, _, found, err := copy.selectVolume(copy.Describe(), volume.Key{Bucket: b.ID(), Object: id}, volume.ID{}, false)
		if err != nil || !found {
			t.Fatal(found, err)
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		data, _, err := h.Get(volume.Key{Bucket: b.ID(), Object: id})
		h.Close()
		if err != nil || string(data) != "before" {
			t.Fatal(string(data), err)
		}
	}
	// A fresh, still unmaterialized clone remains readable through an existing
	// required source replica when the source primary is offline.
	readCopy := replicaTestBucket(t, primary.node, group, "cow-resume-read-replica")
	e = client.Encoder{}
	e.ID(client.ID(a.ID()))
	e.ID(client.ID(readCopy.ID()))
	if _, err = primary.node.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, primary.node)); err != nil {
		t.Fatal(err)
	}
	credentials := primary.node.cfg.Auth.BootstrapAdmin
	primary.close()
	reader, err := client.Dial(ctx, offline.node.cfg.Node.Advertise, credentials.Username, credentials.Password, client.ID(readCopy.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, _, err := reader.Get(ctx, client.ObjectID(id))
	if err != nil || string(data) != "before" {
		t.Fatal("inherited read did not use the surviving required replica", string(data), err)
	}

}

func TestBucketCOWDeletingIntermediateBucketPreservesDifferentNeighborViews(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-delete", "hdd", 16<<20)
	a, _ := createProtocolBucket(t, s, "cow-delete-a")
	b, _ := createProtocolBucket(t, s, "cow-delete-b")
	c, _ := createProtocolBucket(t, s, "cow-delete-c")
	ctx := context.Background()
	actor := userPrincipal(t, s)
	id := volume.ObjectID{1}
	put := func(bucket *Bucket, value string) {
		t.Helper()
		if _, err := bucket.Put(ctx, actor, id, []byte(value), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	clone := func(source, target *Bucket) {
		t.Helper()
		e := client.Encoder{}
		e.ID(client.ID(source.ID()))
		e.ID(client.ID(target.ID()))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
			t.Fatal(err)
		}
	}
	put(a, "first")
	clone(a, b)
	put(b, "second")
	clone(b, c)
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	if _, status, reply := adminBucketMessage(t, s, client.BucketDelete, e.B); status != 202 {
		t.Fatal(status, reply)
	}
	if _, _, err := b.Get(ctx, actor, id); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("deleted namespace remained reachable", err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for {
		for bucket, want := range map[*Bucket]string{a: "first", c: "second"} {
			data, _, err := bucket.Get(ctx, actor, id)
			if err != nil || string(data) != want {
				t.Fatal("deletion changed neighbor contents", bucket.ID(), string(data), err)
			}
		}
		if b.Describe().DeletionComplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deleted backing did not finish materializing")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(a.Describe().ClonePeers) > 0 || len(c.Describe().ClonePeers) > 0 {
		t.Fatal("deleted clone connection survived cleanup")
	}
}

func TestBucketCOWDifferentReplicationPolicyMaterializesInBackground(t *testing.T) {
	first, second := startAutomaticDaemonPair(t, false)
	addTestVolume(t, first.node, "cow-policy", "hdd", 16<<20)
	addTestVolume(t, second.node, "cow-policy", "hdd", 16<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := first.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer volumes unavailable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	source, _ := createProtocolBucket(t, first.node, "cow-policy-source")
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, first.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	target := replicaTestBucket(t, first.node, group, "cow-policy-copy")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := first.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, first.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(source.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{11}
	if _, err = c.Put(ctx, id, []byte("one durable source copy"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err = first.node.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, first.node)); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		ready := true
		for _, node := range []*Server{first.node, second.node} {
			b, err := node.Bucket(target.ID())
			if err != nil || len(b.Describe().ClonePeers) > 0 || b.Describe().CloneDetach {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("different replication policy did not finish background materialization")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, node := range []*Server{first.node, second.node} {
		b, err := node.Bucket(target.ID())
		if err != nil {
			t.Fatal(err)
		}
		v, _, found, err := b.selectVolume(b.Describe(), volume.Key{Bucket: b.ID(), Object: volume.ObjectID(id)}, volume.ID{}, false)
		if err != nil || !found {
			t.Fatal("required copy missing after detach", found, err)
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		data, _, err := h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID(id)})
		h.Close()
		if err != nil || string(data) != "one durable source copy" {
			t.Fatal(string(data), err)
		}
	}
	if _, err = c.Put(ctx, id, []byte("source changed later"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// Clone chains are an undirected graph of namespace dependencies, not content
// revisions. Compare recursive resolution with independently copied logical
// namespaces across deep chains, forks, deletions and writes on intermediate nodes.
func TestBucketCOWRecursiveChainsAndForks(t *testing.T) {
	ctx := context.Background()
	g := cowGraph{}
	m := &cowMemory{objects: map[volume.Key]cowMemoryRecord{}}
	nodes := []volume.ID{{1}}
	want := map[volume.ID]map[volume.ObjectID]string{{1}: {}}
	random := rand.New(rand.NewSource(703))
	check := func() {
		for _, node := range nodes {
			for j := byte(1); j <= 6; j++ {
				object := volume.ObjectID{j}
				expected, ok := want[node][object]
				if !ok {
					expected = "<absent>"
				}
				if got := cowRead(t, g, m, node, object); got != expected {
					t.Fatalf("node=%v object=%v got=%q want=%q", node, object, got, expected)
				}
			}
		}
	}
	for step := 0; step < 120; step++ {
		if step < 12 || step%8 == 0 {
			parent := nodes[len(nodes)-1]
			if step%3 == 0 {
				parent = nodes[random.Intn(len(nodes))]
			}
			child := volume.ID{byte(len(nodes) + 1)}
			g[parent] = append(g[parent], child)
			g[child] = []volume.ID{parent}
			nodes = append(nodes, child)
			want[child] = map[volume.ObjectID]string{}
			for object, data := range want[parent] {
				want[child][object] = data
			}
		}
		node := nodes[random.Intn(len(nodes))]
		object := volume.ObjectID{byte(random.Intn(6) + 1)}
		deleted := step%4 == 0
		data := fmt.Sprintf("node-%d-step-%d", node[0], step)
		if err := m.write(ctx, g, node, object, []byte(data), deleted); err != nil {
			t.Fatal(err)
		}
		if deleted {
			delete(want[node], object)
		} else {
			want[node][object] = data
		}
		check()
	}
}

func TestBucketCOWCloneLogicalQuotaAndCachedMaterialization(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			var s *Server
			if cluster {
				node, _ := startAutomaticDaemonPair(t, false)
				s = node.node
			} else {
				s = testServer(t)
			}
			addTestVolume(t, s, "quota-cow", "hdd", 8<<20)
			source := createQuotaBucket(t, s, "quota-source", "logical", 0, 0)
			exact := createQuotaBucket(t, s, "quota-exact", "logical", 0, 8)
			small := createQuotaBucket(t, s, "quota-small", "logical", 0, 7)
			ctx := context.Background()
			actor := quotaPrincipal(t, s)
			id := volume.ObjectID{7}
			if _, err := source.Put(ctx, actor, id, []byte("original"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			clone := func(dst *Bucket) error {
				e := client.Encoder{}
				e.ID(client.ID(source.ID()))
				e.ID(client.ID(dst.ID()))
				_, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor)
				return err
			}
			if err := clone(small); !errors.Is(err, volume.ErrFull) && statusOf(err) != 507 {
				t.Fatal("clone exceeded logical quota", err)
			}
			if len(small.Describe().ClonePeers) != 0 {
				t.Fatal("rejected clone published a dependency")
			}
			if err := clone(exact); err != nil {
				t.Fatal(err)
			}
			if used, err := exact.logicalUsage(); err != nil || used != 8 {
				t.Fatal("inherited bytes not charged", used, err)
			}
			if _, err := source.PutRange(ctx, actor, id, 0, []byte("changed!"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			if used, err := exact.logicalUsage(); err != nil || used != 8 {
				t.Fatal("physical preservation counted inherited bytes twice", used, err)
			}
			if _, err := exact.Append(ctx, actor, id, []byte("!"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); !errors.Is(err, volume.ErrFull) && statusOf(err) != 507 {
				t.Fatal("append exceeded cloned quota", err)
			}
			if _, err := exact.PutRange(ctx, actor, id, 0, []byte("new data"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal("same-length overwrite blocked", err)
			}
		})
	}
}

func TestBucketCOWRecursiveRangeReadCopiesOnlyObservedBytes(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cow-ranges", "hdd", 32<<20)
	ctx := context.Background()
	actor := userPrincipal(t, s)
	a, _ := createProtocolBucket(t, s, "range-source")
	b, _ := createProtocolBucket(t, s, "range-child")
	c, _ := createProtocolBucket(t, s, "range-grandchild")
	id := volume.ObjectID{43}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 256<<10)
	if _, err := a.Put(ctx, actor, id, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]*Bucket{{a, b}, {b, c}} {
		e := client.Encoder{}
		e.ID(client.ID(pair[0].ID()))
		e.ID(client.ID(pair[1].ID()))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
			t.Fatal(err)
		}
	}
	reads := func() uint64 {
		var n uint64
		for _, v := range a.allVolumes() {
			n += testStorageReadBytes(v.IOStats())
		}
		return n
	}
	before := reads()
	data, _, err := c.GetRange(ctx, actor, id, 12345, 19)
	if err != nil || !bytes.Equal(data, payload[12345:12364]) {
		t.Fatal(string(data), err)
	}
	if n := reads() - before; n >= uint64(len(payload))/2 {
		t.Fatalf("19-byte recursive read copied %d bytes from a %d-byte object", n, len(payload))
	}
	// Both disjoint and overlapping byte conditions must be captured correctly.
	for _, off := range []uint64{1, 12350} {
		var dst bytes.Buffer
		_, n, err := c.cowCopyTo(ctx, id, 12345, 19, &dst, &volume.Options{CompareOffset: off, Compare: payload[off : off+9]})
		if err != nil || n != 19 || !bytes.Equal(dst.Bytes(), data) {
			t.Fatal(n, err)
		}
	}
	for _, v := range c.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		records, err := h.ScanBucketObjects(c.ID(), nil, 1)
		h.Close()
		if err != nil || len(records) != 0 {
			t.Fatal("read materialized clone", err)
		}
	}
}

func TestBucketCOWRecursiveNativeMutations(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "recursive-native", "hdd", 16<<20)
	ctx := context.Background()
	actor := userPrincipal(t, s)
	var buckets []*Bucket
	for _, name := range []string{"a", "b", "c", "d", "fork"} {
		b, _ := createProtocolBucket(t, s, "recursive-"+name)
		buckets = append(buckets, b)
	}
	opt := func() volume.Options { return volume.Options{RequestID: volume.ID(client.NewRequestID())} }
	for i := byte(1); i <= 6; i++ {
		if _, err := buckets[0].Put(ctx, actor, volume.ObjectID{i}, []byte("original"), opt()); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range [][2]int{{0, 1}, {1, 2}, {2, 3}, {1, 4}} {
		e := client.Encoder{}
		e.ID(client.ID(buckets[edge[0]].ID()))
		e.ID(client.ID(buckets[edge[1]].ID()))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
			t.Fatal(err)
		}
	}
	for i := byte(1); i <= 6; i++ {
		target := int(i) % len(buckets)
		b := buckets[target]
		id := volume.ObjectID{i}
		want := ""
		var err error
		switch i {
		case 1:
			_, err = b.Put(ctx, actor, id, []byte("new"), opt())
			want = "new"
		case 2:
			_, err = b.PutRange(ctx, actor, id, 1, []byte("X"), opt())
			want = "oXiginal"
		case 3:
			_, err = b.Append(ctx, actor, id, []byte("!"), opt())
			want = "original!"
		case 4:
			_, err = b.Trunc(ctx, actor, id, 3, opt())
			want = "ori"
		case 5:
			_, err = b.CAS(ctx, actor, id, 0, []byte("original"), []byte("CAS-data"), opt())
			want = "CAS-data"
		case 6:
			err = b.Delete(ctx, actor, id, opt())
		}
		if err != nil {
			t.Fatal(i, err)
		}
		for j, other := range buckets {
			data, _, err := other.Get(ctx, actor, id)
			if j == target && i == 6 {
				if !errors.Is(err, volume.ErrNotFound) {
					t.Fatal("delete leaked", err)
				}
				continue
			}
			expected := "original"
			if j == target {
				expected = want
			}
			if err != nil || string(data) != expected {
				t.Fatalf("op=%d bucket=%d got=%q want=%q error=%v", i, j, data, expected, err)
			}
		}
	}
	// A newly created object must not appear in any transitively linked clone.
	if _, err := buckets[0].Put(ctx, actor, volume.ObjectID{99}, []byte("new-object"), opt()); err != nil {
		t.Fatal(err)
	}
	for _, b := range buckets[1:] {
		if _, _, err := b.Get(ctx, actor, volume.ObjectID{99}); !errors.Is(err, volume.ErrNotFound) {
			t.Fatal("new object leaked", err)
		}
	}
}
