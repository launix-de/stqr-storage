// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func replicaTestBucket(t *testing.T, s *Server, group volume.ID, name string) *Bucket {
	t.Helper()
	id := client.NewRequestID()
	e := client.Encoder{}
	e.U16(1)
	e.ID(id)
	e.Text(name)
	e.ID(client.ID(group))
	e.U8(0)
	e.U64(0)
	e.Bool(true)
	e.U64(0)
	e.U32(540)
	_, status, reply := adminBucketMessage(t, s, client.BucketCreateInGroup, e.B)
	if status != 201 {
		t.Fatalf("create %d %+v", status, reply)
	}
	b, err := s.Bucket(volume.ID(id))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDaemonReplicationAllOfflineRetryAndRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "replica-data", "hdd", 16<<20)
	addTestVolume(t, target.node, "replica-data", "hdd", 16<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("peer volume statistics missing")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "replicated-db")
	entry, ok := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{})
	if !ok || len(entry.Locations) != 2 {
		t.Fatalf("replica placement: %+v", entry)
	}
	other, _ := createProtocolBucket(t, source.node, "independent")
	actor := userPrincipal(t, source.node)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id := volume.ObjectID{4}
	opts := func() volume.Options { return volume.Options{RequestID: volume.ID(client.NewRequestID())} }
	if _, err := b.Put(ctx, actor, id, []byte("base"), opts()); err != nil {
		t.Fatal(err)
	}
	follower, err := target.node.Bucket(b.ID())
	if err != nil {
		t.Fatal(err)
	}
	check := func(bucket *Bucket, want string) {
		t.Helper()
		data, _, err := bucket.Get(ctx, actor, id)
		if err != nil || string(data) != want {
			t.Fatalf("read %q expected %q: %v", data, want, err)
		}
	}
	check(follower, "base")
	retryID := opts()
	result, err := b.Append(ctx, actor, id, []byte("!"), retryID)
	if err != nil || result.Offset != 4 {
		t.Fatalf("append %+v %v", result, err)
	}
	retry, err := b.Append(ctx, actor, id, []byte("!"), retryID)
	if err != nil || retry != result {
		t.Fatalf("retry %+v %v", retry, err)
	}
	check(follower, "base!")
	if _, err = b.PutRange(ctx, actor, id, 0, []byte("B"), opts()); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Trunc(ctx, actor, id, 3, opts()); err != nil {
		t.Fatal(err)
	}
	if _, err = b.CAS(ctx, actor, id, 0, []byte("B"), []byte("C"), opts()); err != nil {
		t.Fatal(err)
	}
	check(follower, "Cas")
	storage.Compression = "gzip"
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "all", Storage: &storage})
	awaitPGVersion(t, source.node, group, 2)
	if _, info, err := follower.Get(ctx, actor, id); err != nil || info.Compression != volume.Gzip || info.Version != 5 {
		t.Fatalf("replica codec conversion %+v %v", info, err)
	}
	// A required offline replica blocks only this write; prepared bytes stay
	// invisible and an unrelated single-copy bucket keeps accepting writes.
	target.close()
	pending := opts()
	short, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = b.Append(short, actor, id, []byte("?"), pending)
	stop()
	if err == nil {
		t.Fatal("ACK with missing required copy")
	}
	check(b, "Cas")
	if _, err = other.Put(ctx, actor, volume.ObjectID{1}, []byte("available"), opts()); err != nil {
		t.Fatal("unrelated bucket stalled", err)
	}
	target = restartDaemonTestNode(t, target)
	deadline = time.Now().Add(10 * time.Second)
	for {
		result, err = b.Append(ctx, actor, id, []byte("?"), pending)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending append did not resume", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if result.Offset != 3 {
		t.Fatal(result)
	}
	follower, _ = target.node.Bucket(b.ID())
	check(follower, "Cas?")
	source = restartDaemonTestNode(t, source)
	b, _ = source.node.Bucket(b.ID())
	check(b, "Cas?")
	deletion := opts()
	deadline = time.Now().Add(10 * time.Second)
	for {
		err = b.Delete(ctx, actor, id, deletion)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if data, _, err := follower.Get(ctx, actor, id); err == nil || bytes.Equal(data, []byte("Cas?")) {
		t.Fatal("deleted replica remained readable", err)
	}
}

func TestDaemonReplicaAddPreservesPrimaryDataAndOriginalReceipts(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source-data", "hdd", 16<<20)
	addTestVolume(t, target.node, "target-data", "hdd", 16<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "add-replica")
	actor := userPrincipal(t, source.node)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	id := volume.ObjectID{8}
	request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original, err := b.Append(ctx, actor, id, []byte("existing"), request)
	if err != nil {
		t.Fatal(err)
	}
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	storage.Compression = "gzip"
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "all", Storage: &storage})
	deadline := time.Now().Add(10 * time.Second)
	var plan rangePlan
	for {
		d := b.Describe()
		p, ok := source.node.scopes.planReplicaAdd(d)
		if ok {
			plan = p
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no replica plan: %+v", d)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	follower, _ := target.node.Bucket(b.ID())
	for _, copy := range []*Bucket{b, follower} {
		data, info, err := copy.Get(ctx, actor, id)
		if err != nil || string(data) != "existing" || info.Version != original.Info.Version {
			t.Fatalf("learner %q %+v %v", data, info, err)
		}
	}
	// A request originally acknowledged before the new copy was added must
	// remain a retry, rather than appending its old payload a second time.
	retry, err := b.Append(ctx, actor, id, []byte("existing"), request)
	if err != nil || retry != original {
		t.Fatalf("old retry %+v %v", retry, err)
	}
	if _, err = b.Append(ctx, actor, id, []byte("!"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	data, _, err := follower.Get(ctx, actor, id)
	if err != nil || string(data) != "existing!" {
		t.Fatalf("active replica %q %v", data, err)
	}
	awaitPGVersion(t, source.node, group, 2)
	if _, info, e := follower.Get(ctx, actor, id); e != nil || info.Compression != volume.Gzip {
		t.Fatalf("combined replica/codec transition %+v %v", info, e)
	}
	storage.Redundancy = BucketRedundancy{Mode: "single", Replicas: 1, ACK: "all"}
	var revision uint64
	for _, g := range source.node.placementGroups.Load().object.Groups {
		if g.ID == group.String() {
			revision = g.Revision
		}
	}
	centralPGRequest(t, source.node, group, revision, PlacementMask{NodeSelection: "all", Storage: &storage})
	deadline = time.Now().Add(10 * time.Second)
	for {
		p, ok := source.node.scopes.planReplicaRemove(b.Describe())
		if ok {
			plan = p
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no replica removal plan: %+v", b.Describe())
		}
		time.Sleep(25 * time.Millisecond)
	}
	wire, encodeErr := plan.marshal()
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if decoded, e := decodeRangePlan(wire); e != nil {
		t.Fatalf("remove wire decode %v plan=%+v decoded=%+v", e, plan, decoded)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal("remove replica", err)
	}
	data, _, err = b.Get(ctx, actor, id)
	if err != nil || string(data) != "existing!" {
		t.Fatal("primary changed after replica removal", err)
	}
	for _, v := range target.node.quotaVolumes() {
		h, e := v.Open(false)
		if e != nil {
			t.Fatal(e)
		}
		_, e = h.RecordInfo(volume.Key{Bucket: b.ID(), Object: id})
		h.Close()
		if e == nil {
			t.Fatal("retired replica allocation not reclaimed")
		}
	}
}

func TestDaemonReplicationQuotaCountsLogicalOnceAndPhysicalPerCopy(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing peer volume")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	for _, physical := range []bool{false, true} {
		id := client.NewRequestID()
		e := client.Encoder{}
		e.U16(1)
		e.ID(id)
		name := "logical-replicas"
		account := uint8(0)
		limit := uint64(16 << 10)
		size := 12 << 10
		if physical {
			name = "physical-replicas"
			account = 1
			limit = 512 << 10
			size = 96 << 10
		}
		e.Text(name)
		e.ID(client.ID(group))
		e.U8(account)
		e.U64(0)
		e.Bool(false)
		e.U64(limit)
		e.U32(540)
		_, status, reply := adminBucketMessage(t, source.node, client.BucketCreateInGroup, e.B)
		if status != 201 {
			t.Fatal(status, reply)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		admin := source.node.cfg.Auth.BootstrapAdmin
		c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.Put(ctx, client.ObjectID{1}, bytes.Repeat([]byte{1}, size), client.PutOptions{Accepted: true}); err != nil {
			t.Fatal(name, err)
		}
		if err = c.Sync(ctx); err != nil {
			t.Fatal(name, "replica accepted barrier", err)
		}
		var logical, allocated uint64
		for _, node := range []*Server{source.node, target.node} {
			b, _ := node.Bucket(volume.ID(id))
			n, err := b.logicalUsage()
			if err != nil {
				t.Fatal(err)
			}
			logical += n
			n, err = b.physicalUsage()
			if err != nil {
				t.Fatal(err)
			}
			allocated += n
		}
		if logical != uint64(size) || allocated < uint64(size*2) {
			t.Fatalf("%s logical=%d physical=%d", name, logical, allocated)
		}
		bucket, _ := source.node.Bucket(volume.ID(id))
		aggregate, aggregateErr := bucket.clusterUsageAs(ctx, userPrincipal(t, source.node), "physical")
		if aggregateErr != nil || aggregate != allocated {
			t.Fatalf("replica physical aggregation %d != %d: %v", aggregate, allocated, aggregateErr)
		}
		if _, err = c.Put(ctx, client.ObjectID{1}, bytes.Repeat([]byte{2}, int(limit+1)), client.PutOptions{}); !errors.Is(err, client.ErrFull) {
			t.Fatalf("%s hard limit: %v", name, err)
		}
		c.Close()
		cancel()
	}
}

func TestDaemonReplicaCapacityRejectionAbortsAndReleasesPrimary(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 1<<20)
	peerVolume := addTestVolume(t, target.node, "data", "hdd", 1<<20)
	target.node.mu.RLock()
	peerStore := target.node.volumes[peerVolume.VolumeID].store
	target.node.mu.RUnlock()
	h, err := peerStore.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(volume.Key{Bucket: volume.ID{0xfe}, Object: volume.ObjectID{1}}, bytes.Repeat([]byte{3}, 768<<10), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing peer volume")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "capacity-abort")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor := userPrincipal(t, source.node)
	id := volume.ObjectID{5}
	request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	payload := bytes.Repeat([]byte{7}, 256<<10)
	if _, err = b.Put(ctx, actor, id, payload, request); statusOf(err) != 507 {
		t.Fatal("required capacity rejection", err)
	}
	if _, _, err = b.Get(ctx, actor, id); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("aborted PUT became readable", err)
	}
	if _, err = b.Put(ctx, actor, id, []byte("small write still fits"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("abort stranded primary reservation", err)
	}
	if _, err = b.Put(ctx, actor, id, payload, request); statusOf(err) != 507 {
		t.Fatal("failed request lost its durable outcome", err)
	}
}

func TestDaemonPrimaryRestartAbortsOrphanedPrepareWithoutLosingAcknowledgedData(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	left := addTestVolume(t, source.node, "data", "hdd", 8<<20)
	right := addTestVolume(t, target.node, "data", "hdd", 8<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing replica volume")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "orphan-recovery")
	actor := userPrincipal(t, source.node)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id := volume.ObjectID{3}
	if _, err := b.Put(ctx, actor, id, []byte("base"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(id))
	binding := volume.ReplicaBinding{Epoch: entry.OwnerEpoch, Sequence: 100, SetHash: replicaSetHash(b.ID(), entry)}
	request := volume.Options{RequestID: volume.ID(client.NewRequestID()), Compression: volume.Codec(volume.Raw)}
	for i, node := range []*Server{source.node, target.node} {
		vid := left.VolumeID
		if i == 1 {
			vid = right.VolumeID
		}
		node.mu.RLock()
		store := node.volumes[vid].store
		node.mu.RUnlock()
		h, err := store.Open(true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.PrepareMutation(3, volume.Key{Bucket: b.ID(), Object: id}, 0, 1, bytes.NewBufferString("?"), nil, request, binding)
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	source = restartDaemonTestNode(t, source)
	b, _ = source.node.Bucket(b.ID())
	if _, err := b.Append(ctx, actor, id, []byte("?"), request); statusOf(err) != 409 {
		t.Fatal("orphan did not retain its abort outcome", err)
	}
	data, _, err := b.Get(ctx, actor, id)
	if err != nil || string(data) != "base" {
		t.Fatal("unacknowledged prepare overwrote confirmed data", string(data), err)
	}
	next := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	deadline = time.Now().Add(10 * time.Second)
	for {
		_, err = b.Append(ctx, actor, id, []byte("!"), next)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("orphan blocked subsequent writes", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	follower, _ := target.node.Bucket(b.ID())
	data, _, err = follower.Get(ctx, actor, id)
	if err != nil || string(data) != "base!" {
		t.Fatal("follower did not reconcile abort/new write", string(data), err)
	}
}

func TestDaemonReplicaSetDoesNotWaitForUnrelatedOfflineThirdMember(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	source.node.mu.RLock()
	cfg := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	seed := PeerConfig{NodeID: cfg.Node.ID, URL: cfg.Node.Advertise}
	cfg.Node.ID = newID()
	cfg.Node.Name = "metadata-only-third"
	cfg.Node.DataDir = "."
	cfg.Volumes = nil
	cfg.Peers = []PeerConfig{seed}
	cfg.ControlVersion = 0
	cfg.RangeVersion = 0
	cfg.QuotaVersion = 0
	cfg.PeerDirectoryVersion = 0
	cfg.BucketCatalogVersion = 0
	cfg.PlacementGroupsVersion = 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	third := startDaemonTestNode(t, filepath.Join(t.TempDir(), "third.yaml"), &cfg, false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		view := source.node.currentSystem.Load()
		if view != nil && len(view.manifest.Membership.Members) == 3 && third.node.currentSystem.Load() != nil && third.node.currentSystem.Load().root == view.root {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("third member did not join")
		}
		time.Sleep(25 * time.Millisecond)
	}
	addTestVolume(t, source.node, "data", "hdd", 8<<20)
	addTestVolume(t, target.node, "data", "hdd", 8<<20)
	deadline = time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missing data volumes")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "required-subset")
	third.close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actor := userPrincipal(t, source.node)
	if _, err := b.Put(ctx, actor, volume.ObjectID{1}, []byte("two required copies, three members"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("data ALL waited for an unrelated member", err)
	}
	if len(source.node.currentSystem.Load().manifest.Membership.Members) != 3 {
		t.Fatal("offline member was removed")
	}
	follower, _ := target.node.Bucket(b.ID())
	data, _, err := follower.Get(ctx, actor, volume.ObjectID{1})
	if err != nil || string(data) != "two required copies, three members" {
		t.Fatal(string(data), err)
	}
}
