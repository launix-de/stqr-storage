// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func centralPGRequest(t *testing.T, s *Server, id volume.ID, revision uint64, mask PlacementMask) client.Message {
	t.Helper()
	raw, err := json.Marshal(mask)
	if err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.U16(2)
	e.ID(client.ID(id))
	e.U64(revision)
	e.Text("Hosting DB")
	e.Blob(raw)
	m, status, reply := adminBucketMessage(t, s, placementGroupSet, e.B)
	if status != 200 && status != 201 && status != 202 {
		t.Fatalf("PG request status %d: %v", status, reply)
	}
	return m
}
func centralBucket(t *testing.T, s *Server, group volume.ID, name string, quota uint64) *Bucket {
	t.Helper()
	id := volume.ID(client.NewRequestID())
	e := client.Encoder{}
	e.U16(1)
	e.ID(client.ID(id))
	e.Text(name)
	e.ID(client.ID(group))
	e.U8(0)
	e.U64(0)
	e.Bool(false)
	e.U64(quota)
	e.U32(540)
	_, status, reply := adminBucketMessage(t, s, client.BucketCreateInGroup, e.B)
	if status != 201 {
		t.Fatalf("central bucket status %d: %v", status, reply)
	}
	b, err := s.Bucket(id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func awaitPGVersion(t *testing.T, s *Server, id volume.ID, version uint64) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		g, ok := s.placementGroups.Load().mask(id.String(), 0)
		if ok && g.Version == version {
			return
		}
		// These fixtures deliberately disable the range scheduler so they
		// can select relocation plans. Codec changes now belong to that same
		// PG path, rather than a global admin bulk-copy callback.
		if s.scopes != nil {
			for _, d := range s.bucketSnapshots() {
				if d.Policy.PlacementGroup != id.String() {
					continue
				}
				if p, ok := s.scopes.planPGReplicaCodec(d); ok {
					ctx, cancel := context.WithTimeout(s.ctx, time.Second)
					err := s.scopes.move(ctx, p)
					cancel()
					if err != nil && statusOf(err) != 503 {
						t.Fatal("PG codec step", err)
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, d := range s.bucketSnapshots() {
		if d.Policy.PlacementGroup == id.String() {
			t.Logf("pending PG bucket=%s policy=%+v desired=%+v inplace=%v ranges=%+v", d.Name, d.Policy, d.DesiredPolicy, s.replicaPolicyInPlace(d), s.placement.Load().bucketEntries(client.ID(newVolumeID(d.ID))))
		}
	}
	t.Fatalf("PG version %d not reached: %+v", version, s.placementGroups.Load().object.Groups)
}

func TestCentralPGUpdatesAllBucketsPreservesQuotaAndPayload(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 8<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	mask := PlacementMask{NodeSelection: "all", Storage: &storage}
	centralPGRequest(t, s, group, 0, mask)
	buckets := make([]*Bucket, 12)
	user, _ := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	actor := principal{User: user}
	payload := bytes.Repeat([]byte("php and database payload "), 800)
	versions := map[string]volume.Info{}
	for i := range buckets {
		b := centralBucket(t, s, group, fmt.Sprintf("customer-%d-db", i), uint64(1<<20+i))
		buckets[i] = b
		info, err := b.Put(context.Background(), actor, volume.ObjectID{1}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())})
		if err != nil {
			t.Fatal(err)
		}
		versions[b.ID().String()] = info
	}
	storage.Compression = "gzip"
	centralPGRequest(t, s, group, 1, mask)
	awaitPGVersion(t, s, group, 2)
	for i, b := range buckets {
		d := b.Describe()
		if d.DesiredPolicy != nil || d.Policy.Compression != "gzip" || d.Policy.PlacementMaskVersion != 2 || d.Policy.Limits.Absolute != uint64(1<<20+i) {
			t.Fatalf("bucket did not inherit policy or lost quota: %+v", d)
		}
		data, info, err := b.Get(context.Background(), actor, volume.ObjectID{1})
		if err != nil || !bytes.Equal(data, payload) || info.Length != versions[b.ID().String()].Length {
			t.Fatal("policy conversion changed payload/version", err)
		}
	}
	path := s.configPath
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.validatePGProjections(); err != nil {
		t.Fatal(err)
	}
}

func TestPGStorageMigrationPreservesLegacyContracts(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 4<<20)
	// Model an old persisted catalog with no PG policy and two shared contracts.
	s.buckets.upgrading = true
	ids := []volume.ID{}
	for i, codec := range []string{"gzip", "gzip", "raw"} {
		b := newTestBucket(t, fmt.Sprintf("old-%d", i))
		d := b.Describe()
		d.Policy.PlacementGroup = ""
		d.Policy.PlacementMaskVersion = 0
		d.Policy.Compression = codec
		saved, err := s.buckets.Save(d, 0)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, saved.ID())
	}
	s.buckets.upgrading = false
	object := s.placementGroups.Load().object
	object.Schema = 1
	for i := range object.Groups {
		object.Groups[i].Effective.Storage = nil
	}
	if err := s.savePlacementGroups(object); err != nil {
		t.Fatal(err)
	}
	path := s.configPath
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	groups := []string{}
	for i, id := range ids {
		b, err := reopened.Bucket(id)
		if err != nil {
			t.Fatal(err)
		}
		p := b.Describe().Policy
		groups = append(groups, p.PlacementGroup)
		want := "gzip"
		if i == 2 {
			want = "raw"
		}
		if p.Compression != want {
			t.Fatal("legacy storage contract changed")
		}
	}
	if groups[0] != groups[1] || groups[0] == groups[2] || groups[2] != defaultPlacementGroup.String() {
		t.Fatal("legacy contracts were not centrally shared", groups)
	}
	if err := reopened.validatePGProjections(); err != nil {
		t.Fatal(err)
	}
}

func TestCentralPGPersistsOfflineIntentAndDoesNotTouchOtherPG(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 4<<20)
	policy := storagePolicy(DefaultBucketPolicy())
	group := volume.ID(client.NewRequestID())
	centralPGRequest(t, s, group, 0, PlacementMask{NodeSelection: "all", Storage: &policy})
	b := centralBucket(t, s, group, "waiting", 1<<20)
	other := centralBucket(t, s, defaultPlacementGroup, "other", 2<<20)
	before := other.Describe()
	// Target only an unavailable node. Local worker must preserve current data
	// placement and the pending desired policy across restart.
	mask := PlacementMask{Storage: &policy, NodeSelection: "list", Nodes: []PlacementNode{{NodeID: volume.ID{0x42}.String(), Mode: VolumesAll}}}
	centralPGRequest(t, s, group, 1, mask)
	time.Sleep(30 * time.Millisecond)
	if d := b.Describe(); d.DesiredPolicy == nil || d.Policy.PlacementMaskVersion != 1 || d.DesiredPolicy.PlacementMaskVersion != 2 {
		t.Fatalf("offline intent prematurely applied: %+v", d)
	}
	if other.Describe().Revision != before.Revision {
		t.Fatal("another PG was changed")
	}
	path := s.configPath
	s.Close()
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	waiting, err := reopened.Bucket(b.ID())
	if err != nil || waiting.Describe().DesiredPolicy == nil {
		t.Fatal("offline intent lost", err)
	}
}

func TestCentralPGClusterChangeUsesCommittedSharedPolicy(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	for _, node := range []*daemonTestNode{source, target} {
		addTestVolume(t, node.node, "data", "hdd", 8<<20)
	}
	s := source.node
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	mask := PlacementMask{NodeSelection: "all", Storage: &storage}
	centralPGRequest(t, s, group, 0, mask)
	var ids []volume.ID
	for i := 0; i < 2; i++ {
		b := centralBucket(t, s, group, fmt.Sprintf("cluster-customer-%d", i), 2<<20)
		ids = append(ids, b.ID())
	}
	storage.Compression = "lz4"
	centralPGRequest(t, s, group, 1, mask)
	awaitPGVersion(t, s, group, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ready := true
		for _, node := range []*Server{source.node, target.node} {
			if node.validatePGProjections() != nil {
				ready = false
				break
			}
			for _, id := range ids {
				b, err := node.Bucket(id)
				if err != nil || b.Describe().Policy.Compression != "lz4" || b.Describe().DesiredPolicy != nil {
					ready = false
					break
				}
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("shared PG policy did not converge on every member")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCentralPGThousandBucketFanoutUsesOneCatalogCommit(t *testing.T) {
	source := testServer(t)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	mask := PlacementMask{NodeSelection: "all", Storage: &storage}
	centralPGRequest(t, source, group, 0, mask)
	source.opMu.Lock()
	preview, err := source.newAdminPreview()
	source.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer preview.cancel()
	defer preview.system.Close()
	hashes, _, version, err := preview.buckets.load()
	if err != nil {
		t.Fatal(err)
	}
	var descriptions []BucketDescription
	for i := 0; i < 1000; i++ {
		b := newTestBucket(t, fmt.Sprintf("tenant-%04d-db", i))
		d := b.Describe()
		d.Policy.PlacementGroup = group.String()
		d.Policy.Limits.Unlimited = false
		d.Policy.Limits.Absolute = uint64(1<<20 + i)
		hash, err := preview.buckets.persistDescription(d)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, hash)
		descriptions = append(descriptions, d)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	var root []byte
	for _, hash := range hashes {
		root = append(root, hash[:]...)
	}
	if err := preview.buckets.write(func(h *volume.Handle) error {
		_, err := h.CAS(BucketCatalogKey(), 0, version, root, volume.Options{WholeObject: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, d := range descriptions {
		preview.publishBucket(d)
	}
	before := preview.system.Stats().Generation
	if err != nil {
		t.Fatal(err)
	}
	storage.Compression = "gzip"
	centralPGRequest(t, preview, group, 1, mask)
	after := preview.system.Stats().Generation
	if after-before > uint64(len(descriptions)+8) {
		t.Fatal("PG fanout performed a catalog rewrite per bucket", after-before)
	}
	for i, d := range descriptions {
		b, _ := preview.Bucket(mustBucketID(d.ID))
		next := b.Describe()
		if next.DesiredPolicy == nil || next.DesiredPolicy.Compression != "gzip" || next.Policy.Limits.Absolute != uint64(1<<20+i) {
			t.Fatal("incomplete central fanout", d.Name)
		}
	}
}
