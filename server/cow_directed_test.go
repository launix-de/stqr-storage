// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDirectedCOWRecursiveLeafWritesWithoutAncestorSync(t *testing.T) {
	s := testServer(t)
	volumes := []*volume.Volume{addReplicaDataVolume(t, s, "directed-one", "hdd", 16<<20), addReplicaDataVolume(t, s, "directed-two", "hdd", 16<<20)}
	http := httptest.NewServer(s.Handler())
	defer http.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	buckets := make([]*Bucket, 4)
	clients := make([]*client.Client, 4)
	for i, name := range []string{"directed-a", "directed-b", "directed-c", "directed-sibling"} {
		buckets[i], _ = createProtocolBucket(t, s, name)
		c, err := client.Dial(ctx, http.URL, admin.Username, admin.Password, client.ID(buckets[i].ID()), "fs")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		clients[i] = c
	}
	object := client.ObjectID{0x91}
	newLeaf := client.ObjectID{0x92}
	newParent := client.ObjectID{0x93}
	original := bytes.Repeat([]byte{'a'}, 210)
	changed := bytes.Repeat([]byte{'b'}, 210)
	accepted := client.PutOptions{Accepted: true}
	if _, err := clients[0].Put(ctx, object, original, accepted); err != nil {
		t.Fatal(err)
	}
	clone := func(from, to int) {
		t.Helper()
		e := client.Encoder{}
		e.ID(client.ID(buckets[from].ID()))
		e.ID(client.ID(buckets[to].ID()))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s)); err != nil {
			t.Fatal(err)
		}
	}
	clone(0, 1)
	clone(1, 2)
	clone(0, 3)
	assert := func(i int, id client.ObjectID, want []byte) {
		t.Helper()
		got, _, err := clients[i].Get(ctx, id)
		if want == nil {
			if !errors.Is(err, client.ErrNotFound) {
				t.Fatalf("bucket%d absent read got %q: %v", i, got, err)
			}
			return
		}
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("bucket%d got %q want %q: %v", i, got, want, err)
		}
	}
	for i := range clients {
		assert(i, object, original)
	}
	var before uint64
	for _, v := range volumes {
		before += v.IOStats().SyncOps
	}
	if _, err := clients[2].CAS(ctx, object, 0, original, changed, client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := clients[2].CAS(ctx, newLeaf, 0, make([]byte, 4), []byte("leaf"), client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	var after uint64
	for _, v := range volumes {
		after += v.IOStats().SyncOps
	}
	if after != before {
		t.Fatalf("leaf writes introduced Sync: before=%d after=%d", before, after)
	}
	for _, i := range []int{0, 1, 3} {
		assert(i, object, original)
		assert(i, newLeaf, nil)
	}
	assert(2, object, changed)
	assert(2, newLeaf, []byte("leaf"))
	// The ancestors must have no physical absence markers for the leaf's new ID.
	for _, v := range volumes {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range []int{0, 1, 3} {
			_, err := h.RecordInfo(volume.Key{Bucket: buckets[i].ID(), Object: volume.ObjectID(newLeaf)})
			if !errors.Is(err, volume.ErrNotFound) {
				t.Fatalf("leaf preserved an ancestor: %v", err)
			}
		}
		h.Close()
	}
	if err := clients[2].Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// Reopen actual fenced disk images, independently of the accepted RAM view.
	found := map[volume.ObjectID][]byte{}
	for _, v := range volumes {
		var filename string
		s.mu.RLock()
		for _, m := range s.volumes {
			if m.store == v {
				filename = m.config.Path
			}
		}
		s.mu.RUnlock()
		raw, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		copyPath := filepath.Join(t.TempDir(), "volume")
		if err = os.WriteFile(copyPath, raw, 0600); err != nil {
			t.Fatal(err)
		}
		reopened, err := volume.New(copyPath, 0, volume.HDD)
		if err != nil {
			t.Fatal(err)
		}
		h, err := reopened.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []client.ObjectID{object, newLeaf} {
			data, _, err := h.Get(volume.Key{Bucket: buckets[2].ID(), Object: volume.ObjectID(id)})
			if err == nil {
				found[volume.ObjectID(id)] = data
			} else if !errors.Is(err, volume.ErrNotFound) {
				t.Fatal(err)
			}
		}
		h.Close()
		reopened.Close()
	}
	if !bytes.Equal(found[volume.ObjectID(object)], changed) || !bytes.Equal(found[volume.ObjectID(newLeaf)], []byte("leaf")) {
		t.Fatal("explicit fence did not persist recursive leaf writes")
	}
	// Parent mutations still preserve descendants, including prior absence.
	if _, err := clients[1].Put(ctx, newParent, []byte("parent"), accepted); err != nil {
		t.Fatal(err)
	}
	assert(2, newParent, nil)
	assert(0, newParent, nil)
	assert(3, newParent, nil)
	if _, err := clients[0].Put(ctx, object, []byte("source changed"), accepted); err != nil {
		t.Fatal(err)
	}
	assert(1, object, original)
	assert(2, object, changed)
	assert(3, object, original)
	// Ordinary catalog edits must retain the required schema and edge directions.
	d := buckets[1].Describe()
	d.Name = "directed-renamed"
	saved, err := s.buckets.Save(d, d.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if got := saved.Describe(); got.Schema != 3 || len(got.ClonePeers) != 2 {
		t.Fatalf("directed schema lost: %+v", got)
	}
	graph, err := s.cowGraph()
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.component(buckets[2].ID())) != 4 {
		t.Fatal("full lifetime graph lost ancestors/siblings")
	}
	// A historical symmetric edge can coexist with new directed descendants.
	// A must not discover a C-only ID by walking the legacy A<->B edge.
	a, legacyB := buckets[0].Describe(), saved.Describe()
	for i := range a.ClonePeers {
		if a.ClonePeers[i].Bucket == legacyB.ID {
			a.ClonePeers[i].Direction = ""
		}
	}
	for i := range legacyB.ClonePeers {
		if legacyB.ClonePeers[i].Bucket == a.ID {
			legacyB.ClonePeers[i].Direction = ""
		}
	}
	if err := s.buckets.saveBatch([]BucketDescription{a, legacyB}); err != nil {
		t.Fatal(err)
	}
	mixed := client.ObjectID{0x94}
	if _, err := clients[2].Put(ctx, mixed, []byte("mixed leaf"), accepted); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1, 3} {
		assert(i, mixed, nil)
	}
	assert(2, mixed, []byte("mixed leaf"))
	t.Logf("two-volume recursive leaf writes: implicit SyncOps=%d; explicit fence reopened both records", after-before)
}

func TestDirectedCOWLegacySchemaAndReciprocity(t *testing.T) {
	// Independent historical JSON: omitted direction is symmetric forever.
	fixture := `{"schema":1,"id":"01000000000000000000000000000000","name":"legacy","state":"active","revision":1,"policy":{"version":1,"storage_strategy":"hdd","compression":"raw","placement_mode":"count","redundancy":{"mode":"single","replicas":1,"ack":"all"},"limits":{"quota_accounting":"logical","absolute_unlimited":true}},"clone_peers":[{"bucket":"02000000000000000000000000000000","link":"03000000000000000000000000000000"}]}`
	var a BucketDescription
	if err := json.Unmarshal([]byte(fixture), &a); err != nil {
		t.Fatal(err)
	}
	if _, err := normalizeBucketDescription(a); err != nil {
		t.Fatal("historical schema rejected", err)
	}
	b := cloneBucketDescription(a)
	b.ID = a.ClonePeers[0].Bucket
	b.Name = "other"
	b.ClonePeers[0].Bucket = a.ID
	if _, err := cowGraphDescriptions([]BucketDescription{a, b}); err != nil {
		t.Fatal(err)
	}
	a.Schema, b.Schema = 3, 3
	a.ClonePeers[0].Direction = "child"
	b.ClonePeers[0].Direction = "parent"
	if _, err := cowGraphDescriptions([]BucketDescription{a, b}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"child", "", "unknown"} {
		broken := cloneBucketDescription(b)
		broken.ClonePeers[0].Direction = bad
		if _, err := cowGraphDescriptions([]BucketDescription{a, broken}); err == nil {
			t.Fatalf("accepted asymmetric direction %q", bad)
		}
	}
	a.Schema = 2
	if _, err := normalizeBucketDescription(a); err == nil {
		t.Fatal("directed edge accepted under historical schema")
	}
}

func TestDirectedCOWUnavailableNeighborMetadata(t *testing.T) {
	s := testServer(t)
	addReplicaDataVolume(t, s, "neighbor-metadata", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "neighbor-metadata")
	g := cowGraph{b.ID(): {volume.ID{0xfe}}}
	// A future graph was supplied, but its required namespace description is
	// unavailable. Resolving no local record must not turn that into absence.
	store := cowServerObjects{server: s, metadata: true, graphDescriptions: map[volume.ID]BucketDescription{}}
	snapshot, err := g.resolve(context.Background(), store, b.ID(), volume.ObjectID{1}, map[volume.ID]bool{})
	snapshot.close()
	if statusOf(err) != 503 {
		t.Fatalf("missing prospective metadata became absence: %v", err)
	}
	_, err = (cowServerObjects{server: s}).cloneNeighbors(volume.ID{0xfd}, false)
	if statusOf(err) != 503 {
		t.Fatalf("missing live metadata became no neighbors: %v", err)
	}
}

func TestDirectedCOWRejectsReadCyclesAndMultipleParents(t *testing.T) {
	a, b, c := volume.ID{1}, volume.ID{2}, volume.ID{3}
	cases := []map[volume.ID]map[volume.ID]string{
		{a: {b: "parent", c: "child"}, b: {c: "parent", a: "child"}, c: {a: "parent", b: "child"}},
		{a: {b: "", c: "child"}, b: {a: "", c: "parent"}, c: {a: "parent", b: "child"}},
		{a: {b: "parent", c: "parent"}, b: {a: "child"}, c: {a: "child"}},
	}
	for i, edges := range cases {
		if err := cowValidateDirectedGraph(edges); statusOf(err) != 503 {
			t.Fatalf("invalid inheritance case%d accepted: %v", i, err)
		}
	}
	if err := cowValidateDirectedGraph(map[volume.ID]map[volume.ID]string{a: {b: ""}, b: {a: "", c: "child"}, c: {b: "parent"}}); err != nil {
		t.Fatal("valid legacy component with directed child rejected", err)
	}
}
