// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPlacementImproveUnboundDaemon(t *testing.T) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	hot := addReplicaDataVolume(t, s, "hot", "hdd", 16<<20)
	cold := addReplicaDataVolume(t, s, "cold", "hdd", 16<<20)
	group, foreign := volume.ID(client.NewRequestID()), volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	centralPGRequest(t, s, group, 0, PlacementMask{Storage: &storage, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{hot.Identity().VolumeID.String(), cold.Identity().VolumeID.String()}}}})
	centralPGRequest(t, s, foreign, 0, PlacementMask{Storage: &storage, Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{hot.Identity().VolumeID.String()}}}})
	b := replicaTestBucket(t, s, group, "unbound")
	fixed := replicaTestBucket(t, s, foreign, "other-pg")
	payload := bytes.Repeat([]byte{0x51}, 1<<20)
	put := func(store *volume.Volume, bucket volume.ID, object byte) {
		t.Helper()
		h, err := store.Open(true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.Put(volume.Key{Bucket: bucket, Object: volume.ObjectID{object}}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())})
		h.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	put(hot, b.ID(), 1)
	put(cold, b.ID(), 2)
	before, _ := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{})
	if before.Locations[0].Volume != (client.ID{}) {
		t.Fatal("fixture is not unpinned")
	}
	m := s.scopes.buildPlacementTargets(group.String(), s.bucketSnapshots())
	if p, ok := s.scopes.nextPlacementTarget(m); ok {
		t.Fatal("healthy unpinned map was consolidated", p)
	}
	fixedBefore, _ := s.placement.Load().lookup(client.ID(fixed.ID()), client.ObjectID{})
	for i := byte(0); i < 11; i++ {
		put(hot, fixed.ID(), i)
	}
	m = s.scopes.buildPlacementTargets(group.String(), s.bucketSnapshots())
	p, ok := s.scopes.nextPlacementTarget(m)
	if !ok || p.Volume != cold.Identity().VolumeID {
		t.Fatal("did not retain the cold cohort", p, m.improvement)
	}
	if p.Physical > 2<<20 {
		t.Fatal("move budget charged already-retained bytes", p.Physical)
	}
	if err := s.scopes.move(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	for _, object := range []byte{1, 2} {
		data, _, err := b.Get(context.Background(), userPrincipal(t, s), volume.ObjectID{object})
		if err != nil || !bytes.Equal(data, payload) {
			t.Fatal("unbound payload", object, err)
		}
	}
	fixedAfter, _ := s.placement.Load().lookup(client.ID(fixed.ID()), client.ObjectID{})
	if !reflect.DeepEqual(fixedBefore, fixedAfter) {
		t.Fatal("other PG changed")
	}
}
