// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func TestPGReplicaCodecUsesLocalGeneration(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 8<<20)
	addTestVolume(t, target.node, "target", "hdd", 8<<20)
	auditSplitWait(t, func() bool {
		d, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		return len(d) >= 2
	})
	policy := storagePolicy(DefaultBucketPolicy())
	policy.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	group := volume.ID(client.NewRequestID())
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &policy})
	b := replicaTestBucket(t, source.node, group, "pg-codec")
	payload := bytes.Repeat([]byte("compressible media and PHP"), 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id := volume.ObjectID{1}
	if _, err := b.Put(ctx, userPrincipal(t, source.node), id, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	policy.Compression = "gzip"
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "all", Storage: &policy})
	p, ok := source.node.scopes.planPGReplicaCodec(b.Describe())
	if !ok {
		t.Fatal("PG codec work was left to global administration")
	}
	if err := source.node.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Server{source.node, target.node} {
		local, _ := s.Bucket(b.ID())
		data, info, err := local.Get(ctx, userPrincipal(t, source.node), id)
		if err != nil || !bytes.Equal(data, payload) || info.Compression != volume.Gzip {
			t.Fatal("PG codec changed contents or left an old replica", info, err)
		}
		if info := s.scopes.pgInfo(group.String()); info.Generation != "1" {
			t.Fatal(info)
		}
	}
	awaitPGVersion(t, source.node, group, 2)
	if _, err := b.PutRange(ctx, userPrincipal(t, source.node), id, 3, []byte("new"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("writes did not resume", err)
	}
}
