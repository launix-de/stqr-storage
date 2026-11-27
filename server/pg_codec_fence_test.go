// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestPGCodecBatchesEachReplicaBeforeReady(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "codec-source", "hdd", 16<<20)
	dst := addReplicaDataVolume(t, target.node, "codec-target", "hdd", 16<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, src), whitelistReplicaMask(target.node, dst)}})
	b := replicaTestBucket(t, source.node, group, "codec-fence")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("batch-compression"), 1024)
	for i := byte(1); i <= 8; i++ {
		if _, err := b.Put(ctx, userPrincipal(t, source.node), volume.ObjectID{i}, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	description := b.Describe()
	oldVersion := description.Policy.Version
	description.Policy.Version++
	description.Policy.Compression = "gzip"
	body := client.Encoder{B: bucketPolicyBody(t, &Bucket{description: description})}
	body.U64(oldVersion)
	admin := source.node.cfg.Auth.BootstrapAdmin
	status, _ := bucketHTTPReply(t, source.node, client.Message{Kind: client.Request, Opcode: client.BucketSetPolicy, ID: client.NewRequestID(), Body: body.B}, admin.Username, admin.Password)
	if status != 200 && status != 202 {
		t.Fatal("codec intent", status)
	}
	plan, ok := source.node.scopes.planPGReplicaCodec(b.Describe())
	if !ok {
		t.Fatal("no replica codec-only plan")
	}
	beforeSrc, beforeDst := src.IOStats(), dst.IOStats()
	if err := source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for i, v := range []*volume.Volume{src, dst} {
		before := []volume.IOStats{beforeSrc, beforeDst}[i]
		if syncs := v.IOStats().SyncOps - before.SyncOps; syncs != 4 {
			t.Fatalf("replica%d conversion issued %d barriers instead of one checkpoint", i, syncs)
		}
		if !v.IsDurableThrough(v.Stats().Generation) {
			t.Fatal("codec READY did not persist replica", i)
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		for n := byte(1); n <= 8; n++ {
			got, info, err := h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{n}})
			if err != nil || !bytes.Equal(got, payload) || info.Compression != volume.Gzip {
				h.Close()
				t.Fatal("replica conversion", i, n, info, err)
			}
		}
		h.Close()
	}
}
