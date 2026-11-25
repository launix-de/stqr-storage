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

func TestTicket32ReplicatedOverwriteResultReclamation(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "reclaimed-replica", "hdd", 16<<20)
	addTestVolume(t, target.node, "reclaimed-replica", "hdd", 16<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		dest, _ := source.node.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica statistics unavailable")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "reclaimed-replicas")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{32}
	for i := 0; i < 100; i++ {
		before, after := bytes.Repeat([]byte{byte(i)}, 8192), bytes.Repeat([]byte{byte(i + 1)}, 8192)
		if _, err := c.Put(ctx, id, before, client.PutOptions{}); err != nil {
			t.Fatalf("PUT %d: %v", i, err)
		}
		if _, err := c.CAS(ctx, id, 0, before, after, client.CASOptions{}); err != nil {
			t.Fatalf("CAS %d: %v", i, err)
		}
	}
	for _, s := range []*Server{source.node, target.node} {
		for _, v := range s.quotaVolumes() {
			h, err := v.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			err = h.ScanRequests(func(r volume.RequestRecord) error {
				if r.Key.Bucket == b.ID() {
					n++
				}
				return nil
			})
			if err == nil {
				err = h.ScanPendingReplicas(func(r volume.ReplicaRecord) error {
					if r.Request.Key.Bucket == b.ID() {
						n++
					}
					return nil
				})
			}
			records, re := h.ScanReplicaRecords(nil, 4096)
			for _, r := range records {
				if r.Request.Key.Bucket == b.ID() {
					n++
				}
			}
			h.Close()
			if err != nil || re != nil || n != 0 {
				t.Fatalf("replica retained %d operation records: %v %v", n, err, re)
			}
		}
	}
	request := client.NewRequestID()
	first, err := c.Append(ctx, id, []byte("tail"), client.AppendOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := c.Append(ctx, id, []byte("tail"), client.AppendOptions{RequestID: request})
	if err != nil || retry.StartOffset != first.StartOffset {
		t.Fatal("replicated APPEND retry", retry, err)
	}
	got, _, err := c.Get(ctx, id)
	if err != nil || len(got) != 8196 || !bytes.Equal(got[8192:], []byte("tail")) {
		t.Fatal("replicated append repeated", len(got), err)
	}
}
