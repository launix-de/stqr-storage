// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func awaitReplicaDestinations(t *testing.T, s *Server, count int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		destinations, _ := s.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(destinations) >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("replica destination statistics missing")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestDaemonReplicaLossRebuildsRequiredCopies(t *testing.T) {
	for _, lostRole := range []int{0, 1} {
		t.Run([]string{"primary", "follower"}[lostRole], func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			third := addReplicaThirdNode(t, source)
			fourth := addReplicaThirdNode(t, source)
			nodes := []*daemonTestNode{source, target, third, fourth}
			byID := map[client.ID]*daemonTestNode{}
			for _, node := range nodes {
				addTestVolume(t, node.node, "data", "hdd", 8<<20)
				byID[node.node.control.ledger.node] = node
			}
			awaitReplicaDestinations(t, source.node, 4)
			group := volume.ID(client.NewRequestID())
			storage := storagePolicy(DefaultBucketPolicy())
			storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 3, ACK: "all"}
			centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
			bucket := replicaTestBucket(t, source.node, group, "recover-three-copies")
			actor := userPrincipal(t, source.node)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			object := volume.ObjectID{4}
			options := volume.Options{RequestID: volume.ID(client.NewRequestID())}
			original := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return bucket.Append(ctx, actor, object, []byte("once"), options) })
			entry, _ := source.node.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID(object))
			if len(entry.Locations) != 3 {
				t.Fatal("initial copies", entry)
			}
			victim := byID[entry.Locations[lostRole].Node]
			epoch := source.node.currentSystem.Load().manifest.Membership.Epoch
			victimID := victim.node.control.ledger.node
			victim.close()
			var coordinator *Server
			for id, node := range byID {
				if id != victimID && (coordinator == nil || id.String() < coordinator.control.ledger.node.String()) {
					coordinator = node.node
				}
			}
			e := client.Encoder{}
			e.V(1)
			e.ID(victimID)
			e.Bool(true)
			e.Bool(true)
			e.U64(epoch)
			message := client.Message{Kind: client.Request, Opcode: client.ForceDisconnect, ID: client.NewRequestID(), Body: e.B}
			body, status, err := coordinator.forceDisconnect(message, actor)
			if status == 202 {
				coordinator.controller.mu.Lock()
				_, resumeErr := coordinator.controller.resume(ctx)
				coordinator.controller.mu.Unlock()
				t.Fatalf("force pending: %v", resumeErr)
			}
			if err != nil || status != 200 {
				t.Fatalf("force exclusion: status=%d body=%x error=%v", status, body, err)
			}
			entry, _ = coordinator.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID(object))
			if entry.State != 0 || len(entry.Locations) != 2 {
				t.Fatalf("surviving data discarded: %+v", entry)
			}
			primary := byID[entry.Locations[0].Node].node
			live, _ := primary.Bucket(bucket.ID())
			if _, err := live.Append(ctx, actor, object, []byte("too-early"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); statusOf(err) != 503 {
				t.Fatal("acknowledged write before required replicas were restored", err)
			}
			awaitReplicaDestinations(t, primary, 3)
			p, ok := primary.scopes.planReplicaAdd(live.Describe())
			if !ok {
				t.Fatal("missing replica was not scheduled")
			}
			if err := primary.scopes.move(ctx, p); err != nil {
				t.Fatal("copy replacement", err)
			}
			entry, _ = primary.placement.Load().lookup(client.ID(bucket.ID()), client.ObjectID(object))
			if len(entry.Locations) != 3 {
				t.Fatal("required replica count not restored", entry)
			}
			if replay, err := live.Append(ctx, actor, object, []byte("once"), options); err != nil || replay != original {
				t.Fatal("original append retry changed", replay, err)
			}
			for _, loc := range entry.Locations {
				copy, _ := byID[loc.Node].node.Bucket(bucket.ID())
				data, _, err := copy.Get(ctx, actor, object)
				if err != nil || string(data) != "once" {
					t.Fatal("replacement data", string(data), err)
				}
			}
		})
	}
}

func TestDaemonReplicaPrimariesRotateWithoutPayloadCopies(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	third := addReplicaThirdNode(t, source)
	nodes := []*daemonTestNode{source, target, third}
	for _, node := range nodes {
		addTestVolume(t, node.node, "data", "hdd", 8<<20)
	}
	awaitReplicaDestinations(t, source.node, 3)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 3, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	var buckets []*Bucket
	for _, name := range []string{"one", "two", "three"} {
		buckets = append(buckets, replicaTestBucket(t, source.node, group, name))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for step := 0; step < 9; step++ {
		moved := false
		for _, node := range nodes {
			p, ok := node.node.scopes.planPrimaryBalance()
			if !ok {
				continue
			}
			if !p.ReplicaPromote {
				t.Fatal("role balancing requested payload copy")
			}
			if err := node.node.scopes.move(ctx, p); err != nil {
				t.Fatal(err)
			}
			moved = true
		}
		if !moved {
			break
		}
	}
	counts := map[client.ID]int{}
	for _, b := range buckets {
		for _, node := range nodes {
			if err := node.node.scopes.syncArchives(ctx); err != nil {
				t.Fatal(err)
			}
		}
		entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{})
		counts[entry.Locations[0].Node]++
	}
	for _, node := range nodes {
		if counts[node.node.control.ledger.node] != 1 {
			t.Fatalf("primaries did not rotate: %+v", counts)
		}
		if _, ok := node.node.scopes.planPrimaryBalance(); ok {
			t.Fatal("role assignment does not converge")
		}
	}
}
