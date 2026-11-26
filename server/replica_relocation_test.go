// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func retryReplicaMutation(t *testing.T, ctx context.Context, f func() (volume.Result, error)) volume.Result {
	t.Helper()
	for {
		result, err := f()
		if err == nil {
			return result
		}
		if statusOf(err) != 503 || ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func awaitReplicaPlan(t *testing.T, s *Server, bucket volume.ID) rangePlan {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, err := s.Bucket(bucket)
		if err != nil {
			t.Fatal(err)
		}
		if p, ok := s.scopes.planReplicaRelocation(b.Describe()); ok {
			return p
		}
		if p, ok := s.scopes.planReplicaRemove(b.Describe()); ok {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("no relocation plan: %+v", b.Describe())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func addReplicaDataVolume(t *testing.T, s *Server, name, class string, size int64) *volume.Volume {
	t.Helper()
	op := addTestVolume(t, s, name, class, size)
	s.mu.RLock()
	managed := s.volumes[op.VolumeID]
	s.mu.RUnlock()
	if managed == nil {
		t.Fatal("new volume missing")
	}
	return managed.store
}

func whitelistReplicaMask(s *Server, v *volume.Volume) PlacementNode {
	return PlacementNode{NodeID: s.control.ledger.node.String(), Mode: VolumesWhitelist, Volumes: []string{v.Identity().VolumeID.String()}}
}

func TestDaemonReplicaVolumeRelocationPreservesDataAndReceipts(t *testing.T) {
	for _, tc := range []struct{ before, after, codec string }{{"hdd", "hdd", "raw"}, {"hdd", "hdd", "gzip"}, {"hdd", "ssd", "raw"}, {"ssd", "hdd", "gzip"}} {
		codec := tc.codec
		t.Run(tc.before+"-"+tc.after+"-"+codec, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			oldSource := addReplicaDataVolume(t, source.node, "old", tc.before, 8<<20)
			oldTarget := addReplicaDataVolume(t, target.node, "old", tc.before, 8<<20)
			newSource := addReplicaDataVolume(t, source.node, "new", tc.after, 8<<20)
			newTarget := addReplicaDataVolume(t, target.node, "new", tc.after, 8<<20)
			group := volume.ID(client.NewRequestID())
			storage := storagePolicy(DefaultBucketPolicy())
			storage.StorageStrategy = tc.before
			storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
			centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, oldSource), whitelistReplicaMask(target.node, oldTarget)}})
			idBucket := client.NewRequestID()
			e := client.Encoder{}
			e.U16(1)
			e.ID(idBucket)
			e.Text("relocated-volumes")
			e.ID(client.ID(group))
			e.U8(1)
			e.U64(0)
			e.Bool(false)
			e.U64(512 << 10)
			e.U32(540)
			_, status, reply := adminBucketMessage(t, source.node, client.BucketCreateInGroup, e.B)
			if status != 201 {
				t.Fatalf("physical replicated bucket: %d %v", status, reply)
			}
			b, err := source.node.Bucket(volume.ID(idBucket))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			actor := userPrincipal(t, source.node)
			id := volume.ObjectID{4}
			request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
			original := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return b.Append(ctx, actor, id, []byte("before"), request) })
			if _, err = b.Put(ctx, actor, volume.ObjectID{5}, []byte("deleted"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			if err = b.Delete(ctx, actor, volume.ObjectID{5}, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			storage.Compression = codec
			storage.StorageStrategy = tc.after
			centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, newSource), whitelistReplicaMask(target.node, newTarget)}})
			for i := 0; i < 2; i++ {
				p := awaitReplicaPlan(t, source.node, b.ID())
				if !p.ReplicaRelocate {
					t.Fatalf("expected volume relocation: %+v", p)
				}
				if err = source.node.scopes.move(ctx, p); err != nil {
					t.Fatal(err)
				}
				// The PG still has an old effective mask during the first move.
				// Newly pinned copies must nevertheless serve reads and writes.
				if _, _, err = b.Get(ctx, actor, id); err != nil {
					t.Fatal("read during mixed mask versions", err)
				}
				nextRequest := volume.Options{RequestID: volume.ID(client.NewRequestID())}
				retryReplicaMutation(t, ctx, func() (volume.Result, error) { return b.Append(ctx, actor, id, []byte("!"), nextRequest) })
			}
			awaitPGVersion(t, source.node, group, 2)
			for _, node := range []*Server{source.node, target.node} {
				copy, err := node.Bucket(b.ID())
				if err != nil {
					t.Fatal(err)
				}
				data, _, err := copy.Get(ctx, actor, id)
				if err != nil || string(data) != "before!!" {
					t.Fatalf("relocated copy %q %v", data, err)
				}
				if _, _, err = copy.Get(ctx, actor, volume.ObjectID{5}); err == nil {
					t.Fatal("deleted object resurrected")
				}
			}
			retry := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return b.Append(ctx, actor, id, []byte("before"), request) })
			if retry != original {
				t.Fatalf("original receipt changed: %+v %v", retry, err)
			}
			for _, store := range []*volume.Volume{oldSource, oldTarget} {
				h, err := store.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				_, err = h.RecordInfo(volume.Key{Bucket: b.ID(), Object: id})
				h.Close()
				if err == nil {
					t.Fatal("retired volume still owns old object")
				}
			}
			source = restartDaemonTestNode(t, source)
			target = restartDaemonTestNode(t, target)
			for _, node := range []*Server{source.node, target.node} {
				copy, err := node.Bucket(b.ID())
				if err != nil {
					t.Fatal(err)
				}
				data, _, err := copy.Get(ctx, actor, id)
				if err != nil || string(data) != "before!!" {
					t.Fatalf("restart %q %v", data, err)
				}
			}
		})
	}
}

func addReplicaThirdNode(t *testing.T, source *daemonTestNode) *daemonTestNode {
	t.Helper()
	expectedMembers := len(source.node.currentSystem.Load().manifest.Membership.Members) + 1
	source.node.mu.RLock()
	cfg := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	seed := PeerConfig{NodeID: cfg.Node.ID, URL: cfg.Node.Advertise}
	cfg.Node.ID, cfg.Node.Name, cfg.Node.DataDir = newID(), "third", "."
	cfg.Volumes = nil
	cfg.Peers = []PeerConfig{seed}
	cfg.ControlVersion, cfg.RangeVersion, cfg.QuotaVersion, cfg.PeerDirectoryVersion, cfg.BucketCatalogVersion, cfg.PlacementGroupsVersion = 0, 0, 0, 0, 0, 0
	cfg.JoinPending = true
	cfg.Auth.BootstrapAdmin = BootstrapAdmin{}
	third := startDaemonTestNode(t, filepath.Join(t.TempDir(), "third.yaml"), &cfg, false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, b := source.node.currentSystem.Load(), third.node.currentSystem.Load()
		if a != nil && b != nil && a.root == b.root && len(a.manifest.Membership.Members) == expectedMembers && !third.node.joinPending() {
			return third
		}
		if time.Now().After(deadline) {
			t.Fatal("third member did not join")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestDaemonReplicaNodeReplacementPromotesAndPreservesQuota(t *testing.T) {
	testReplicaNodeReplacementQuota(t, false)
}

func TestDaemonQuotaBorrowsFromRetiredReplicaOwner(t *testing.T) {
	testReplicaNodeReplacementQuota(t, true)
}

func testReplicaNodeReplacementQuota(t *testing.T, retireCredits bool) {
	t.Helper()
	source, target := startAutomaticDaemonPair(t, false)
	third := addReplicaThirdNode(t, source)
	old := addReplicaDataVolume(t, source.node, "data", "hdd", 8<<20)
	retained := addReplicaDataVolume(t, target.node, "data", "hdd", 8<<20)
	added := addReplicaDataVolume(t, third.node, "data", "hdd", 8<<20)
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, old), whitelistReplicaMask(target.node, retained)}})
	b := centralBucket(t, source.node, group, "replaced-primary", 64<<10)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	actor := userPrincipal(t, source.node)
	id := volume.ObjectID{1}
	request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	original := retryReplicaMutation(t, ctx, func() (volume.Result, error) { return b.Append(ctx, actor, id, []byte("confirmed"), request) })
	var err error
	source.node.mu.RLock()
	config := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	conn, err := client.Dial(ctx, config.Node.Advertise, config.Auth.BootstrapAdmin.Username, config.Auth.BootstrapAdmin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, _, err := conn.Get(ctx, client.ObjectID(id)); err != nil {
		t.Fatal(err)
	}
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(target.node, retained), whitelistReplicaMask(third.node, added)}})
	p := awaitReplicaPlan(t, source.node, b.ID())
	if !p.ReplicaPromote {
		t.Fatalf("expected primary role handoff: %+v", p)
	}
	if err = source.node.scopes.move(ctx, p); err != nil {
		t.Fatal("promote", err)
	}
	primary, _ := target.node.Bucket(b.ID())
	allowance, err := target.node.quotaAllowance(client.ID(b.ID()))
	if err != nil || allowance < uint64(len("confirmed")) {
		t.Fatalf("primary credits were not transferred: %d %v", allowance, err)
	}
	if _, err = conn.PutRange(ctx, client.ObjectID(id), 0, []byte("C"), client.PutOptions{}); err != nil {
		t.Fatal("new primary has no quota/write authority", err)
	}
	p = awaitReplicaPlan(t, target.node, b.ID())
	if !p.ReplicaAdd {
		t.Fatalf("old copy removed before building replacement: %+v", p)
	}
	if err = target.node.scopes.move(ctx, p); err != nil {
		t.Fatal("add replacement", err)
	}
	// A missing old member is not an administrative permission to retire it.
	source.close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if online, _ := target.node.scopes.replicaLocationState(p.Before.Entries[0].Locations[1]); !online {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("offline observation did not arrive")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, ok := target.node.scopes.planReplicaRemove(primary.Describe()); ok {
		t.Fatal("offline old copy was removed")
	}
	source = restartDaemonTestNode(t, source)
	p = awaitReplicaPlan(t, target.node, b.ID())
	if !p.ReplicaRemove || p.Target != source.node.control.ledger.node {
		t.Fatalf("wrong retired copy: %+v", p)
	}
	if err = target.node.scopes.move(ctx, p); err != nil {
		t.Fatal("retire old primary", err)
	}
	awaitPGVersion(t, target.node, group, 2)
	if retireCredits {
		// Reproduce the valid post-handoff balances independently of worker
		// timing: only live bytes moved with the primary, while all unused
		// current-epoch credits remain on a live node outside the new mask.
		for _, donor := range []*Server{target.node, third.node} {
			donor.opMu.Lock()
			allowance, allowanceErr := donor.quotaAllowance(client.ID(b.ID()))
			copy, bucketErr := donor.Bucket(b.ID())
			if allowanceErr != nil || bucketErr != nil {
				donor.opMu.Unlock()
				t.Fatal("donor state", allowanceErr, bucketErr)
			}
			used, usedErr := copy.accountedUsage()
			if usedErr != nil || used > allowance {
				donor.opMu.Unlock()
				t.Fatal("donor usage", used, allowance, usedErr)
			}
			free := allowance - used
			if free == 0 {
				donor.opMu.Unlock()
				continue
			}
			allocation := findQuota(donor.currentSystem.Load(), client.ID(b.ID()))
			grant, grantErr := donor.quotas.ledger.debit(quotaGrant{ID: client.NewRequestID(), Bucket: client.ID(b.ID()), Epoch: allocation.Epoch, Source: donor.control.ledger.node, Target: source.node.control.ledger.node, Bytes: free}, free)
			donor.opMu.Unlock()
			if grantErr != nil {
				t.Fatal(grantErr)
			}
			wire, err := encodeQuotaGrant(grant)
			if err != nil {
				t.Fatal(err)
			}
			ack, err := source.node.quotaPeerMessage(client.Message{Kind: client.Request, Opcode: peerQuotaGrant, Body: wire}, grant.Source)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := client.DecodeProof(ack)
			if err != nil {
				t.Fatal(err)
			}
			if err := donor.quotas.ledger.settle(grant.ID, proof, grant.Target); err != nil {
				t.Fatal(err)
			}
		}
		for i, node := range []*Server{source.node, target.node, third.node} {
			node.opMu.Lock()
			allowance, err := node.quotaAllowance(client.ID(b.ID()))
			node.opMu.Unlock()
			want := []uint64{(64 << 10) - 9, 9, 0}[i]
			if err != nil || allowance != want {
				t.Fatalf("post-handoff node %d credits: got %d (%v), want %d", i, allowance, err, want)
			}
		}
		members, _, err := target.node.pgMembers(primary.Describe())
		if err != nil || hasControlNode(members, source.node.control.ledger.node) {
			t.Fatal("retired quota holder is still a write-placement member", members, err)
		}
		retired, _ := source.node.Bucket(b.ID())
		_, err = retired.Append(ctx, actor, id, []byte("forbidden"), volume.Options{RequestID: volume.ID(client.NewRequestID())})
		if _, redirected := err.(*objectRedirect); !redirected {
			t.Fatal("unused credits allowed the retired node to write", err)
		}
	}
	retry, err := primary.Append(ctx, actor, id, []byte("confirmed"), request)
	if err != nil || retry != original {
		t.Fatalf("handoff retry %+v %v", retry, err)
	}
	growth := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	retryReplicaMutation(t, ctx, func() (volume.Result, error) {
		result, err := primary.Append(ctx, actor, id, []byte("!?"), growth)
		if err != nil && statusOf(err) != 503 {
			// Both quota admission and the volume allocator can return ErrFull.
			// Preserve their state on failure without changing the retry contract.
			t.Logf("replacement growth append failed: %v", err)
			for _, node := range []*Server{source.node, target.node, third.node} {
				node.opMu.Lock()
				allowance, allowanceErr := node.quotaAllowance(client.ID(b.ID()))
				copy, bucketErr := node.Bucket(b.ID())
				var used, pending uint64
				var usedErr, pendingErr error
				if bucketErr == nil {
					used, usedErr = copy.accountedUsage()
					pending, pendingErr = node.pendingReplicaGrowth(b.ID())
				}
				node.opMu.Unlock()
				t.Logf("growth failure node=%s allowance=%d (%v) used=%d (%v) pending=%d (%v) bucket=%v", node.control.ledger.node, allowance, allowanceErr, used, usedErr, pending, pendingErr, bucketErr)
				node.quotas.mu.Lock()
				demands := append([]quotaDemand(nil), node.quotas.demands...)
				node.quotas.mu.Unlock()
				t.Logf("quota allocation: %+v; demands: %+v", findQuota(node.currentSystem.Load(), client.ID(b.ID())), demands)
			}
			for _, store := range []*volume.Volume{retained, added} {
				t.Logf("replica storage: %+v", store.Stats())
			}
		}
		return result, err
	})
	for _, node := range []*Server{target.node, third.node} {
		copy, _ := node.Bucket(b.ID())
		data, _, err := copy.Get(ctx, actor, id)
		if err != nil || string(data) != "Confirmed!?" {
			t.Fatalf("replacement %q %v", data, err)
		}
	}
	usage, err := primary.clusterUsageAs(ctx, actor, "logical")
	if err != nil || usage != uint64(len("Confirmed!?")) {
		t.Fatalf("logical usage counted replicas: %d %v", usage, err)
	}
	if retireCredits {
		var total uint64
		for _, node := range []*Server{source.node, target.node, third.node} {
			node.opMu.Lock()
			allowance, err := node.quotaAllowance(client.ID(b.ID()))
			node.opMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			total += allowance
		}
		if total != 64<<10 {
			t.Fatal("borrowing changed the global quota", total)
		}
	}
}

// Stop at the real durable READY/decision boundary, without mocking storage,
// peer RPCs or journal replay. The caller restarts a participant before finish.
func stageReplicaHandoff(t *testing.T, ctx context.Context, s *Server, p rangePlan, decide bool) {
	t.Helper()
	r := s.scopes
	if err := r.reserve(p); err != nil {
		t.Fatal(err)
	}
	if err := r.sendHistory(ctx, p.Target, p.id()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, p.Target, peerRangeReserve, mustRangeWire(p)); err != nil {
		t.Fatal(err)
	}
	if !p.ReplicaPromote {
		if _, _, err := r.sendSnapshot(ctx, p, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.fence(p.id()); err != nil {
		t.Fatal(err)
	}
	var digest [32]byte
	var target client.Proof
	var used uint64
	if p.ReplicaPromote {
		e := rangeMessage(peerReplicaTopologyReady, p.id())
		body, err := r.call(ctx, p.Target, peerReplicaTopologyReady, e.B)
		if err != nil {
			t.Fatal(err)
		}
		d := client.Decoder{B: body}
		target, err = client.DecodeProof(d.Take(56))
		used = d.U64()
		if err != nil || d.Done() != nil {
			t.Fatal("invalid topology READY", err)
		}
		digest = replicaTopologyDigest(p, used)
	} else {
		var err error
		digest, target, err = r.sendSnapshot(ctx, p, true)
		if err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		used = r.txns[p.id()].pendingUsed
		r.mu.Unlock()
	}
	source, err := r.ready(p.id(), digest, true, used, 0)
	if err != nil {
		t.Fatal(err)
	}
	if decide {
		if _, err := r.commit(rangeCommit{ID: p.id(), Digest: digest, LogicalUsed: used, Source: source, Target: target}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDaemonReplicaHandoffRestartBoundaries(t *testing.T) {
	for _, promote := range []bool{false, true} {
		for _, decide := range []bool{false, true} {
			for _, restartSource := range []bool{false, true} {
				name := "volume"
				if promote {
					name = "primary"
				}
				if decide {
					name += "/decided"
				} else {
					name += "/ready"
				}
				if restartSource {
					name += "/source"
				} else {
					name += "/target"
				}
				t.Run(name, func(t *testing.T) {
					source, target := startAutomaticDaemonPair(t, false)
					oldSource := addReplicaDataVolume(t, source.node, "old", "hdd", 8<<20)
					oldTarget := addReplicaDataVolume(t, target.node, "old", "hdd", 8<<20)
					newTarget := addReplicaDataVolume(t, target.node, "new", "hdd", 8<<20)
					group := volume.ID(client.NewRequestID())
					storage := storagePolicy(DefaultBucketPolicy())
					storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
					centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, oldSource), whitelistReplicaMask(target.node, oldTarget)}})
					b := replicaTestBucket(t, source.node, group, "restart-handoff")
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					actor := userPrincipal(t, source.node)
					id := volume.ObjectID{3}
					request := volume.Options{RequestID: volume.ID(client.NewRequestID())}
					original, err := b.Append(ctx, actor, id, []byte("durable"), request)
					if err != nil {
						t.Fatal(err)
					}
					var p rangePlan
					if promote {
						before, _ := source.node.routingPatch(b.ID(), id)
						entry := before.Entries[0]
						next := entry
						next.OwnerEpoch++
						next.Locations = append([]client.Location(nil), entry.Locations...)
						next.Locations[0], next.Locations[1] = next.Locations[1], next.Locations[0]
						next.Locations[0].Role, next.Locations[1].Role = 0, 1
						after := before
						after.Entries = []client.RangeEntry{next}
						p = rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Moving: entry, Source: source.node.control.ledger.node, Target: target.node.control.ledger.node, Volume: volume.ID(entry.Locations[1].Volume), ReplicaPromote: true}
					} else {
						centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, oldSource), whitelistReplicaMask(target.node, newTarget)}})
						p = awaitReplicaPlan(t, source.node, b.ID())
					}
					stageReplicaHandoff(t, ctx, source.node, p, decide)
					if restartSource {
						source = restartDaemonTestNode(t, source)
					} else {
						target = restartDaemonTestNode(t, target)
					}
					for {
						err := source.node.scopes.move(ctx, p)
						if err == nil {
							break
						}
						if statusOf(err) != 503 || ctx.Err() != nil {
							t.Fatal("resume", err)
						}
						time.Sleep(25 * time.Millisecond)
					}
					primary := source.node
					if promote {
						primary = target.node
					}
					bucket, _ := primary.Bucket(b.ID())
					// The resumed handoff can finish before its PG policy fence is
					// released. Retry the same logical append across that 503;
					// its original reply and one-copy payload must survive.
					retry := retryReplicaMutation(t, ctx, func() (volume.Result, error) {
						return bucket.Append(ctx, actor, id, []byte("durable"), request)
					})
					if retry != original {
						t.Fatalf("lost original reply: %+v != %+v", retry, original)
					}
					nextRequest := volume.Options{RequestID: volume.ID(client.NewRequestID())}
					retryReplicaMutation(t, ctx, func() (volume.Result, error) { return bucket.Append(ctx, actor, id, []byte("!"), nextRequest) })
					for _, node := range []*Server{source.node, target.node} {
						copy, _ := node.Bucket(b.ID())
						data, _, err := copy.Get(ctx, actor, id)
						if err != nil || string(data) != "durable!" {
							t.Fatalf("restart data %q %v", data, err)
						}
					}
				})
			}
		}
	}
}

func TestDaemonReplicaAutomaticVolumeDrain(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "follower"
		if primary {
			name = "primary"
		}
		t.Run(name, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			third := addReplicaThirdNode(t, source)
			first := addReplicaDataVolume(t, source.node, "data", "hdd", 8<<20)
			second := addReplicaDataVolume(t, target.node, "data", "hdd", 8<<20)
			addReplicaDataVolume(t, third.node, "replacement", "hdd", 8<<20)
			group := volume.ID(client.NewRequestID())
			storage := storagePolicy(DefaultBucketPolicy())
			storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
			// The third node has capacity but is not needed by the initial pinned set.
			centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
			b := replicaTestBucket(t, source.node, group, "automatic-replica-drain")
			entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{})
			withdrawn, retiring := second, target
			if primary {
				withdrawn, retiring = first, source
			}
			// Pick the actual follower if initial candidate ordering selected node 3.
			if !primary {
				loc := entry.Locations[1]
				for _, node := range []*daemonTestNode{source, target, third} {
					if node.node.control.ledger.node == loc.Node {
						retiring = node
						node.node.mu.RLock()
						withdrawn = node.node.volumes[loc.Volume.String()].store
						node.node.mu.RUnlock()
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			source.node.mu.RLock()
			cfg := copyConfig(source.node.cfg)
			source.node.mu.RUnlock()
			conn, err := client.Dial(ctx, cfg.Node.Advertise, cfg.Auth.BootstrapAdmin.Username, cfg.Auth.BootstrapAdmin.Password, client.BucketID(b.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			request := client.AppendOptions{RequestID: client.NewRequestID()}
			original, err := conn.Append(ctx, client.ObjectID{1}, []byte("base"), request)
			if err != nil {
				t.Fatal(err)
			}
			vid := withdrawn.Identity().VolumeID.String()
			if _, err := retiring.node.RemoveVolume(newID(), vid); err != nil {
				t.Fatal(err)
			}
			for _, node := range []*daemonTestNode{source, target, third} {
				node.node.scopes.start()
			}
			for i := 0; i < 10; i++ {
				if _, err := conn.Append(ctx, client.ObjectID{1}, []byte("!"), client.AppendOptions{}); err != nil {
					t.Fatal("append during automatic drain", err)
				}
			}
			for {
				retiring.node.mu.RLock()
				_, attached := retiring.node.volumes[vid]
				retiring.node.mu.RUnlock()
				if !attached {
					break
				}
				if ctx.Err() != nil {
					logDaemonRangeState(t, source, target)
					logDaemonRangeState(t, target, third)
					t.Fatal("automatic replica drain did not finish", ctx.Err())
				}
				time.Sleep(25 * time.Millisecond)
			}
			retry, err := conn.Append(ctx, client.ObjectID{1}, []byte("base"), request)
			if err != nil || retry != original {
				t.Fatalf("drain changed original response: %+v %v", retry, err)
			}
			data, _, err := conn.Get(ctx, client.ObjectID{1})
			if err != nil || string(data) != "base!!!!!!!!!!" {
				t.Fatalf("drain data %q %v", data, err)
			}
			entry, _ = source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{1})
			if len(entry.Locations) != 2 {
				t.Fatalf("temporary third copy was not retired: %+v", entry)
			}
			for _, loc := range entry.Locations {
				if loc.Volume.String() == vid {
					t.Fatal("drained volume still in placement")
				}
			}
		})
	}
}

func TestReplicaTopologyCapabilityAndPlanValidation(t *testing.T) {
	view := &systemView{manifest: systemManifest{ReplicationProtocol: 2}}
	if supportsReplicaPolicies(view, client.CapabilityReplication) {
		t.Fatal("old peer admitted to topology history")
	}
	if !supportsReplicaPolicies(view, client.CapabilityReplication|client.CapabilityReplicaTopology) {
		t.Fatal("new peer rejected")
	}
	view.manifest.ReplicationProtocol = 1
	if !supportsReplicaPolicies(view, client.CapabilityReplication) {
		t.Fatal("legacy replication history unreadable")
	}
	before := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: client.ID{3}, ToEnd: true,
		Addresses: []client.NodeAddress{{Node: client.ID{1}, Addresses: []string{"http://127.0.0.1:7419"}}, {Node: client.ID{2}, Addresses: []string{"http://127.0.0.1:7420"}}},
		Entries:   []client.RangeEntry{{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{4}}, {Node: client.ID{2}, Volume: client.ID{5}, Role: 1}}}}}
	for _, promote := range []bool{false, true} {
		next := before.Entries[0]
		next.OwnerEpoch++
		next.Locations = append([]client.Location(nil), next.Locations...)
		p := rangePlan{Attempt: client.NewRequestID(), Before: before, After: before, Moving: before.Entries[0], Source: client.ID{1}, Target: client.ID{2}, Volume: volume.ID{6}, ReplicaRelocate: true}
		if promote {
			next.Locations[0], next.Locations[1] = next.Locations[1], next.Locations[0]
			next.Locations[0].Role, next.Locations[1].Role = 0, 1
			p.ReplicaRelocate, p.ReplicaPromote, p.Volume = false, true, volume.ID{5}
		} else {
			next.Locations[1].Volume = client.ID{6}
		}
		p.After.Entries = []client.RangeEntry{next}
		wire, err := p.marshal()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeRangePlan(wire)
		if err != nil || decoded.id() != p.id() {
			t.Fatal("unstable topology wire identity", err)
		}
		for _, mutate := range []func(*rangePlan){func(p *rangePlan) { p.ReplicaAdd = true }, func(p *rangePlan) { p.Volume = volume.ID{9} }, func(p *rangePlan) { p.Source = client.ID{2} }, func(p *rangePlan) { p.Attempt = client.ID{} }} {
			bad := p
			mutate(&bad)
			if bad.validate() == nil {
				t.Fatal("invalid topology plan accepted", promote)
			}
		}
	}
}

func TestDaemonReplicaDisjointNodeMaskReplacement(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	third := addReplicaThirdNode(t, source)
	fourth := addReplicaThirdNode(t, source)
	nodes := []*daemonTestNode{source, target, third, fourth}
	stores := make([]*volume.Volume, len(nodes))
	for i, node := range nodes {
		stores[i] = addReplicaDataVolume(t, node.node, "data", "hdd", 8<<20)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, stores[0]), whitelistReplicaMask(target.node, stores[1])}})
	b := replicaTestBucket(t, source.node, group, "entire-replica-set")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	source.node.mu.RLock()
	cfg := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	conn, err := client.Dial(ctx, cfg.Node.Advertise, cfg.Auth.BootstrapAdmin.Username, cfg.Auth.BootstrapAdmin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request := client.AppendOptions{RequestID: client.NewRequestID()}
	original, err := conn.Append(ctx, client.ObjectID{1}, []byte("base"), request)
	if err != nil {
		t.Fatal(err)
	}
	storage.Compression = "gzip"
	centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(third.node, stores[2]), whitelistReplicaMask(fourth.node, stores[3])}})
	count := 0
	for {
		entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{1})
		if len(entry.Locations) == 2 && (entry.Locations[0].Node == third.node.control.ledger.node || entry.Locations[0].Node == fourth.node.control.ledger.node) && (entry.Locations[1].Node == third.node.control.ledger.node || entry.Locations[1].Node == fourth.node.control.ledger.node) {
			break
		}
		if count > 8 {
			t.Fatalf("replacement did not converge: %+v", entry)
		}
		var primary *Server
		for _, node := range nodes {
			if node.node.control.ledger.node == entry.Locations[0].Node {
				primary = node.node
			}
		}
		if primary == nil {
			t.Fatal("primary missing")
		}
		p := awaitReplicaPlan(t, primary, b.ID())
		if err := primary.scopes.move(ctx, p); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Append(ctx, client.ObjectID{1}, []byte("!"), client.AppendOptions{}); err != nil {
			t.Fatal("write during disjoint mask transition", err)
		}
		count++
	}
	// Explicit-plan fixtures must run codec work on the new primary,
	// which can be either replacement node depending on capacity/ID ordering.
	entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID{1})
	for _, node := range nodes {
		if node.node.control.ledger.node == entry.Locations[0].Node {
			awaitPGVersion(t, node.node, group, 2)
			break
		}
	}
	awaitPGVersion(t, third.node, group, 2)
	retry, err := conn.Append(ctx, client.ObjectID{1}, []byte("base"), request)
	if err != nil || retry != original {
		t.Fatalf("original response not retained: %+v %v", retry, err)
	}
	want := "base"
	for i := 0; i < count; i++ {
		want += "!"
	}
	for _, node := range []*Server{third.node, fourth.node} {
		bucket, _ := node.Bucket(b.ID())
		data, info, err := bucket.Get(ctx, userPrincipal(t, source.node), volume.ObjectID{1})
		if err != nil || string(data) != want || info.Compression != volume.Gzip {
			t.Fatalf("new set %q %+v %v", data, info, err)
		}
	}
	for i := 0; i < 2; i++ {
		h, err := stores[i].Open(false)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.RecordInfo(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
		h.Close()
		if err == nil {
			t.Fatal("retired set still owns payload")
		}
	}
}

func TestDaemonReplicaEverywhereNodeDrain(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	third := addReplicaThirdNode(t, source)
	for _, node := range []*daemonTestNode{source, target, third} {
		addReplicaDataVolume(t, node.node, "data", "hdd", 8<<20)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.PlacementMode = "everywhere"
	storage.Redundancy = BucketRedundancy{Mode: "replication", ACK: "all"}
	centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, source.node, group, "everywhere-drain")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	source.node.mu.RLock()
	cfg := copyConfig(source.node.cfg)
	source.node.mu.RUnlock()
	conn, err := client.Dial(ctx, cfg.Node.Advertise, cfg.Auth.BootstrapAdmin.Username, cfg.Auth.BootstrapAdmin.Password, client.BucketID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Append(ctx, client.ObjectID{1}, []byte("before"), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	actor := userPrincipal(t, source.node)
	if err := source.node.beginNodeDrain(actor); err != nil {
		t.Fatal(err)
	}
	promote := awaitReplicaPlan(t, source.node, b.ID())
	if !promote.ReplicaPromote {
		t.Fatal("EVERYWHERE drain did not hand off primary")
	}
	if err := source.node.scopes.move(ctx, promote); err != nil {
		t.Fatal(err)
	}
	var nextPrimary *Server
	for _, node := range []*daemonTestNode{target, third} {
		if node.node.control.ledger.node == promote.Target {
			nextPrimary = node.node
		}
	}
	remove := awaitReplicaPlan(t, nextPrimary, b.ID())
	if !remove.ReplicaRemove {
		t.Fatal("EVERYWHERE drain did not retire leaving node")
	}
	if err := nextPrimary.scopes.move(ctx, remove); err != nil {
		t.Fatal(err)
	}
	if len(nextPrimary.currentSystem.Load().manifest.Membership.Members) != 3 {
		t.Fatal("fixture left membership before verifying the ACK barrier")
	}
	blocked, _ := nextPrimary.Bucket(b.ID())
	if _, err := blocked.Append(ctx, actor, volume.ObjectID{1}, []byte("must wait"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); statusOf(err) != 503 {
		t.Fatal("EVERYWHERE write acknowledged before membership shrink", err)
	}
	for _, node := range []*daemonTestNode{source, target, third} {
		node.node.scopes.start()
	}
	if _, err := conn.Append(ctx, client.ObjectID{1}, []byte("!"), client.AppendOptions{}); err != nil {
		t.Fatal("append across node leave", err)
	}
	for {
		view := target.node.currentSystem.Load()
		if view != nil && len(view.manifest.Membership.Members) == 2 && !hasControlNode(membershipIDs(view.manifest.Membership), source.node.control.ledger.node) {
			break
		}
		if ctx.Err() != nil {
			logDaemonRangeState(t, source, target)
			t.Fatal("EVERYWHERE drain did not complete", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	// The SDK must follow the new authority even when its original contact left.
	if _, err := conn.Append(ctx, client.ObjectID{1}, []byte("?"), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*Server{target.node, third.node} {
		copy, _ := node.Bucket(b.ID())
		data, _, err := copy.Get(ctx, actor, volume.ObjectID{1})
		if err != nil || string(data) != "before!?" {
			t.Fatalf("surviving EVERYWHERE copy %q %v", data, err)
		}
	}
}

func TestDaemonReplicaHandoffRetainsAbortedOutcomes(t *testing.T) {
	for _, promote := range []bool{false, true} {
		name := "volume"
		if promote {
			name = "primary"
		}
		t.Run(name, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			oldSource := addReplicaDataVolume(t, source.node, "old", "hdd", 8<<20)
			oldTarget := addReplicaDataVolume(t, target.node, "old", "hdd", 8<<20)
			newSource := addReplicaDataVolume(t, source.node, "new", "hdd", 8<<20)
			group := volume.ID(client.NewRequestID())
			storage := storagePolicy(DefaultBucketPolicy())
			storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
			centralPGRequest(t, source.node, group, 0, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, oldSource), whitelistReplicaMask(target.node, oldTarget)}})
			b := replicaTestBucket(t, source.node, group, "failed-outcome")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			actor := userPrincipal(t, source.node)
			id := volume.ObjectID{1}
			if _, err := b.Put(ctx, actor, id, []byte("base"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			entry, _ := source.node.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(id))
			request := volume.Options{RequestID: volume.ID(client.NewRequestID()), Compression: volume.Codec(volume.Raw)}
			binding := volume.ReplicaBinding{Epoch: entry.OwnerEpoch, Sequence: 100, SetHash: replicaSetHash(b.ID(), entry)}
			// Represent a prepare the follower rejected before recording it. The
			// authoritative old primary retains the definitive 507 outcome.
			h, err := oldSource.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.PrepareMutation(3, volume.Key{Bucket: b.ID(), Object: id}, 0, 4, bytes.NewReader([]byte("fail")), nil, request, binding)
			if err == nil {
				err = h.AbortReplicaPending(request.RequestID, binding, 507)
			}
			if err == nil {
				err = h.FinishReplicaAbort(request.RequestID, binding)
			}
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			var p rangePlan
			if promote {
				before, _ := source.node.routingPatch(b.ID(), id)
				next := before.Entries[0]
				next.OwnerEpoch++
				next.Locations = append([]client.Location(nil), next.Locations...)
				next.Locations[0], next.Locations[1] = next.Locations[1], next.Locations[0]
				next.Locations[0].Role, next.Locations[1].Role = 0, 1
				after := before
				after.Entries = []client.RangeEntry{next}
				cost, err := source.node.scopes.replicaAbortCost(before, entry)
				if err != nil {
					t.Fatal(err)
				}
				p = rangePlan{Attempt: client.NewRequestID(), Before: before, After: after, Moving: entry, Source: source.node.control.ledger.node, Target: target.node.control.ledger.node, Volume: volume.ID(entry.Locations[1].Volume), Physical: cost, ReplicaPromote: true}
			} else {
				centralPGRequest(t, source.node, group, 1, PlacementMask{NodeSelection: "list", Storage: &storage, Nodes: []PlacementNode{whitelistReplicaMask(source.node, newSource), whitelistReplicaMask(target.node, oldTarget)}})
				p = awaitReplicaPlan(t, source.node, b.ID())
			}
			if err := source.node.scopes.move(ctx, p); err != nil {
				t.Fatal(err)
			}
			primary := source
			if promote {
				primary = target
			}
			primary = restartDaemonTestNode(t, primary)
			copy, _ := primary.node.Bucket(b.ID())
			deadline := time.Now().Add(10 * time.Second)
			for {
				_, err := copy.Append(ctx, actor, id, []byte("fail"), request)
				if statusOf(err) == 507 {
					break
				}
				if statusOf(err) != 503 || time.Now().After(deadline) {
					t.Fatal("terminal failed request was replayed after handoff", err)
				}
				time.Sleep(25 * time.Millisecond)
			}
			data, _, err := copy.Get(ctx, actor, id)
			if err != nil || string(data) != "base" {
				t.Fatalf("aborted payload became visible: %q %v", data, err)
			}
		})
	}
}
