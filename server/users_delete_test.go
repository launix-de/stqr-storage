// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func userDeleteRequest(user client.ID, option *bool) client.Message {
	e := client.Encoder{}
	e.ID(user)
	if option != nil {
		e.Bool(*option)
	}
	return userMessage(client.UserDelete, e)
}

func grantUserDeleteBucket(t *testing.T, s *Server, bucket *Bucket, user client.ID, permissions BucketPermissions) {
	t.Helper()
	e := client.Encoder{}
	e.ID(client.ID(bucket.ID()))
	e.ID(user)
	e.U32(uint32(permissions))
	if body, status := s.dispatchAs(userMessage(client.BucketGrant, e), nil, userPrincipal(t, s)); status != 200 {
		t.Fatalf("grant: %d %x", status, body)
	}
}

func TestUserDeleteExclusiveBuckets(t *testing.T) {
	for _, mode := range []string{"delete", "keep", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			s := testServer(t)
			create, user := createUserMessage("departing", "secret")
			if _, status := s.dispatchAs(create, nil, userPrincipal(t, s)); status != 201 {
				t.Fatal(status)
			}
			create, other := createUserMessage("remaining", "secret")
			if _, status := s.dispatchAs(create, nil, userPrincipal(t, s)); status != 201 {
				t.Fatal(status)
			}
			update := client.Encoder{}
			update.ID(other)
			update.Text("remaining")
			update.U8(0)
			update.Bool(true)
			if _, status := s.dispatchAs(userMessage(client.UserUpdate, update), nil, userPrincipal(t, s)); status != 200 {
				t.Fatal(status)
			}
			only, _ := createProtocolBucket(t, s, "exclusive")
			shared, _ := createProtocolBucket(t, s, "shared")
			unassigned, _ := createProtocolBucket(t, s, "unassigned")
			foreign, _ := createProtocolBucket(t, s, "foreign")
			for _, bucket := range []*Bucket{only, shared} {
				grantUserDeleteBucket(t, s, bucket, user, BucketAllPermissions)
			}
			grantUserDeleteBucket(t, s, shared, other, BucketRead)
			grantUserDeleteBucket(t, s, foreign, other, BucketAllPermissions)
			var option *bool
			if mode != "legacy" {
				value := mode == "delete"
				option = &value
			}
			request := userDeleteRequest(user, option)
			for retry := 0; retry < 2; retry++ {
				if body, status := s.dispatchAs(request, nil, userPrincipal(t, s)); status != 200 {
					t.Fatalf("delete/retry: %d %x", status, body)
				}
			}
			if _, err := s.findAccount("departing"); err == nil {
				t.Fatal("account remains")
			}
			want := BucketActive
			if mode == "delete" {
				want = BucketDeleting
			}
			if only.Describe().State != want {
				t.Fatalf("exclusive state: %s", only.Describe().State)
			}
			for _, bucket := range []*Bucket{shared, unassigned, foreign} {
				if bucket.Describe().State != BucketActive {
					t.Fatalf("unrelated bucket deleted: %s", bucket.Describe().Name)
				}
			}
			if option != nil {
				changed := request
				changed.Body = append([]byte(nil), request.Body...)
				changed.Body[16] ^= 1
				if _, status := s.dispatchAs(changed, nil, userPrincipal(t, s)); status != 400 {
					t.Fatal("retry changed its deletion choice", status)
				}
			}
		})
	}
}

func TestUserDeleteBucketsDraftIsAtomicAndRejectsMaintenance(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 4<<20)
	create, user := createUserMessage("draft-owner", "secret")
	if _, status := s.dispatchAs(create, nil, userPrincipal(t, s)); status != 201 {
		t.Fatal(status)
	}
	first, _ := createProtocolBucket(t, s, "first")
	second, _ := createProtocolBucket(t, s, "second")
	for _, bucket := range []*Bucket{first, second} {
		grantUserDeleteBucket(t, s, bucket, user, BucketAllPermissions)
	}
	object := volume.ObjectID{1}
	if _, err := first.Put(context.Background(), userPrincipal(t, s), object, []byte("keep until commit"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	value := true
	request := userDeleteRequest(user, &value)
	second.mu.Lock()
	second.maintenance = &connectionAuthorization{}
	second.mu.Unlock()
	_, err := s.prepareAdminDraft(request, userPrincipal(t, s))
	second.mu.Lock()
	second.maintenance = nil
	second.mu.Unlock()
	if statusOf(err) != 409 {
		t.Fatal("maintenance bypassed", err)
	}
	draft, err := s.prepareAdminDraft(request, userPrincipal(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer draft.close()
	if _, err := draft.server.findAccount("draft-owner"); err == nil {
		t.Fatal("draft retained account")
	}
	for _, bucket := range []*Bucket{first, second} {
		private, _ := draft.server.Bucket(bucket.ID())
		if private.Describe().State != BucketDeleting || bucket.Describe().State != BucketActive {
			t.Fatal("partial publication")
		}
	}
	if _, err := s.findAccount("draft-owner"); err != nil {
		t.Fatal("uncommitted account deletion", err)
	}
	if len(draft.server.bucketJobs) != 0 {
		t.Fatal("draft started physical cleanup")
	}
	if data, _, err := first.Get(context.Background(), userPrincipal(t, s), object); err != nil || string(data) != "keep until commit" {
		t.Fatal("uncommitted data deletion", err)
	}
}

func TestDaemonUserDeleteBucketsCommitsAcrossClusterAndRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t)
	s := source.node
	create, user := createUserMessage("cluster-owner", "secret")
	if _, status := s.dispatchAs(create, nil, userPrincipal(t, s)); status != 201 {
		t.Fatal(status)
	}
	owned, _ := createProtocolBucket(t, s, "owned")
	shared, _ := createProtocolBucket(t, s, "shared")
	for _, bucket := range []*Bucket{owned, shared} {
		grantUserDeleteBucket(t, s, bucket, user, BucketAllPermissions)
	}
	admin, _ := parseID(userPrincipal(t, s).User.ID)
	grantUserDeleteBucket(t, s, shared, client.ID(admin), BucketRead)
	value := true
	request := userDeleteRequest(user, &value)
	if body, status := target.node.dispatchAs(request, nil, userPrincipal(t, s)); status != 200 {
		t.Fatalf("remote deletion: %d %x", status, body)
	}
	for _, node := range []*Server{s, target.node} {
		if _, err := node.findAccount("cluster-owner"); err == nil {
			t.Fatal("deleted account remains on member")
		}
		bucket, _ := node.Bucket(owned.ID())
		if bucket.Describe().State != BucketDeleting {
			t.Fatal("member missed deletion")
		}
		bucket, _ = node.Bucket(shared.ID())
		if bucket.Describe().State != BucketActive {
			t.Fatal("shared bucket deleted")
		}
	}
	source = restartDaemonTestNode(t, source)
	targetID, _ := parseID(target.node.cfg.Node.ID)
	awaitPeerObservation(t, source.node, client.ID(targetID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	if body, status := source.node.dispatchAs(request, nil, userPrincipal(t, source.node)); status != 200 {
		t.Fatalf("retry after restart: %d %x", status, body)
	}
}
