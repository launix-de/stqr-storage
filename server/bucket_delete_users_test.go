// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func bucketDeleteRequest(bucket *Bucket, option *bool) client.Message {
	e := client.Encoder{}
	e.ID(client.ID(bucket.ID()))
	if option != nil {
		e.Bool(*option)
	}
	return client.Message{Kind: client.Request, Opcode: client.BucketDelete, ID: client.NewRequestID(), Body: e.B}
}
func createDeleteTestUser(t *testing.T, s *Server, name string) client.ID {
	t.Helper()
	m, id := createUserMessage(name, "secret")
	if body, status := s.dispatchAs(m, nil, userPrincipal(t, s)); status != 201 {
		t.Fatalf("create %s: %d %x", name, status, body)
	}
	return id
}
func TestBucketDeleteOrphanUsers(t *testing.T) {
	for _, mode := range []string{"delete", "keep", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			s := testServer(t)
			target, _ := createProtocolBucket(t, s, "departing")
			active, _ := createProtocolBucket(t, s, "active")
			locked, _ := createProtocolBucket(t, s, "locked")
			deleting, _ := createProtocolBucket(t, s, "already-deleting")
			names := []string{"only", "shared", "locked-reader", "only-deleting-other", "disabled", "outsider", "administrator"}
			users := map[string]client.ID{}
			for _, name := range names {
				users[name] = createDeleteTestUser(t, s, name)
				if name != "outsider" {
					grantUserDeleteBucket(t, s, target, users[name], BucketRead)
				}
			}
			grantUserDeleteBucket(t, s, active, users["shared"], BucketRead)
			grantUserDeleteBucket(t, s, locked, users["locked-reader"], BucketRead)
			grantUserDeleteBucket(t, s, deleting, users["only-deleting-other"], BucketRead)
			for _, pair := range []struct {
				b     *Bucket
				state BucketState
			}{{locked, BucketLocked}, {deleting, BucketDeleting}} {
				d := pair.b.Describe()
				d.State = pair.state
				if _, err := s.buckets.Save(d, d.Revision); err != nil {
					t.Fatal(err)
				}
			}
			for _, name := range []string{"disabled", "administrator"} {
				e := client.Encoder{}
				e.ID(users[name])
				e.Text(name)
				role := uint8(0)
				if name == "administrator" {
					role = 1
				}
				e.U8(role)
				e.Bool(name == "disabled")
				if _, status := s.dispatchAs(userMessage(client.UserUpdate, e), nil, userPrincipal(t, s)); status != 200 {
					t.Fatal(status)
				}
			}
			var option *bool
			if mode != "legacy" {
				v := mode == "delete"
				option = &v
			}
			request := bucketDeleteRequest(target, option)
			for retry := range 2 {
				if body, status := s.dispatchAs(request, nil, userPrincipal(t, s)); status != 202 && (retry == 0 || status != 200) {
					t.Fatalf("delete/retry: %d %x", status, body)
				}
			}
			var accounts accountsImage
			if err := s.readBlob("accounts", &accounts); err != nil {
				t.Fatal(err)
			}
			present := map[string]bool{}
			for _, account := range accounts.Users {
				present[account.ID] = true
			}
			for _, name := range names {
				removed := !present[users[name].String()]
				wantRemoved := mode == "delete" && (name == "only" || name == "only-deleting-other" || name == "disabled")
				if removed != wantRemoved {
					t.Fatalf("account %s, removed=%v want %v", name, removed, wantRemoved)
				}
			}
			if target.Describe().State != BucketDeleting {
				t.Fatal("bucket not marked deleting")
			}
			if mode != "legacy" {
				changed := request
				changed.Body = append([]byte(nil), request.Body...)
				changed.Body[16] ^= 1
				if _, status := s.dispatchAs(changed, nil, userPrincipal(t, s)); status != 400 {
					t.Fatal("reused operation changed cleanup choice", status)
				}
			}
		})
	}
}
func TestBucketDeleteUsersDraftAtomicAndValidation(t *testing.T) {
	s := testServer(t)
	bucket, _ := createProtocolBucket(t, s, "atomic")
	user := createDeleteTestUser(t, s, "last-bucket-owner")
	grantUserDeleteBucket(t, s, bucket, user, BucketRead)
	yes := true
	request := bucketDeleteRequest(bucket, &yes)
	malformed := request
	malformed.Body = append([]byte(nil), request.Body...)
	malformed.Body[16] = 2
	if _, status := s.dispatchAs(malformed, nil, userPrincipal(t, s)); status != 400 {
		t.Fatal("invalid option accepted", status)
	}
	bucket.mu.Lock()
	bucket.maintenance = &connectionAuthorization{}
	bucket.mu.Unlock()
	if _, err := s.prepareAdminDraft(request, userPrincipal(t, s)); statusOf(err) != 409 {
		t.Fatal("maintenance bypassed", err)
	}
	bucket.mu.Lock()
	bucket.maintenance = nil
	bucket.mu.Unlock()
	draft, err := s.prepareAdminDraft(request, userPrincipal(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer draft.close()
	if _, err = s.findAccount("last-bucket-owner"); err != nil || bucket.Describe().State != BucketActive {
		t.Fatal("uncommitted preview changed live state", err)
	}
	if _, err = draft.server.findAccount("last-bucket-owner"); err == nil {
		t.Fatal("preview retained orphan account")
	}
	b, err := draft.server.Bucket(bucket.ID())
	if err != nil || b.Describe().State != BucketDeleting {
		t.Fatal("preview lost deletion intent", err)
	}
}
func TestDaemonBucketDeleteUsersAcrossClusterAndRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	s := source.node
	bucket, _ := createProtocolBucket(t, s, "last")
	other, _ := createProtocolBucket(t, s, "other")
	orphan := createDeleteTestUser(t, s, "orphan")
	shared := createDeleteTestUser(t, s, "shared")
	grantUserDeleteBucket(t, s, bucket, orphan, BucketRead)
	grantUserDeleteBucket(t, s, bucket, shared, BucketRead)
	grantUserDeleteBucket(t, s, other, shared, BucketRead)
	yes := true
	request := bucketDeleteRequest(bucket, &yes)
	if body, status := s.dispatchAs(request, nil, userPrincipal(t, s)); status != 202 {
		t.Fatalf("delete: %d %x", status, body)
	}
	for _, node := range []*Server{s, target.node} {
		if _, err := node.findAccount("orphan"); err == nil {
			t.Fatal("orphan not removed on member")
		}
		if _, err := node.findAccount("shared"); err != nil {
			t.Fatal("shared account removed", err)
		}
	}
	source = restartDaemonTestNode(t, source)
	targetID, _ := parseID(target.node.cfg.Node.ID)
	awaitPeerObservation(t, source.node, client.ID(targetID), func(o *peerObservation) bool { return o.online && o.snapshot != nil })
	if _, err := source.node.findAccount("orphan"); err == nil {
		t.Fatal("orphan resurrected after restart")
	}
	if body, status := source.node.dispatchAs(request, nil, userPrincipal(t, source.node)); status != 202 && status != 200 {
		t.Fatalf("replay after restart: %d %x", status, body)
	}
	// Deleting this user's remaining bucket must now remove the account; the
	// earlier DELETING bucket is no longer a valid namespace protecting it.
	if body, status := source.node.dispatchAs(bucketDeleteRequest(other, &yes), nil, userPrincipal(t, source.node)); status != 202 {
		t.Fatalf("last shared bucket: %d %x", status, body)
	}
	for _, node := range []*Server{source.node, target.node} {
		if _, err := node.findAccount("shared"); err == nil {
			t.Fatal("last valid bucket left an orphan account")
		}
	}
}
