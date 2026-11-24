// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"golang.org/x/crypto/bcrypt"
)

func TestAdminDraftUserMutationDoesNotPublish(t *testing.T) {
	s := testServer(t)
	actor := userPrincipal(t, s)
	before, err := os.ReadFile(s.configPath)
	if err != nil {
		t.Fatal(err)
	}
	generation := s.system.Stats().Generation
	password := "draft-secret-never-persist-raw"
	e := client.Encoder{}
	id := client.NewRequestID()
	e.ID(id)
	e.Text("new-draft-user")
	e.U8(0)
	e.Bool(false)
	e.Text(password)
	m := client.Message{Kind: client.Request, Opcode: client.UserCreate, ID: client.NewRequestID(), Body: e.B}
	draft, err := s.prepareAdminDraft(m, actor)
	if err != nil {
		t.Fatal(err)
	}
	defer draft.close()
	if draft.status != 201 || len(draft.result) == 0 {
		t.Fatal("missing public result")
	}
	if _, err := s.findAccount("new-draft-user"); err == nil {
		t.Fatal("uncommitted account became live")
	}
	user, err := draft.server.findAccount("new-draft-user")
	if err != nil || user.ID != id.String() || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		t.Fatal("draft did not reuse native user validation/hashing", err)
	}
	var accounts accountsImage
	if err := draft.server.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(accounts)
	if bytes.Contains(raw, []byte(password)) || len(accounts.Receipts) == 0 {
		t.Fatal("draft retained plaintext or lost deduplication receipt")
	}
	node, _ := parseID(s.running.Node.ID)
	cluster, _ := parseID(s.running.ClusterID)
	membership := client.Membership{Epoch: 1, Cluster: client.ID(cluster), Members: []client.Member{{ID: client.ID(node)}}}
	bundle, err := draft.server.captureSystemImage(membership, 1, 1, 1, map[client.ID]volume.ID{client.ID(node): s.system.Identity().VolumeID}, nil)
	if err != nil {
		t.Fatal("draft cannot become a proposed system graph", err)
	}
	for _, object := range bundle.Objects {
		if bytes.Contains(object, []byte(password)) {
			t.Fatal("plaintext entered proposed system graph")
		}
	}
	if err := draft.server.writeBlob("private-growth-test", strings.Repeat("x", 1500000)); err != nil || draft.server.system.Size() <= 1<<20 {
		t.Fatal("private metadata growth failed", err)
	}
	after, err := os.ReadFile(s.configPath)
	if err != nil || !bytes.Equal(before, after) || s.system.Stats().Generation != generation {
		t.Fatal("private draft changed source metadata/config", err)
	}
	if _, err := s.prepareAdminDraft(m, principal{User: account{ID: actor.User.ID, Name: actor.User.Name, Role: "ADMIN", PasswordHash: "stale"}, connection: &connectionAuthorization{admin: true}}); statusOf(err) != 403 {
		t.Fatal("stale capability bypassed account validation", err)
	}
}

func TestAdminDraftBucketDeletionCannotTouchData(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 2<<20)
	b, _ := createProtocolBucket(t, s, "preview-delete")
	actor := userPrincipal(t, s)
	object := volume.ObjectID{1}
	want := []byte("uncommitted admin changes must leave this intact")
	info, err := b.Put(context.Background(), actor, object, want, volume.Options{RequestID: volume.ID(client.NewRequestID())})
	if err != nil {
		t.Fatal(err)
	}
	before := b.Describe()
	data := b.localVolumes()[0]
	generation := data.Stats().Generation
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	m := client.Message{Kind: client.Request, Opcode: client.BucketDelete, ID: client.NewRequestID(), Body: e.B}
	draft, err := s.prepareAdminDraft(m, actor)
	if err != nil {
		t.Fatal(err)
	}
	defer draft.close()
	private, err := draft.server.Bucket(b.ID())
	if err != nil || private.Describe().State != BucketDeleting || draft.status != 202 {
		t.Fatal("deletion intent missing", err)
	}
	if len(draft.server.bucketJobs) != 0 {
		t.Fatal("draft started background cleanup")
	}
	if err := draft.server.bucketLifecycleOnce(b.ID()); statusOf(err) != 503 {
		t.Fatal("private draft ran user-data cleanup", err)
	}
	if _, err := private.Put(context.Background(), actor, object, []byte("overwrite"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); statusOf(err) != 503 {
		t.Fatal("private draft mutated borrowed user volume", err)
	}
	if b.Describe().Revision != before.Revision || b.Describe().State != BucketActive || data.Stats().Generation != generation {
		t.Fatal("live data/description changed")
	}
	if err := draft.close(); err != nil {
		t.Fatal(err)
	}
	got, current, err := b.Get(context.Background(), actor, object)
	if err != nil || !bytes.Equal(got, want) || current.Length != info.Length {
		t.Fatal("closing private draft closed or modified the borrowed data volume", err)
	}
	b.mu.Lock()
	b.maintenance = &connectionAuthorization{}
	b.mu.Unlock()
	_, err = s.prepareAdminDraft(m, actor)
	b.mu.Lock()
	b.maintenance = nil
	b.mu.Unlock()
	if statusOf(err) != 409 {
		t.Fatal("draft bypassed live maintenance exclusion", err)
	}
}
