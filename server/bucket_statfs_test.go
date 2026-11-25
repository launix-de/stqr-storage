// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestBucketSpaceUnlimitedQuotaAndGrant(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "space-data", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "space")
	user := bucketProtocolUser(t, s)
	uid, _ := parseID(user.ID)
	grant := client.Encoder{}
	grant.ID(client.ID(b.ID()))
	grant.ID(client.ID(uid))
	grant.U32(uint32(BucketAppend))
	if _, status, _ := adminBucketMessage(t, s, client.BucketGrant, grant.B); status != 200 {
		t.Fatal(status)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before, err := c.Space(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := s.bucketQuotaCapacity(b.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if before.QuotaLimited || before.Used != 0 || before.Free != expected.Available || before.Total != before.Free || before.Free == 0 {
		t.Fatalf("unlimited: %+v expected %+v", before, expected)
	}
	if _, err := c.Append(ctx, client.ObjectID{8}, make([]byte, 8192), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	after, err := c.Space(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Used != 8192 || after.Free >= before.Free {
		t.Fatalf("after write: %+v -> %+v", before, after)
	}
	// An append-only account can inspect capacity without gaining payload reads.
	if _, _, err := c.Get(ctx, client.ObjectID{8}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal(err)
	}
	quota := client.Encoder{}
	quota.ID(client.ID(b.ID()))
	quota.U8(0)
	quota.U64(0)
	quota.Bool(false)
	quota.U64(12288)
	quota.U32(150000)
	quota.U64(b.Describe().Policy.Version)
	if _, status, _ := adminBucketMessage(t, s, client.BucketSetQuota, quota.B); status != 200 {
		t.Fatal(status)
	}
	limited, err := c.Space(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !limited.QuotaLimited || limited.Quota != 12288 || limited.Total != 12288 || limited.Available != 4096 {
		t.Fatalf("quota: %+v", limited)
	}
	if _, err := c.Append(ctx, client.ObjectID{8}, make([]byte, 4096), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	full, err := c.Space(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if full.Free != 0 || full.Available != 0 || full.Used != 12288 {
		t.Fatalf("full: %+v", full)
	}
	// A user cannot inspect another bucket without its grant.
	other, _ := createProtocolBucket(t, s, "private-space")
	request := client.Encoder{}
	request.ID(client.ID(other.ID()))
	actor := userPrincipal(t, s)
	actor.User.Name = user.Name
	actor.User.ID = user.ID
	actor.User.Role = "USER"
	actor.User.PasswordHash = user.PasswordHash
	if _, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.BucketStatFS, ID: client.NewRequestID(), Body: request.B}, nil, actor); status != 403 {
		t.Fatal("unauthorized space query", status)
	}
	actor.connection = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAppend}
	if _, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.BucketStatFS, ID: client.NewRequestID(), Body: request.B}, nil, actor); status != 403 {
		t.Fatal("bound connection switched bucket", status)
	}

	draft := newTestBucket(t, "zero-quota")
	draft.description.Policy.Limits.Unlimited = false
	draft.description.Policy.Limits.Absolute = 0
	if _, status, _ := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	zero, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(draft.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer zero.Close()
	space, err := zero.Space(ctx)
	if err != nil || !space.QuotaLimited || space.Total != 0 || space.Available != 0 {
		t.Fatalf("zero quota: %+v %v", space, err)
	}
}
