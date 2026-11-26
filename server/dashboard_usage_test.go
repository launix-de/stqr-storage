// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDashboardUnlimitedUsageUsesRAMAndIncludesOccupiedBytes(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "unlimited-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "unlimited-usage")
	actor := userPrincipal(t, s)
	if _, err := b.Put(context.Background(), actor, volume.ObjectID{1}, make([]byte, 8192), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if !b.Describe().Policy.Limits.Unlimited {
		t.Fatal("fixture unexpectedly has a finite quota")
	}
	before := s.system.IOStats()
	page := readAdminTestPage(t, s, client.BucketListPage, adminPageBody("", 50, "unlimited-usage"))
	if len(page.Buckets) != 1 {
		t.Fatal("bucket not listed")
	}
	entry := page.Buckets[0]
	if entry.UsedBytes == nil || *entry.UsedBytes != 8192 {
		t.Fatal("logical occupancy missing", entry)
	}
	if entry.OccupiedPhysicalBytes == nil || *entry.OccupiedPhysicalBytes < 8192 {
		t.Fatal("physical occupancy missing", entry)
	}
	if after := s.system.IOStats(); after.ReadOps != before.ReadOps {
		t.Fatal("unlimited listing read admin volume")
	}
}

func TestDashboardUnlimitedPhysicalUsageSurvivesUnknownCOWLogicalUsage(t *testing.T) {
	id := client.NewRequestID()
	node := client.NewRequestID()
	image := dashboardBucketUsage{local: node, values: map[client.ID]map[client.ID]client.BucketStats{node: {id: {ID: id, LogicalBytes: 8192, AttributedPhysical: 12288}}}}
	d := BucketDescription{ID: id.String(), ClonePeers: []BucketClonePeer{{Bucket: client.NewRequestID().String()}}, Policy: DefaultBucketPolicy()}
	entry := bucketListEntry{BucketDescription: d}
	image.annotate(&entry)
	if entry.UsedBytes != nil || entry.UsageError == "" {
		t.Fatal("COW logical usage fabricated")
	}
	if entry.OccupiedPhysicalBytes == nil || *entry.OccupiedPhysicalBytes != 12288 {
		t.Fatal("known physical occupancy hidden")
	}
}

func TestDashboardUserTotalsIncludeUnloadedUsersAndExcludeNonAccounts(t *testing.T) {
	s := testServer(t)
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	accounts.Users = append(accounts.Users, account{ID: newID(), Name: "zz-last-page", Role: "USER", PasswordHash: accounts.Users[0].PasswordHash})
	if err := s.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	counter := func(id string, read, write uint64) {
		value := &bucketIO{}
		value.read.Store(read)
		value.write.Store(write)
		s.userIO.Store(id, value)
	}
	counter(accounts.Users[0].ID, 100, 200)
	counter(accounts.Users[1].ID, 300, 400)
	counter(newID(), 10000, 20000) // deleted/non-user identities do not contribute
	for _, body := range [][]byte{adminPageBody("", 1, ""), adminPageBody("", 1, "zz-last-page"), adminPageBody("", 1, "", accounts.Users[0].ID)} {
		raw, status, err := s.executeUsers(client.Message{Kind: client.Request, Opcode: client.UserListPage, Body: body}, userPrincipal(t, s))
		if err != nil || status != 200 {
			t.Fatal(status, err)
		}
		d := client.Decoder{B: raw}
		var page struct {
			Users []userPageEntry `json:"users"`
			Read  string          `json:"read_bytes_total"`
			Write string          `json:"write_bytes_total"`
		}
		if err = json.Unmarshal([]byte(d.Text()), &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Users) != 1 || page.Read != "400" || page.Write != "600" {
			t.Fatal("page/filter changed global user traffic", page)
		}
	}
}
