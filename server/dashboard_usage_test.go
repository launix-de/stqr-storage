// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDashboardUnlimitedUsageUsesRAMAndIncludesOccupiedBytes(t *testing.T) {
	s := testServer(t)
	op := addTestVolume(t, s, "unlimited-data", "hdd", 8<<20)
	dataVolume := s.volumes[op.VolumeID].store
	b, _ := createProtocolBucket(t, s, "unlimited-usage")
	actor := userPrincipal(t, s)
	if _, err := b.Put(context.Background(), actor, volume.ObjectID{1}, make([]byte, 8192), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	if !b.Describe().Policy.Limits.Unlimited {
		t.Fatal("fixture unexpectedly has a finite quota")
	}
	control := dataVolume.IOStats()
	if err := scanRangeRequests(s.ctx, dataVolume, func(volume.RequestRecord) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if after := dataVolume.IOStats(); after.ReadOps <= control.ReadOps {
		t.Fatal("metadata IO positive control did not read", control, after)
	}
	before := map[*volume.Volume]volume.IOStats{s.system: s.system.IOStats(), dataVolume: dataVolume.IOStats()}
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
	for store, prior := range before {
		if after := store.IOStats(); after.ReadOps != prior.ReadOps || after.WriteOps != prior.WriteOps || after.SyncOps != prior.SyncOps {
			t.Fatal("unlimited listing touched storage", prior, after)
		}
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

func TestDashboardMissingPlacementIsUnknownButKnownEmptyIsZero(t *testing.T) {
	id, node := client.NewRequestID(), client.NewRequestID()
	image := dashboardBucketUsage{controller: true, local: node, placement: &placementImage{}, values: map[client.ID]map[client.ID]client.BucketStats{node: {id: {ID: id}}}}
	d := BucketDescription{ID: id.String(), Policy: DefaultBucketPolicy()}
	entry := bucketListEntry{BucketDescription: d}
	image.annotate(&entry)
	if entry.UsedBytes != nil || entry.OccupiedPhysicalBytes != nil || entry.UsageError == "" {
		t.Fatal("missing placement fabricated zero usage", entry)
	}
	patch := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: id, ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: node}}}}, Addresses: []client.NodeAddress{{Node: node, Addresses: []string{"http://node:7419"}}}}
	var err error
	image.placement, err = buildPlacement([]client.MapPatch{patch})
	if err != nil {
		t.Fatal(err)
	}
	entry = bucketListEntry{BucketDescription: d}
	image.annotate(&entry)
	if entry.UsedBytes == nil || *entry.UsedBytes != 0 || entry.OccupiedPhysicalBytes == nil || *entry.OccupiedPhysicalBytes != 0 || entry.UsageError != "" {
		t.Fatal("known empty owner lost its zero", entry)
	}
}

func TestDashboardUsageRejectsSnapshotFromPreviousPeerBoot(t *testing.T) {
	s := testServer(t)
	current := s.CollectStats(client.StatsFilter{})
	remote, oldBoot, newBoot := client.NewRequestID(), client.NewRequestID(), client.NewRequestID()
	old := current
	old.Node, old.BootID = remote, oldBoot
	observation := &peerObservation{online: true, connection: &peerConnection{Node: remote, Boot: newBoot}, snapshot: &old, lastSeen: time.Now()}
	if s.peerStats == nil {
		s.peerStats = make(map[client.ID]*peerObservation)
	}
	s.peerStats[remote] = observation
	if _, ok := s.dashboardBucketUsage().values[remote]; ok {
		t.Fatal("new peer incarnation reused old boot counters")
	}
	fresh := old
	fresh.BootID = newBoot
	observation.snapshot = &fresh
	if _, ok := s.dashboardBucketUsage().values[remote]; !ok {
		t.Fatal("matching current boot snapshot was discarded")
	}
	observation.lastSeen = time.Now().Add(-11 * time.Second)
	if _, ok := s.dashboardBucketUsage().values[remote]; ok {
		t.Fatal("stale current boot counters accepted")
	}
}

func TestDashboardUsagePagesRemainAdministrative(t *testing.T) {
	s := testServer(t)
	ordinary := bucketProtocolUser(t, s)
	for _, actor := range []principal{{}, {User: ordinary}, {ClusterPeer: true}} {
		if _, status, err := s.executeBucketListPage(adminPageBody("", 1, ""), actor); statusOf(err) != 403 {
			t.Fatal("non-admin bucket usage page", status, err)
		}
		if _, status, err := s.executeUsers(client.Message{Kind: client.Request, Opcode: client.UserListPage, Body: adminPageBody("", 1, "")}, actor); statusOf(err) != 403 {
			t.Fatal("non-admin user totals page", status, err)
		}
	}
	for _, op := range []uint16{client.BucketListPage, client.UserListPage} {
		readAdminTestPage(t, s, op, adminPageBody("", 1, ""))
	}
}

func TestDashboardUnavailableLocalAccountingIsUnknown(t *testing.T) {
	s := testServer(t)
	op := addTestVolume(t, s, "unknown-local", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "known-empty")
	s.opMu.Lock()
	err := s.refreshQuotaProtectionLocked()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	known := s.quotaUsage.Load()
	s.quotaUsage.Store(nil)
	entry := bucketListEntry{BucketDescription: b.Describe()}
	s.dashboardBucketUsage().annotate(&entry)
	if entry.UsedBytes != nil || entry.OccupiedPhysicalBytes != nil || entry.UsageError == "" {
		t.Fatal("absent local accounting fabricated zero usage", entry)
	}
	partial := &quotaUsageImage{volumes: make(map[volume.ID][]volume.BucketStorage)}
	for id, rows := range known.volumes {
		if id.String() != op.VolumeID {
			partial.volumes[id] = rows
		}
	}
	s.quotaUsage.Store(partial)
	entry = bucketListEntry{BucketDescription: b.Describe()}
	s.dashboardBucketUsage().annotate(&entry)
	if entry.UsedBytes != nil || entry.OccupiedPhysicalBytes != nil {
		t.Fatal("missing attached volume accounting fabricated zero usage", entry)
	}
	s.quotaUsage.Store(known)
	entry = bucketListEntry{BucketDescription: b.Describe()}
	s.dashboardBucketUsage().annotate(&entry)
	if entry.UsedBytes == nil || *entry.UsedBytes != 0 || entry.OccupiedPhysicalBytes == nil || *entry.OccupiedPhysicalBytes != 0 {
		t.Fatal("complete known empty accounting lost zero", entry)
	}
}

func TestDashboardReplicaUsageCountsEachNodeOnce(t *testing.T) {
	id, a, b := client.NewRequestID(), client.NewRequestID(), client.NewRequestID()
	left := client.RangeEntry{PrefixBits: 1, OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: a}, {Node: b, Role: 1}}}
	right := left
	right.Start[0] = 128
	right.Locations = []client.Location{{Node: b}, {Node: a, Role: 1}}
	patch := client.MapPatch{Membership: 1, Generation: 1, Revision: 1, Bucket: id, ToEnd: true, Entries: []client.RangeEntry{left, right}, Addresses: []client.NodeAddress{{Node: a, Addresses: []string{"http://a:7419"}}, {Node: b, Addresses: []string{"http://b:7419"}}}}
	placement, err := buildPlacement([]client.MapPatch{patch})
	if err != nil {
		t.Fatal(err)
	}
	image := dashboardBucketUsage{controller: true, local: a, placement: placement, values: map[client.ID]map[client.ID]client.BucketStats{a: {id: {ID: id, LogicalBytes: 100, AttributedPhysical: 120}}, b: {id: {ID: id, LogicalBytes: 100, AttributedPhysical: 130}}}}
	entry := bucketListEntry{BucketDescription: BucketDescription{ID: id.String(), Policy: DefaultBucketPolicy()}}
	image.annotate(&entry)
	if entry.UsedBytes != nil || entry.UsageError == "" || entry.OccupiedPhysicalBytes == nil || *entry.OccupiedPhysicalBytes != 250 {
		t.Fatal("replicated counters were duplicated or logical ambiguity hidden", entry)
	}
	delete(image.values, b)
	entry = bucketListEntry{BucketDescription: entry.BucketDescription}
	image.annotate(&entry)
	if entry.OccupiedPhysicalBytes != nil {
		t.Fatal("missing replica fabricated partial physical usage", entry)
	}
}
