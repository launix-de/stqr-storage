// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestSystemMetadataRAMLookupsDoNotReadAdminVolume(t *testing.T) {
	s := testServer(t)
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		t.Fatal(err)
	}
	// Include unrelated accounts and accumulated retry receipts. A lookup must not
	// read or decode this entire object, even after the account table grows.
	for i := 0; i < 200; i++ {
		image.Users = append(image.Users, account{ID: newID(), Name: fmt.Sprintf("ram-user-%03d", i), Role: "USER", PasswordHash: image.Users[0].PasswordHash})
		image.Receipts = append(image.Receipts, accountReceipt{ID: newID(), Actor: image.Users[0].ID, Digest: "test", Status: 200})
	}
	target := image.Users[len(image.Users)-1]
	keyID := "ram-access-key-0001"
	sealed, err := s.sealS3Secret(target.ID, keyID, "ram-test-secret-0001")
	if err != nil {
		t.Fatal(err)
	}
	image.Users[len(image.Users)-1].S3Keys = []s3AccessKey{{ID: keyID, Sealed: sealed}}
	if err = s.writeBlob("accounts", image); err != nil {
		t.Fatal(err)
	}
	draft := newTestBucket(t, "ram-index-bucket").Describe()
	draft.Grants = []BucketGrant{{UserID: target.ID, Permissions: BucketRead | BucketWrite}}
	bucket, err := s.buckets.Save(draft, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := bucketID(target.ID)
	before := s.system.IOStats()
	var cursor s3MaintenanceCursor
	for i := 0; i < 20; i++ {
		if got, e := s.findAccount(target.Name); e != nil || got.ID != target.ID {
			t.Fatal("name lookup", e)
		}
		if got, e := s.findAccountID(target.ID); e != nil || got.Name != target.Name {
			t.Fatal("ID lookup", e)
		}
		if got, secret, e := s.s3KeyUser(keyID); e != nil || got.ID != target.ID || secret != "ram-test-secret-0001" {
			t.Fatal("S3 lookup", e)
		}
		if e := s.validBucketUsers(draft.Grants); e != nil {
			t.Fatal(e)
		}
		byName, e := s.BucketByName(draft.Name)
		if e != nil {
			t.Fatal(e)
		}
		byID, e := s.Bucket(bucket.ID())
		if e != nil || byID != byName {
			t.Fatal("bucket index", e)
		}
		info, e := byID.Info(id, false)
		if e != nil || info.Permissions != BucketRead|BucketWrite {
			t.Fatal("ACL index", e)
		}
		s.CollectStats(client.StatsFilter{})
		if e := s.maintainS3Page(context.Background(), time.Now(), &cursor); e != nil {
			t.Fatal(e)
		}
		if e := s.maintainS3(context.Background(), time.Now()); e != nil {
			t.Fatal(e)
		}
		if _, e := s.findAccount("nonexistent"); e == nil {
			t.Fatal("missing account accepted")
		}
	}
	request := httptest.NewRequest("GET", "http://localhost/", nil)
	request.SetBasicAuth(s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password)
	if _, err = s.authenticate(request); err != nil {
		t.Fatal(err)
	}
	after := s.system.IOStats()
	if after.ReadOps != before.ReadOps || after.ReadBytes != before.ReadBytes || after.ViewReadOps != before.ViewReadOps || after.ViewReadBytes != before.ViewReadBytes {
		t.Fatalf("runtime metadata caused positioned %d reads / %d bytes and mapped/RAM %d views / %d bytes", after.ReadOps-before.ReadOps, after.ReadBytes-before.ReadBytes, after.ViewReadOps-before.ViewReadOps, after.ViewReadBytes-before.ViewReadBytes)
	}
	// Positive control: this oracle must observe an actual persistent read.
	var stored accountsImage
	if err := s.readStoredBlob("accounts", &stored); err != nil {
		t.Fatal(err)
	}
	measured := s.system.IOStats()
	positioned := measured.ReadOps > after.ReadOps && measured.ReadBytes > after.ReadBytes
	viewed := measured.ViewReadOps > after.ViewReadOps && measured.ViewReadBytes > after.ViewReadBytes
	if !positioned && !viewed {
		t.Fatal("neither backend read family detected the persistent-read control")
	}
}

func TestSystemMetadataRAMSnapshotsAndRestart(t *testing.T) {
	s := testServer(t)
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		t.Fatal(err)
	}
	image.Users = append(image.Users, account{ID: newID(), Name: "snapshot-user", Role: "USER", PasswordHash: image.Users[0].PasswordHash, S3Keys: []s3AccessKey{{ID: "snapshot-key-0001", Sealed: "sealed-test-value"}}})
	user := image.Users[1]
	if err := s.writeBlob("accounts", image); err != nil {
		t.Fatal(err)
	}
	image.Users[1].Name = "caller-mutated"
	image.Users[1].S3Keys[0].ID = "caller-mutated-key"
	var detached accountsImage
	if err := s.readBlob("accounts", &detached); err != nil {
		t.Fatal(err)
	}
	detached.Users[1].Name = "snapshot-mutated"
	detached.Users[1].S3Keys[0].ID = "snapshot-mutated-key"
	found, err := s.findAccount(user.Name)
	if err != nil || found.ID != user.ID || found.S3Keys[0].ID != "snapshot-key-0001" {
		t.Fatal("published slices aliased caller", err)
	}
	found.S3Keys[0].ID = "lookup-mutated-key"
	again, err := s.findAccount(user.Name)
	if err != nil || again.S3Keys[0].ID != "snapshot-key-0001" {
		t.Fatal("lookup returned mutable index slice", err)
	}
	path := s.configPath
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	before := reopened.system.IOStats()
	got, err := reopened.findAccountID(user.ID)
	if err != nil || got.Name != user.Name || got.S3Keys[0].ID != "snapshot-key-0001" {
		t.Fatal("restart did not rebuild index", err)
	}
	if after := reopened.system.IOStats(); after.ReadOps != before.ReadOps || after.ViewReadOps != before.ViewReadOps {
		t.Fatal("first lookup after restart read disk")
	}
}

type accountFailIO struct {
	volume.VolumeIO
	fail atomic.Bool
}

func (d *accountFailIO) Sync() error {
	if d.fail.Load() {
		return io.ErrClosedPipe
	}
	return d.VolumeIO.Sync()
}

func TestSystemMetadataRAMFailedPersistenceDoesNotPublish(t *testing.T) {
	mem, err := volume.NewMemory(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	dev := &accountFailIO{VolumeIO: mem}
	v, err := volume.Format(dev, volume.FormatOptions{Class: volume.SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	id := v.Identity().VolumeID.String()
	s := &Server{system: v, volumes: map[string]*managedVolume{id: {store: v, config: VolumeConfig{System: true}}}}
	s.cfg.Node.ID = newID()
	if err = s.bootstrapPlacementGroups(); err != nil {
		t.Fatal(err)
	}
	old := accountsImage{Schema: 1, Users: []account{{ID: newID(), Name: "before", Role: "ADMIN", PasswordHash: "test"}}}
	if err = s.writeBlob("accounts", old); err != nil {
		t.Fatal(err)
	}
	before := s.accounts.Load()
	next := cloneAccountsImage(old)
	next.Users[0].Name = "unconfirmed"
	dev.fail.Store(true)
	if err = s.writeBlob("accounts", next); err == nil {
		t.Fatal("injected sync failure accepted")
	}
	if s.accounts.Load() != before {
		t.Fatal("failed durable write published new accounts")
	}
	if _, err = s.findAccount("before"); err != nil {
		t.Fatal("old serving index lost", err)
	}
	if _, err = s.findAccount("unconfirmed"); err == nil {
		t.Fatal("unconfirmed account visible")
	}
}

func TestSystemMetadataRAMConcurrentPublication(t *testing.T) {
	s := testServer(t)
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		t.Fatal(err)
	}
	image.Users = append(image.Users, account{ID: newID(), Name: "changing", Role: "USER", PasswordHash: image.Users[0].PasswordHash})
	if err := s.writeBlob("accounts", image); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				x, err := s.accountIndex()
				if err != nil {
					t.Error(err)
					return
				}
				position, ok := x.byName["changing"]
				if !ok {
					t.Error("missing name index")
					return
				}
				user := x.image.Users[position]
				if x.byID[user.ID] != position || user.Name != "changing" {
					t.Error("mixed publication")
					return
				}
				returned, err := s.findAccount("changing")
				if err == nil {
					returned.S3Keys = append(returned.S3Keys, s3AccessKey{ID: "owned"})
				}
			}
		}()
	}
	for i := 0; i < 10; i++ {
		next := cloneAccountsImage(image)
		next.Users[1].Disabled = i%2 == 0
		if err := s.writeBlob("accounts", next); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestSystemMetadataRAMGrantsUpdateTogether(t *testing.T) {
	s := testServer(t)
	user := bucketProtocolUser(t, s)
	userID, _ := bucketID(user.ID)
	d := newTestBucket(t, "changing-grants").Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	saved, err := s.buckets.Save(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	live, err := s.Bucket(saved.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info, e := live.Info(userID, false); e != nil || info.Permissions != BucketRead {
		t.Fatal(e)
	}
	d = saved.Describe()
	d.Grants[0].Permissions = BucketWrite
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if info, e := live.Info(userID, false); e != nil || info.Permissions != BucketWrite {
		t.Fatal("ACL did not update with description", e)
	}
	d = live.Describe()
	d.State = BucketLocked
	if _, err = s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if info, e := live.Info(userID, false); e != nil || info.Permissions != 0 {
		t.Fatal("locked ACL gave write rights", e)
	}
}

func TestSystemMetadataRAMCorruptRecoveryFailsClosed(t *testing.T) {
	s := testServer(t)
	// Simulate damaged persistent recovery metadata. The serving RAM snapshot
	// remains valid until restart; reopening must fail, never create empty users.
	if err := s.metadataWrite(func(h *volume.Handle) error {
		_, err := h.Put(systemKey("accounts"), []byte(`{"schema":999,"users":[]}`), volume.Options{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	path := s.configPath
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := openTransportTestNode(Options{ConfigPath: path})
	if opened != nil {
		opened.Close()
	}
	if err == nil {
		t.Fatal("corrupt account metadata silently bootstrapped")
	}
}
