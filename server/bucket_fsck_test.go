// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func waitFsckReleased(t *testing.T, b *Bucket) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for b.underMaintenance() {
		if time.Now().After(deadline) {
			t.Fatal("maintenance retained after close")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBucketFsckNetworkExclusiveSweep(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "fsck-data", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "fsck")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	dial := func() *client.Client {
		t.Helper()
		c, e := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()), "fs")
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	c := dial()
	defer c.Close()
	f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.Create(ctx, client.FilesystemRootID(), "kept", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("kept"), 24000)
	if _, err = f.WriteAt(ctx, file.ID, want, 0); err != nil {
		t.Fatal(err)
	}
	orphan := client.ObjectID{} // zero address must be included in the scan
	if _, err = c.Put(ctx, orphan, []byte("unreachable"), client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	// A competing live connection makes maintenance fail without deleting.
	other := dial()
	if _, err = c.CheckFilesystem(ctx); err == nil {
		t.Fatal("fsck allowed with another mount connected")
	}
	if _, _, err = other.Get(ctx, orphan); err != nil {
		t.Fatal("rejected fsck changed data", err)
	}
	c.Close()
	other.Close()
	// Wait for the socket registry to drain before reacquiring maintenance.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.wsMu.Lock()
		n := len(s.sockets)
		s.wsMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sockets did not close")
		}
		time.Sleep(time.Millisecond)
	}
	c = dial()
	defer c.Close()
	// Measure the actual unreachable inventory independently of the sweep.
	// This fixture has an inline root directory, one file, one index and one
	// data object; completed intents are already retired online.
	fixtureFS, err := client.OpenFilesystem(ctx, c, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fixtureFS.ReadDir(ctx, client.FilesystemRootID())
	if err != nil || len(entries) != 1 || entries[0].ID != file.ID {
		t.Fatal("unexpected live namespace", entries, err)
	}
	rootRaw, _, err := c.Get(ctx, client.FilesystemRootID())
	if err != nil || len(rootRaw) < 36 || rootRaw[35] != 0 {
		t.Fatal("fixture root is not inline", err)
	}
	fileRaw, _, err := c.Get(ctx, file.ID)
	if err != nil || len(fileRaw) < 69 || string(fileRaw[:6]) != "STFS\x07\x00" || fileRaw[35] != 1 || fileRaw[36] != 1 {
		t.Fatal("fixture file is not a depth-one STFS v7 file", err)
	}
	var indexID, dataID client.ObjectID
	copy(indexID[:], fileRaw[37:69])
	indexRaw, _, err := c.Get(ctx, indexID)
	if err != nil {
		t.Fatal(err)
	}
	d := client.Decoder{B: indexRaw}
	if string(d.Take(4)) != "STIX" || d.U16() != 1 || d.U32() != 1 || d.U64() != uint64(len(want)) {
		t.Fatal("unexpected fixture data index")
	}
	copy(dataID[:], d.Take(32))
	if d.U32() != 0 || d.Done() != nil {
		t.Fatal("unexpected fixture data reference")
	}
	if data, _, err := c.Get(ctx, dataID); err != nil || !bytes.Equal(data, want) {
		t.Fatal("fixture data differs", err)
	}
	reachable := map[client.ObjectID]bool{client.FilesystemRootID(): true, file.ID: true, indexID: true, dataID: true}
	var expectedScanned, expectedDeleted, expectedBytes uint64
	var after *client.ObjectID
	for {
		page, next, err := c.ScanObjects(ctx, after, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page {
			expectedScanned++
			if reachable[object.ID] {
				continue
			}
			raw, _, err := c.Get(ctx, object.ID)
			if err != nil || uint64(len(raw)) != object.Info.Length {
				t.Fatal("inventory payload mismatch", err)
			}
			switch object.ID {
			case orphan:
				if string(raw) != "unreachable" {
					t.Fatal("explicit orphan differs")
				}
			case client.ObjectReferenceRootID():
				refs := client.Decoder{B: raw}
				if string(refs.Take(4)) != "ORF1" || refs.ID() != client.ID(b.ID()) || refs.U32() != 0 || refs.Done() != nil {
					t.Fatal("coordination root still has live references")
				}
			default:
				t.Fatal("unexpected unreachable object", object.ID)
			}
			expectedDeleted++
			expectedBytes += uint64(len(raw))
		}
		if next == nil {
			break
		}
		after = next
	}
	if expectedDeleted == 0 || expectedScanned != uint64(len(reachable))+expectedDeleted {
		t.Fatal("incomplete fixture inventory")
	}
	result, err := c.CheckFilesystem(ctx)
	if err != nil || result.Scanned != expectedScanned || result.Reachable != uint64(len(reachable)) || result.Deleted != expectedDeleted || result.DeletedBytes != expectedBytes {
		t.Fatalf("%+v %v", result, err)
	}
	if _, _, err = c.Get(ctx, orphan); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("orphan retained", err)
	}
	got := make([]byte, len(want))
	liveFS, err := client.OpenFilesystem(ctx, c, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = liveFS.ReadAt(ctx, file.ID, got, 0); err != nil || !bytes.Equal(got, want) {
		t.Fatal("live file damaged", err)
	}
	if _, err = c.Put(ctx, client.ObjectID{5}, []byte("blocked"), client.PutOptions{RequestID: client.NewRequestID()}); err == nil {
		t.Fatal("maintenance connection can write new references")
	}
	newClient, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err == nil {
		newClient.Close()
		t.Fatal("another mount entered during fsck")
	}
	// HTTP object requests and lifecycle mutations cannot bypass exclusivity.
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.B = append(e.B, make([]byte, 32)...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	status, _ := bucketHTTPReply(t, s, client.Message{Kind: client.Request, Opcode: 0x0107, ID: client.NewRequestID(), Body: e.B}, admin.Username, admin.Password)
	if status != 409 {
		t.Fatalf("HTTP bypass status %d", status)
	}
	e = client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.Text("renamed")
	_, status, _ = adminBucketMessage(t, s, client.BucketRename, e.B)
	if status != 409 {
		t.Fatal("admin bypass", status)
	}
	c.Close()
	waitFsckReleased(t, b)
	c = dial()
	defer c.Close()
	result, err = c.CheckFilesystem(ctx)
	if err != nil || result.Deleted != 0 {
		t.Fatal(result, err)
	}
	// Dropping ownership must never transparently reconnect the old fsck client.
	s.wsMu.Lock()
	for ws := range s.sockets {
		if ws.authorization != nil && ws.authorization.bucket == b {
			ws.close()
		}
	}
	s.wsMu.Unlock()
	waitFsckReleased(t, b)
	if _, err = c.Stat(ctx, client.FilesystemRootID()); err == nil {
		t.Fatal("fsck silently reconnected after losing maintenance")
	}
	c.Close()
	waitFsckReleased(t, b)
	// Ordinary writes and reconnects become available again after maintenance.
	c = dial()
	defer c.Close()
	if _, err = c.Put(ctx, orphan, []byte("after"), client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
}

func TestBucketScanMergesTombstonesAndPages(t *testing.T) {
	s := testServer(t)
	v1 := addTestVolume(t, s, "scan-a", "hdd", 4<<20)
	v2 := addTestVolume(t, s, "scan-b", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "scan")
	// Construct a stale live copy and newer tombstone across volumes.
	for _, id := range []string{v1.VolumeID, v2.VolumeID} {
		v := s.volumes[id].store
		h, e := v.Open(true)
		if e != nil {
			t.Fatal(e)
		}
		for i := byte(0); i < 4; i++ {
			_, e = h.Put(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{i}}, []byte{i}, volume.Options{})
			if e != nil {
				t.Fatal(e)
			}
		}
		if id == v2.VolumeID {
			e = h.Delete(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}}, volume.Options{})
			if e != nil {
				t.Fatal(e)
			}
		}
		h.Close()
	}
	if _, _, _, err := b.scan(context.Background(), b.Describe(), nil, 2); statusOf(err) != 503 {
		t.Fatal("conflicting copies need authoritative placement, not a content counter", err)
	}
	// Once maintenance removes the obsolete copy, pagination uses the tombstone.
	h, err := s.volumes[v1.VolumeID].store.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Purge(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	page, next, more, err := b.scan(context.Background(), b.Describe(), nil, 2)
	if err != nil || !more || next != (volume.ObjectID{1}) || len(page) != 1 || page[0].Key.Object != (volume.ObjectID{}) {
		t.Fatal(page, next, more, err)
	}
	page, _, more, err = b.scan(context.Background(), b.Describe(), &next, 2)
	if err != nil || more || len(page) != 2 || page[0].Key.Object != (volume.ObjectID{2}) || page[1].Key.Object != (volume.ObjectID{3}) {
		t.Fatal(page, more, err)
	}
}

type fsckCrashOnRootLock struct{ client.FilesystemStore }

func (s fsckCrashOnRootLock) CAS(ctx context.Context, id client.ObjectID, offset uint64, expected, replacement []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	info, err := s.FilesystemStore.CAS(ctx, id, offset, expected, replacement, opt)
	if err == nil && id == client.FilesystemRootID() && bytes.HasPrefix(replacement, []byte("STFL")) {
		return client.ObjectInfo{}, context.Canceled
	}
	return info, err
}

func TestBucketFsckRecoversInterruptedRmdir(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "recovery", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "recovery")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	f, err := client.OpenFilesystem(ctx, fsckCrashOnRootLock{c}, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Create(ctx, client.FilesystemRootID(), "empty", 0755, 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if err = f.Rmdir(ctx, client.FilesystemRootID(), "empty"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.wsMu.Lock()
		n := len(s.sockets)
		s.wsMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not close")
		}
		time.Sleep(time.Millisecond)
	}
	c, err = client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result, err := c.CheckFilesystem(ctx)
	if err != nil || result.Reachable != 1 || result.Deleted < 2 {
		t.Fatal(result, err)
	}
	f, err = client.OpenFilesystem(ctx, c, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := f.ReadDir(ctx, client.FilesystemRootID())
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}
