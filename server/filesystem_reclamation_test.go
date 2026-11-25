// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestTicket32FilesystemPhysicalReclamation(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "filesystem-reclamation", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "filesystem-reclamation")
	// Exercise real upgrades, authorization and offline collection as a normal
	// bucket user. The standard MinCost fixture keeps repeated authentication
	// fast enough to measure all 100 storage lifecycle cycles under -race.
	user := bucketProtocolUser(t, s)
	description := b.Describe()
	description.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketAllPermissions}}
	if _, err := s.buckets.Save(description, description.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	started := time.Now()
	var dialTime time.Duration
	dialCount := 0
	dial := func() *client.Client {
		t.Helper()
		begin := time.Now()
		c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()))
		dialTime += time.Since(begin)
		dialCount++
		if err != nil {
			t.Fatalf("native connection %d after %s (cumulative dial time %s, context: %v): %v", dialCount, time.Since(started), dialTime, ctx.Err(), err)
		}
		return c
	}
	closeMount := func(c *client.Client) {
		c.Close()
		deadline := time.Now().Add(3 * time.Second)
		for {
			s.wsMu.Lock()
			connected := false
			for ws := range s.sockets {
				if ws.authorization != nil && ws.authorization.bucket == b {
					connected = true
					break
				}
			}
			s.wsMu.Unlock()
			if !connected {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("mount session did not close before offline collection")
			}
			time.Sleep(time.Millisecond)
		}
	}
	c := dial()
	f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.Create(ctx, client.FilesystemRootID(), "replaced", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("a"), 8192)
	if _, err := f.WriteAt(ctx, file.ID, data, 0); err != nil {
		t.Fatal(err)
	}
	closeMount(c)
	collect := func() volume.Stats {
		maintenance := dial()
		if _, err := maintenance.CheckFilesystem(ctx); err != nil {
			t.Fatal(err)
		}
		closeMount(maintenance)
		return b.allVolumes()[0].Stats()
	}
	baseline := collect()
	last := baseline
	for i := 0; i < 100; i++ {
		c = dial()
		f, err = client.OpenFilesystem(ctx, c, client.FSOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.Repeat([]byte{byte(i)}, 8192)
		if _, err := f.WriteAt(ctx, file.ID, data, 0); err != nil {
			t.Fatal(err)
		}
		closeMount(c)
		stats := collect()
		last = stats
		if stats.MetadataPhysical > baseline.MetadataPhysical+16*volume.PageSize {
			t.Fatalf("filesystem collection retained physical metadata at %d: before=%+v now=%+v", i, baseline, stats)
		}
		if stats.Objects != baseline.Objects || stats.DataPhysical != baseline.DataPhysical || stats.LogicalBytes != baseline.LogicalBytes {
			t.Fatalf("iteration %d retained filesystem payload: baseline=%+v now=%+v", i, baseline, stats)
		}
		if (i+1)%25 == 0 {
			t.Logf("overwrite/collection cycles completed: %d/100 in %s; %d native connections took %s", i+1, time.Since(started), dialCount, dialTime)
		}
	}
	c = dial()
	f, err = client.OpenFilesystem(ctx, c, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if n, err := f.ReadAt(ctx, file.ID, got, 0); err != nil || n != len(got) || !bytes.Equal(got, data) {
		t.Fatalf("reopened file: %d %v", n, err)
	}
	if err := f.Unlink(ctx, client.FilesystemRootID(), "replaced"); err != nil {
		t.Fatal(err)
	}
	closeMount(c)
	final := collect()
	if final.Objects != 1 || final.DataPhysical >= baseline.DataPhysical {
		t.Fatalf("unlinked file payload retained: %+v", final)
	}
	t.Logf("after 100 native overwrite/collection cycles: live objects=%d data=%d metadata=%d admission-free=%d; settled operation metadata was reclaimed", last.Objects, last.DataPhysical, last.MetadataPhysical, last.AdmissionFreePhysical)
	if err := b.allVolumes()[0].Scrub(); err != nil {
		t.Fatal(err)
	}
}
