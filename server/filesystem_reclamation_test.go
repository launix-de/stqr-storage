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
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	dial := func() *client.Client {
		c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()), "fs")
		if err != nil {
			t.Fatal(err)
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
