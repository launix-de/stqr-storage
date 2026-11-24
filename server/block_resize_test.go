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
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func TestNativeBlockResizePersistsAcrossLeaseReopen(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "block-grow", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "block-grow")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	open := func() *nbd.Bucket {
		t.Helper()
		c, err := nbd.Open(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), false)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := open()
	defer c.Close()
	m, err := c.Create(ctx, nbd.Metadata{Name: "disk", Size: nbd.StripeSize + 512})
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x51}, 512)
	if err = c.WriteAt(ctx, m, payload, nbd.StripeSize); err != nil {
		t.Fatal(err)
	}
	grown, err := c.Resize(ctx, m, 3*nbd.StripeSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Resize(ctx, m, 2*nbd.StripeSize); !errors.Is(err, client.ErrRange) {
		t.Fatal("stale handle shrank disk", err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	waitFsckReleased(t, b)
	c = open()
	defer c.Close()
	actual, err := c.Lookup(ctx, m.Name)
	if err != nil || actual != grown {
		t.Fatal(actual, err)
	}
	got := make([]byte, 1024)
	if err = c.ReadAt(ctx, actual, got, nbd.StripeSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:512], payload) || !bytes.Equal(got[512:], make([]byte, 512)) {
		t.Fatal("growth damaged boundary stripe")
	}
	if err = c.WriteAt(ctx, actual, payload, actual.Size-512); err != nil {
		t.Fatal(err)
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}
