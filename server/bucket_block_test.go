// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"net/http/httptest"
	"testing"
	"time"
)

func TestBlockLeaseRangesAndReadonly(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "block", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "block")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	dial := func() *client.Client {
		t.Helper()
		c, e := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()))
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	c := dial()
	defer c.Close()
	if e := c.BeginBlockMaintenance(ctx, false); e != nil {
		t.Fatal(e)
	}
	id := client.ObjectID{33}
	payload := bytes.Repeat([]byte{7}, 512)
	if _, e := c.PutRange(ctx, id, 4096, payload, client.PutOptions{Accepted: true, RequestID: client.NewRequestID()}); e != nil {
		t.Fatal("block ranged write", e)
	}
	if e := c.Sync(ctx); e != nil {
		t.Fatal("flush", e)
	}
	got, info, e := c.GetRange(ctx, id, 4096, 512)
	if e != nil || !bytes.Equal(got, payload) || info.Length != 4608 {
		t.Fatal("ranged read", info, e)
	}
	if other, e := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID())); e == nil {
		other.Close()
		t.Fatal("exclusive bucket opened elsewhere")
	}
	c.Close()
	waitFsckReleased(t, b)
	c = dial()
	defer c.Close()
	if e := c.BeginBlockMaintenance(ctx, true); e != nil {
		t.Fatal(e)
	}
	if _, e := c.PutRange(ctx, id, 0, payload, client.PutOptions{RequestID: client.NewRequestID()}); e == nil {
		t.Fatal("readonly block lease permitted write")
	}
	if e := c.Delete(ctx, id, client.DeleteOptions{RequestID: client.NewRequestID()}); e == nil {
		t.Fatal("readonly block lease permitted delete")
	}
	if _, _, e := c.Get(ctx, id); e != nil {
		t.Fatal("readonly read", e)
	}
	if e := c.Sync(ctx); e != nil {
		t.Fatal("readonly flush", e)
	}
}
