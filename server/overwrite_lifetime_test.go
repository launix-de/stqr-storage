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

func TestTicket32NativeOverwriteResultsAreBoundedAndAppendRetries(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "overwrite-results", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "overwrite-results")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{32}
	for i := 0; i < 100; i++ {
		before := bytes.Repeat([]byte{byte(i)}, 8192)
		if _, err = c.Put(ctx, id, before, client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		after := bytes.Repeat([]byte{byte(i + 1)}, 8192)
		if _, err = c.CAS(ctx, id, 0, before, after, client.CASOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err = c.CAS(ctx, id, 0, before, after, client.CASOptions{}); !errors.Is(err, client.ErrPrecondition) {
			t.Fatal("CAS returned a historical result", err)
		}
		got, _, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, after) {
			t.Fatal("GET did not return current contents", err)
		}
	}
	v := b.allVolumes()[0]
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	requests := 0
	err = h.ScanRequests(func(r volume.RequestRecord) error {
		if r.Key.Bucket == b.ID() {
			requests++
		}
		return nil
	})
	h.Close()
	if err != nil || requests != 0 {
		t.Fatalf("ordinary PUT/CAS retained %d results: %v", requests, err)
	}
	log := client.ObjectID{33}
	request := client.NewRequestID()
	first, err := c.Append(ctx, log, []byte("first"), client.AppendOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Append(ctx, log, []byte("second"), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	retry, err := c.Append(ctx, log, []byte("first"), client.AppendOptions{RequestID: request})
	if err != nil || retry.StartOffset != first.StartOffset {
		t.Fatal("APPEND retry lost original offset", retry, err)
	}
	got, _, err := c.Get(ctx, log)
	if err != nil || !bytes.Equal(got, []byte("firstsecond")) {
		t.Fatal("APPEND retried twice", string(got), err)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
}
