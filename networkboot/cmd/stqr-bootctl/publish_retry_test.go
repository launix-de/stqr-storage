// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestControlWaitsWithSameOperationID(t *testing.T) {
	calls := 0
	var id client.ID
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		m, err := client.DecodeMessage(raw)
		if err != nil {
			t.Error(err)
			return
		}
		if calls == 0 {
			id = m.ID
		} else if id != m.ID {
			t.Error("retry changed operation identity")
		}
		calls++
		status := 503
		if calls == 3 {
			status = 200
		}
		w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(status, nil, "range handoff must finish before global control")}))
	}))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := control(ctx, config{URI: host.URL}, client.BucketCreate, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatal("unexpected retry count", calls)
	}
}
func TestControlRetryCanBeCancelled(t *testing.T) {
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		m, _ := client.DecodeMessage(raw)
		w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(503, nil, "busy")}))
	}))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := control(ctx, config{URI: host.URL}, client.BucketCreate, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("retry ignored cancellation", err)
	}
}
func TestPublicationLockPreventsDuplicateImport(t *testing.T) {
	dir := t.TempDir()
	release, err := lockPublication(dir)
	if err != nil {
		t.Fatal(err)
	}
	if closeOther, err := lockPublication(dir); err == nil {
		closeOther()
		t.Fatal("second publisher acquired staging directory")
	}
	release()
	release, err = lockPublication(dir)
	if err != nil {
		t.Fatal("publication lock retained after close", err)
	}
	release()
}
