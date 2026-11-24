// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestControlRetriesRejectedRangeHandoff(t *testing.T) {
	var count atomic.Int32
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		raw, _ := io.ReadAll(r.Body)
		m, err := client.DecodeMessage(raw)
		if err != nil {
			t.Error(err)
			return
		}
		status := 200
		if count.Add(1) == 1 {
			status = 503
		}
		w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(status, nil, "handoff")}))
	}))
	defer host.Close()
	if _, err := control(context.Background(), config{URI: host.URL}, client.UserDelete, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2 {
		t.Fatal(count.Load())
	}
}
func TestParseBootIDRejectsMalformed(t *testing.T) {
	for _, s := range []string{"", "0", "0000000000000000000000000000000x"} {
		if _, err := parseBootID(s); err == nil {
			t.Fatal(s)
		}
	}
}
