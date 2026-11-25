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

func TestCreateUpperIncludesQuotaInInitialBucketMessage(t *testing.T) {
	for _, quota := range []uint64{0, 4 << 30} {
		var count atomic.Int32
		host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			defer r.Body.Close()
			raw, _ := io.ReadAll(r.Body)
			m, err := client.DecodeMessage(raw)
			if err != nil || m.Opcode != client.BucketCreate {
				t.Errorf("unexpected create: %v opcode %d", err, m.Opcode)
				return
			}
			d := client.Decoder{B: m.Body}
			d.ID()
			d.U64()
			d.Text()
			for i := 0; i < 5; i++ {
				d.U8()
			}
			d.U16()
			d.U16()
			d.U16()
			d.Bool()
			d.U16()
			d.U64()
			d.U8()
			d.U64()
			unlimited, limit := d.Bool(), d.U64()
			d.U32()
			if d.Done() != nil || unlimited != (quota == 0) || limit != quota {
				t.Errorf("initial quota unlimited=%v limit=%d error=%v", unlimited, limit, d.Err)
			}
			w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(201, nil, "")}))
		}))
		if _, err := createIDWithQuota(context.Background(), config{URI: host.URL}, "upper", client.NewRequestID(), quota); err != nil {
			t.Fatal(err)
		}
		host.Close()
		if count.Load() != 1 {
			t.Fatal("extra admin calls", count.Load())
		}
	}
}
