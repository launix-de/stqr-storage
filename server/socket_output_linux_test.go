// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

func TestSocketOutputKernelBudget(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	remote, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	local, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if newSocketOutputPort(tls.Server(local, &tls.Config{})) != nil {
		t.Fatal("TLS bypassed")
	}
	if newSocketOutputPort(struct{ net.Conn }{local}) != nil {
		t.Fatal("opaque wrapper bypassed")
	}
	p := newSocketOutputPort(local)
	if p == nil {
		t.Fatal("TCP kernel instrumentation unavailable")
	}
	budget, err := p.budget()
	if err != nil || budget.capacity <= 0 {
		t.Fatal(budget, err)
	}
	// Force a capped send buffer and fill it without allowing the receiver to
	// drain. Nonblocking sends must stop without borrowing a storage lock.
	if err := local.(*net.TCPConn).SetWriteBuffer(4096); err != nil {
		t.Fatal(err)
	}
	if err := remote.(*net.TCPConn).SetReadBuffer(4096); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1<<20)
	deadline := time.Now().Add(time.Second)
	for {
		n, err := p.tryWrite(data)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not apply backpressure")
		}
	}
	budget, err = p.budget()
	if err != nil || budget.queued == 0 {
		t.Fatal("kernel occupancy not measured", budget, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &socketOutputConn{Conn: local, port: p}
	if ok, err := c.wait(ctx, inlineObjectDownload, false); ok || err == nil {
		t.Fatal("cancelled admission passed", ok, err)
	}
	t.Logf("actual kernel send capacity %d, queued %d", budget.capacity, budget.queued)
}
