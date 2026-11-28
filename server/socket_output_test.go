// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestSocketOutputAdmission(t *testing.T) {
	p := &testSocketOutputPort{capacity: 8 << 20}
	c := &socketOutputConn{port: p}
	p.queued.Store(p.capacity)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if ok, err := c.beginBorrow(ctx, inlineObjectDownload); ok || !errors.Is(err, context.DeadlineExceeded) || c.borrowed {
		t.Fatal(ok, err, c.borrowed)
	}
	p.queued.Store(0)
	if ok, err := c.beginBorrow(context.Background(), inlineObjectDownload); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err := c.finishBorrow(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.capacity = 4096
	if ok, err := c.beginBorrow(context.Background(), inlineObjectDownload); ok || err != nil {
		t.Fatal("small socket must capture", ok, err)
	}
	c.port = nil
	if ok, err := c.beginBorrow(context.Background(), 1); ok || err != nil {
		t.Fatal("opaque socket must capture", ok, err)
	}
}

func TestSocketOutputOwnsPartialSuffix(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	p := &testSocketOutputPort{conn: left, capacity: 8 << 20, partial: true}
	c := &socketOutputConn{Conn: left, port: p}
	if ok, err := c.beginBorrow(context.Background(), 100); !ok || err != nil {
		t.Fatal(ok, err)
	}
	payload := bytes.Repeat([]byte("p"), 100)
	prefix := make(chan []byte, 1)
	go func() { b := make([]byte, 7); _, _ = io.ReadFull(right, b); prefix <- b }()
	if n, err := c.Write(payload); n != 100 || err != nil {
		t.Fatal(n, err)
	}
	if !bytes.Equal(<-prefix, payload[:7]) {
		t.Fatal("wrong prefix")
	}
	for i := range payload {
		payload[i] = 'x'
	}
	got := make(chan []byte, 1)
	go func() { b := make([]byte, 93); _, _ = io.ReadFull(right, b); got <- b }()
	if err := c.finishBorrow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-got, bytes.Repeat([]byte("p"), 93)) {
		t.Fatal("suffix retained borrowed memory")
	}
}

type acceptedSocketPort struct{ payload []byte }

func (p *acceptedSocketPort) budget() (socketOutputBudget, error) {
	return socketOutputBudget{capacity: 8 << 20}, nil
}
func (p *acceptedSocketPort) tryWrite(data []byte) (int, error) {
	if len(data) > 0 && &data[0] != &p.payload[0] {
		panic("payload copied before kernel send")
	}
	return len(data), nil
}
func TestSocketOutputAcceptedPayloadHasNoOwnedCopy(t *testing.T) {
	payload := make([]byte, inlineObjectDownload)
	p := &acceptedSocketPort{payload: payload}
	c := &socketOutputConn{port: p}
	if n := testing.AllocsPerRun(10, func() {
		if ok, err := c.beginBorrow(context.Background(), int64(len(payload))); !ok || err != nil {
			panic("admission failed")
		}
		if _, err := c.Write(payload); err != nil {
			panic(err)
		}
		if c.pending != nil {
			panic("accepted payload retained")
		}
		if err := c.finishBorrow(context.Background()); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatal("accepted direct output allocated", n)
	}
}
