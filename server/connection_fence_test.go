// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func connectionTestSession(s *Server) *wsSession {
	ws := &wsSession{server: s, ctx: context.Background(), done: make(chan struct{}), responses: make(chan []byte, 32)}
	ws.connectionFenceCap.Store(true)
	return ws
}
func connectionTestMessage(op uint16, kind uint8) client.Message {
	return client.Message{Opcode: op, Kind: kind, ID: client.NewRequestID()}
}
func connectionTestPut(t *testing.T, v *volume.Volume, k volume.Key, data string) uint64 {
	t.Helper()
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	if _, e = h.Put(k, []byte(data), volume.Options{Accepted: true}); e != nil {
		t.Fatal(e)
	}
	g, e := h.Generation()
	if e != nil {
		t.Fatal(e)
	}
	return g
}
func connectionTestStatus(body []byte) int { d := client.Decoder{B: body}; return int(d.V()) }
func TestConnectionFenceAdmissionCut(t *testing.T) {
	s := testServer(t)
	ws := connectionTestSession(s)
	v, e := volume.NewRAM(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	work := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	if !ws.admitConnectionMessage(work) {
		t.Fatal("admission")
	}
	ctx := ws.connectionContext(ws.ctx, work.ID)
	fence := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(fence)
	later := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	ws.admitConnectionMessage(later)
	done := make(chan int, 1)
	go func() { _, status := ws.executeConnectionSync(context.Background(), fence); done <- status }()
	select {
	case <-done:
		t.Fatal("fence overtook preceding unpublished worker")
	case <-time.After(20 * time.Millisecond):
	}
	g := connectionTestPut(t, v, volume.Key{Object: volume.ObjectID{1}}, "accepted")
	touchConnectionVolume(ctx, v, g)
	ws.completeConnectionMessage(work, 201, client.ResponseBody(201, nil, ""))
	select {
	case status := <-done:
		if status != 200 {
			t.Fatal(status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("later worker extended preceding fence")
	}
	if !v.IsDurableThrough(g) {
		t.Fatal("ACK before durability")
	}
	ws.fenceMu.Lock()
	remaining := len(ws.connectionTouches)
	ws.fenceMu.Unlock()
	if remaining != 0 {
		t.Fatal("completed touch retained")
	}
}
func TestConnectionFenceParallelVolumesAndSharedFlush(t *testing.T) {
	s := testServer(t)
	ws := connectionTestSession(s)
	var volumes []*volume.Volume
	var gates []*serverFlushGate
	for i := 0; i < 2; i++ {
		mem, e := volume.NewMemory(16 << 20)
		if e != nil {
			t.Fatal(e)
		}
		d := &serverFlushGate{VolumeIO: mem}
		v, e := volume.Format(d, volume.FormatOptions{Class: volume.SSD})
		if e != nil {
			t.Fatal(e)
		}
		defer v.Close()
		volumes = append(volumes, v)
		gates = append(gates, d)
	}
	job := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	ws.admitConnectionMessage(job)
	ctx := ws.connectionContext(ws.ctx, job.ID)
	targets := make([]uint64, 2)
	for i, v := range volumes {
		targets[i] = connectionTestPut(t, v, volume.Key{Object: volume.ObjectID{byte(i + 1)}}, "bytes")
		touchConnectionVolume(ctx, v, targets[i])
	}
	ws.completeConnectionMessage(job, 201, nil)
	var releases []chan struct{}
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, c := range releases {
				close(c)
			}
		})
	}
	defer release()
	for _, d := range gates {
		d.mu.Lock()
		d.entered = make(chan struct{})
		d.release = make(chan struct{})
		releases = append(releases, d.release)
		d.mu.Unlock()
	}
	entered := []<-chan struct{}{gates[0].entered, gates[1].entered}
	first := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(first)
	done := make(chan int, 1)
	go func() { _, status := ws.executeConnectionSync(ws.ctx, first); done <- status }()
	for _, c := range entered {
		select {
		case <-c:
		case <-time.After(time.Second):
			t.Fatal("volume checkpoints ran serially")
		}
	}
	// Another socket may wait for exactly the same physical checkpoints.
	other := connectionTestSession(s)
	for i, v := range volumes {
		other.observeConnectionVolume(v, targets[i])
	}
	second := connectionTestMessage(client.ConnectionSync, client.Request)
	other.admitConnectionMessage(second)
	done2 := make(chan int, 1)
	go func() { _, status := other.executeConnectionSync(other.ctx, second); done2 <- status }()
	release()
	for _, ch := range []<-chan int{done, done2} {
		select {
		case status := <-ch:
			if status != 200 {
				t.Fatal(status)
			}
		case <-time.After(time.Second):
			t.Fatal("checkpoint did not finish")
		}
	}
	for _, v := range volumes {
		before := v.IOStats().SyncOps
		third := connectionTestMessage(client.ConnectionSync, client.Request)
		other.admitConnectionMessage(third)
		if _, status := other.executeConnectionSync(other.ctx, third); status != 200 {
			t.Fatal(status)
		}
		if v.IOStats().SyncOps != before {
			t.Fatal("repeated physical sync after another socket flushed")
		}
	}
}
func TestConnectionFenceErrorsBelongToEveryCapturedCut(t *testing.T) {
	s := testServer(t)
	ws := connectionTestSession(s)
	job := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	ws.admitConnectionMessage(job)
	a, b := connectionTestMessage(client.ConnectionSync, client.Request), connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(a)
	ws.admitConnectionMessage(b)
	ws.completeConnectionMessage(job, 507, client.ResponseBody(507, nil, "quota exhausted"))
	for _, f := range []client.Message{a, b} {
		body, status := ws.executeConnectionSync(ws.ctx, f)
		if status != 507 || !bytes.Contains(body, []byte("quota exhausted")) {
			t.Fatal(status, string(body))
		}
	}
	c := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(c)
	if _, status := ws.executeConnectionSync(ws.ctx, c); status != 200 {
		t.Fatal("consumed failure poisoned later fence", status)
	}
}
func connectionTestRaw(t *testing.T, s *Server, b *Bucket) *websocket.Conn {
	t.Helper()
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	a := s.cfg.Auth.BootstrapAdmin
	header := http.Header{}
	header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(a.Username+":"+a.Password)))
	header.Set(client.BucketHeader, b.ID().String())
	header.Set(client.FormatHeader, "fs")
	c, _, e := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).Dial("ws"+host.URL[4:]+"/api/ws", header)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	hello := connectionTestMessage(client.Hello, client.Request)
	enc := client.Encoder{}
	enc.U16(2)
	enc.U16(2)
	enc.U64(client.CapabilityDurability | client.CapabilityConnectionSync)
	enc.Text("fence-test")
	hello.Body = enc.B
	connectionTestSend(t, c, hello)
	m := connectionTestRead(t, c)
	if m.ID != hello.ID || connectionTestStatus(m.Body) != 200 {
		t.Fatal("HELLO", m)
	}
	return c
}
func connectionTestSend(t *testing.T, c *websocket.Conn, m client.Message) {
	t.Helper()
	if e := c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(m)); e != nil {
		t.Fatal(e)
	}
}
func connectionTestRead(t *testing.T, c *websocket.Conn) client.Message {
	t.Helper()
	for {
		_, b, e := c.ReadMessage()
		if e != nil {
			t.Fatal(e)
		}
		m, e := client.DecodeMessage(b)
		if e != nil {
			t.Fatal(e)
		}
		if m.Kind == client.Response {
			return m
		}
	}
}
func connectionTestRef(b *Bucket, id client.ObjectID) client.Encoder {
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.B = append(e.B, id[:]...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	return e
}
func connectionTestWirePut(b *Bucket, id client.ObjectID, payload []byte, absent bool) client.Message {
	m := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	e := connectionTestRef(b, id)
	e.U16(0x0102)
	e.Bool(false)
	e.Bool(absent)
	e.Bool(false)
	e.U8(0)
	e.U64(uint64(len(payload)))
	digest := sha256.Sum256(payload)
	e.B = append(e.B, digest[:]...)
	e.Blob(payload)
	m.Body = e.B
	return m
}
func TestAsyncConnectionBurstAndDeferredCASFailure(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "async-burst")
	c := connectionTestRaw(t, s, b)
	const count = 48
	for i := 0; i < count; i++ {
		connectionTestSend(t, c, connectionTestWirePut(b, client.ObjectID{byte(i + 1)}, []byte("payload"), false))
	}
	fence := connectionTestMessage(client.ConnectionSync, client.Request)
	connectionTestSend(t, c, fence)
	reply := connectionTestRead(t, c)
	if reply.ID != fence.ID || connectionTestStatus(reply.Body) != 200 {
		t.Fatalf("burst produced success ACK/error before fence: opcode=%x status=%d", reply.Opcode, connectionTestStatus(reply.Body))
	}
	// Compare-and-swap creation semantics must fail visibly, never produce a
	// misleading successful aggregate ACK just because no single ACK was asked.
	failed := connectionTestWirePut(b, client.ObjectID{1}, []byte("bad"), true)
	connectionTestSend(t, c, failed)
	fence = connectionTestMessage(client.ConnectionSync, client.Request)
	connectionTestSend(t, c, fence)
	reply = connectionTestRead(t, c)
	if reply.ID != failed.ID || connectionTestStatus(reply.Body) != 412 {
		t.Fatal("missing original failure", reply.Opcode, connectionTestStatus(reply.Body))
	}
	reply = connectionTestRead(t, c)
	if reply.ID != fence.ID || connectionTestStatus(reply.Body) != 412 {
		t.Fatal("false aggregate success", reply.Opcode, connectionTestStatus(reply.Body))
	}
	for i := 0; i < count; i++ {
		data, _, err := b.Get(context.Background(), quotaPrincipal(t, s), volume.ObjectID{byte(i + 1)})
		if err != nil || string(data) != "payload" {
			t.Fatal(i, string(data), err)
		}
	}
}
