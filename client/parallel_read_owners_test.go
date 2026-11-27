// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Each fixture owns only its map half. A misrouted read fails rather than being
// forwarded, so successful assembly proves direct owner routing.
func TestParallelReadOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	started := make(chan byte, 8)
	var connections [2]atomic.Int32
	root, a, b := ObjectID{1}, ObjectID{2}, ObjectID{200}
	index := encodeFSIndex([]fsSegment{{id: a, length: 32}, {id: b, length: 32}})
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ws, e := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
			if e != nil {
				return
			}
			defer ws.Close()
			connections[i].Add(1)
			var mu sync.Mutex
			send := func(m Message, status int, body []byte) {
				mu.Lock()
				defer mu.Unlock()
				_ = ws.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(status, body, "")}))
			}
			for {
				_, raw, e := ws.ReadMessage()
				if e != nil {
					return
				}
				m, e := DecodeMessage(raw)
				if e != nil {
					t.Error(e)
					return
				}
				if m.Opcode == Hello {
					out := Encoder{}
					out.U16(2)
					out.U64(CapabilityConnectionSync | CapabilityDurability)
					out.ID(ID{byte(i + 1)})
					out.ID(ID{42})
					out.U64(1)
					out.U64(1)
					send(m, 200, out.B)
					continue
				}
				if m.Opcode != objectGet && m.Opcode != objectGetRange {
					t.Errorf("unexpected opcode %d", m.Opcode)
					send(m, 400, nil)
					continue
				}
				d := Decoder{B: m.Body}
				d.ID()
				var id ObjectID
				copy(id[:], d.Take(32))
				d.Take(32)
				off, n := uint64(0), uint64(32)
				if m.Opcode == objectGetRange {
					off = d.U64()
					n = d.U64()
				}
				if (id[0] >= 128) != (i == 1) {
					t.Errorf("read routed to wrong owner %x", id)
					send(m, 403, nil)
					continue
				}
				go func(m Message, id ObjectID, off, n uint64) {
					var data []byte
					if id == root {
						data = index
					} else {
						started <- byte(i)
						if id == a {
							select {
							case <-release:
							case <-ctx.Done():
								return
							}
						}
						data = bytes.Repeat([]byte{byte(i + 7)}, 32)
					}
					out := Encoder{}
					fixtureInfo(&out, 0, uint64(len(data)))
					if m.Opcode == objectGetRange {
						out.U64(off)
						out.U64(n)
						out.U64(uint64(len(data)))
						data = data[off : off+n]
					}
					sum := sha256.Sum256(data)
					out.U8(0)
					out.U64(uint64(len(data)))
					out.B = append(out.B, sum[:]...)
					out.Blob(data)
					send(m, 200, out.B)
				}(m, id, off, n)
			}
		}))
		defer servers[i].Close()
	}
	c := dialConnectionTest(t, servers[0])
	patch := testMapPatch()
	patch.Bucket = c.bucket
	patch.Policies[0].Bucket = c.bucket
	left, right, e := SplitRange(patch.Entries[0])
	if e != nil {
		t.Fatal(e)
	}
	left.Locations = []Location{{Node: ID{1}}}
	right.Locations = []Location{{Node: ID{2}}}
	patch.Entries = []RangeEntry{left, right}
	patch.Addresses = []NodeAddress{{Node: ID{1}, Addresses: []string{servers[0].URL}}, {Node: ID{2}, Addresses: []string{servers[1].URL}}}
	if _, e = c.routes.Apply(patch); e != nil {
		t.Fatal(e)
	}
	c.rememberRouters(patch)
	c.mu.Lock()
	c.clusterRouting = true
	c.mu.Unlock()
	f := &Filesystem{store: c}
	dst := make([]byte, 64)
	done := make(chan error, 1)
	go func() { done <- f.readSegment(ctx, fsSegment{id: root, length: 64}, 1, dst, 0) }()
	seen := map[byte]bool{}
	for len(seen) < 2 {
		select {
		case owner := <-started:
			seen[owner] = true
		case <-ctx.Done():
			t.Fatal("owner reads serialized", ctx.Err())
		}
	}
	release <- struct{}{}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(dst, append(bytes.Repeat([]byte{7}, 32), bytes.Repeat([]byte{8}, 32)...)) {
		t.Fatal("assembly")
	}
	if connections[0].Load() != 1 || connections[1].Load() != 1 {
		t.Fatal("redundant owner connections")
	}
}
