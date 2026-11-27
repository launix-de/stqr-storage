// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wireObjectPeer is an independent protocol fixture, not a second volume engine.
// It validates the exact bucket/reference/precondition/transfer field ordering.
type wireObjectPeer struct {
	helloCaps        atomic.Uint64
	acceptedHeaders  atomic.Int64
	nativeHeaders    atomic.Int64
	fences           atomic.Int64
	stallHandshake   atomic.Bool
	handshakeStarted chan struct{}
	handshakeGate    chan struct{}
	t                *testing.T
	bucket           ID
	mu               sync.Mutex
	data             map[ObjectID][]byte
	versions         map[ObjectID]uint64
	results          map[ID]wireFixtureResult
	seen             map[ID]int
	dropNext         atomic.Bool
	holdNext         atomic.Bool
	stallRead        atomic.Bool
	corruptRead      atomic.Bool
	readGate         chan struct{}
	commits          atomic.Int64
}
type wireFixtureResult struct {
	opcode uint16
	result []byte
}
type fixtureUpload struct {
	message  Message
	object   ObjectID
	offset   uint64
	expected uint64
	digest   [32]byte
	bytes    bytes.Buffer
	part     uint64
}

func newWirePeer(t *testing.T) (*wireObjectPeer, *httptest.Server) {
	p := &wireObjectPeer{handshakeStarted: make(chan struct{}), handshakeGate: make(chan struct{}), readGate: make(chan struct{}), t: t, bucket: ID{7}, data: map[ObjectID][]byte{}, versions: map[ObjectID]uint64{}, results: map[ID]wireFixtureResult{}, seen: map[ID]int{}}
	server := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(server.Close)
	return p, server
}
func fixtureInfo(e *Encoder, version, length uint64) {
	e.U64(length)
	e.Bool(false)
	e.U32(0)
	e.ID(ID{})
	e.U8(0)
	e.U16(1)
}
func (p *wireObjectPeer) serve(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "user" || pass != "credential" {
		w.WriteHeader(401)
		return
	}
	if r.Header.Get(FormatHeader) != "fs" {
		p.t.Error("missing explicit format on initial or reconnected session")
		http.Error(w, "missing format", 400)
		return
	}
	if r.Header.Get(BucketHeader) != p.bucket.String() {
		w.WriteHeader(403)
		return
	}
	if p.stallHandshake.CompareAndSwap(true, false) {
		close(p.handshakeStarted)
		select {
		case <-p.handshakeGate:
		case <-r.Context().Done():
			return
		}
	}
	up := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}
	c, e := up.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
	uploads := map[ID]*fixtureUpload{}
	acceptedIDs := map[ID]bool{}
	send := func(m Message, status int, result []byte) error {
		body := ResponseBody(status, result, "")
		if acceptedIDs[m.ID] {
			m.Opcode = ObjectAccepted
			mark := CommitMark{Boot: ID{99}, Sequence: uint64(p.commits.Load())}
			h := Encoder{}
			h.V(1)
			h.V(DurabilityHeader)
			h.Blob(mark.MarshalBinary())
			e := Encoder{}
			e.V(uint64(status))
			e.Blob(h.B)
			e.B = append(e.B, result...)
			body = e.B
		}
		return c.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: body}))
	}
	commit := func(m Message, id ObjectID, off uint64, data []byte) error {
		p.mu.Lock()
		p.seen[m.ID]++
		if old, ok := p.results[m.ID]; ok {
			p.mu.Unlock()
			return send(m, 200, old.result)
		}
		switch m.Opcode {
		case objectPut:
			p.data[id] = bytes.Clone(data)
		case objectPutRange:
			end := off + uint64(len(data))
			old := p.data[id]
			if end > uint64(len(old)) {
				old = append(old, make([]byte, int(end)-len(old))...)
			}
			copy(old[off:], data)
			p.data[id] = old
		case objectAppend:
			off = uint64(len(p.data[id]))
			p.data[id] = append(p.data[id], data...)
		}
		p.versions[id]++
		result := Encoder{}
		fixtureInfo(&result, p.versions[id], uint64(len(p.data[id])))
		if m.Opcode == objectAppend {
			result.U64(off)
		}
		if m.Opcode == objectPutRange {
			result.U64(off)
			result.U64(uint64(len(data)))
			result.U64(uint64(len(p.data[id])))
		}
		p.results[m.ID] = wireFixtureResult{m.Opcode, result.B}
		p.mu.Unlock()
		p.commits.Add(1)
		if p.dropNext.CompareAndSwap(true, false) {
			c.Close()
			return io.ErrClosedPipe
		}
		if p.holdNext.CompareAndSwap(true, false) {
			return nil
		}
		return send(m, 200, result.B)
	}
	for {
		kind, b, e := c.ReadMessage()
		if e != nil {
			return
		}
		if kind != websocket.BinaryMessage {
			p.t.Error("client sent non-binary message")
			return
		}
		m, e := DecodeMessage(b)
		if e != nil {
			p.t.Error(e)
			return
		}
		if m.Opcode == ObjectAccepted {
			if len(m.Body) < 82 {
				p.t.Error("short accepted header")
				return
			}
			acceptedIDs[m.ID] = true
			p.acceptedHeaders.Add(1)
			m.Opcode = uint16(m.Body[80]) | uint16(m.Body[81])<<8
			m.Body = append(append([]byte(nil), m.Body[:80]...), m.Body[82:]...)
		} else if m.Opcode >= objectPut && m.Opcode <= objectAppend {
			p.nativeHeaders.Add(1)
		}
		d := Decoder{B: m.Body}
		if m.Opcode == BucketSync {
			d.ID()
			mark, err := DecodeCommitMark(d.Take(24))
			if err != nil || d.Done() != nil {
				p.t.Error("bad bucket fence")
				return
			}
			p.fences.Add(1)
			send(m, 200, mark.MarshalBinary())
			continue
		}
		if m.Opcode == Hello {
			d.U16()
			d.U16()
			d.U64()
			d.Text()
			if d.Done() != nil {
				p.t.Error("invalid hello")
				return
			}
			result := Encoder{}
			result.U16(2)
			result.U64(p.helloCaps.Load())
			result.ID(ID{1})
			result.ID(ID{2})
			result.U64(1)
			result.U64(1)
			send(m, 200, result.B)
			continue
		}
		if m.Opcode == 7 {
			continue
		}
		if m.Opcode == 5 || m.Opcode == 6 {
			operation := d.ID()
			stage := uploads[operation]
			if stage == nil {
				p.t.Error("chunk without request")
				return
			}
			if m.Opcode == 5 {
				if d.U64() != stage.part || d.U64() != uint64(stage.bytes.Len()) {
					p.t.Error("chunk index/offset")
					return
				}
				part := d.Blob()
				if len(part) > transferChunk {
					p.t.Error("chunk too large")
					return
				}
				stage.bytes.Write(part)
				stage.part++
			} else {
				length := d.U64()
				digest := d.Take(32)
				sum := sha256.Sum256(stage.bytes.Bytes())
				if length != stage.expected || uint64(stage.bytes.Len()) != length || !bytes.Equal(digest, sum[:]) || sum != stage.digest {
					p.t.Error("upload digest/length")
					return
				}
				delete(uploads, operation)
				if e = commit(stage.message, stage.object, stage.offset, stage.bytes.Bytes()); e != nil {
					return
				}
			}
			if d.Done() != nil {
				p.t.Error(d.Done())
				return
			}
			continue
		}
		bucket := d.ID()
		var id ObjectID
		copy(id[:], d.Take(32))
		d.U64()
		d.U64()
		d.ID()
		if bucket != p.bucket {
			p.t.Error("client switched buckets")
			return
		}
		switch m.Opcode {
		case objectPut, objectPutRange, objectAppend:
			off := uint64(0)
			if m.Opcode == objectPutRange {
				off = d.U64()
			}
			if d.Bool() {
				d.U64()
			}
			d.Bool()
			if d.Bool() {
				d.U64()
				d.Blob()
			}
			mode, length, digest, data := decodeTransfer(&d)
			if d.Done() != nil {
				p.t.Error(d.Done())
				return
			}
			if mode == 1 {
				if len(data) != 0 {
					p.t.Error("stream mode with inline payload")
					return
				}
				uploads[m.ID] = &fixtureUpload{message: m, object: id, offset: off, expected: length, digest: digest}
				continue
			}
			if length != uint64(len(data)) || sha256.Sum256(data) != digest {
				p.t.Error("inline digest/length")
				return
			}
			if e = commit(m, id, off, data); e != nil {
				return
			}
		case objectGet, objectGetRange, objectStat:
			p.mu.Lock()
			data, exists := p.data[id]
			data = bytes.Clone(data)
			version := p.versions[id]
			p.mu.Unlock()
			if !exists {
				send(m, 404, nil)
				continue
			}
			result := Encoder{}
			fixtureInfo(&result, version, uint64(len(data)))
			off := uint64(0)
			length := uint64(len(data))
			if m.Opcode == objectGetRange {
				off = d.U64()
				length = d.U64()
				if off > uint64(len(data)) {
					off = uint64(len(data))
				}
				length = min(length, uint64(len(data))-off)
				result.U64(off)
				result.U64(length)
				result.U64(uint64(len(data)))
			}
			if m.Opcode != objectStat && d.Bool() {
				if d.Bool() {
					d.U64()
				}
				d.U64()
				d.Blob()
			}
			if d.Done() != nil {
				p.t.Error("object reference or range layout")
				return
			}
			if m.Opcode == objectStat {
				send(m, 200, result.B)
				continue
			}
			payload := data[off : off+length]
			digest := sha256.Sum256(payload)
			if len(payload) > transferChunk {
				begin := Encoder{}
				begin.ID(m.ID)
				begin.U64(uint64(len(payload)))
				begin.B = append(begin.B, digest[:]...)
				begin.Bool(false)
				begin.Bool(false)
				c.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Push, Opcode: 13, ID: NewRequestID(), Body: begin.B}))
				if p.stallRead.CompareAndSwap(true, false) {
					<-p.readGate
				}
				var part uint64
				for at := 0; at < len(payload); at += transferChunk {
					chunk := Encoder{}
					chunk.ID(m.ID)
					chunk.U64(part)
					chunk.U64(uint64(at))
					chunk.Blob(payload[at:min(at+transferChunk, len(payload))])
					c.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Push, Opcode: 5, ID: NewRequestID(), Body: chunk.B}))
					part++
				}
				end := Encoder{}
				end.ID(m.ID)
				end.U64(uint64(len(payload)))
				end.B = append(end.B, digest[:]...)
				if p.corruptRead.CompareAndSwap(true, false) {
					end.B[len(end.B)-1] ^= 1
				}
				c.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Push, Opcode: 6, ID: NewRequestID(), Body: end.B}))
				result.U8(1)
				result.U64(uint64(len(payload)))
				result.B = append(result.B, digest[:]...)
				result.Blob(nil)
			} else {
				result.U8(0)
				result.U64(uint64(len(payload)))
				result.B = append(result.B, digest[:]...)
				result.Blob(payload)
			}
			status := 200
			if m.Opcode == objectGetRange {
				status = 206
			}
			send(m, status, result.B)
		case objectCAS:
			send(m, 412, nil)
		default:
			send(m, 501, nil)
		}
	}
}
func TestBucketConnectionObjectsStreamingAndRequestMultiplexing(t *testing.T) {
	peer, server := newWirePeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, e := Dial(ctx, server.URL, "user", "credential", peer.bucket, "fs")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	id := ObjectID{4}
	info, e := c.Put(ctx, id, []byte("abc"), PutOptions{})
	if e != nil || info.Length == 0 {
		t.Fatal(info, e)
	}
	b, info, e := c.GetRange(ctx, id, 1, 2)
	if e != nil || string(b) != "bc" || info.Length != 3 {
		t.Fatal(string(b), info, e)
	}
	if _, e = c.CAS(ctx, id, 0, []byte("a"), []byte("x"), CASOptions{}); !errors.Is(e, ErrPrecondition) {
		t.Fatal("status mapping", e)
	}
	data := bytes.Repeat([]byte("streamed"), transferChunk/4)
	bigID := ObjectID{5}
	if _, e = c.PutFrom(ctx, bigID, bytes.NewReader(data), uint64(len(data)), PutOptions{}); e != nil {
		t.Fatal(e)
	}
	var out bytes.Buffer
	if _, e = c.GetTo(ctx, bigID, &out); e != nil || !bytes.Equal(out.Bytes(), data) {
		t.Fatal("streamed get mismatch", e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _, e := c.Get(ctx, id)
			if e != nil || string(b) != "abc" {
				t.Error("multiplexed response mismatch", e)
			}
		}()
	}
	wg.Wait()
	if _, _, e = c.Get(ctx, ObjectID{99}); !errors.Is(e, ErrNotFound) {
		t.Fatal("not-found status", e)
	}
	c.Close()
	if _, _, e = c.Get(ctx, id); !errors.Is(e, ErrClosed) {
		t.Fatal("closed connection", e)
	}
}
func TestBucketConnectionReconnectPreservesMutationID(t *testing.T) {
	peer, server := newWirePeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, e := Dial(ctx, server.URL, "user", "credential", peer.bucket, "fs")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	peer.dropNext.Store(true)
	id := NewRequestID()
	result, e := c.Append(ctx, ObjectID{7}, []byte("once"), AppendOptions{RequestID: id})
	if e != nil || result.Length != 4 {
		t.Fatal("reconnect result", result, e)
	}
	peer.mu.Lock()
	seen := peer.seen[id]
	peer.mu.Unlock()
	if seen != 2 || peer.commits.Load() != 1 {
		t.Fatalf("retry changed identity or duplicated mutation: %d %d", seen, peer.commits.Load())
	}
}
func TestBucketConnectionCancellationUnknownOutcome(t *testing.T) {
	peer, server := newWirePeer(t)
	c, e := Dial(context.Background(), server.URL, "user", "credential", peer.bucket, "fs")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	peer.holdNext.Store(true)
	id := NewRequestID()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, e = c.Put(ctx, ObjectID{8}, []byte("confirmed"), PutOptions{RequestID: id})
	var unknown *UnknownOutcomeError
	if !errors.As(e, &unknown) || unknown.RequestID != id || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("lost mutation identity: %v", e)
	}
	retryCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	info, e := c.Put(retryCtx, ObjectID{8}, []byte("confirmed"), PutOptions{RequestID: id})
	if e != nil || info.Length == 0 || peer.commits.Load() != 1 {
		t.Fatal("unknown-outcome retry duplicated write", info, e)
	}
}
func TestBucketConnectionRequiresAllConstructorFields(t *testing.T) {
	for _, tc := range []struct {
		url, user, pass string
		bucket          ID
	}{{"http://host", "", "password", ID{1}}, {"http://host", "user", "", ID{1}}, {"http://host", "user", "password", ID{}}, {"http://user:secret@host", "user", "password", ID{1}}, {"https://host", "user", "password", ID{1}}} {
		if _, e := Dial(context.Background(), tc.url, tc.user, tc.pass, tc.bucket, "fs"); e == nil {
			t.Fatal("missing/invalid connection field accepted")
		}
	}
	var c Client
	if _, _, e := c.Get(context.Background(), ObjectID{}); e != ErrClosed {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.PutFrom(ctx, ObjectID{}, failingReader{}, 100, PutOptions{}); e != context.Canceled {
		t.Fatal(e)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { panic("closed/canceled connection consumed source") }

func TestStreamCancellationAndDigestFailureRemoveSpoolFiles(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	peer, server := newWirePeer(t)
	c, e := Dial(context.Background(), server.URL, "user", "credential", peer.bucket, "fs")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	id := ObjectID{23}
	payload := bytes.Repeat([]byte("x"), 2*transferChunk)
	if _, e = c.Put(context.Background(), id, payload, PutOptions{}); e != nil {
		t.Fatal(e)
	}
	peer.stallRead.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, _, e := c.Get(ctx, id); result <- e }()
	found := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		files, _ := os.ReadDir(temp)
		if len(files) > 0 {
			found = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !found {
		close(peer.readGate)
		cancel()
		t.Fatal("stream did not start")
	}
	cancel()
	if e = <-result; !errors.Is(e, context.Canceled) {
		close(peer.readGate)
		t.Fatal(e)
	}
	files, _ := os.ReadDir(temp)
	if len(files) != 0 {
		close(peer.readGate)
		t.Fatal("canceled stream left spool files")
	}
	close(peer.readGate)
	ctx2, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, e = c.Stat(ctx2, id); e != nil {
		t.Fatal(e)
	}
	peer.corruptRead.Store(true)
	if _, _, e = c.Get(ctx2, id); !errors.Is(e, ErrProtocol) {
		t.Fatal("bad transfer digest accepted", e)
	}
	files, _ = os.ReadDir(temp)
	if len(files) != 0 {
		t.Fatal("invalid stream left spool files")
	}
}

func TestWaitingForReconnectHonorsEachCallerContext(t *testing.T) {
	peer, server := newWirePeer(t)
	c, e := Dial(context.Background(), server.URL, "user", "credential", peer.bucket, "fs")
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	peer.stallHandshake.Store(true)
	c.transport.fail(io.EOF)
	ownerCtx, stopOwner := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopOwner()
	ownerDone := make(chan error, 1)
	go func() { _, _, e := c.Get(ownerCtx, ObjectID{1}); ownerDone <- e }()
	select {
	case <-peer.handshakeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect was not attempted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { _, _, e := c.Get(ctx, ObjectID{2}); second <- e }()
	select {
	case e := <-second:
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("a reconnect blocked another caller's cancellation")
	}
	c.Close()
	close(peer.handshakeGate)
	select {
	case <-ownerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop reconnect")
	}
}
