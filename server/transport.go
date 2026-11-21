// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// Each node transport session has independent bounded receive and send queues.
// The socket read loop only validates/enqueues; dispatch never blocks framing.
// Requests retain replies, F messages get no reply, telemetry keeps the latest
// complete snapshot. TODO(multinode): reuse these queues for authenticated peers;
// introduce reliable ordered streams when object APPEND dispatch is implemented.
const receiveQueueCapacity = 16
const sendQueueCapacity = 32

type wsSession struct {
	server       *Server
	conn         *websocket.Conn
	request      *http.Request
	principal    principal
	done         chan struct{}
	once         sync.Once
	responses    chan []byte
	telemetry    chan [][]byte
	requests     chan client.Message
	subMu        sync.Mutex
	subscription client.ID
	filter       client.StatsFilter
}

func (ws *wsSession) close() { ws.once.Do(func() { close(ws.done); ws.conn.Close() }) }

// The upgrade already proved the password. Revalidate the authoritative
// account, role, verifier and session on every message without repeating bcrypt.
func (ws *wsSession) principalNow() (principal, bool) {
	p := ws.principal
	if !ws.server.allowed(ws.request) {
		return p, false
	}
	if p.Cookie {
		ws.server.authMu.Lock()
		session, ok := ws.server.sessions[p.SessionToken]
		ws.server.authMu.Unlock()
		if !ok || time.Now().After(session.Expires) || session.UserID != p.User.ID {
			return p, false
		}
	}
	user, e := ws.server.findAccount(p.User.Name)
	if e != nil || user.ID != p.User.ID || !constantEqual(user.PasswordHash, p.User.PasswordHash) || user.Role != "ADMIN" && user.Role != "USER" || p.Cookie && user.Role != "ADMIN" {
		return p, false
	}
	p.User = user
	return p, true
}
func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request, p principal) {
	if p.Cookie {
		ok := false
		for _, sub := range websocket.Subprotocols(r) {
			if constantEqual(sub, "csrf."+p.CSRF) {
				ok = true
			}
		}
		if !ok {
			httpError(w, 403, errors.New("WebSocket CSRF subprotocol required"))
			return
		}
	}
	hasProtocol := false
	for _, p := range websocket.Subprotocols(r) {
		if p == "stqr.v1" {
			hasProtocol = true
		}
	}
	if !hasProtocol {
		httpError(w, 400, errors.New("stqr.v1 subprotocol required"))
		return
	}
	up := websocket.Upgrader{Subprotocols: []string{"stqr.v1"}, CheckOrigin: sameOrigin, EnableCompression: false, ReadBufferSize: 4096, WriteBufferSize: 4096}
	conn, e := up.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	conn.SetReadLimit(client.MaxMessage)
	ws := &wsSession{server: s, conn: conn, request: r, principal: p, done: make(chan struct{}), responses: make(chan []byte, sendQueueCapacity), telemetry: make(chan [][]byte, 1), requests: make(chan client.Message, receiveQueueCapacity)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ws.close()
		return
	}
	s.workers.Add(2)
	s.wsMu.Lock()
	s.sockets[ws] = struct{}{}
	s.wsMu.Unlock()
	s.mu.Unlock()
	defer func() { ws.close(); s.wsMu.Lock(); delete(s.sockets, ws); s.wsMu.Unlock() }()
	go func() { defer s.workers.Done(); ws.writeLoop() }()
	go func() { defer s.workers.Done(); ws.requestLoop() }()
	for {
		kind, b, e := conn.ReadMessage()
		if e != nil {
			return
		}
		if kind != websocket.BinaryMessage {
			return
		}
		m, e := client.DecodeMessage(b)
		if e != nil || m.Kind != client.Request && m.Kind != client.FireAndForget {
			return
		}
		select {
		case ws.requests <- m:
		case <-ws.done:
			return
		default:
			return
		}
	}
}
func (ws *wsSession) writeLoop() {
	defer ws.close()
	for {
		var batch [][]byte
		select {
		case <-ws.done:
			return
		case b := <-ws.responses:
			batch = [][]byte{b}
		case batch = <-ws.telemetry:
		}
		for _, b := range batch {
			ws.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if ws.conn.WriteMessage(websocket.BinaryMessage, b) != nil {
				return
			}
		}
	}
}
func (ws *wsSession) requestLoop() {
	defer ws.close()
	for {
		select {
		case <-ws.done:
			return
		case m := <-ws.requests:
			p, ok := ws.principalNow()
			if !ok {
				return
			}
			body, _ := ws.server.dispatchAs(m, ws, p)
			if m.Kind == client.Request {
				b := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				select {
				case ws.responses <- b:
				case <-ws.done:
					return
				}
			}
		}
	}
}
func (ws *wsSession) subscribe(id client.ID, f client.StatsFilter) {
	ws.subMu.Lock()
	ws.subscription = id
	ws.filter = f
	ws.subMu.Unlock()
	if snap := ws.server.latest.Load(); snap != nil {
		ws.pushStats(*snap)
	}
}
func (ws *wsSession) unsubscribe(id client.ID) {
	ws.subMu.Lock()
	if ws.subscription == id {
		ws.subscription = client.ID{}
	}
	ws.subMu.Unlock()
}
func (ws *wsSession) pushStats(snapshot client.StatsSnapshot) {
	ws.subMu.Lock()
	id, f := ws.subscription, ws.filter
	ws.subMu.Unlock()
	if id == (client.ID{}) {
		return
	}
	p, ok := ws.principalNow()
	if !ok || p.User.Role != "ADMIN" {
		ws.close()
		return
	}
	data := client.EncodeStats(filterSnapshot(snapshot, f))
	digest := sha256.Sum256(data)
	const chunk = 1 << 20
	count := (len(data) + chunk - 1) / chunk
	// One queue slot owns an entire snapshot batch. Coalescing never leaves
	// fragments of an older snapshot mixed with a newer one.
	if count > 64 {
		ws.close()
		return
	}
	batch := make([][]byte, 0, count)
	for part := 0; part < count; part++ {
		e := client.Encoder{}
		e.ID(id)
		e.ID(snapshot.Node)
		e.ID(snapshot.BootID)
		e.U64(snapshot.Sequence)
		e.U32(uint32(part))
		e.U32(uint32(count))
		e.B = append(e.B, digest[:]...)
		e.Blob(data[part*chunk : min(len(data), (part+1)*chunk)])
		eventID, _ := parseID(newID())
		batch = append(batch, client.EncodeMessage(client.Message{Kind: client.Push, Opcode: client.StatsSnapshotMessage, ID: client.ID(eventID), Body: e.B}))
	}
	select {
	case ws.telemetry <- batch:
	default:
		select {
		case <-ws.telemetry:
		default:
		}
		select {
		case ws.telemetry <- batch:
		default:
		}
	}
}
