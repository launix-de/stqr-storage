// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// Each node transport session has independent bounded receive and send queues.
// The socket read loop only validates/enqueues; dispatch never blocks framing.
// Requests retain replies, F messages get no reply, telemetry keeps the latest
// complete snapshot. Authenticated peers and object APPEND use the same bounded
// native transport; separate transport delivery classes remain future work.
const receiveQueueCapacity = 16
const sendQueueCapacity = 32

type terminalFrame struct {
	wire []byte
	done chan struct{}
}

type wsSession struct {
	blockControl       atomic.Bool
	durabilityCap      atomic.Bool
	versionsSubscribed bool   // guarded by subMu
	versionSent        string // guarded by subMu

	terminal        chan terminalFrame
	server          *Server
	ctx             context.Context
	cancel          context.CancelFunc
	uploadMu        sync.Mutex
	uploads         map[client.ID]*serverObjectUpload
	stagedBytes     uint64
	objectMu        sync.Mutex
	objectContexts  map[client.ID]objectRequestContext
	conn            *websocket.Conn
	request         *http.Request
	principal       principal
	authorization   *connectionAuthorization
	done            chan struct{}
	once            sync.Once
	responses       chan []byte
	telemetry       *statsSendQueue
	requests        chan client.Message
	controlRequests chan client.Message
	leaseRequests   chan client.Message
	subMu           sync.Mutex
	subscription    client.ID
	filter          client.StatsFilter
	peerMu          sync.Mutex
	peerPending     map[client.ID]peerPending
}

func (ws *wsSession) close() {
	ws.once.Do(func() {
		if ws.authorization != nil {
			ws.authorization.invalid.Store(true)
		}
		close(ws.done)
		if ws.cancel != nil {
			ws.cancel()
		}
		ws.conn.Close()
	})
}

// Authentication and capabilities were fixed at upgrade. Revocation is pushed
// into the connection by administrative changes; this path performs no lookups.
func (ws *wsSession) principalNow() (principal, bool) {
	return ws.principal, ws.authorization != nil && !ws.authorization.invalid.Load()
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
	for _, sub := range websocket.Subprotocols(r) {
		if sub == "stqr.v2" || sub == "stqr.v1" && p.isPeer() {
			hasProtocol = true
		}
	}
	if !hasProtocol {
		httpError(w, 400, errors.New("stqr.v2 subprotocol required"))
		return
	}
	s.opMu.Lock()
	authorization, p, authErr := s.authorizeConnection(r, p)
	if authErr != nil {
		s.opMu.Unlock()
		httpError(w, statusOf(authErr), authErr)
		return
	}
	if (s.shortStopping.Load() || s.drainClosing.Load()) && !p.isPeer() {
		s.opMu.Unlock()
		httpError(w, 503, errors.New("node is shutting down"))
		return
	}
	up := websocket.Upgrader{Subprotocols: []string{"stqr.v2", "stqr.v1"}, CheckOrigin: sameOrigin, EnableCompression: false, ReadBufferSize: 4096, WriteBufferSize: 4096}
	responseHeaders := http.Header{}
	if p.isPeer() {
		profile, err := decodeProfile(r.Header)
		if err != nil {
			s.opMu.Unlock()
			httpError(w, 400, err)
			return
		}
		authorization.peerProfile = profile
		responseHeaders.Set(compatibilityHeader, profileHeader(s.protocolVersion()))
	}
	conn, e := up.Upgrade(w, r, responseHeaders)
	if e != nil {
		s.opMu.Unlock()
		return
	}
	// Credentials have served their purpose; the live socket keeps capabilities.
	r.Header.Del("Authorization")
	p.User.PasswordHash = ""
	conn.SetReadLimit(client.MaxMessage)
	sessionCtx, sessionCancel := context.WithCancel(s.ctx)
	ws := &wsSession{authorization: authorization, ctx: sessionCtx, cancel: sessionCancel, server: s, conn: conn, request: r, principal: p, done: make(chan struct{}), terminal: make(chan terminalFrame, 1), responses: make(chan []byte, sendQueueCapacity), telemetry: newStatsSendQueue(), requests: make(chan client.Message, receiveQueueCapacity)}
	if p.isPeer() {
		ws.controlRequests = make(chan client.Message, receiveQueueCapacity)
		ws.leaseRequests = make(chan client.Message, receiveQueueCapacity)
	}
	s.mu.Lock()
	if s.closed || (s.shortStopping.Load() || s.drainClosing.Load()) && !p.isPeer() {
		s.mu.Unlock()
		s.opMu.Unlock()
		ws.close()
		return
	}
	s.workers.Add(2)
	if ws.controlRequests != nil {
		s.workers.Add(2)
	}
	s.wsMu.Lock()
	s.sockets[ws] = struct{}{}
	s.wsMu.Unlock()
	s.mu.Unlock()
	s.opMu.Unlock()
	if !authorization.expires.IsZero() {
		expiry := time.AfterFunc(time.Until(authorization.expires), func() { authorization.invalid.Store(true); s.endRevokedSubscriptions(ws) })
		defer expiry.Stop()
	}
	defer func() { ws.close(); s.wsMu.Lock(); delete(s.sockets, ws); s.wsMu.Unlock() }()
	go func() { defer s.workers.Done(); ws.writeLoop() }()
	go func() { defer s.workers.Done(); ws.requestLoop() }()
	if ws.controlRequests != nil {
		go func() { defer s.workers.Done(); ws.controlRequestLoop() }()
		go func() { defer s.workers.Done(); ws.auxiliaryRequestLoop(ws.leaseRequests) }()
	}
	for {
		kind, b, e := conn.ReadMessage()
		if e != nil {
			return
		}
		if kind != websocket.BinaryMessage {
			return
		}
		m, e := client.DecodeMessage(b)
		if e != nil {
			return
		}
		if m.Kind == client.Response && ws.receivePeerReply(m) {
			continue
		}
		if m.Kind != client.Request && m.Kind != client.FireAndForget {
			return
		}
		if isLeaseCoordinatorRequest(m) && ws.leaseRequests != nil && authorization.peerVerified.Load() {
			select {
			case ws.leaseRequests <- m:
			case <-ws.done:
				return
			default:
				return
			}
			continue
		}
		if isControlPeerRequest(m) && ws.controlRequests != nil && authorization.peerVerified.Load() {
			select {
			case ws.controlRequests <- m:
			case <-ws.done:
				return
			default:
				return
			}
			continue
		}
		if m.Opcode >= 0x0100 && m.Opcode <= 0x0108 || m.Opcode == client.ObjectSubscribe || m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketObjectLease || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketStatFS || m.Opcode == client.BucketSync || m.Opcode == client.BucketMaintenance || m.Opcode == client.BucketScan {
			if !ws.registerObjectContext(m.ID) {
				return
			}
		}
		if m.Opcode == 7 {
			decoder := client.Decoder{B: m.Body}
			id := decoder.ID()
			if decoder.Done() != nil {
				return
			}
			ws.cancelObjectContext(id)
		}
		// Upload chunks use TCP backpressure when staging falls behind. Closing
		// every full queue would make a valid large upload retry forever.
		if m.Opcode == 5 || m.Opcode == 6 || m.Opcode == 7 {
			select {
			case ws.requests <- m:
			case <-ws.done:
				return
			}
			continue
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
		case terminal := <-ws.terminal:
			ws.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_ = ws.conn.WriteMessage(websocket.BinaryMessage, terminal.wire)
			close(terminal.done)
			return
		case b := <-ws.responses:
			batch = [][]byte{b}
		case <-ws.telemetry.wake:
			batch = ws.telemetry.take()
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
	defer ws.releaseMaintenance()
	defer ws.server.objectSubscriptions.closeSession(ws)
	defer func() {
		ws.objectMu.Lock()
		for id, r := range ws.objectContexts {
			r.cancel()
			delete(ws.objectContexts, id)
		}
		ws.objectMu.Unlock()
	}()
	defer func() {
		ws.uploadMu.Lock()
		ids := make([]client.ID, 0, len(ws.uploads))
		for id := range ws.uploads {
			ids = append(ids, id)
		}
		ws.uploadMu.Unlock()
		for _, id := range ids {
			ws.discardUpload(id)
		}
	}()
	slots := make(chan struct{}, receiveQueueCapacity)
	var calls sync.WaitGroup
	defer func() { ws.close(); calls.Wait() }()
	startCall := func(run func()) bool {
		select {
		case slots <- struct{}{}:
			calls.Add(1)
			go func() { defer calls.Done(); defer func() { <-slots }(); run() }()
			return true
		default:
			return false
		}
	}
	for {
		select {
		case <-ws.done:
			return
		case m := <-ws.requests:
			p, ok := ws.principalNow()
			if !ok {
				return
			}
			if m.Opcode == 5 || m.Opcode == 6 || m.Opcode == 7 {
				if err := ws.objectTransfer(m, p, startCall); err != nil {
					return
				}
				continue
			}
			if ws.server.launchPGRequest(ws.ctx, m, p, func(body []byte) {
				wire := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				select {
				case ws.responses <- wire:
				case <-ws.done:
				}
			}) {
				continue
			}
			run := func() { ws.executeRequest(m, p) }
			// Global mutations serialize under the controller lock, not the socket
			// reader: a delayed commit must not pin independent dashboard queries.
			parallel := m.Kind == client.Request && (globalAdminMutation(m.Opcode) || m.Opcode >= 0x0100 && m.Opcode <= 0x0108 || m.Opcode == client.ObjectAccepted || m.Opcode == client.ObjectSubscribe || m.Opcode == client.BucketStatFS || m.Opcode == client.BucketSync || m.Opcode == client.BucketObjectList)
			// Upload headers and their following chunks may arrive back-to-back.
			// Establish staging before consuming the next frame, even when the
			// sender does not wait for a response to its upload header.
			if objectUploadHeader(m) {
				parallel = false
			}
			if parallel {
				if !startCall(run) {
					ws.releaseObjectContext(m.ID)
					if err := ws.sendMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(503, nil, "client request concurrency budget exhausted")}); err != nil {
						return
					}
				}
				continue
			}
			run()
		}
	}
}
func (ws *wsSession) executeRequest(m client.Message, p principal) {
	body, status := ws.server.dispatchAs(m, ws, p)
	if status == 0 {
		return
	}
	if m.Opcode >= 0x0100 && m.Opcode <= 0x0108 || m.Opcode == client.ObjectSubscribe || m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketObjectLease || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketStatFS || m.Opcode == client.BucketSync || m.Opcode == client.BucketMaintenance || m.Opcode == client.BucketScan {
		ws.releaseObjectContext(m.ID)
	}
	if m.Kind == client.Request {
		b := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
		select {
		case ws.responses <- b:
		case <-ws.done:
			return
		}
	}
}
func (ws *wsSession) subscribe(id client.ID, f client.StatsFilter) {
	ws.subMu.Lock()
	ws.subscription = id
	ws.filter = f
	ws.telemetry.clear()
	ws.subMu.Unlock()
	for _, snap := range ws.server.statsSnapshots(f) {
		ws.pushStats(snap)
	}
}
func (ws *wsSession) unsubscribe(id client.ID) {
	ws.subMu.Lock()
	if ws.subscription == id {
		ws.subscription = client.ID{}
		ws.telemetry.clear()
	}
	ws.subMu.Unlock()
}
func (ws *wsSession) pushStats(snapshot client.StatsSnapshot) {
	if ws.authorization != nil && ws.authorization.clusterPeer && checkPeerOperation(ws.server.protocolVersion(), ws.authorization.peerProfile, client.StatsSnapshotMessage) != nil {
		return
	}
	ws.subMu.Lock()
	id, f := ws.subscription, ws.filter
	ws.subMu.Unlock()
	if id == (client.ID{}) || !matches(f.Nodes, snapshot.Node) {
		return
	}
	if f.Scope == 0 {
		ws.server.mu.RLock()
		local := ws.server.cfg.Node.ID == snapshot.Node.String()
		ws.server.mu.RUnlock()
		if !local {
			return
		}
	}
	p, ok := ws.principalNow()
	if !ok || (!p.isAdmin() && !p.isPeer()) {
		return
	}
	data := client.EncodeStats(filterSnapshot(snapshot, f))
	digest := sha256.Sum256(data)
	const chunk = 1 << 20
	count := (len(data) + chunk - 1) / chunk
	// Each source owns one complete pending batch; another source cannot
	// overwrite it. Coalescing never mixes incomplete snapshot fragments.
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
	ws.subMu.Lock()
	defer ws.subMu.Unlock()
	if ws.subscription != id {
		return
	}
	if !ws.telemetry.put(snapshot.Node, batch) {
		// The consumer must reconnect/resubscribe rather than receiving silently
		// incomplete cluster telemetry when its bounded budget is exhausted.
		ws.close()
	}
}

// Delegated administration may synchronously coordinate its caller. Keep one
// bounded, separate worker so the ordinary peer queue can process those control
// callbacks. Object operations retain their original ordered session worker.
func (ws *wsSession) controlRequestLoop() { ws.auxiliaryRequestLoop(ws.controlRequests) }
func (ws *wsSession) auxiliaryRequestLoop(queue <-chan client.Message) {
	defer ws.close()
	for {
		select {
		case <-ws.done:
			return
		case m := <-queue:
			p, ok := ws.principalNow()
			if !ok {
				return
			}
			if ws.server.launchPGRequest(ws.ctx, m, p, func(body []byte) {
				wire := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				select {
				case ws.responses <- wire:
				case <-ws.done:
				}
			}) {
				continue
			}
			body, status := ws.server.dispatchAs(m, ws, p)
			if status != 0 && m.Kind == client.Request {
				wire := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				select {
				case ws.responses <- wire:
				case <-ws.done:
					return
				}
			}
		}
	}
}
