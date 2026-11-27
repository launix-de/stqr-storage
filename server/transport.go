// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Each storage-bearing message starts its own goroutine; the transport never
// sorts completions or waits for a preceding operation before dispatch. Bounded
// framing/send buffers provide TCP backpressure, not a storage worker pool.
// One writer serializes WebSocket frames. Negotiated no-ACK mutations report
// failures individually and through ConnectionSync; successful writes need no
// reply. Closing a socket discards its RAM-only tracking without a checkpoint.
// Telemetry keeps the latest complete snapshot. Delivery classes are future work.
const receiveQueueCapacity = 16
const sendQueueCapacity = 32
const maxPendingUploadFrames = 64
const maxPendingUploadFrameBytes = 16 << 20

type terminalFrame struct {
	wire []byte
	done chan struct{}
}

type wsSession struct {
	acceptedReferencesCap   atomic.Bool
	acceptedStreamsCap      atomic.Bool
	connectionFenceCap      atomic.Bool
	fenceMu                 sync.Mutex
	connectionSequence      uint64
	connectionJobs          map[client.ID]*connectionWork
	connectionTouches       map[*volume.Volume]uint64
	connectionFences        map[client.ID]*connectionFence
	connectionFailures      []connectionFailure
	uploadTails             map[client.ID]chan struct{}
	frameMu                 sync.Mutex
	pendingUploadFrames     int
	pendingUploadFrameBytes int
	frameWake               chan struct{}

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
		if ws.conn != nil {
			ws.conn.Close()
		}
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
	// Reject malformed upgrades before a first connection can bind a format.
	key, keyErr := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if !websocket.IsWebSocketUpgrade(r) || r.Header.Get("Sec-WebSocket-Version") != "13" || keyErr != nil || len(key) != 16 {
		httpError(w, 400, errors.New("valid WebSocket upgrade required"))
		return
	}
	// Opening may persist an unset format. Check every connection prerequisite
	// first, including session, lease and shutdown restrictions. The unset-format
	// exception is only for this side-effect-free preflight, never registration.
	s.opMu.Lock()
	preflight, _, authErr := s.authorizeConnection(r, p, true)
	// Freeze the authorized ID before format binding releases admission. A name
	// may be renamed/reused while a cluster metadata commit is in progress.
	if authErr == nil && preflight.bucket != nil {
		r.Header.Set(client.BucketHeader, preflight.bucketID.String())
	}
	s.opMu.Unlock()
	if authErr != nil {
		httpError(w, statusOf(authErr), authErr)
		return
	}
	if raw := r.Header.Get(client.BucketHeader); raw != "" {
		if err := s.openBucketFormat(r.Context(), raw, r.Header.Get(client.FormatHeader), p); err != nil {
			httpError(w, statusOf(err), err)
			return
		}
	}

	// Binding uses the existing cluster metadata commit without holding opMu.
	// Recheck the fresh capability and committed format under the registration
	// lock so ACL/session/lease changes during that commit cannot admit a socket.
	s.opMu.Lock()
	authorization, p, authErr := s.authorizeConnection(r, p, false)
	if authErr != nil {
		s.opMu.Unlock()
		httpError(w, statusOf(authErr), authErr)
		return
	}
	up := websocket.Upgrader{Subprotocols: []string{"stqr.v2", "stqr.v1"}, CheckOrigin: s.browserOriginAllowed, EnableCompression: false, ReadBufferSize: 4096, WriteBufferSize: 4096}
	responseHeaders := http.Header{}
	if authorization.bucket != nil {
		responseHeaders.Set(client.BucketHeader, authorization.bucketID.String())
	}
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
			}
			continue
		}
		if isControlPeerRequest(m) && ws.controlRequests != nil && authorization.peerVerified.Load() {
			select {
			case ws.controlRequests <- m:
			case <-ws.done:
				return
			}
			continue
		}
		if m.Opcode != 5 && m.Opcode != 6 && m.Opcode != 7 {
			if !ws.admitConnectionMessage(m) {
				return
			}
		}
		if usesObjectRequestContext(m.Opcode) {
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
			if !ws.reserveUploadFrame(len(m.Body)) {
				return
			}
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
	var calls sync.WaitGroup
	defer func() {
		ws.close()
		calls.Wait()
		// No checkpoint on disconnect. Drop RAM-only bookkeeping only after
		// admitted callbacks have stopped writing their completion records.
		ws.fenceMu.Lock()
		ws.connectionJobs = nil
		ws.connectionTouches = nil
		ws.connectionFences = nil
		ws.connectionFailures = nil
		ws.uploadTails = nil
		ws.fenceMu.Unlock()
	}()
	startCall := func(run func()) bool {
		calls.Add(1)
		go func() { defer calls.Done(); run() }()
		return true
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
				d := client.Decoder{B: m.Body}
				id := d.ID()
				previous, finished := ws.connectionUploadFrame(id)
				startCall(func() {
					defer ws.releaseUploadFrame(len(m.Body))
					defer ws.finishConnectionUploadFrames(id, finished)
					if previous != nil {
						select {
						case <-previous:
						case <-ws.done:
							return
						}
					}
					if err := ws.objectTransfer(m, p, startCall); err != nil {
						ws.close()
					}
				})
				continue
			}
			if ws.server.launchPGRequest(ws.ctx, m, p, func(body []byte) {
				defer ws.completeConnectionMessage(m, 200, body)
				wire := client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				select {
				case ws.responses <- wire:
				case <-ws.done:
				}
			}) {
				continue
			}
			// Global mutations serialize under the controller lock, never the
			// socket reader. Storage headers run independently as well: transfer
			// frames wait only for their own parent's headerReady, above.
			if m.Opcode == client.Hello || m.Opcode == client.Ping {
				ws.executeRequest(m, p)
			} else {
				startCall(func() { ws.executeRequest(m, p) })
			}

		}
	}
}
func (ws *wsSession) executeRequest(m client.Message, p principal) {
	defer ws.connectionHeaderReady(m.ID)
	body, status := ws.server.dispatchAs(m, ws, p)
	if status == 0 {
		return
	}
	defer ws.completeConnectionMessage(m, status, body)
	if usesObjectRequestContext(m.Opcode) {
		ws.releaseObjectContext(m.ID)
	}
	if m.Kind == client.Request || m.Kind == client.FireAndForget && ws.connectionFenceCap.Load() && (status >= 300 || m.Opcode == 0x0100 || m.Opcode == 0x0101 || m.Opcode == 0x0107 || m.Opcode == client.ConnectionSync) {
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

// Peer framing channels are drained independently; each storage-bearing
// callback runs in its own goroutine, exactly like ordinary client messages.
func (ws *wsSession) controlRequestLoop() { ws.auxiliaryRequestLoop(ws.controlRequests) }
func (ws *wsSession) auxiliaryRequestLoop(queue <-chan client.Message) {
	var calls sync.WaitGroup
	defer func() { ws.close(); calls.Wait() }()
	for {
		select {
		case <-ws.done:
			return
		case m := <-queue:
			p, ok := ws.principalNow()
			if !ok {
				return
			}
			calls.Add(1)
			go func() {
				defer calls.Done()
				if ws.server.launchPGRequest(ws.ctx, m, p, func(body []byte) {
					_ = ws.sendMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				}) {
					return
				}
				body, status := ws.server.dispatchAs(m, ws, p)
				if status != 0 && (m.Kind == client.Request || status >= 300) {
					_ = ws.sendMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
				}
			}()
		}
	}
}

// Reserve before enqueueing, so waiting callbacks and the receive queue share
// one budget. Only upload frame admission waits; storage calls keep independent
// goroutines. The receiver holds at most one additional decoded frame.
func (ws *wsSession) reserveUploadFrame(n int) bool {
	if n > maxPendingUploadFrameBytes {
		return false
	}
	for {
		ws.frameMu.Lock()
		select {
		case <-ws.done:
			ws.frameMu.Unlock()
			return false
		default:
		}
		if ws.frameWake == nil {
			ws.frameWake = make(chan struct{})
		}
		if ws.pendingUploadFrames < maxPendingUploadFrames && n <= maxPendingUploadFrameBytes-ws.pendingUploadFrameBytes {
			ws.pendingUploadFrames++
			ws.pendingUploadFrameBytes += n
			ws.frameMu.Unlock()
			return true
		}
		wake := ws.frameWake
		ws.frameMu.Unlock()
		select {
		case <-wake:
		case <-ws.done:
			return false
		}
	}
}
func (ws *wsSession) releaseUploadFrame(n int) {
	ws.frameMu.Lock()
	ws.pendingUploadFrames--
	ws.pendingUploadFrameBytes -= n
	close(ws.frameWake)
	ws.frameWake = make(chan struct{})
	ws.frameMu.Unlock()
}
