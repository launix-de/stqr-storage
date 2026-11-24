// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// nodeHello binds a token-authenticated socket to its claimed identities. The
// response describes the current committed membership; connecting alone is not admission.
func (s *Server) nodeHello(m client.Message, ws *wsSession, p principal) ([]byte, error) {
	if ws == nil || p.connection == nil || m.Kind != client.Request {
		return nil, fail(400, "NODE_HELLO requires a peer WebSocket request")
	}
	hello, err := client.DecodeNodeHello(m.Body)
	if err != nil {
		return nil, fail(400, "invalid NODE_HELLO")
	}
	s.mu.RLock()
	cluster, _ := parseID(s.cfg.ClusterID)
	node, _ := parseID(s.cfg.Node.ID)
	generation, advertise := s.cfg.Generation, s.cfg.Node.Advertise
	s.mu.RUnlock()
	claimed, err := parseID(p.User.ID)
	if err != nil || hello.Node != client.ID(claimed) {
		return nil, fail(403, "peer identity does not match authenticated node")
	}
	if hello.Cluster != client.ID(cluster) {
		return nil, fail(409, "foreign cluster")
	}
	if hello.Node == client.ID(node) {
		return nil, fail(409, "duplicate local node identity")
	}
	addresses := []string{}
	if advertise != "" {
		addresses = append(addresses, advertise)
	}
	ack := client.NodeHelloAck{Membership: client.Membership{Epoch: 1, Cluster: client.ID(cluster), Members: []client.Member{{ID: client.ID(node), Addresses: addresses}}}, Generation: generation, MapRevision: generation}
	if s.control != nil {
		s.control.ledger.mu.Lock()
		base := cloneControlBase(s.control.ledger.current)
		s.control.ledger.mu.Unlock()
		if hasControlNode(base.Membership.Excluded, hello.Node) {
			return nil, fail(403, "node is excluded from committed membership")
		}
		ack = client.NodeHelloAck{Membership: base.Membership, ControlSequence: base.Sequence, Generation: base.Generation, MapRevision: base.MapRevision}
	}
	if s.joinPending() {
		ack = client.NodeHelloAck{Membership: client.Membership{Cluster: client.ID(cluster)}}
	}
	ack.Boot = s.boot
	body, err := ack.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if p.connection.peerVerified.Load() {
		if p.connection.peerBoot != hello.Boot {
			return nil, fail(409, "peer process identity changed on a live socket")
		}
	} else {
		p.connection.peerBoot = hello.Boot
	}
	p.connection.peerVerified.Store(true)
	s.rememberVersion(hello.Node, p.connection.peerProfile)
	if len(hello.Addresses) > 0 && !s.joinPending() {
		if err := s.rememberPeer(hello.Node, hello.Addresses[0]); err != nil {
			p.connection.peerVerified.Store(false)
			return nil, err
		}
	}
	return body, nil
}

type peerReply struct {
	body []byte
	err  error
}
type peerPending struct {
	opcode uint16
	result chan peerReply
}

// peerConnection is transport infrastructure. Losing it does not change
// membership, ownership, generation, coordinator or required replica counts.
type peerConnection struct {
	server          *Server
	conn            *websocket.Conn
	ctx             context.Context
	cancel          context.CancelFunc
	once            sync.Once
	mu              sync.Mutex
	pending         map[client.ID]peerPending
	outgoing        chan client.Message
	pushes          chan client.Message
	Node            client.ID
	Boot            client.ID
	Membership      client.Membership
	Capabilities    uint64
	Profile         protocolProfile
	incoming        chan client.Message
	controlIncoming chan client.Message
	leaseIncoming   chan client.Message
	ready           chan struct{}
}

func (s *Server) dialPeer(ctx context.Context, address string, expected client.ID) (*peerConnection, error) {
	if err := validateURL(address); err != nil {
		return nil, err
	}
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	u.Scheme = "ws"
	u.Path = "/api/ws"
	u.RawQuery = ""
	u.Fragment = ""
	s.mu.RLock()
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	token := s.cfg.Auth.ClusterToken
	boot := s.boot
	advertise := s.cfg.Node.Advertise
	s.mu.RUnlock()
	headers := http.Header{"Authorization": []string{"Basic " + base64.StdEncoding.EncodeToString([]byte("node-"+node.String()+":"+token))}}
	headers.Set(compatibilityHeader, profileHeader(s.protocolVersion()))
	dialer := websocket.Dialer{Subprotocols: []string{"stqr.v2", "stqr.v1"}, HandshakeTimeout: 10 * time.Second, EnableCompression: false}
	conn, response, err := dialer.DialContext(ctx, u.String(), headers)
	if err != nil {
		if response != nil {
			response.Body.Close()
			return nil, fail(response.StatusCode, "peer WebSocket upgrade rejected")
		}
		return nil, err
	}
	profile, err := decodeProfile(response.Header)
	if err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetReadLimit(client.MaxMessage)
	lifetime, cancel := context.WithCancel(s.ctx)
	p := &peerConnection{server: s, conn: conn, ctx: lifetime, cancel: cancel, pending: map[client.ID]peerPending{}, outgoing: make(chan client.Message, sendQueueCapacity), pushes: make(chan client.Message, 64), incoming: make(chan client.Message, receiveQueueCapacity), ready: make(chan struct{})}
	p.controlIncoming = make(chan client.Message, receiveQueueCapacity)
	p.leaseIncoming = make(chan client.Message, receiveQueueCapacity)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		p.close()
		return nil, errors.New("server is stopping")
	}
	s.workers.Add(5)
	s.mu.Unlock()
	go func() { defer s.workers.Done(); p.writeLoop() }()
	go func() { defer s.workers.Done(); p.readLoop() }()
	go func() { defer s.workers.Done(); p.requestLoop() }()
	go func() { defer s.workers.Done(); p.controlRequestLoop() }()
	go func() { defer s.workers.Done(); p.dispatchRequestQueue(p.leaseIncoming) }()
	success := false
	defer func() {
		if !success {
			p.close()
		}
	}()
	e := client.Encoder{}
	e.U16(1)
	e.U16(2)
	e.U64(client.CapabilityPeerTransport)
	e.Text("stqr-peer")
	result, err := p.request(ctx, client.Hello, e.B)
	if err != nil {
		return nil, err
	}
	d := client.Decoder{B: result}
	version, capabilities := d.U16(), d.U64()
	remoteNode, remoteCluster := d.ID(), d.ID()
	membership := d.U64()
	d.U64()
	if d.Done() != nil || (version != 1 && version != 2) || capabilities&client.CapabilityPeerTransport == 0 || remoteCluster != client.ID(cluster) || remoteNode == (client.ID{}) || remoteNode == client.ID(node) || (expected != (client.ID{}) && remoteNode != expected) {
		return nil, fail(409, "peer identity/cluster/capability mismatch")
	}
	addresses := []string{}
	if advertise != "" {
		addresses = append(addresses, advertise)
	}
	localMembership := uint64(1)
	if s.control != nil {
		s.control.ledger.mu.Lock()
		localMembership = s.control.ledger.current.Membership.Epoch
		s.control.ledger.mu.Unlock()
	}
	if s.joinPending() {
		localMembership = 0
	}
	body, err := (client.NodeHello{Cluster: client.ID(cluster), Node: client.ID(node), Membership: localMembership, Addresses: addresses, Boot: boot}).MarshalBinary()
	if err != nil {
		return nil, err
	}
	result, err = p.request(ctx, client.NodeHelloMessage, body)
	if err != nil {
		return nil, err
	}
	ack, err := client.DecodeNodeHelloAck(result)
	if err != nil || ack.Membership.Cluster != remoteCluster || ack.Membership.Epoch != membership {
		return nil, fail(409, "peer membership response mismatch")
	}
	if capabilities&client.CapabilityExclusion != 0 && ack.Boot == (client.ID{}) {
		return nil, fail(409, "exclusion-capable peer omitted its process identity")
	}
	present := false
	for _, member := range ack.Membership.Members {
		if member.ID == remoteNode {
			present = true
		}
	}
	if capabilities&client.CapabilityPendingNode != 0 {
		if ack.Membership.Epoch != 0 || len(ack.Membership.Members) != 0 || ack.Generation != 0 {
			return nil, fail(409, "pending peer claimed committed membership")
		}
	} else if !present && capabilities&client.CapabilityRetiringNode == 0 {
		return nil, fail(409, "peer is absent from its own current membership")
	}
	s.rememberVersion(remoteNode, profile)
	if !wireCompatible(s.protocolVersion(), profile) {
		return nil, fail(426, "peer wire protocol incompatible; ownership and membership retained")
	}
	p.Profile = profile
	p.Node = remoteNode
	p.Boot = ack.Boot
	p.Membership = ack.Membership
	p.Capabilities = capabilities
	close(p.ready)
	success = true
	return p, nil
}
func (p *peerConnection) close() {
	p.once.Do(func() {
		p.cancel()
		p.conn.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for id, pending := range p.pending {
			pending.result <- peerReply{err: errors.New("peer connection closed")}
			delete(p.pending, id)
		}
	})
}
func (p *peerConnection) request(ctx context.Context, opcode uint16, body []byte) ([]byte, error) {
	if p.Node != (client.ID{}) {
		if err := checkPeerMessage(p.server.protocolVersion(), p.Profile, client.Message{Opcode: opcode, Body: body}); err != nil {
			return nil, err
		}
	}
	if len(body) > client.MaxMessage-20 {
		return nil, client.ErrProtocol
	}
	body = append([]byte(nil), body...)
	id := client.NewRequestID()
	pending := peerPending{opcode: opcode, result: make(chan peerReply, 1)}
	p.mu.Lock()
	if p.ctx.Err() != nil {
		p.mu.Unlock()
		return nil, p.ctx.Err()
	}
	if len(p.pending) >= receiveQueueCapacity {
		p.mu.Unlock()
		return nil, client.ErrWouldBlock
	}
	p.pending[id] = pending
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	select {
	case p.outgoing <- client.Message{Kind: client.Request, Opcode: opcode, ID: id, Body: body}:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	default:
		return nil, client.ErrWouldBlock
	}
	select {
	case result := <-pending.result:
		return result.body, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}
func (p *peerConnection) writeLoop() {
	defer p.close()
	for {
		select {
		case <-p.ctx.Done():
			return
		case m := <-p.outgoing:
			p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if p.conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(m)) != nil {
				return
			}
		}
	}
}
func (p *peerConnection) readLoop() {
	defer p.close()
	for {
		kind, b, err := p.conn.ReadMessage()
		if err != nil || kind != websocket.BinaryMessage {
			return
		}
		m, err := client.DecodeMessage(b)
		if err != nil {
			return
		}
		switch m.Kind {
		case client.Response:
			p.mu.Lock()
			pending, ok := p.pending[m.ID]
			if ok {
				delete(p.pending, m.ID)
			}
			p.mu.Unlock()
			if !ok {
				continue
			}
			pending.result <- decodePeerReply(m, pending.opcode)
		case client.Request, client.FireAndForget:
			queue := p.incoming
			if isLeaseCoordinatorRequest(m) {
				queue = p.leaseIncoming
			} else if isControlPeerRequest(m) {
				queue = p.controlIncoming
			}
			select {
			case queue <- m:
			case <-p.ctx.Done():
				return
			default:
				return
			}
		case client.Push:
			select {
			case p.pushes <- m:
			case <-p.ctx.Done():
				return
			default:
				return
			}
		default:
			return
		}
	}
}
func (p *peerConnection) nextPush(ctx context.Context) (client.Message, error) {
	select {
	case m := <-p.pushes:
		return m, nil
	case <-ctx.Done():
		return client.Message{}, ctx.Err()
	case <-p.ctx.Done():
		return client.Message{}, p.ctx.Err()
	}
}
