// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"

	"github.com/launix-de/stqr-storage/client"
)

// Both directions share the authenticated socket. A node behind a firewall
// does not need a second connection just to answer a reverse control request.
func decodePeerReply(m client.Message, opcode uint16) peerReply {
	if m.Opcode != opcode {
		return peerReply{err: client.ErrProtocol}
	}
	d := client.Decoder{B: m.Body}
	status := d.V()
	headers := d.Blob()
	if d.Err != nil || status < 100 || status > 599 {
		return peerReply{err: client.ErrProtocol}
	}
	result := peerReply{body: append([]byte(nil), d.B...)}
	h := client.Decoder{B: headers}
	message := "peer request rejected"
	for i, n := 0, h.Count(64); i < n; i++ {
		tag, body := h.V(), h.Blob()
		if tag == 3 {
			detail := client.Decoder{B: body}
			message = detail.Text()
			if detail.Done() != nil {
				return peerReply{err: client.ErrProtocol}
			}
		}
	}
	if h.Done() != nil {
		return peerReply{err: client.ErrProtocol}
	}
	if status >= 300 {
		result.err = fail(int(status), fmt.Sprintf("peer status %d: %s", status, message))
	}
	return result
}

func (ws *wsSession) requestPeer(ctx context.Context, opcode uint16, body []byte) ([]byte, error) {
	if ws.authorization == nil || !ws.authorization.peerVerified.Load() || len(body) > client.MaxMessage-20 {
		return nil, client.ErrProtocol
	}
	if err := checkPeerMessage(ws.server.protocolVersion(), ws.authorization.peerProfile, client.Message{Opcode: opcode, Body: body}); err != nil {
		return nil, err
	}
	return ws.requestDuplex(ctx, opcode, body)
}

func (ws *wsSession) requestDuplex(ctx context.Context, opcode uint16, body []byte) ([]byte, error) {
	id := client.NewRequestID()
	pending := peerPending{opcode: opcode, result: make(chan peerReply, 1)}
	ws.peerMu.Lock()
	if len(ws.peerPending) >= receiveQueueCapacity {
		ws.peerMu.Unlock()
		return nil, client.ErrWouldBlock
	}
	if ws.peerPending == nil {
		ws.peerPending = make(map[client.ID]peerPending)
	}
	ws.peerPending[id] = pending
	ws.peerMu.Unlock()
	defer func() { ws.peerMu.Lock(); delete(ws.peerPending, id); ws.peerMu.Unlock() }()
	m := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: opcode, ID: id, Body: body})
	select {
	case ws.responses <- m:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ws.done:
		return nil, context.Canceled
	default:
		return nil, client.ErrWouldBlock
	}
	select {
	case reply := <-pending.result:
		return reply.body, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ws.done:
		return nil, context.Canceled
	}
}

func (ws *wsSession) receivePeerReply(m client.Message) bool {
	if ws.authorization == nil || !ws.authorization.peerVerified.Load() {
		return false
	}
	ws.peerMu.Lock()
	pending, ok := ws.peerPending[m.ID]
	delete(ws.peerPending, m.ID)
	ws.peerMu.Unlock()
	if ok {
		pending.result <- decodePeerReply(m, pending.opcode)
	}
	return true
}

func (p *peerConnection) requestLoop()        { p.dispatchRequestQueue(p.incoming) }
func (p *peerConnection) controlRequestLoop() { p.dispatchRequestQueue(p.controlIncoming) }
func (p *peerConnection) dispatchRequestQueue(queue <-chan client.Message) {
	defer p.close()
	select {
	case <-p.ready:
	case <-p.ctx.Done():
		return
	}
	identity := principal{User: account{ID: p.Node.String(), Name: "node-" + p.Node.String(), Role: "NODE"}, ClusterPeer: true, connection: &connectionAuthorization{clusterPeer: true, peerBoot: p.Boot, peerProfile: p.Profile}}
	identity.connection.peerVerified.Store(true)
	for {
		select {
		case <-p.ctx.Done():
			return
		case m := <-queue:
			if p.server.launchPGRequest(p.ctx, m, identity, func(body []byte) {
				select {
				case p.outgoing <- client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body}:
				case <-p.ctx.Done():
				}
			}) {
				continue
			}
			body, _ := p.server.dispatchAs(m, nil, identity)
			if m.Kind == client.Request {
				select {
				case p.outgoing <- client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body}:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}
}
