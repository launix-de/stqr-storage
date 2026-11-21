// SPDX-License-Identifier: GPL-3.0-only
package server

import "github.com/launix-de/stqr-storage/client"

func (s *Server) nodeAdmin(m client.Message, ws *wsSession, p principal) ([]byte, int) {
	failure := func(status int, detail string) ([]byte, int) {
		return client.ResponseBody(status, nil, detail), status
	}
	if !p.isAdmin() {
		return failure(403, "administrator required")
	}
	request, err := client.DecodeNodeAdmin(m.Body)
	if err != nil {
		return failure(400, "invalid node administration request")
	}
	inner := client.Message{Kind: client.Request, Opcode: request.Opcode, ID: m.ID, Body: request.Body}
	s.mu.RLock()
	local := s.cfg.Node.ID == request.Target.String()
	s.mu.RUnlock()
	if local {
		return s.dispatchAs(inner, nil, p)
	}
	s.peerStatsMu.RLock()
	observation := s.peerStats[request.Target]
	var peer *peerConnection
	if observation != nil && observation.online {
		peer = observation.connection
	}
	s.peerStatsMu.RUnlock()
	if peer == nil || peer.ctx.Err() != nil {
		return failure(503, "target node is currently unavailable")
	}
	if peer.Capabilities&client.CapabilityNodeAdmin == 0 {
		return failure(501, "target node does not support delegated administration")
	}
	actor, err := parseID(p.User.ID)
	if err != nil {
		return failure(403, "invalid administrator identity")
	}
	body, err := (client.PeerAdmin{Actor: client.ID(actor), Username: p.User.Name, Operation: m.ID, Opcode: request.Opcode, Body: request.Body}).MarshalBinary()
	if err != nil {
		return failure(400, "invalid delegated administration request")
	}
	ctx := s.ctx
	if ws != nil {
		ctx = ws.ctx
	}
	result, err := peer.request(ctx, client.PeerAdminRequest, body)
	if err != nil {
		return failure(statusOf(err), "target node administration failed")
	}
	d := client.Decoder{B: result}
	status := d.V()
	d.Blob()
	if d.Err != nil || status < 100 || status > 599 {
		return failure(502, "invalid target node response")
	}
	return result, int(status)
}

func (s *Server) peerAdmin(m client.Message) ([]byte, error) {
	request, err := client.DecodePeerAdmin(m.Body)
	if err != nil {
		return nil, fail(400, "invalid peer administration request")
	}
	// The trusted peer delegates an identity, never a claimed role. Revalidate
	// against our system bucket before dispatch; mutations revalidate again under
	// opMu. Neither passwords nor remote volume configurations are replicated here.
	user, err := s.findAccount(request.Username)
	if err != nil || user.ID != request.Actor.String() || user.Role != "ADMIN" || user.Disabled {
		return nil, fail(403, "delegated administrator is not authorized on target")
	}
	inner := client.Message{Kind: client.Request, Opcode: request.Opcode, ID: request.Operation, Body: request.Body}
	result, _ := s.dispatchAs(inner, nil, principal{User: user})
	return result, nil
}
