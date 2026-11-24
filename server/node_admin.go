// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"

	"github.com/launix-de/stqr-storage/client"
)

// Delegated control operations must leave the ordinary duplex worker free for
// PREPARE and READY callbacks. Both queues are bounded by the peer transport.
func isControlPeerRequest(m client.Message) bool {
	if m.Opcode == peerCowMaterialize {
		return true
	}
	if m.Opcode == peerBucketCopy {
		return true
	}

	if m.Opcode == peerForceAdmin {
		return true
	}
	if m.Opcode == client.PeerAdminRequest {
		request, err := client.DecodePeerAdmin(m.Body)
		return err == nil && request.Opcode == client.VolumeRecoveryCommit
	}
	return false
}

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

func (s *Server) peerAdmin(m client.Message, ws *wsSession, peer principal) ([]byte, error) {
	request, err := client.DecodePeerAdmin(m.Body)
	if err != nil {
		return nil, fail(400, "invalid peer administration request")
	}
	// The trusted peer delegates an identity, never a claimed role. Revalidate
	// against our system bucket before dispatch; mutations revalidate again under
	// opMu. Neither passwords nor remote volume configurations are replicated here.
	var actor principal
	if s.joinPending() {
		// A pending candidate has no authoritative account replica. Authenticate
		// this operation against its explicitly configured seed over the same
		// duplex link. Direct browser/client authentication remains disabled.
		if ws == nil {
			return nil, fail(503, "pending-node administration requires the seed link")
		}
		s.mu.RLock()
		trusted := false
		for _, contact := range s.cfg.Peers {
			trusted = trusted || contact.NodeID == peer.User.ID
		}
		s.mu.RUnlock()
		if !trusted {
			return nil, fail(403, "only a configured seed may administer a pending node")
		}
		e := client.Encoder{}
		e.ID(request.Actor)
		e.Text(request.Username)
		body, err := ws.requestPeer(ws.ctx, client.PeerVerifyAdmin, e.B)
		if err != nil {
			return nil, fail(403, "seed did not authorize the delegated administrator")
		}
		d := client.Decoder{B: body}
		id, name := d.ID(), d.Text()
		if d.Done() != nil || id != request.Actor || name != request.Username {
			return nil, fail(403, "invalid delegated administrator proof")
		}
		actor = principal{User: account{ID: id.String(), Name: name, Role: "ADMIN"}, connection: &connectionAuthorization{admin: true}}
	} else {
		user, err := s.findAccount(request.Username)
		if err != nil || user.ID != request.Actor.String() || user.Role != "ADMIN" || user.Disabled {
			return nil, fail(403, "delegated administrator is not authorized on target")
		}
		actor = principal{User: user}
	}
	inner := client.Message{Kind: client.Request, Opcode: request.Opcode, ID: request.Operation, Body: request.Body}
	result, _ := s.dispatchAs(inner, nil, actor)
	return result, nil
}

func (s *Server) verifyDelegatedAdmin(body []byte) ([]byte, error) {
	if s.joinPending() {
		return nil, fail(503, "node has no committed account authority")
	}
	d := client.Decoder{B: body}
	id, name := d.ID(), d.Text()
	if d.Done() != nil || id == (client.ID{}) {
		return nil, fail(400, "invalid delegated administrator identity")
	}
	user, err := s.findAccount(name)
	if err != nil || user.ID != id.String() || user.Role != "ADMIN" || user.Disabled {
		return nil, fail(403, "delegated administrator is not authorized")
	}
	e := client.Encoder{}
	e.ID(id)
	e.Text(name)
	return e.B, nil
}

// Lease coordination must not queue behind long delegated copies, nor occupy
// the ordinary worker needed by quota/control callbacks while participants drain.
func isLeaseCoordinatorRequest(m client.Message) bool {
	if m.Opcode >= peerCowCapture && m.Opcode <= peerCowSync && m.Opcode != peerCowMaterialize {
		return true
	}
	if m.Opcode != peerBucketLease {
		return false
	}
	var r struct{ Action uint8 }
	return len(m.Body) <= 8192 && json.Unmarshal(m.Body, &r) == nil && (r.Action == 0 || r.Action == 2)
}
