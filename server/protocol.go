// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func encodeAck(op Operation) []byte {
	e := client.Encoder{}
	id, _ := parseID(op.ID)
	e.ID(client.ID(id))
	state := uint8(0)
	switch op.State {
	case "running":
		state = 1
	case "fulfilled":
		state = 2
	case "failed":
		state = 3
	}
	e.U8(state)
	entity := op.VolumeID
	if entity == "" {
		entity = op.BucketID
	}
	e.Bool(entity != "")
	if entity != "" {
		vid, _ := parseID(entity)
		e.ID(client.ID(vid))
	}
	return e.B
}
func jsonText(v any) []byte {
	b, _ := json.Marshal(v)
	e := client.Encoder{}
	e.Text(string(b))
	return e.B
}

// Normal bucket connections can negotiate and use only object/routing messages.
// This gate is shared by HTTP and WebSocket, not claimed by a client enum.
func (s *Server) dispatchAs(m client.Message, ws *wsSession, p principal) ([]byte, int) {
	if p.connection != nil {
		if p.connection.invalid.Load() {
			return client.ResponseBody(403, nil, "connection authorization revoked"), 403
		}
		if p.connection.frontendPermission != 0 {
			if err := checkPeerMessage(s.protocolVersion(), p.connection.peerProfile, m); err != nil {
				return client.ResponseBody(426, nil, err.Error()), 426
			}
		}
		if p.connection.bucket != nil && m.Opcode != client.Hello && m.Opcode != client.Ping && !(m.Opcode >= 0x0100 && m.Opcode <= client.BucketObjectList || m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketObjectLease || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketStatFS || m.Opcode == client.BucketSync || m.Opcode == client.ConnectionSync || m.Opcode == client.ObjectReference) {
			return client.ResponseBody(403, nil, "bucket connection has no administrative privileges"), 403
		}
	}
	if s.storageWarming.Load() && !storageWarmReadMessage(m) {
		return client.ResponseBody(503, nil, s.storageUnavailable().Error()), 503
	}
	if m.Opcode == client.BucketFormatOpen {
		return s.openSocketBucketFormat(m, ws, p)
	}
	if err := checkSocketBucketFormat(m, p); err != nil {
		status := statusOf(err)
		return client.ResponseBody(status, nil, err.Error()), status
	}

	if m.Opcode >= clusterJoinStatus && m.Opcode <= clusterJoinPrepare {
		if ws != nil && m.Opcode == clusterJoinPrepare {
			return client.ResponseBody(400, nil, "use the native HTTP message endpoint for join"), 400
		}
		body, status, err := s.executeClusterJoin(m, p)
		if err != nil {
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, body, ""), status
	}
	if m.Opcode == nodeVersionsMessage {
		if !p.isAdmin() {
			return client.ResponseBody(403, nil, "administrator required"), 403
		}
		if ws == nil || m.Kind != client.Request {
			return client.ResponseBody(400, nil, "version subscription requires a WebSocket request"), 400
		}
		d := client.Decoder{B: m.Body}
		enabled := d.Bool()
		if d.Done() != nil {
			return client.ResponseBody(400, nil, "invalid version subscription"), 400
		}
		ws.subMu.Lock()
		ws.versionsSubscribed = enabled
		ws.versionSent = ""
		ws.subMu.Unlock()
		return client.ResponseBody(200, jsonText(s.versionsView()), ""), 200
	}
	if m.Opcode == client.NodeHelloMessage {
		if !p.isPeer() {
			return client.ResponseBody(403, nil, "cluster peer required"), 403
		}
		result, err := s.nodeHello(m, ws, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(200, result, ""), 200
	}
	if p.isPeer() {
		if m.Opcode == client.Hello || m.Opcode == client.Ping {
			return s.dispatch(m, ws)
		}
		if p.connection == nil || !p.connection.peerVerified.Load() {
			return client.ResponseBody(403, nil, "NODE_HELLO identity binding required"), 403
		}
		if err := checkPeerMessage(s.protocolVersion(), p.connection.peerProfile, m); err != nil {
			return client.ResponseBody(426, nil, err.Error()), 426
		}
		if view := s.currentSystem.Load(); view != nil {
			node, _ := parseID(p.User.ID)
			if hasControlNode(view.manifest.Membership.Excluded, client.ID(node)) {
				return client.ResponseBody(403, nil, "peer is excluded from committed membership"), 403
			}
		}
		if m.Opcode == peerBucketFormat {
			result, err := s.receiveBucketFormat(s.ctx, m.Body)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}

		if m.Opcode == peerRecoveryReady {
			actor, _ := parseID(p.User.ID)
			result, err := s.recoveryReady(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceAdoptCommit {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveForceAdoptCommit(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceConstruction {
			actor, _ := parseID(p.User.ID)
			err := s.receiveForceConstruction(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, nil, ""), 200
		}
		if m.Opcode == peerForceControlState {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveForceControlState(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode >= peerCowCapture && m.Opcode <= peerCowSync {
			actor, _ := parseID(p.User.ID)
			view := s.currentSystem.Load()
			if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), client.ID(actor)) {
				return client.ResponseBody(403, nil, "admitted clone peer required"), 403
			}
			result, err := s.cowPeerMessage(s.ctx, m)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketCopy {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveBucketCopy(s.ctx, m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode >= peerPGRebuildRequest && m.Opcode <= peerPGRebuildRanges {
			actor, _ := parseID(p.User.ID)
			ctx := s.ctx
			if p.requestContext != nil {
				ctx = p.requestContext
			} else if ws != nil {
				ctx = ws.ctx
			}
			result, err := s.pgRebuildPeer(ctx, m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketLease {
			actor, _ := parseID(p.User.ID)
			ctx := s.ctx
			if p.requestContext != nil {
				ctx = p.requestContext
			} else if ws != nil {
				ctx = ws.ctx
			}
			result, err := s.receiveBucketLease(ctx, m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceReplicaState {
			actor, _ := parseID(p.User.ID)
			result, err := s.forceReplicaState(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceScopes {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveForceScopes(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceScopeEnvelope {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveForceScopeEnvelope(m.Body, client.ID(actor), p.connection.peerBoot)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerForceAdmin {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveForceAdmin(s.ctx, m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketUsage {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveBucketUsage(s.ctx, m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if quotaPeerOpcode(m.Opcode) {
			actor, _ := parseID(p.User.ID)
			result, err := s.quotaPeerMessage(m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if controlPeerOpcode(m.Opcode) {
			actor, err := parseID(p.User.ID)
			if err != nil {
				return client.ResponseBody(403, nil, "invalid peer identity"), 403
			}
			result, err := s.executePeerControl(m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerClusterStop {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveClusterStop(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketInventory {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveBucketInventory(s.ctx, m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerVerifyFrontend {
			actor, _ := parseID(p.User.ID)
			result, err := s.verifyFrontend(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketCodec {
			actor, _ := parseID(p.User.ID)
			result, err := s.applyBucketCodec(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerBucketPurge {
			actor, _ := parseID(p.User.ID)
			result, err := s.purgeDeletedBucket(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerFrozenBucketUsage {
			actor, _ := parseID(p.User.ID)
			result, err := s.frozenBucketUsage(m.Body, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerNodeLeave {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveNodeLeave(m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerControlInfo || m.Opcode == peerSystemChunk {
			result, err := s.systemPeerMessage(m)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerReplicaAbortHistory {
			actor, _ := parseID(p.User.ID)
			result, err := s.receiveReplicaAbortHistory(m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == peerReplicaForget || m.Opcode >= peerReplicaPrepare && m.Opcode <= peerReplicaAbort || m.Opcode >= peerReplicaChunk && m.Opcode <= peerReplicaTopologyCleanup {
			actor, _ := parseID(p.User.ID)
			result, err := s.replicaPeerMessage(m, client.ID(actor))
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode >= peerRangeReserve && m.Opcode <= peerRangeRAMLostCommit {
			actor, _ := parseID(p.User.ID)
			result, err := s.rangePeerMessage(m, client.ID(actor), p.connection.peerBoot)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == client.PeerAdminRequest {
			result, err := s.peerAdmin(m, ws, p)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == client.PeerVerifyAdmin {
			result, err := s.verifyDelegatedAdmin(m.Body)
			if err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, result, ""), 200
		}
		if m.Opcode == client.PeerDirectoryMessage {
			announcement, err := client.DecodePeerDirectory(m.Body)
			s.mu.RLock()
			cluster := s.cfg.ClusterID
			s.mu.RUnlock()
			if err != nil || announcement.Sender.String() != p.User.ID || announcement.Cluster.String() != cluster {
				return client.ResponseBody(400, nil, "invalid peer directory announcement"), 400
			}
			if err = s.mergePeerDirectory(announcement.Nodes); err != nil {
				status := statusOf(err)
				return client.ResponseBody(status, nil, err.Error()), status
			}
			return client.ResponseBody(200, nil, ""), 200
		}
		if m.Opcode == client.StatsGet || m.Opcode == client.StatsSubscribe || m.Opcode == client.StatsUnsubscribe || (m.Opcode >= 0x1000 && m.Opcode < 0x1100) {
			return s.dispatch(m, ws)
		}
		return client.ResponseBody(403, nil, "cluster peer operation required"), 403
	}

	if !p.isAdmin() && m.Opcode != client.Hello && m.Opcode != client.Ping && !(m.Opcode >= 0x0100 && m.Opcode <= client.BucketObjectList || m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketObjectLease || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketStatFS || m.Opcode == client.BucketSync || m.Opcode == client.ConnectionSync || m.Opcode == client.ObjectReference) {
		return client.ResponseBody(403, nil, "administrator required"), 403
	}

	if s.clusterPaused() || s.forceFrozen() {
		switch m.Opcode {
		case client.VolumeAddWithGroups, client.VolumePlacementRelease, client.VolumeAdd, client.VolumeRemove, client.VolumeRescan, client.VolumeGrow, client.NodeConfigSet, client.NodeDrain:
			return client.ResponseBody(503, nil, "cluster shutdown is pending"), 503
		}
	}
	if m.Opcode == client.VolumeRecoveryStage || m.Opcode == client.VolumeRecoveryCommit || m.Opcode == client.VolumeRecoveryRelease {
		result, status, err := s.executeVolumeRecovery(m, p)
		if err != nil {
			status = statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if m.Opcode == client.ForceDisconnect || m.Opcode == client.NodeRecoveryBegin {
		result, status, err := s.forceDisconnect(m, p)
		if err != nil {
			status = statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if m.Opcode == client.NodeShutdown {
		result, err := s.nodeShutdownAdmin(m, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(202, result, ""), 202
	}
	if m.Opcode == client.NodeDrain {
		result, err := s.nodeDrainAdmin(m, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(202, result, ""), 202
	}
	if m.Opcode == client.NodeAdminRequest {
		return s.nodeAdmin(m, ws, p)
	}
	if m.Opcode == client.ClusterShutdown {
		d := client.Decoder{B: m.Body}
		if d.U8() != 2 || d.Done() != nil || s.controller == nil || m.Kind != client.Request || m.ID == (client.ID{}) {
			return client.ResponseBody(400, nil, "invalid cluster shutdown request"), 400
		}
		result, err := s.beginClusterShutdown(m.ID, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(202, result, ""), 202
	}
	if s.joinPending() && (m.Opcode >= 0x0100 && m.Opcode <= 0x01ff || bucketAdminOpcode(m.Opcode) || userOpcode(m.Opcode)) {
		return client.ResponseBody(503, nil, "node is waiting for committed cluster admission"), 503
	}
	// Metadata uses the authenticated connection capability, never role/rights
	// claimed in message contents. Changes revoke cached socket grants.
	if s.controller != nil && globalAdminMutation(m.Opcode) {
		if s.shortStopping.Load() || s.clusterPaused() || s.forceFrozen() {
			return client.ResponseBody(503, nil, "node is pausing for maintenance"), 503
		}
		result, status, err := s.executeGlobalAdmin(m, p)
		if err != nil {
			status = statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if m.Opcode == client.MapGetMessage {
		result, err := s.getObjectMap(m, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(200, result, ""), 200
	}
	if m.Opcode == client.BucketInfoGet {
		result, err := s.bucketInfo(m, p)
		if err != nil {
			status := statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(200, result, ""), 200
	}
	if m.Opcode == client.BucketMaintenanceStatus {
		if err := s.requireAdmin(p); err != nil {
			return client.ResponseBody(403, nil, err.Error()), 403
		}
		if len(m.Body) != 0 {
			return client.ResponseBody(400, nil, "empty body required"), 400
		}
		if ids, epoch, err := s.leaseMembers(); err == nil && len(ids) > 1 {
			l := bucketLease{Epoch: epoch, Token: client.NewRequestID()}
			for _, node := range ids {
				ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
				_, err := s.callLease(ctx, node, 4, l)
				cancel()
				if err != nil {
					return client.ResponseBody(statusOf(err), nil, err.Error()), statusOf(err)
				}
			}
		}
		return client.ResponseBody(200, nil, ""), 200
	}
	if m.Opcode == client.BucketClear || m.Opcode == client.BucketClone {
		result, err := s.executeBucketCopy(s.ctx, m, p)
		if err != nil {
			status := objectStatus(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(200, result, ""), 200
	}
	if bucketAdminOpcode(m.Opcode) {
		result, status, err := s.executeBuckets(m, p)
		if err != nil {
			status = statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if placementAdminOpcode(m.Opcode) {
		result, status, err := s.executePlacementAdmin(m, p)
		if err != nil {
			status = statusOf(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if (m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketSync || m.Opcode == client.ConnectionSync || m.Opcode == client.ObjectReference) && (ws == nil || !ws.durabilityCap.Load()) {
		return client.ResponseBody(501, nil, "durability capability was not negotiated"), 501
	}
	if m.Opcode == client.ObjectReference {
		ctx := s.ctx
		if ws != nil {
			ctx = ws.objectContext(m.ID)
		}
		result, err := s.executeObjectReference(ctx, m, ws, p)
		if err != nil {
			var redirect *objectRedirect
			if errors.As(err, &redirect) {
				return redirectResponse(redirect.patch), 301
			}
			status := objectStatus(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		var bucket volume.ID
		if len(m.Body) >= len(bucket) {
			copy(bucket[:], m.Body[:len(bucket)])
		}
		return s.durabilityResponse(200, result, bucket), 200
	}
	if m.Opcode == client.BucketStatFS {
		ctx := s.ctx
		if ws != nil {
			ctx = ws.objectContext(m.ID)
		}
		result, err := s.executeBucketStatFS(ctx, m, p)
		if err != nil {
			status := objectStatus(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(200, result, ""), 200
	}
	if m.Opcode == client.ConnectionSync {
		if ws == nil {
			return client.ResponseBody(400, nil, "connection sync requires a WebSocket"), 400
		}
		return ws.executeConnectionSync(ws.ctx, m)
	}
	if m.Opcode == client.BucketSync {
		ctx := s.ctx
		if ws != nil {
			ctx = ws.objectContext(m.ID)
		}
		result, status, err := s.executeSync(ctx, m, ws, p)
		if err != nil {
			status = objectStatus(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if m.Opcode == client.ObjectSubscribe || m.Opcode == client.ObjectUnsubscribe || m.Opcode == client.SubscriptionAck {
		result, status, err := s.executeObjectSubscription(m, ws, p)
		if err != nil {
			var redirect *objectRedirect
			if errors.As(err, &redirect) {
				return redirectResponse(redirect.patch), 301
			}
			status = objectStatus(err)
			return client.ResponseBody(status, nil, err.Error()), status
		}
		if m.Opcode == client.ObjectSubscribe {
			var bucket volume.ID
			if len(m.Body) >= 16 {
				copy(bucket[:], m.Body[:16])
			}
			return s.durabilityResponse(status, result, bucket), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	if m.Opcode >= 0x0100 && m.Opcode <= 0x0108 || m.Opcode == client.ObjectAccepted || m.Opcode == client.BucketObjectLease || m.Opcode == client.BucketMaintenanceEnd || m.Opcode == client.BucketMaintenance || m.Opcode == client.BucketScan || m.Opcode == client.BucketObjectList {
		result, status, err := s.executeObjects(m, ws, p)
		if errors.Is(err, errObjectStaged) {
			return nil, 0
		}
		if err != nil {
			status = objectStatus(err)
			var redirect *objectRedirect
			if errors.As(err, &redirect) {
				return redirectResponse(redirect.patch), 301
			}
			return client.ResponseBody(status, nil, err.Error()), status
		}
		var bucket volume.ID
		if len(m.Body) >= 16 {
			copy(bucket[:], m.Body[:16])
		}
		return s.durabilityResponse(status, result, bucket), status
	}
	if userOpcode(m.Opcode) {
		result, status, e := s.executeUsers(m, p)
		if e != nil {
			status = statusOf(e)
			return client.ResponseBody(status, nil, e.Error()), status
		}
		return client.ResponseBody(status, result, ""), status
	}
	return s.dispatch(m, ws)
}
func (s *Server) dispatch(m client.Message, ws *wsSession) ([]byte, int) {
	result, status, err := s.execute(m, ws)
	if err != nil {
		status = statusOf(err)
		return client.ResponseBody(status, nil, err.Error()), status
	}
	if m.Opcode == client.StatsGet {
		e := client.Encoder{}
		e.V(uint64(status))
		h := client.Encoder{}
		h.V(1)
		h.V(9)
		h.Blob([]byte{0})
		e.Blob(h.B)
		e.B = append(e.B, result...)
		return e.B, status
	}
	return client.ResponseBody(status, result, ""), status
}
func (s *Server) execute(m client.Message, ws *wsSession) ([]byte, int, error) {
	d := client.Decoder{B: m.Body}
	guard := func() error {
		if e := d.Done(); e != nil {
			return fail(400, e.Error())
		}
		return nil
	}
	switch m.Opcode {
	case client.Hello:
		min, max := d.U16(), d.U16()
		requestedCapabilities := d.U64()
		referenceSupported := ws == nil || referencePrincipalSupported(ws.principal)
		if !referenceSupported {
			requestedCapabilities &^= client.CapabilityObjectReferences | client.CapabilityAcceptedReferences
		}
		if ws != nil && ws.authorization != nil {
			ws.authorization.objectReferences.Store(requestedCapabilities&client.CapabilityObjectReferences != 0)
		}
		if ws != nil {
			ws.durabilityCap.Store(requestedCapabilities&client.CapabilityDurability != 0)
			ws.connectionFenceCap.Store(requestedCapabilities&client.CapabilityConnectionSync != 0)
			ws.acceptedReferencesCap.Store(requestedCapabilities&client.CapabilityAcceptedReferences != 0)
			ws.acceptedStreamsCap.Store(requestedCapabilities&capabilityAcceptedStreams != 0)
		}
		d.Text()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		if min > 2 || max < 1 || (max < 2 && (ws == nil || ws.authorization == nil || !ws.authorization.clusterPeer)) {
			return nil, 0, fail(400, "no compatible protocol version")
		}
		s.mu.RLock()
		c := s.cfg
		s.mu.RUnlock()
		e := client.Encoder{}
		if max >= 2 {
			e.U16(2)
		} else {
			e.U16(1)
		}
		caps, membership, revision := capabilityAcceptedStreams|client.CapabilityAcceptedReferences|client.CapabilityConnectionSync|client.CapabilityBucketFormat|client.CapabilityObjectReferences|client.CapabilityPeerTransport|client.CapabilityNodeAdmin|client.CapabilityObjectSubscriptions|client.CapabilityDurability, uint64(1), c.Generation
		if !referenceSupported {
			caps &^= client.CapabilityObjectReferences | client.CapabilityAcceptedReferences
		}
		if s.control != nil {
			caps |= client.CapabilityClusterControl
			s.control.ledger.mu.Lock()
			membership = s.control.ledger.current.Membership.Epoch
			revision = s.control.ledger.current.MapRevision
			s.control.ledger.mu.Unlock()
		}
		if s.scopes != nil {
			caps |= client.CapabilityRangeHandoff | client.CapabilityExclusion | client.CapabilityControlRecovery | client.CapabilityVolumeRecovery | client.CapabilityRecoveryFenceClearance | client.CapabilityPhysicalQuotas | client.CapabilityReplication | client.CapabilityReplicaTopology | client.CapabilityCompactAuthority
		}
		if s.retiring() {
			caps |= client.CapabilityRetiringNode
		}
		if c.JoinPending {
			caps |= client.CapabilityPendingNode
			membership, revision = 0, 0
		}
		e.U64(caps)
		node, _ := parseID(c.Node.ID)
		cluster, _ := parseID(c.ClusterID)
		e.ID(client.ID(node))
		e.ID(client.ID(cluster))
		e.U64(membership)
		e.U64(revision)
		return e.B, 200, nil
	case client.Ping:
		n := d.U64()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		e := client.Encoder{}
		e.U64(n)
		return e.B, 200, nil
	case client.StatsGet:
		f := client.DecodeStatsFilter(&d)
		if e := guard(); e != nil {
			return nil, 0, e
		}
		return client.EncodeStats(s.CollectStats(f)), 200, nil
	case client.StatsSubscribe:
		sub := d.ID()
		f := client.DecodeStatsFilter(&d)
		interval := d.U32()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		if ws == nil {
			return nil, 0, fail(400, "statistics pushes require WebSocket")
		}
		if interval != 3000 {
			return nil, 0, fail(400, "this implementation supports a 3000 ms statistics interval")
		}
		if sub == (client.ID{}) {
			return nil, 0, fail(400, "subscription ID cannot be zero")
		}
		ws.subscribe(sub, f)
		e := client.Encoder{}
		e.ID(sub)
		return e.B, 200, nil
	case client.StatsUnsubscribe:
		sub := d.ID()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		if ws != nil {
			ws.unsubscribe(sub)
		}
		return nil, 200, nil
	case client.VolumeAdd, client.VolumeAddWithGroups:
		name := d.Text()
		source := d.U8()
		path := d.Text()
		class := d.U8()
		size := d.U64()
		var groups []string
		if m.Opcode == client.VolumeAddWithGroups {
			if d.U16() != 1 {
				return nil, 0, client.ErrProtocol
			}
			for i, n := 0, d.Count(10000); i < n; i++ {
				groups = append(groups, d.ID().String())
			}
		}
		if e := guard(); e != nil {
			return nil, 0, e
		}
		if size > math.MaxInt64 || class > 2 {
			return nil, 0, fail(400, "invalid volume size/class")
		}
		op, e := s.AddVolume(m.ID.String(), AddVolumeRequest{PlacementGroups: groups, Name: name, Source: source, Path: path, Class: []string{"hdd", "ssd", "ram"}[class], SizeBytes: int64(size)})
		if e != nil {
			return nil, 0, e
		}
		if op.State == "failed" {
			return nil, 0, fail(op.Status, op.Detail)
		}
		return encodeAck(op), op.Status, nil
	case client.VolumePlacementRelease:
		id := d.ID()
		if err := guard(); err != nil {
			return nil, 0, err
		}
		op, err := s.releaseVolumePlacement(m.ID.String(), id.String())
		if err != nil {
			return nil, 0, err
		}
		return encodeAck(op), op.Status, nil
	case client.VolumeRescan:
		id := d.ID()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		op, e := s.RescanVolume(m.ID.String(), id.String())
		if e != nil {
			return nil, 0, e
		}
		if op.State == "failed" {
			return nil, 0, fail(op.Status, op.Detail)
		}
		return encodeAck(op), op.Status, nil
	case client.VolumeRemove:
		id := d.ID()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		op, e := s.RemoveVolume(m.ID.String(), id.String())
		if e != nil {
			return nil, 0, e
		}
		if op.State == "failed" {
			return nil, 0, fail(op.Status, op.Detail)
		}
		return encodeAck(op), op.Status, nil
	case client.AdminOperationStatus:
		id := d.ID()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		op, e := s.operation(id.String())
		if e != nil {
			return nil, 0, e
		}
		out := client.Encoder{B: encodeAck(op)}
		out.V(uint64(op.Status))
		out.Text(op.Detail)
		return out.B, 200, nil
	case client.NodeConfigGet:
		if e := guard(); e != nil {
			return nil, 0, e
		}
		return jsonText(s.configView()), 200, nil
	case client.NodeConfigSet:
		raw := d.Text()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		var patch map[string]json.RawMessage
		if e := json.Unmarshal([]byte(raw), &patch); e != nil || len(patch) == 0 || len(patch) > 32 {
			return nil, 0, fail(400, "expected a nonempty configuration field object")
		}
		if e := s.updateConfig(patch); e != nil {
			return nil, 0, e
		}
		return jsonText(s.configView()), 200, nil
	case client.VolumeGrowInfo:
		id := d.ID()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		info, e := s.growthInfo(id.String())
		if e != nil {
			return nil, 0, e
		}
		out := client.Encoder{}
		out.U8(info.Kind)
		out.U64(info.Current)
		out.U64(info.Available)
		out.U64(info.Maximum)
		return out.B, 200, nil
	case client.VolumeGrow:
		id := d.ID()
		size := d.U64()
		if e := guard(); e != nil {
			return nil, 0, e
		}
		if size > math.MaxInt64 {
			return nil, 0, fail(400, "invalid volume size")
		}
		op, e := s.GrowVolume(m.ID.String(), id.String(), int64(size))
		if e != nil {
			return nil, 0, e
		}
		if op.State == "failed" {
			return nil, 0, fail(op.Status, op.Detail)
		}
		return encodeAck(op), op.Status, nil
	case client.VolumeCapacityGet:
		if e := guard(); e != nil {
			return nil, 0, e
		}
		capacity, e := s.volumeCapacity()
		if e != nil {
			return nil, 0, e
		}
		out := client.Encoder{}
		out.U64(capacity.FileAvailable)
		out.U64(capacity.RAMAvailable)
		out.U64(capacity.RAMLimit)
		return out.B, 200, nil
	case client.VolumeCandidates:
		if e := guard(); e != nil {
			return nil, 0, e
		}
		v, e := s.candidates()
		if e != nil {
			return nil, 0, e
		}
		return jsonText(v), 200, nil
	default:
		// TODO(multinode): NODE_HELLO, control/placement, replica and migration
		// dispatchers. Unsupported messages never receive a fabricated success ACK.
		return nil, 0, fail(501, "message is not implemented in the single-node administration stage")
	}
}
