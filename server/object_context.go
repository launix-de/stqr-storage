// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
)

type objectRequestContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Register before enqueueing so CANCEL also reaches a request still waiting in
// the bounded queue. The read loop can cancel I/O without waiting for dispatch.
func (ws *wsSession) registerObjectContext(id client.ID) bool {
	ws.objectMu.Lock()
	defer ws.objectMu.Unlock()
	if ws.objectContexts == nil {
		ws.objectContexts = map[client.ID]objectRequestContext{}
	}
	if _, ok := ws.objectContexts[id]; ok {
		return false
	}
	ctx, cancel := context.WithCancel(ws.ctx)
	ws.objectContexts[id] = objectRequestContext{ctx: ctx, cancel: cancel}
	return true
}
func (ws *wsSession) objectContext(id client.ID) context.Context {
	ws.objectMu.Lock()
	defer ws.objectMu.Unlock()
	if r, ok := ws.objectContexts[id]; ok {
		return ws.connectionContext(r.ctx, id)
	}
	return ws.connectionContext(ws.ctx, id)
}
func (ws *wsSession) cancelObjectContext(id client.ID) {
	ws.objectMu.Lock()
	defer ws.objectMu.Unlock()
	if r, ok := ws.objectContexts[id]; ok {
		r.cancel()
	}
}
func (ws *wsSession) releaseObjectContext(id client.ID) {
	ws.objectMu.Lock()
	defer ws.objectMu.Unlock()
	if r, ok := ws.objectContexts[id]; ok {
		r.cancel()
		delete(ws.objectContexts, id)
	}
}

// usesObjectRequestContext is shared by reader registration and completion.
// ObjectReference uses its own accepted trailer, not ObjectAccepted's envelope,
// but must still receive per-request CANCEL and retire its context on response.
func usesObjectRequestContext(op uint16) bool {
	return op >= 0x0100 && op <= 0x0108 || op == client.ObjectSubscribe || op == client.ObjectAccepted || op == client.ObjectReference || op == client.BucketObjectLease || op == client.BucketMaintenanceEnd || op == client.BucketStatFS || op == client.BucketSync || op == client.BucketMaintenance || op == client.BucketScan
}
