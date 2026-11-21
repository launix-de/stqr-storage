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
		return r.ctx
	}
	return ws.ctx
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
