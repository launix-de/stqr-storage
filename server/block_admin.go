// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"github.com/launix-de/stqr-storage/volume"
)

func (s *Server) registerBlockControl(m client.Message, ws *wsSession) ([]byte, int) {
	reject := func(code int, detail string) ([]byte, int) { return client.ResponseBody(code, nil, detail), code }
	if m.Kind != client.Request {
		return reject(400, "block control registration requires a request")
	}
	if ws == nil || ws.authorization == nil || ws.authorization.bucket == nil || !ws.authorization.blockDevice.Load() {
		return reject(403, "block lease owner required")
	}
	a := ws.authorization
	b := a.bucket
	b.mu.RLock()
	owner := b.maintenance == a || a.leaseOwner.Load() && b.lease != nil && !b.lease.Released && a.lease.Load() != nil && a.lease.Load().Token == b.lease.Token
	b.mu.RUnlock()
	d := client.Decoder{B: m.Body}
	if d.U16() != 1 || d.Done() != nil {
		return reject(400, "invalid block control registration")
	}
	if !owner {
		return reject(403, "original block lease socket required")
	}
	ws.blockControl.Store(true)
	return client.ResponseBody(200, nil, ""), 200
}

func blockAdminBody(bucket volume.ID, owner client.ID, request []byte) []byte {
	e := client.Encoder{}
	e.U16(1)
	e.ID(client.ID(bucket))
	e.ID(owner)
	e.Blob(request)
	return e.B
}

func (s *Server) blockDeviceAdmin(m client.Message, ws *wsSession, p principal) ([]byte, int) {
	failure := func(err error) ([]byte, int) {
		status := objectStatus(err)
		var native *client.StatusError
		switch {
		case errors.As(err, &native):
			status = native.Status
		case errors.Is(err, client.ErrRange), errors.Is(err, client.ErrProtocol):
			status = 400
		case errors.Is(err, client.ErrNotFound):
			status = 404
		case errors.Is(err, client.ErrPrecondition):
			status = 409
		case errors.Is(err, context.DeadlineExceeded):
			status = 504
		case errors.Is(err, client.ErrClosed), errors.Is(err, client.ErrWouldBlock):
			status = 503
		}
		return client.ResponseBody(status, nil, err.Error()), status
	}
	if err := s.requireAdmin(p); err != nil {
		return failure(err)
	}
	if m.Kind != client.Request {
		return failure(fail(400, "block administration requires a request"))
	}
	d := client.Decoder{B: m.Body}
	version, id, expectedOwner, request := d.U16(), volume.ID(d.ID()), d.ID(), d.Blob()
	r, err := nbd.DecodeManagementRequest(request)
	if version != 1 || d.Done() != nil || err != nil || id == systemBucket || (m.Opcode == client.BlockDeviceList) != (r.Action == 1) {
		return failure(fail(400, "invalid block device administration request"))
	}
	if s.clusterPaused() || s.forceFrozen() || s.joinPending() {
		return failure(fail(503, "cluster control transition in progress"))
	}
	b, err := s.Bucket(id)
	if err != nil {
		return failure(err)
	}
	description := b.Describe()
	if description.State != BucketActive || description.DesiredPolicy != nil {
		return failure(fail(409, "bucket lifecycle transition in progress"))
	}
	base := s.ctx
	if ws != nil {
		base = ws.ctx
	}
	ctx, cancel := context.WithTimeout(base, 30*time.Second)
	defer cancel()
	local, _ := parseID(s.running.Node.ID)
	owner := client.ID(local)
	l := s.leaseRecord(b)
	live := l != nil && !l.Released
	if live && !l.Block {
		return failure(fail(409, "bucket is under filesystem maintenance"))
	}
	readonly := live && l.ReadOnly
	if live {
		owner = l.Owner
	}
	if expectedOwner != (client.ID{}) && expectedOwner != owner {
		return failure(fail(409, "block lease owner changed; refresh the device list"))
	}
	if live && owner != client.ID(local) {
		if expectedOwner != (client.ID{}) {
			return failure(fail(409, "block lease owner changed; refresh the device list"))
		}
		body, err := (client.NodeAdmin{Target: owner, Opcode: m.Opcode, Body: blockAdminBody(id, owner, request)}).MarshalBinary()
		if err != nil {
			return failure(err)
		}
		return s.nodeAdmin(client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: m.ID, Body: body}, &wsSession{ctx: ctx}, p)
	}
	var target *wsSession
	s.wsMu.Lock()
	for session := range s.sockets {
		a := session.authorization
		if a == nil || a.bucket != b || a.invalid.Load() || !session.blockControl.Load() {
			continue
		}
		if live {
			claim := a.lease.Load()
			if !a.leaseOwner.Load() || claim == nil || claim.Token != l.Token {
				continue
			}
		} else {
			b.mu.RLock()
			active := b.maintenance == a
			b.mu.RUnlock()
			if !active {
				continue
			}
			live, readonly = true, a.blockReadOnly.Load()
		}
		target = session
		break
	}
	s.wsMu.Unlock()
	var result []byte
	if target != nil {
		if readonly && r.Action == 2 {
			return failure(fail(403, "readonly exporter cannot resize devices"))
		}
		result, err = target.requestDuplex(ctx, client.BucketBlockControl, request)
	} else if live || b.underMaintenance() {
		return failure(fail(409, "the block lease owner is unavailable or does not support dashboard management; reconnect it with an updated exporter"))
	} else {
		// No exporter is present. Acquire the same cluster-wide lease used by
		// the CLI; never read or mutate around another client's exclusive lease.
		actor := p
		actor.frontendPermission = BucketWrite
		if r.Action == 1 {
			actor.frontendPermission = BucketRead
		}
		c, e := s.openFrontend(ctx, id, actor)
		if e != nil {
			return failure(e)
		}
		defer c.Close()
		if e = c.BeginBlockMaintenance(ctx, r.Action == 1); e != nil {
			return failure(e)
		}
		catalog := nbd.NewCatalog(c)
		if e = catalog.CheckFormat(ctx); e != nil {
			if errors.Is(e, nbd.ErrLegacyFormat) {
				return failure(fail(409, "this bucket contains a filesystem rather than named block devices"))
			}
			return failure(e)
		}
		result, err = catalog.Manage(ctx, request, false)
	}
	if err != nil {
		return failure(err)
	}
	items, _, err := nbd.DecodeManagementResult(result)
	if err != nil {
		return failure(err)
	}
	if r.Action == 1 && len(items) > int(r.Limit) || r.Action == 2 && (len(items) != 1 || items[0].ID != r.Device || items[0].Size != r.Size) {
		return failure(fail(502, "invalid exporter response"))
	}
	e := client.Encoder{}
	e.Bool(live)
	e.Bool(readonly)
	e.ID(owner)
	e.Blob(result)
	return client.ResponseBody(200, e.B, ""), 200
}
