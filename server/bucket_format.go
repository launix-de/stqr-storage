// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketFormat uint16 = 0x1090
const internalObjectFormat = "__stqr_internal_objects"

// Values are opaque and case sensitive. No frontend schema is known here.
func validBucketFormat(format string, empty bool) bool {
	return (empty || format != "") && len(format) <= 255 && utf8.ValidString(format) && !strings.ContainsFunc(format, func(r rune) bool { return r < 32 || r == 127 }) && strings.TrimSpace(format) == format
}

func (s *Server) formatAccess(id volume.ID, p principal) (*Bucket, error) {
	user, err := s.findAccount(p.User.Name)
	if err != nil || user.ID != p.User.ID || user.Disabled || frontendAccountRevision(user) != frontendAccountRevision(p.User) {
		return nil, ErrBucketForbidden
	}
	b, err := s.Bucket(id)
	if err != nil {
		return nil, err
	}
	uid, err := bucketID(user.ID)
	if err != nil {
		return nil, ErrBucketForbidden
	}
	info, err := b.Info(uid, user.Role == "ADMIN")
	if err != nil || info.Permissions == 0 || b.Describe().State == BucketDeleting {
		return nil, ErrBucketForbidden
	}
	return b, nil
}

func (s *Server) openBucketFormat(ctx context.Context, raw, format string, p principal) error {
	if !validBucketFormat(format, false) {
		return fail(400, "a nonempty bucket format is required")
	}
	id, err := bucketID(raw)
	if err != nil || id == systemBucket {
		return ErrBucketForbidden
	}
	b, err := s.formatAccess(id, p)
	if err != nil {
		return err
	}
	// Private, short-lived object maintenance grants never interpret payloads
	// or select the application's format. Public credentials cannot use this.
	if p.frontendPermission != 0 && format == internalObjectFormat && b.Describe().Format == "" {
		return nil
	}
	if old := b.Describe().Format; old != "" {
		if old != format {
			return fail(409, fmt.Sprintf("bucket format mismatch: expected %q, got %q", b.Describe().Format, format))
		}
		return nil
	}
	if s.controller == nil {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		return s.setEmptyBucketFormat(id, format, p)
	}
	nodes, _, err := s.leaseMembers()
	if err != nil {
		return err
	}
	s.peerStatsMu.RLock()
	for _, node := range nodes {
		if node == s.control.ledger.node {
			continue
		}
		o := s.peerStats[node]
		if o == nil || !o.online || o.connection.Capabilities&client.CapabilityBucketFormat == 0 {
			s.peerStatsMu.RUnlock()
			return fail(426, "all cluster members must support bucket format binding")
		}
	}
	s.peerStatsMu.RUnlock()
	if len(nodes) == 0 {
		return fail(503, "format coordinator unavailable")
	}
	if nodes[0] != s.control.ledger.node {
		e := client.Encoder{}
		e.ID(client.ID(id))
		e.Text(format)
		e.Text(p.User.Name)
		e.ID(client.ID(mustBucketActor(p.User.ID)))
		revision := frontendAccountRevision(p.User)
		e.B = append(e.B, revision[:]...)
		_, err := (peerControlParticipants{runtime: s.control}).call(ctx, nodes[0], peerBucketFormat, e.B)
		return err
	}
	return s.commitBucketFormat(ctx, id, format, p)
}
func mustBucketActor(raw string) volume.ID { id, _ := bucketID(raw); return id }

func (s *Server) receiveBucketFormat(ctx context.Context, body []byte) ([]byte, error) {
	d := client.Decoder{B: body}
	id, format, name, actor := volume.ID(d.ID()), d.Text(), d.Text(), d.ID()
	var revision [32]byte
	copy(revision[:], d.Take(32))
	if d.Done() != nil || !validBucketFormat(format, false) {
		return nil, client.ErrProtocol
	}
	user, err := s.findAccount(name)
	if err != nil || user.ID != actor.String() || frontendAccountRevision(user) != revision {
		return nil, ErrBucketForbidden
	}
	nodes, _, err := s.leaseMembers()
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 || nodes[0] != s.control.ledger.node {
		return nil, fail(503, "format coordinator changed; retry opening bucket")
	}
	return nil, s.commitBucketFormat(ctx, id, format, principal{User: user})
}

func (s *Server) setEmptyBucketFormat(id volume.ID, format string, p principal) error {
	b, err := s.formatAccess(id, p)
	if err != nil {
		return err
	}
	d := b.Describe()
	if d.Format != "" {
		if d.Format != format {
			return fail(409, fmt.Sprintf("bucket format mismatch: expected %q, got %q", b.Describe().Format, format))
		}
		return nil
	}
	d.Format, d.Schema = format, 2
	_, err = s.buckets.Save(d, d.Revision)
	return err
}

func (s *Server) commitBucketFormat(ctx context.Context, id volume.ID, format string, p principal) error {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.resume(ctx); err != nil {
		return err
	}
	if b, err := s.formatAccess(id, p); err != nil {
		return err
	} else if old := b.Describe().Format; old != "" {
		if old != format {
			return fail(409, fmt.Sprintf("bucket format mismatch: expected %q, got %q", b.Describe().Format, format))
		}
		return nil
	}
	_, _, err := c.change(ctx, nil, func() (*adminDraft, error) {
		s.opMu.Lock()
		preview, err := s.newAdminPreview()
		s.opMu.Unlock()
		if err != nil {
			return nil, err
		}
		draft := &adminDraft{server: preview, status: 200}
		if err = preview.setEmptyBucketFormat(id, format, p); err != nil {
			draft.close()
			return nil, err
		}
		return draft, nil
	})
	return err
}

// Administrative sockets have no HTTP bucket header. Their object clients
// declare a format explicitly before accessing a namespace on that socket.
func (s *Server) openSocketBucketFormat(m client.Message, ws *wsSession, p principal) ([]byte, int) {
	failReply := func(err error) ([]byte, int) {
		status := statusOf(err)
		return client.ResponseBody(status, nil, err.Error()), status
	}
	if ws == nil || p.connection == nil || !p.isAdmin() || p.connection.bucket != nil || m.Kind != client.Request {
		return failReply(fail(400, "format open requires an administrative WebSocket request"))
	}
	d := client.Decoder{B: m.Body}
	id, format := d.ID(), d.Text()
	if d.Done() != nil {
		return failReply(fail(400, "invalid format open"))
	}
	// WebSocket principals erase the verifier after upgrade. Resolve the current
	// account only after validating its revocable connection capability.
	user, err := s.findAccount(p.User.Name)
	if err != nil || user.ID != p.connection.accountID || frontendAccountRevision(user) != p.connection.accountRevision {
		return failReply(ErrBucketForbidden)
	}
	if err = s.openBucketFormat(ws.ctx, id.String(), format, principal{User: user}); err != nil {
		return failReply(err)
	}
	p.connection.openedFormats.Store(id, format)
	return client.ResponseBody(200, nil, ""), 200
}
func socketFormatRequired(op uint16) bool {
	return op >= 0x0100 && op <= 0x0108 || op == client.ObjectAccepted || op == client.BucketObjectList || op == client.BucketScan || op == client.BucketMaintenance || op == client.BucketObjectLease || op == client.ObjectSubscribe
}
func checkSocketBucketFormat(m client.Message, p principal) error {
	if p.connection == nil || p.connection.bucket != nil || p.isPeer() || !socketFormatRequired(m.Opcode) {
		return nil
	}
	if len(m.Body) < 16 {
		return fail(400, "invalid bucket object request")
	}
	d := client.Decoder{B: m.Body}
	id := d.ID()
	if _, ok := p.connection.openedFormats.Load(id); !ok {
		return fail(400, "open the bucket with an explicit format before object requests")
	}
	return nil
}
