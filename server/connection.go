// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// connectionAuthorization is established once, before WebSocket upgrade.
// Roles, bucket pointer and permissions are immutable; administrative changes
// revoke the capability instead of making data requests reread account/ACL data.
type connectionAuthorization struct {
	admin, clusterPeer bool
	bucket             *Bucket
	bucketID           volume.ID
	permissions        BucketPermissions
	invalid            atomic.Bool
	peerVerified       atomic.Bool
	expires            time.Time
}

func (p principal) isAdmin() bool {
	if p.connection != nil {
		return p.connection.admin && !p.connection.invalid.Load()
	}
	return p.User.Role == "ADMIN" && !p.User.Disabled
}
func (p principal) isPeer() bool {
	if p.connection != nil {
		return p.connection.clusterPeer && !p.connection.invalid.Load()
	}
	return p.ClusterPeer
}

// requireAdmin is used by mutation handlers while holding opMu. HTTP requests
// still revalidate their principal; socket requests use the cached capability.
func (s *Server) requireAdmin(p principal) error {
	if p.connection != nil {
		if p.isAdmin() {
			return nil
		}
		return ErrBucketForbidden
	}
	current, err := s.findAccount(p.User.Name)
	if err != nil || current.ID != p.User.ID || current.Role != "ADMIN" || !constantEqual(current.PasswordHash, p.User.PasswordHash) {
		return ErrBucketForbidden
	}
	return nil
}

// Called with opMu held through registration: a grant/account change cannot
// slip between validation and insertion into the live socket registry.
func (s *Server) authorizeConnection(r *http.Request, p principal) (*connectionAuthorization, principal, error) {
	auth := &connectionAuthorization{}
	if p.ClusterPeer {
		if !p.isPeer() {
			return nil, p, ErrBucketForbidden
		}
		auth.clusterPeer = true
	} else {
		current, err := s.findAccount(p.User.Name)
		if err != nil || current.ID != p.User.ID || !constantEqual(current.PasswordHash, p.User.PasswordHash) {
			return nil, p, ErrBucketForbidden
		}
		if current.Role != "ADMIN" && current.Role != "USER" {
			return nil, p, ErrBucketForbidden
		}
		p.User = current
		auth.admin = current.Role == "ADMIN"
	}
	if p.Cookie {
		s.authMu.Lock()
		session, ok := s.sessions[p.SessionToken]
		s.authMu.Unlock()
		if !ok || session.UserID != p.User.ID || !time.Now().Before(session.Expires) {
			return nil, p, ErrBucketForbidden
		}
		auth.expires = session.Expires
	}
	if raw := r.Header.Get(client.BucketHeader); raw != "" {
		id, err := bucketID(raw)
		if err != nil {
			return nil, p, fail(400, "invalid bucket ID")
		}
		if auth.clusterPeer || id == systemBucket {
			return nil, p, ErrBucketForbidden
		}
		b, err := s.Bucket(id)
		if err != nil {
			return nil, p, err
		}
		user, err := bucketID(p.User.ID)
		if err != nil {
			return nil, p, ErrBucketForbidden
		}
		info, err := b.Info(user, auth.admin)
		if err != nil {
			return nil, p, err
		}
		if b.underMaintenance() {
			return nil, p, fail(409, "bucket is under exclusive maintenance")
		}
		if info.Permissions == 0 || b.Describe().State == BucketDeleting {
			return nil, p, ErrBucketForbidden
		}
		auth.bucket = b
		auth.bucketID = id
		auth.permissions = info.Permissions
	} else if !auth.admin && !auth.clusterPeer {
		return nil, p, fail(400, "a bucket ID is required for client connections")
	}
	p.connection = auth
	return auth, p, nil
}
func (s *Server) invalidateUserConnections(id string) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		if ws.principal.User.ID == id && ws.authorization != nil && !ws.authorization.clusterPeer {
			ws.authorization.invalid.Store(true)
		}
	}
}
func (s *Server) invalidateSessionConnections(token string) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		if ws.principal.Cookie && ws.principal.SessionToken == token && ws.authorization != nil {
			ws.authorization.invalid.Store(true)
		}
	}
}
func (s *Server) invalidateDisallowedConnections() {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		if !s.allowed(ws.request) && ws.authorization != nil {
			ws.authorization.invalid.Store(true)
		}
	}
}

// Publication invalidates only connections whose effective capability changed.
// Codec, placement, rename and retry-receipt updates keep the bucket pointer live.
func (s *Server) invalidateBucketConnections(b *Bucket, d BucketDescription) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		auth := ws.authorization
		if auth == nil || auth.bucket != b {
			continue
		}
		permissions := BucketPermissions(0)
		if auth.admin {
			permissions = BucketAllPermissions
		} else {
			for _, g := range d.Grants {
				if g.UserID == ws.principal.User.ID {
					permissions = g.Permissions
					break
				}
			}
		}
		if d.State != BucketActive {
			permissions &= BucketRead
		}
		if d.DeletionComplete || d.State == BucketDeleting || permissions != auth.permissions {
			auth.invalid.Store(true)
		}
	}
}
