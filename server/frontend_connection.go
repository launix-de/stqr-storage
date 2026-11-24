// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// This namespace was already reserved from user creation. Only an admitted
// node with the cluster credential can open this private bucket-only session.
// It carries no claimed role and is never returned to an HTTP/S3 caller.
const frontendCredentialPrefix = "node-frontend-"
const peerVerifyFrontend uint16 = 0x104e

type frontendGrant struct {
	ctx       context.Context
	bucket    volume.ID
	principal principal
	digest    [32]byte
}

type frontendClient struct {
	*client.Client
	release func()
}

func (c *frontendClient) Close() error { defer c.release(); return c.Client.Close() }

func frontendAccountRevision(user account) [32]byte {
	b, _ := json.Marshal(user)
	return sha256.Sum256(b)
}

func (s *Server) frontendCredentials(ctx context.Context, bucket volume.ID, p principal) (string, string, func(), error) {
	if p.frontendPermission != BucketRead && p.frontendPermission != BucketWrite && p.frontendPermission != BucketAppend && p.frontendPermission != BucketDelete {
		return "", "", nil, ErrBucketForbidden
	}
	user, err := parseID(p.User.ID)
	if err != nil || bucket == systemBucket {
		return "", "", nil, ErrBucketForbidden
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return "", "", nil, err
	}
	if _, err := b.authorize(p, p.frontendPermission); err != nil {
		return "", "", nil, err
	}
	if err := ctx.Err(); err != nil {
		return "", "", nil, err
	}
	s.mu.RLock()
	node, nodeErr := parseID(s.cfg.Node.ID)
	cluster, clusterErr := parseID(s.cfg.ClusterID)
	secret := s.cfg.Auth.ClusterToken
	s.mu.RUnlock()
	if nodeErr != nil || clusterErr != nil || secret == "" {
		return "", "", nil, ErrBucketForbidden
	}
	e := client.Encoder{}
	e.U16(1)
	e.ID(client.ID(node))
	e.ID(client.ID(cluster))
	e.ID(client.ID(bucket))
	e.ID(client.ID(user))
	e.U32(uint32(p.frontendPermission))
	revision := frontendAccountRevision(p.User)
	e.B = append(e.B, revision[:]...)
	grant := client.NewRequestID()
	e.ID(grant)
	s.opMu.Lock()
	if s.drainClosing.Load() || s.shortStopping.Load() {
		s.opMu.Unlock()
		return "", "", nil, fail(503, "node is finishing shutdown")
	}
	s.authMu.Lock()
	if s.frontendGrants == nil {
		s.frontendGrants = make(map[client.ID]frontendGrant)
	}
	if len(s.frontendGrants) >= 256 {
		s.authMu.Unlock()
		s.opMu.Unlock()
		return "", "", nil, fail(503, "frontend request capacity exhausted")
	}
	s.frontendGrants[grant] = frontendGrant{ctx: ctx, bucket: bucket, principal: p, digest: sha256.Sum256(e.B)}
	s.authMu.Unlock()
	s.opMu.Unlock()
	release := func() { s.authMu.Lock(); delete(s.frontendGrants, grant); s.authMu.Unlock() }
	stop := context.AfterFunc(ctx, release)
	return frontendCredentialPrefix + base64.RawURLEncoding.EncodeToString(e.B), secret, func() { stop(); release() }, nil
}

func (s *Server) authenticateFrontend(r *http.Request, name, password string) (principal, error) {
	denied := func() (principal, error) { return principal{}, ErrBucketForbidden }
	// The special credential is not a dashboard login or an HTTP admin token.
	if r.Method != http.MethodGet || r.URL.Path != "/api/ws" || len(name) > 256 {
		return denied()
	}
	s.mu.RLock()
	secret, cluster := s.cfg.Auth.ClusterToken, s.cfg.ClusterID
	s.mu.RUnlock()
	if secret == "" || !constantEqual(secret, password) {
		return denied()
	}
	wire, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(name, frontendCredentialPrefix))
	if err != nil {
		return denied()
	}
	d := client.Decoder{B: wire}
	schema, issuer, clusterID, bucket, user, required := d.U16(), d.ID(), d.ID(), d.ID(), d.ID(), BucketPermissions(d.U32())
	var revision [32]byte
	copy(revision[:], d.Take(32))
	grant := d.ID()
	if schema != 1 || d.Done() != nil || clusterID.String() != cluster || bucket == client.ID(systemBucket) || bucket.String() != r.Header.Get(client.BucketHeader) || required != BucketRead && required != BucketWrite && required != BucketAppend && required != BucketDelete {
		return denied()
	}
	view := s.currentSystem.Load()
	if view == nil || s.joinPending() || !hasControlNode(membershipIDs(view.manifest.Membership), issuer) {
		return denied()
	}
	e := client.Encoder{}
	e.ID(grant)
	digest := sha256.Sum256(wire)
	e.B = append(e.B, digest[:]...)
	verified, err := (peerControlParticipants{runtime: s.control}).call(r.Context(), issuer, peerVerifyFrontend, e.B)
	if err != nil {
		if status := statusOf(err); status == 401 || status == 403 {
			return denied()
		}
		// Immediately after SHORT/restart the authenticated issuer's peer
		// connection may still be reconnecting. That is unavailable evidence,
		// not a rejected credential; preserve 503 so the SDK can wait/retry.
		return principal{}, fail(503, "frontend issuer verification is unavailable")
	}
	if len(verified) != 0 {
		return denied()
	}
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		return principal{}, err
	}
	for _, account := range image.Users {
		if account.ID == user.String() && !account.Disabled && (account.Role == "ADMIN" || account.Role == "USER") && frontendAccountRevision(account) == revision {
			return principal{User: account, frontendPermission: required, frontendIssuer: issuer}, nil
		}
	}
	return denied()
}

// The issuer attests an actually live, already authenticated HTTP operation.
// Knowledge of CLUSTER_TOKEN plus catalog IDs cannot invent a user session.
func (s *Server) verifyFrontend(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	id := d.ID()
	var digest [32]byte
	copy(digest[:], d.Take(32))
	view := s.currentSystem.Load()
	if d.Done() != nil || view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, ErrBucketForbidden
	}
	s.authMu.Lock()
	grant, exists := s.frontendGrants[id]
	s.authMu.Unlock()
	if !exists || grant.digest != digest || grant.ctx.Err() != nil {
		return nil, ErrBucketForbidden
	}
	b, err := s.Bucket(grant.bucket)
	if err != nil {
		return nil, err
	}
	// This attests the originating HTTP capability. Namespace exclusion is
	// checked separately by bindConnection, including a lease token on auxiliary
	// transports. Do not reject an owner's routed socket merely because its
	// original HTTP principal predates acquisition of that lease.
	current, err := s.findAccount(grant.principal.User.Name)
	if err != nil || current.ID != grant.principal.User.ID || current.Disabled || frontendAccountRevision(current) != frontendAccountRevision(grant.principal.User) {
		return nil, ErrBucketForbidden
	}
	uid, err := bucketID(current.ID)
	if err != nil {
		return nil, err
	}
	info, err := b.Info(uid, current.Role == "ADMIN")
	if err != nil {
		return nil, err
	}
	if info.Permissions&grant.principal.frontendPermission == 0 {
		return nil, ErrBucketForbidden
	}
	return nil, nil
}

func (s *Server) invalidateFrontendIssuers(membership client.Membership) {
	members := membershipIDs(membership)
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for ws := range s.sockets {
		auth := ws.authorization
		if auth != nil && auth.frontendPermission != 0 && !hasControlNode(members, auth.frontendIssuer) {
			auth.invalid.Store(true)
		}
	}
}

func (s *Server) frontendEndpoint(ctx context.Context) (string, error) {
	s.mu.RLock()
	listen, advertised := s.cfg.Node.Listen, s.cfg.Node.Advertise
	s.mu.RUnlock()
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	var candidates []string
	if host != "" && host != "0.0.0.0" && host != "::" {
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return "", err
		}
		for _, address := range addresses {
			candidates = append(candidates, address.String())
		}
	} else {
		candidates = append(candidates, "127.0.0.1")
		if host != "0.0.0.0" {
			candidates = append(candidates, "::1")
		}
		if u, err := url.Parse(advertised); err == nil && net.ParseIP(u.Hostname()) != nil && (host != "0.0.0.0" || net.ParseIP(u.Hostname()).To4() != nil) {
			candidates = append(candidates, u.Hostname())
		}
	}
	for _, address := range candidates {
		// Preserve the configured subnet restriction, including for this
		// process's private frontend connection to its own listener.
		if s.allowed(&http.Request{RemoteAddr: net.JoinHostPort(address, "0")}) {
			return "http://" + net.JoinHostPort(address, port), nil
		}
	}
	return "", fail(503, "no allowed local address for the object frontend")
}

func (s *Server) openFrontend(ctx context.Context, bucket volume.ID, p principal) (*frontendClient, error) {
	name, password, release, err := s.frontendCredentials(ctx, bucket, p)
	if err != nil {
		return nil, err
	}
	endpoint, err := s.frontendEndpoint(ctx)
	if err != nil {
		release()
		return nil, err
	}
	c, err := client.Dial(ctx, endpoint, name, password, client.BucketID(bucket))
	if err != nil {
		release()
		return nil, err
	}
	return &frontendClient{Client: c, release: release}, nil
}
