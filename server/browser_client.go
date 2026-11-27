// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// BrowserOrigins is deliberately independent of AllowSubnets: the latter
// controls the caller's IP, the former the origin of the page running its code.
func (s *Server) browserOriginAllowed(r *http.Request) bool {
	if sameOrigin(r) {
		return true
	}
	u, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	s.mu.RLock()
	origins := append([]string(nil), s.cfg.BrowserOrigins...)
	s.mu.RUnlock()
	for _, allowed := range origins {
		if allowed == u.String() {
			return true
		}
		// CIDR entries apply only to literal IP origins, with an explicit scheme.
		scheme, cidr, ok := strings.Cut(allowed, "://")
		if !ok || scheme != u.Scheme {
			continue
		}
		prefix, err := netip.ParsePrefix(cidr)
		ip, ipErr := netip.ParseAddr(u.Hostname())
		if err == nil && ipErr == nil && prefix.Contains(ip.Unmap()) {
			return true
		}
	}
	return false
}
func browserPath(path string) bool {
	return path == "/api/ws" || path == "/api/bucket-connect" || path == "/api/bucket-tunnel"
}

// Browser WebSocket constructors cannot set HTTP headers. Encode only the
// native connection selectors/credentials, never URL query credentials.
func browserHeaders(r *http.Request) error {
	if !browserPath(r.URL.Path) {
		return nil
	}
	seen := map[string]bool{}
	for _, value := range websocket.Subprotocols(r) {
		for prefix, header := range map[string]string{"bucket.": client.BucketHeader, "format.": client.FormatHeader, "auth.": "Authorization", "maintenance.": bucketLeaseHeader} {
			if !strings.HasPrefix(value, prefix) {
				continue
			}
			if seen[header] || r.Header.Get(header) != "" {
				return errors.New("duplicate browser connection selector")
			}
			raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, prefix))
			if err != nil || len(raw) > 2048 || strings.ContainsAny(string(raw), "\r\n\x00") {
				return errors.New("invalid browser connection selector")
			}
			seen[header] = true
			r.Header.Set(header, string(raw))
		}
	}
	return nil
}
func (s *Server) serveBrowserConnect(w http.ResponseWriter, r *http.Request, p principal) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	s.opMu.Lock()
	auth, _, err := s.authorizeConnection(r, p, true)
	s.opMu.Unlock()
	if err != nil {
		httpError(w, statusOf(err), err)
		return
	}
	if auth.bucket == nil {
		httpError(w, 400, errors.New("a bucket is required"))
		return
	}
	writeJSON(w, 200, map[string]string{"bucket": auth.bucketID.String()})
}

const browserCredentialPrefix = "node-browser-"

// Tunnel delegation is never returned to JavaScript. It retains native account
// continuity and the original cookie session, and exists only while this tunnel
// request is alive. The MAC is not the cluster credential.
func (s *Server) browserTunnelCredentials(ctx context.Context, p principal, bucket, format string) (string, string, func(), error) {
	id, err := bucketID(bucket)
	if err != nil {
		return "", "", nil, err
	}
	_, _, err = s.referenceAccountSnapshot(p)
	if err != nil {
		return "", "", nil, err
	}
	s.mu.RLock()
	issuer, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	secret := s.cfg.Auth.ClusterToken
	s.mu.RUnlock()
	grant := client.NewRequestID()
	e := client.Encoder{}
	e.U16(1)
	e.ID(client.ID(issuer))
	e.ID(client.ID(cluster))
	e.ID(client.ID(id))
	e.Text(format)
	e.Text(p.User.Name)
	revision := frontendAccountRevision(p.User)
	e.B = append(e.B, revision[:]...)
	e.ID(grant)
	digest := sha256.Sum256(e.B)
	s.authMu.Lock()
	if s.frontendGrants == nil {
		s.frontendGrants = map[client.ID]frontendGrant{}
	}
	if len(s.frontendGrants) >= 256 {
		s.authMu.Unlock()
		return "", "", nil, fail(503, "browser tunnel capacity exhausted")
	}
	s.frontendGrants[grant] = frontendGrant{ctx: ctx, bucket: id, principal: p, digest: digest, browser: true}
	s.authMu.Unlock()
	release := func() { s.authMu.Lock(); delete(s.frontendGrants, grant); s.authMu.Unlock() }
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(browserCredentialPrefix))
	mac.Write(e.B)
	return browserCredentialPrefix + base64.RawURLEncoding.EncodeToString(e.B), hex.EncodeToString(mac.Sum(nil)), release, nil
}
func (s *Server) authenticateBrowserTunnel(r *http.Request, name, password string) (principal, error) {
	denied := func() (principal, error) { return principal{}, ErrBucketForbidden }
	if r.Method != "GET" || r.URL.Path != "/api/ws" || len(name) > 2048 {
		return denied()
	}
	wire, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(name, browserCredentialPrefix))
	if err != nil {
		return denied()
	}
	s.mu.RLock()
	secret, cluster, local := s.cfg.Auth.ClusterToken, s.cfg.ClusterID, s.cfg.Node.ID
	s.mu.RUnlock()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(browserCredentialPrefix))
	mac.Write(wire)
	if secret == "" || !constantEqual(password, hex.EncodeToString(mac.Sum(nil))) {
		return denied()
	}
	d := client.Decoder{B: wire}
	schema, issuer, clusterID, bucket, format, username := d.U16(), d.ID(), d.ID(), d.ID(), d.Text(), d.Text()
	var revision [32]byte
	copy(revision[:], d.Take(32))
	grant := d.ID()
	if d.Done() != nil || schema != 1 || clusterID.String() != cluster || bucket.String() != r.Header.Get(client.BucketHeader) || format != r.Header.Get(client.FormatHeader) || bucket == client.ID(systemBucket) {
		return denied()
	}
	proof := client.Encoder{}
	proof.ID(grant)
	digest := sha256.Sum256(wire)
	proof.B = append(proof.B, digest[:]...)
	if issuer.String() == local {
		_, err = s.verifyFrontend(proof.B, issuer)
	} else if s.control != nil {
		_, err = (peerControlParticipants{runtime: s.control}).call(r.Context(), issuer, peerVerifyFrontend, proof.B)
	} else {
		return denied()
	}
	if err != nil {
		return principal{}, err
	}
	user, stamp, err := s.findAccountWithReferenceStamp(username)
	if err != nil || frontendAccountRevision(user) != revision {
		return denied()
	}
	return principal{User: user, referenceAuth: stamp}, nil
}

// The gateway knows only a committed node identity and relays native frames.
// It neither decodes application objects nor introduces another storage API.
func (s *Server) serveBucketTunnel(w http.ResponseWriter, r *http.Request, p principal) {
	if r.Method != "GET" || !websocket.IsWebSocketUpgrade(r) {
		httpError(w, 400, errors.New("WebSocket upgrade required"))
		return
	}
	if !s.browserOriginAllowed(r) {
		httpError(w, 403, errors.New("origin rejected"))
		return
	}
	if p.Cookie {
		ok := false
		for _, sub := range websocket.Subprotocols(r) {
			ok = ok || constantEqual(sub, "csrf."+p.CSRF)
		}
		if !ok {
			httpError(w, 403, errors.New("WebSocket CSRF subprotocol required"))
			return
		}
	}
	s.opMu.Lock()
	auth, _, err := s.authorizeConnection(r, p, true)
	s.opMu.Unlock()
	if err != nil {
		httpError(w, statusOf(err), err)
		return
	}
	if auth.bucket == nil {
		httpError(w, 400, errors.New("bucket required"))
		return
	}
	target, err := parseID(r.URL.Query().Get("node"))
	if err != nil {
		httpError(w, 400, errors.New("invalid node ID"))
		return
	}
	var address string
	view := s.currentSystem.Load()
	if view != nil {
		for _, member := range view.manifest.Membership.Members {
			if member.ID == client.ID(target) && len(member.Addresses) > 0 {
				address = member.Addresses[0]
			}
		}
	}
	if address == "" {
		httpError(w, 503, errors.New("target is not an addressable cluster member"))
		return
	}
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "http" || u.User != nil {
		httpError(w, 502, errors.New("invalid committed node address"))
		return
	}
	u.Scheme = "ws"
	u.Path = "/api/ws"
	u.RawQuery = ""
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	name, password, release, err := s.browserTunnelCredentials(ctx, p, auth.bucketID.String(), r.Header.Get(client.FormatHeader))
	if err != nil {
		httpError(w, statusOf(err), err)
		return
	}
	defer release()
	headers := http.Header{}
	headers.Set(client.BucketHeader, auth.bucketID.String())
	headers.Set(client.FormatHeader, r.Header.Get(client.FormatHeader))
	headers.Set(bucketLeaseHeader, r.Header.Get(bucketLeaseHeader))
	headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(name+":"+password)))
	dialer := websocket.Dialer{Subprotocols: []string{"stqr.v2"}, HandshakeTimeout: 15 * time.Second, EnableCompression: false}
	upstream, response, err := dialer.DialContext(ctx, u.String(), headers)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		httpError(w, 503, errors.New("target bucket connection unavailable"))
		return
	}
	defer upstream.Close()
	upstream.SetReadLimit(client.MaxMessage)
	up := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}, CheckOrigin: s.browserOriginAllowed, EnableCompression: false}
	downstream, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer downstream.Close()
	downstream.SetReadLimit(client.MaxMessage)
	done := make(chan struct{}, 2)
	relay := func(dst, src *websocket.Conn) {
		defer func() { done <- struct{}{} }()
		for {
			kind, data, err := src.ReadMessage()
			if err != nil || kind != websocket.BinaryMessage {
				return
			}
			if err = dst.WriteMessage(kind, data); err != nil {
				return
			}
		}
	}
	go relay(upstream, downstream)
	go relay(downstream, upstream)
	// Recheck the original session/account while the otherwise opaque relay is
	// alive. Logout, password/grant changes and expiry cannot leave a tunnel live.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			current, err := s.authenticate(r)
			if err != nil || current.User.ID != p.User.ID {
				return
			}
			s.opMu.Lock()
			_, _, err = s.authorizeConnection(r, current, false)
			s.opMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}
