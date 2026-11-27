// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"

	"github.com/launix-de/stqr-storage/client"
	jssdk "github.com/launix-de/stqr-storage/client/js"
)

//go:embed dashboard/*
var dashboard embed.FS

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func httpError(w http.ResponseWriter, status int, e error) {
	writeJSON(w, status, map[string]any{"error": e.Error(), "status": status})
}
func readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
func (s *Server) allowed(r *http.Request) bool {
	host, _, e := net.SplitHostPort(r.RemoteAddr)
	if e != nil {
		return false
	}
	ip, e := netip.ParseAddr(host)
	if e != nil {
		return false
	}
	ip = ip.Unmap()
	s.mu.RLock()
	subnets := append([]string(nil), s.cfg.AllowSubnets...)
	s.mu.RUnlock()
	if len(subnets) == 0 {
		return true
	}
	for _, raw := range subnets {
		p, e := netip.ParsePrefix(raw)
		if e == nil && p.Contains(ip) {
			return true
		}
	}
	return false
}
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, e := url.Parse(origin)
	return e == nil && u.Scheme == "http" && u.Host == r.Host && u.Path == "" && u.RawQuery == "" && u.User == nil
}
func (s *Server) Handler() http.Handler {
	assets, _ := fs.Sub(dashboard, "dashboard")
	files := http.FileServer(http.FS(assets))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		styleNonce := client.NewRequestID().String()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'nonce-"+styleNonce+"'; connect-src 'self' ws:; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		if !s.allowed(r) {
			httpError(w, 403, errors.New("source address is outside the allowed subnets"))
			return
		}
		if browserPath(r.URL.Path) {
			if !s.browserOriginAllowed(r) {
				httpError(w, 403, errors.New("origin rejected"))
				return
			}
			if origin := r.Header.Get("Origin"); origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, X-STQR-Bucket, X-STQR-Format, X-CSRF-Token")
				w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			}
			if r.Method == "OPTIONS" {
				w.WriteHeader(204)
				return
			}
			if err := browserHeaders(r); err != nil {
				httpError(w, 400, err)
				return
			}
		}
		if r.URL.Path == "/s3" || strings.HasPrefix(r.URL.Path, "/s3/") {
			s.serveS3(w, r)
			return
		}
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			b, _ := dashboard.ReadFile("dashboard/index.html")
			b = bytes.Replace(b, []byte("<head>"), []byte(`<head><meta name="stqr-style-nonce" content="`+styleNonce+`">`), 1)
			if s.joinPending() || s.joinRequested.Load() {
				b = bytes.Replace(b, []byte(`id="join-login-pending" hidden`), []byte(`id="join-login-pending"`), 1)
				b = bytes.Replace(b, []byte(`class="login-note"`), []byte(`class="login-note" hidden`), 1)
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(b)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/assets/sdk/") && r.Method == http.MethodGet {
			http.StripPrefix("/assets/sdk/", http.FileServer(http.FS(jssdk.Files))).ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/assets/") && r.Method == http.MethodGet {
			http.StripPrefix("/assets/", files).ServeHTTP(w, r)
			return
		}
		p, e := s.authenticate(r)
		if e != nil {
			if statusOf(e) == 503 {
				httpError(w, 503, e)
				return
			}
			if r.URL.Path != "/api/session" {
				w.Header().Set("WWW-Authenticate", `Basic realm="STQR", charset="UTF-8"`)
			}
			httpError(w, 401, errors.New("authentication required"))
			return
		}
		if p.User.Role != "ADMIN" && r.URL.Path != "/api/ws" && r.URL.Path != "/api/message" && r.URL.Path != "/api/bucket-connect" && r.URL.Path != "/api/bucket-tunnel" {
			httpError(w, 403, errors.New("administrator required"))
			return
		}
		if !sameOrigin(r) && !(browserPath(r.URL.Path) && s.browserOriginAllowed(r)) {
			httpError(w, 403, errors.New("origin rejected"))
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" && p.Cookie && !constantEqual(r.Header.Get("X-CSRF-Token"), p.CSRF) {
			httpError(w, 403, errors.New("CSRF token required"))
			return
		}
		if s.storageWarming.Load() && r.Method != "GET" && r.Method != "HEAD" && r.URL.Path != "/api/message" && r.URL.Path != "/api/session" {
			httpError(w, 503, s.storageUnavailable())
			return
		}
		switch {
		case r.URL.Path == "/api/format-plugins" || strings.HasPrefix(r.URL.Path, "/format-plugins/"):
			s.serveFormatPlugins(w, r)
		case r.URL.Path == "/api/bucket-connect":
			s.serveBrowserConnect(w, r, p)
		case r.URL.Path == "/api/bucket-tunnel":
			s.serveBucketTunnel(w, r, p)
		case r.URL.Path == "/api/session":
			switch r.Method {
			case "GET":
				if !p.Cookie {
					s.createSession(w, p)
				} else {
					writeJSON(w, 200, map[string]any{"username": p.User.Name, "csrf_token": p.CSRF})
				}
			case "POST":
				s.createSession(w, p)
			case "DELETE":
				s.opMu.Lock()
				s.authMu.Lock()
				token := p.SessionToken
				if token == "" {
					if c, e := r.Cookie("stqr_session"); e == nil {
						token = c.Value
					}
				}
				delete(s.sessions, token)
				s.authMu.Unlock()
				s.invalidateSessionConnections(token)
				s.opMu.Unlock()
				http.SetCookie(w, &http.Cookie{Name: "stqr_session", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
				writeJSON(w, 200, map[string]bool{"ok": true})
			default:
				w.WriteHeader(405)
			}
		case strings.HasPrefix(r.URL.Path, "/api/users/"):
			s.serveS3Keys(w, r, p)
		case r.URL.Path == "/api/health" && r.Method == "GET":
			state := "active"
			if s.storageWarming.Load() {
				state = "warming"
			}
			if s.storageWarmError.Load() != nil {
				state = "recovery-failed"
			}
			writeJSON(w, 200, map[string]string{"state": state})
		case r.URL.Path == "/api/ws" && r.Method == "GET":
			s.serveWebSocket(w, r, p)
		case r.URL.Path == "/api/message" && r.Method == "POST":
			b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, client.MaxMessage))
			if e != nil {
				httpError(w, 413, e)
				return
			}
			m, e := client.DecodeMessage(b)
			if e != nil || m.Kind != client.Request {
				httpError(w, 400, errors.New("expected one binary STQR request"))
				return
			}
			if m.Opcode == clusterJoinInfo && p.User.Role == "ADMIN" {
				w.Header().Set(compatibilityHeader, profileHeader(s.protocolVersion()))
				profile, err := decodeProfile(r.Header)
				if err == nil {
					err = checkPeerMessage(s.protocolVersion(), profile, m)
				}
				if err != nil {
					httpError(w, 426, err)
					return
				}
			}
			formatMessage := m
			if m.Opcode == client.NodeAdminRequest {
				if inner, err := client.DecodeNodeAdmin(m.Body); err == nil {
					formatMessage.Opcode, formatMessage.Body = inner.Opcode, inner.Body
				}
			}
			if socketFormatRequired(formatMessage.Opcode) && len(formatMessage.Body) >= 16 && !p.isPeer() {
				id := client.Decoder{B: formatMessage.Body}
				bucket := id.ID()
				if err := s.openBucketFormat(r.Context(), bucket.String(), r.Header.Get(client.FormatHeader), p); err != nil {
					status := statusOf(err)
					w.Header().Set("Content-Type", "application/octet-stream")
					w.WriteHeader(status)
					w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(status, nil, err.Error())}))
					return
				}
			}

			body, status := s.dispatchAs(m, nil, p)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(status)
			w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body}))
			if m.Opcode == clusterJoinPrepare && status == 202 {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				select {
				case s.joinSignal <- struct{}{}:
				default:
				}
			}
		case r.URL.Path == "/api/volumes" && r.Method == "GET":
			writeJSON(w, 200, s.CollectStats(client.StatsFilter{}))
		case r.URL.Path == "/api/volumes" && r.Method == "POST":
			var q AddVolumeRequest
			if e = readJSON(w, r, &q); e != nil {
				httpError(w, 400, e)
				return
			}
			op, e := s.AddVolume(r.Header.Get("Idempotency-Key"), q)
			if e != nil {
				httpError(w, statusOf(e), e)
				return
			}
			writeJSON(w, op.Status, op)
		case strings.HasPrefix(r.URL.Path, "/api/volumes/") && r.Method == "DELETE":
			op, e := s.RemoveVolume(r.Header.Get("Idempotency-Key"), strings.TrimPrefix(r.URL.Path, "/api/volumes/"))
			if e != nil {
				httpError(w, statusOf(e), e)
				return
			}
			writeJSON(w, op.Status, op)
		default:
			httpError(w, 404, errors.New("endpoint not found"))
		}
	})
}
