// SPDX-License-Identifier: GPL-3.0-only
package server

import (
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
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self' ws:; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		if !s.allowed(r) {
			httpError(w, 403, errors.New("source address is outside the allowed subnets"))
			return
		}
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			b, _ := dashboard.ReadFile("dashboard/index.html")
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(b)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/assets/") && r.Method == http.MethodGet {
			http.StripPrefix("/assets/", files).ServeHTTP(w, r)
			return
		}
		p, e := s.authenticate(r)
		if e != nil {
			if r.URL.Path != "/api/session" {
				w.Header().Set("WWW-Authenticate", `Basic realm="STQR", charset="UTF-8"`)
			}
			httpError(w, 401, errors.New("authentication required"))
			return
		}
		if p.User.Role != "ADMIN" && r.URL.Path != "/api/ws" && r.URL.Path != "/api/message" {
			httpError(w, 403, errors.New("administrator required"))
			return
		}
		if !sameOrigin(r) {
			httpError(w, 403, errors.New("origin rejected"))
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" && p.Cookie && !constantEqual(r.Header.Get("X-CSRF-Token"), p.CSRF) {
			httpError(w, 403, errors.New("CSRF token required"))
			return
		}
		switch {
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
				s.authMu.Lock()
				token := p.SessionToken
				if token == "" {
					if c, e := r.Cookie("stqr_session"); e == nil {
						token = c.Value
					}
				}
				delete(s.sessions, token)
				s.authMu.Unlock()
				http.SetCookie(w, &http.Cookie{Name: "stqr_session", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
				writeJSON(w, 200, map[string]bool{"ok": true})
			default:
				w.WriteHeader(405)
			}
		case r.URL.Path == "/api/health" && r.Method == "GET":
			writeJSON(w, 200, map[string]string{"state": "active", "mode": "single-node"})
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
			body, status := s.dispatchAs(m, nil, p)
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(status)
			w.Write(client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body}))
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
