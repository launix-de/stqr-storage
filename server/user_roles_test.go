// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func updateUserRole(id client.ID, name string, role uint8, disabled bool) client.Message {
	e := client.Encoder{}
	e.ID(id)
	e.Text(name)
	e.U8(role)
	e.Bool(disabled)
	return userMessage(client.UserUpdate, e)
}
func userLogin(t *testing.T, s *Server, name, password string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "http://stqr.test/api/session", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.SetBasicAuth(name, password)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}
func TestRolePromotionEnablesLoginAndDemotionRevokesIt(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	create, id := createUserMessage("new-user", "role-password")
	if _, status := s.dispatchAs(create, nil, p); status != 201 {
		t.Fatal(status)
	}
	if w := userLogin(t, s, "new-user", "role-password"); w.Code != 403 {
		t.Fatal("ordinary user dashboard login", w.Code)
	}
	promote := updateUserRole(id, "new-user", 1, false)
	for i := 0; i < 2; i++ {
		if _, status := s.dispatchAs(promote, nil, p); status != 200 {
			t.Fatal("promotion/retry", status)
		}
	}
	w := userLogin(t, s, "new-user", "role-password")
	if w.Code != 200 {
		t.Fatal("promoted user cannot log in", w.Code)
	}
	cookie := w.Result().Cookies()[0]
	promoted, e := s.findAccount("new-user")
	if e != nil || promoted.Role != "ADMIN" {
		t.Fatal(e)
	}
	auth := &connectionAuthorization{admin: true}
	ws := &wsSession{authorization: auth, principal: principal{User: promoted}}
	s.wsMu.Lock()
	s.sockets[ws] = struct{}{}
	s.wsMu.Unlock()
	defer func() { s.wsMu.Lock(); delete(s.sockets, ws); s.wsMu.Unlock() }()
	if _, status := s.dispatchAs(updateUserRole(id, "new-user", 0, false), nil, p); status != 200 {
		t.Fatal("demotion", status)
	}
	if !auth.invalid.Load() {
		t.Fatal("existing admin capability survived demotion")
	}
	r := httptest.NewRequest("GET", "http://stqr.test/api/session", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.AddCookie(cookie)
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, r)
	if out.Code != http.StatusUnauthorized {
		t.Fatal("old admin session survived", out.Code)
	}
	if w = userLogin(t, s, "new-user", "role-password"); w.Code != 403 {
		t.Fatal("demoted user can enter dashboard", w.Code)
	}
	if _, status := s.dispatchAs(userMessage(client.UserList, client.Encoder{}), nil, principal{User: promoted}); status != 403 {
		t.Fatal("stale admin principal authorized")
	}
}
func TestRoleUpdateRejectsSelfChangesAndPersistsOthers(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	self, _ := parseID(p.User.ID)
	for _, change := range []client.Message{updateUserRole(client.ID(self), p.User.Name, 0, false), updateUserRole(client.ID(self), p.User.Name, 1, true)} {
		if _, status := s.dispatchAs(change, nil, p); status != 409 {
			t.Fatal("own role or enabled state was changed", status)
		}
	}
	create, id := createUserMessage("another-admin", "same-password")
	// USER_CREATE already has a role field; creating admins must be usable too.
	e := client.Encoder{}
	e.ID(id)
	e.Text("another-admin")
	e.U8(1)
	e.Bool(false)
	e.Text("same-password")
	create.Body = e.B
	if _, status := s.dispatchAs(create, nil, p); status != 201 {
		t.Fatal("create admin", status)
	}
	original, e2 := s.findAccount("another-admin")
	if e2 != nil {
		t.Fatal(e2)
	}
	if _, status := s.dispatchAs(updateUserRole(id, "another-admin", 2, false), nil, p); status != 400 {
		t.Fatal("NODE role accepted for account", status)
	}
	ordinary := p
	ordinary.User.Role = "USER"
	if _, status := s.dispatchAs(updateUserRole(id, "another-admin", 1, false), nil, ordinary); status != 403 {
		t.Fatal("user could change roles", status)
	}
	if _, status := s.dispatchAs(updateUserRole(client.ID(self), p.User.Name, 0, false), nil, p); status != 409 {
		t.Fatal("self-demotion allowed when another admin exists", status)
	}
	if _, status := s.dispatchAs(updateUserRole(id, "another-admin", 0, false), nil, p); status != 200 {
		t.Fatal("changing another user's role failed", status)
	}
	path := s.configPath
	s.Close()
	next, e3 := Open(Options{ConfigPath: filepath.Clean(path)})
	if e3 != nil {
		t.Fatal(e3)
	}
	defer next.Close()
	retained, e3 := next.findAccount("another-admin")
	if e3 != nil || retained.Role != "USER" || retained.PasswordHash != original.PasswordHash {
		t.Fatal("role change lost existing password", e3)
	}
	demoted, e3 := next.findAccount(p.User.Name)
	if e3 != nil || demoted.Role != "ADMIN" {
		t.Fatal("own role was changed", e3)
	}
}
