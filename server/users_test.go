// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"golang.org/x/crypto/bcrypt"
)

func userPrincipal(t *testing.T, s *Server) principal {
	t.Helper()
	u, e := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if e != nil {
		t.Fatal(e)
	}
	return principal{User: u}
}
func userMessage(op uint16, body client.Encoder) client.Message {
	id, _ := parseID(newID())
	return client.Message{Kind: client.Request, Opcode: op, ID: client.ID(id), Body: body.B}
}
func createUserMessage(name, password string) (client.Message, client.ID) {
	id, _ := parseID(newID())
	target := client.ID(id)
	e := client.Encoder{}
	e.ID(target)
	e.Text(name)
	e.U8(0)
	e.Bool(false)
	e.Text(password)
	return userMessage(client.UserCreate, e), target
}
func TestUserCommandsPersistAndDeduplicateWithoutSecrets(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	m, id := createUserMessage("alice", "initial-secret")
	for i := 0; i < 2; i++ {
		body, status := s.dispatchAs(m, nil, p)
		if status != 201 {
			t.Fatalf("create: %d %x", status, body)
		}
	}
	u, e := s.findAccount("alice")
	if e != nil || u.ID != id.String() || u.Role != "USER" || bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("initial-secret")) != nil {
		t.Fatal("invalid stored user", e)
	}
	bad := m
	bad.Body = append(append([]byte{}, m.Body...), 0)
	if _, status := s.dispatchAs(bad, nil, p); status != 400 {
		t.Fatal("operation ID reuse accepted")
	}
	duplicate, _ := createUserMessage("alice", "other-secret")
	if _, status := s.dispatchAs(duplicate, nil, p); status != 409 {
		t.Fatal("duplicate username accepted")
	}
	list, status := s.dispatchAs(userMessage(client.UserList, client.Encoder{}), nil, p)
	if status != 200 || bytes.Contains(list, []byte("password_hash")) || bytes.Contains(list, []byte(u.PasswordHash)) {
		t.Fatal("list leaked verifier")
	}
	d := client.Decoder{B: list}
	d.V()
	d.Blob()
	if d.Count(100) != 2 {
		t.Fatal("wrong user count")
	}
	for i := 0; i < 2; i++ {
		d.ID()
		d.Text()
		d.U8()
		d.Bool()
	}
	if d.Done() != nil {
		t.Fatal("USER_LIST layout")
	}
	enc := client.Encoder{}
	enc.ID(id)
	enc.Text("replacement-secret")
	change := userMessage(client.UserSetPassword, enc)
	s.authMu.Lock()
	s.sessions["alice-session"] = session{Username: "alice", UserID: u.ID}
	s.authMu.Unlock()
	for i := 0; i < 2; i++ {
		if _, status = s.dispatchAs(change, nil, p); status != 200 {
			t.Fatal("password change", status)
		}
	}
	s.authMu.Lock()
	_, present := s.sessions["alice-session"]
	s.authMu.Unlock()
	if present {
		t.Fatal("password change retained session")
	}
	oldSocket := &wsSession{server: s, principal: principal{User: u}, request: httptest.NewRequest("GET", "http://localhost/api/ws", nil)}
	oldSocket.request.RemoteAddr = "127.0.0.1:1234"
	if _, valid := oldSocket.principalNow(); valid {
		t.Fatal("old password socket remains authorized")
	}
	h, e := s.system.Open(false)
	if e != nil {
		t.Fatal(e)
	}
	raw, _, e := h.Get(systemKey("accounts"))
	h.Close()
	if e != nil {
		t.Fatal(e)
	}
	for _, secret := range []string{"initial-secret", "replacement-secret"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("plaintext secret persisted")
		}
	}
	path := s.configPath
	s.Close()
	next, e := openTransportTestNode(Options{ConfigPath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	u, e = next.findAccount("alice")
	if e != nil || bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte("replacement-secret")) != nil {
		t.Fatal("new password not persisted", e)
	}
	if _, status = next.dispatchAs(m, nil, userPrincipal(t, next)); status != 201 {
		t.Fatal("create retry lost after restart")
	}
	enc = client.Encoder{}
	enc.ID(id)
	remove := userMessage(client.UserDelete, enc)
	for i := 0; i < 2; i++ {
		if _, status = next.dispatchAs(remove, nil, userPrincipal(t, next)); status != 200 {
			t.Fatal("delete or retry", status)
		}
	}
	if _, e = next.findAccount("alice"); e == nil {
		t.Fatal("deleted user can authenticate")
	}
}
func TestUserAuthorizationAndValidation(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	own, _ := parseID(p.User.ID)
	enc := client.Encoder{}
	enc.ID(client.ID(own))
	if _, status := s.dispatchAs(userMessage(client.UserDelete, enc), nil, p); status != 409 {
		t.Fatal("self deletion allowed")
	}
	ordinary := p
	ordinary.User.Role = "USER"
	for _, opcode := range []uint16{client.UserList, client.UserCreate, client.UserSetPassword, client.UserDelete} {
		if _, status := s.dispatchAs(userMessage(opcode, client.Encoder{}), nil, ordinary); status != 403 {
			t.Fatal("ordinary user admin access", opcode, status)
		}
	}
	for _, name := range []string{"", " spaced ", "name:colon", "node-spoof", "bad\nname", strings.Repeat("x", 129)} {
		m, _ := createUserMessage(name, "pw")
		if _, status := s.dispatchAs(m, nil, p); status != 400 {
			t.Fatal("invalid name accepted", name, status)
		}
	}
	for _, password := range []string{"", strings.Repeat("ä", 37)} {
		m, _ := createUserMessage("invalid-password", password)
		if _, status := s.dispatchAs(m, nil, p); status != 400 {
			t.Fatal("invalid password accepted", status)
		}
	}
	m, _ := createUserMessage("good-user", "valid")
	m.ID = client.ID{}
	if _, status := s.dispatchAs(m, nil, p); status != 400 {
		t.Fatal("zero request ID accepted")
	}
	m, _ = createUserMessage("good-user", "valid")
	m.Body = m.Body[:len(m.Body)-1]
	if _, status := s.dispatchAs(m, nil, p); status != 400 {
		t.Fatal("truncated message accepted")
	}
	// Revoked credentials cannot mutate accounts even with a queued principal.
	u := p.User
	enc = client.Encoder{}
	enc.ID(client.ID(own))
	enc.Text("changed-admin")
	if _, status := s.dispatchAs(userMessage(client.UserSetPassword, enc), nil, p); status != 200 {
		t.Fatal("own password change failed", status)
	}
	if _, status := s.dispatchAs(userMessage(client.UserList, client.Encoder{}), nil, principal{User: u}); status != 403 {
		t.Fatal("stale credentials authorized")
	}
}
func TestConcurrentUserCreationDoesNotLoseAccounts(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	var wg sync.WaitGroup
	for _, name := range []string{"one", "two", "three"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			m, _ := createUserMessage(name, "password")
			if _, status := s.dispatchAs(m, nil, p); status != 201 {
				t.Errorf("create %s: %d", name, status)
			}
		}(name)
	}
	wg.Wait()
	var image accountsImage
	if e := s.readBlob("accounts", &image); e != nil || len(image.Users) != 4 {
		t.Fatal("concurrent update lost", e)
	}
}
