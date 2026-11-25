// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"golang.org/x/crypto/bcrypt"
)

func bucketInfoRequest(name string) client.Message {
	e := client.Encoder{}
	e.Text(name)
	return client.Message{Kind: client.Request, Opcode: client.BucketInfoGet, ID: client.NewRequestID(), Body: e.B}
}
func bucketProtocolUser(t *testing.T, s *Server) account {
	t.Helper()
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("bucket-test-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	user := account{ID: newID(), Name: "bucket-user", Role: "USER", PasswordHash: string(hash)}
	image.Users = append(image.Users, user)
	if err = s.writeBlob("accounts", image); err != nil {
		t.Fatal(err)
	}
	return user
}
func bucketHTTPReply(t *testing.T, s *Server, m client.Message, username, password string) (int, client.Message) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://localhost/api/message", bytes.NewReader(client.EncodeMessage(m)))
	req.RemoteAddr = "127.0.0.1:12345"
	req.SetBasicAuth(username, password)
	format := "fs"
	if len(m.Body) >= 16 {
		d := client.Decoder{B: m.Body}
		if b, err := s.Bucket(volume.ID(d.ID())); err == nil && b.Describe().Format != "" {
			format = b.Describe().Format
		}
	}
	req.Header.Set(client.FormatHeader, format)
	recorder := httptest.NewRecorder()
	s.Handler().ServeHTTP(recorder, req)
	reply, err := client.DecodeMessage(recorder.Body.Bytes())
	if err != nil {
		t.Fatal("not a STQR response", recorder.Code, err)
	}
	if reply.Kind != client.Response || reply.ID != m.ID || reply.Opcode != m.Opcode {
		t.Fatal("uncorrelated response", reply)
	}
	return recorder.Code, reply
}
func bucketReplyResult(t *testing.T, reply client.Message, wantStatus int) []byte {
	t.Helper()
	d := client.Decoder{B: reply.Body}
	status := d.V()
	d.Blob()
	if d.Err != nil || status != uint64(wantStatus) {
		t.Fatalf("status %d, want %d: %v", status, wantStatus, d.Err)
	}
	if wantStatus >= 400 && len(d.B) != 0 {
		t.Fatal("error exposes a result")
	}
	return d.B
}
func assertBucketWire(t *testing.T, result []byte, id volume.ID, name string, permissions BucketPermissions, state uint8) {
	t.Helper()
	d := client.Decoder{B: result}
	if d.ID() != client.ID(id) || d.U64() != 1 || d.Text() != name || d.U8() != state {
		t.Fatal("wrong bucket identity/version/state")
	}
	if len(result) < 4 || binary.LittleEndian.Uint32(result[len(result)-4:]) != uint32(permissions) {
		t.Fatal("wrong effective permissions")
	}
}
func TestBucketInfoHTTPAuthenticationAndPolicy(t *testing.T) {
	s := testServer(t)
	user := bucketProtocolUser(t, s)
	b := newTestBucket(t, "application")
	description := b.Describe()
	description.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead | BucketAppend}}
	desired := description.Policy
	desired.Version = 2
	desired.StorageStrategy = "ssd"
	description.DesiredPolicy = &desired
	b, err := s.buckets.Save(description, 0)
	if err != nil {
		t.Fatal(err)
	}
	private := newTestBucket(t, "private")
	if _, err = s.buckets.Save(private.Describe(), 0); err != nil {
		t.Fatal(err)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	cases := []struct {
		name, user, password, bucket string
		status                       int
		id                           volume.ID
		permissions                  BucketPermissions
	}{
		{"admin-system", admin.Username, admin.Password, "system", 200, systemBucket, BucketAllPermissions},
		{"admin-application", admin.Username, admin.Password, "application", 200, b.ID(), BucketAllPermissions},
		{"granted-user", user.Name, "bucket-test-password", "application", 200, b.ID(), BucketRead | BucketAppend},
		{"private-denied", user.Name, "bucket-test-password", "private", 403, volume.ID{}, 0},
		{"system-denied", user.Name, "bucket-test-password", "system", 403, volume.ID{}, 0},
		{"missing", user.Name, "bucket-test-password", "missing", 404, volume.ID{}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reply := bucketHTTPReply(t, s, bucketInfoRequest(tc.bucket), tc.user, tc.password)
			if status != tc.status {
				t.Fatalf("HTTP %d != %d", status, tc.status)
			}
			result := bucketReplyResult(t, reply, tc.status)
			if tc.status == 200 {
				assertBucketWire(t, result, tc.id, tc.bucket, tc.permissions, 0)
				if tc.bucket == "application" {
					d := client.Decoder{B: result}
					d.ID()
					d.U64()
					d.Text()
					d.U8()
					if d.U8() != 0 {
						t.Fatal("desired SSD policy replaced current HDD policy")
					}
				}
			}
		})
	}
	malformed := []client.Message{bucketInfoRequest(""), bucketInfoRequest("bad\x00name"), bucketInfoRequest(strings.Repeat("x", 256)), {Kind: client.Request, Opcode: client.BucketInfoGet, ID: client.NewRequestID(), Body: []byte{0x80}}}
	trailing := bucketInfoRequest("application")
	trailing.Body = append(trailing.Body, 0)
	malformed = append(malformed, trailing)
	for _, request := range malformed {
		status, reply := bucketHTTPReply(t, s, request, admin.Username, admin.Password)
		if status != 400 {
			t.Fatal(status)
		}
		bucketReplyResult(t, reply, 400)
	}
	// Invalid authentication is rejected before metadata decoding/lookup.
	request := httptest.NewRequest(http.MethodPost, "http://localhost/api/message", bytes.NewReader(client.EncodeMessage(bucketInfoRequest("system"))))
	request.RemoteAddr = "127.0.0.1:12345"
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatal(response.Code)
	}
	description = b.Describe()
	description.State = BucketLocked
	if _, err = s.buckets.Save(description, 1); err != nil {
		t.Fatal(err)
	}
	status, reply := bucketHTTPReply(t, s, bucketInfoRequest("application"), user.Name, "bucket-test-password")
	if status != 200 {
		t.Fatal(status)
	}
	assertBucketWire(t, bucketReplyResult(t, reply, 200), b.ID(), "application", BucketRead, 1)
}
func TestBucketInfoWebSocketRevocation(t *testing.T) {
	s := testServer(t)
	user := bucketProtocolUser(t, s)
	b := newTestBucket(t, "shared")
	description := b.Describe()
	description.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	b, err := s.buckets.Save(description, 0)
	if err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	request, _ := http.NewRequest(http.MethodGet, host.URL, nil)
	request.SetBasicAuth(user.Name, "bucket-test-password")
	request.Header.Set(client.BucketHeader, b.ID().String())
	request.Header.Set(client.FormatHeader, "fs")
	dialer := websocket.Dialer{Subprotocols: []string{"stqr.v2"}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/ws", request.Header)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reply := requestMessage(t, conn, bucketInfoRequest("shared"))
	assertBucketWire(t, bucketReplyResult(t, reply, 200), b.ID(), "shared", BucketRead, 0)
	description = b.Describe()
	description.Grants = nil
	if _, err = s.buckets.Save(description, 1); err != nil {
		t.Fatal(err)
	}
	// A revoked connection capability is closed before the next request runs.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err = conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(bucketInfoRequest("shared"))); err == nil {
		_, _, err = conn.ReadMessage()
	}
	var revoked *websocket.CloseError
	if !errors.As(err, &revoked) {
		t.Fatalf("revoked socket remains usable or timed out: %v", err)
	}
}
func TestBucketCatalogServerMigrationAndRestart(t *testing.T) {
	s := testServer(t)
	if s.cfg.BucketCatalogVersion != 1 {
		t.Fatal("bootstrap marker not set")
	}
	b := newTestBucket(t, "retained")
	if _, err := s.buckets.Save(b.Describe(), 0); err != nil {
		t.Fatal(err)
	}
	// Simulate an interrupted upgrade: root committed, config marker not yet saved.
	path := s.configPath
	cfg := copyConfig(s.cfg)
	cfg.BucketCatalogVersion = 0
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.buckets.Lookup(b.ID()); err != nil {
		reopened.Close()
		t.Fatal("migration replaced existing root", err)
	}
	if reopened.cfg.BucketCatalogVersion != 1 {
		t.Fatal("migration marker not completed")
	}
	// A missing root after activation must fail startup, never initialize empty.
	h, err := reopened.system.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Delete(BucketCatalogKey(), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	if _, err = openTransportTestNode(Options{ConfigPath: path}); !errors.Is(err, ErrBucketCatalogMissing) {
		t.Fatal("lost catalog was silently replaced", err)
	}
}
func TestBucketCatalogServerUpgradeFromAccountsOnly(t *testing.T) {
	s := testServer(t)
	path := s.configPath
	cfg := copyConfig(s.cfg)
	cfg.BucketCatalogVersion = 0
	h, err := s.system.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Delete(BucketCatalogKey(), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err = saveConfig(path, cfg); err != nil {
		t.Fatal(err)
	}
	upgraded, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if _, err = upgraded.buckets.LookupName("system"); err != nil {
		t.Fatal(err)
	}
	if _, err = upgraded.findAccount(cfg.Auth.BootstrapAdmin.Username); err != nil {
		t.Fatal("upgrade lost account", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(data, []byte("bucket_catalog_version: 1")) {
		t.Fatal("upgrade marker not persisted", err)
	}
}
func TestBucketInfoUsesRAMAndStartupRejectsCorruption(t *testing.T) {
	s := testServer(t)
	h, err := s.system.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(BucketCatalogKey(), make([]byte, 32), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	status, reply := bucketHTTPReply(t, s, bucketInfoRequest("system"), admin.Username, admin.Password)
	if status != 200 {
		t.Fatalf("cached bucket metadata unavailable: HTTP %d", status)
	}
	bucketReplyResult(t, reply, 200)
	path := s.configPath
	s.Close()
	if _, err = openTransportTestNode(Options{ConfigPath: path}); !errors.Is(err, ErrBucketCatalogCorrupt) {
		t.Fatal("corrupt catalog not rejected during startup", err)
	}
}
