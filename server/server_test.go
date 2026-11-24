// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"golang.org/x/crypto/bcrypt"
)

// These fixtures preserve isolated single-node/transport behavior. Production
// daemon integration tests call Open directly with full control enabled.
func openTransportTestNode(opts Options) (*Server, error) {
	opts.transportOnly = true
	return Open(opts)
}

func testServer(t *testing.T) *Server {
	t.Helper()
	s, e := openTransportTestNode(Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml"), values: map[string]string{"system-volume-bytes": "262144"}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestBootstrapConfigAccountsAndAutoGrowth(t *testing.T) {
	s := testServer(t)
	original := copyConfig(s.cfg)
	st, e := os.Stat(s.configPath)
	if e != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("config permissions: %v %v", st, e)
	}
	var users accountsImage
	if e = s.readBlob("accounts", &users); e != nil {
		t.Fatal(e)
	}
	if len(users.Users) != 1 || bcrypt.CompareHashAndPassword([]byte(users.Users[0].PasswordHash), []byte(original.Auth.BootstrapAdmin.Password)) != nil {
		t.Fatal("bootstrap verifier missing")
	}
	raw, _ := json.Marshal(users)
	if bytes.Contains(raw, []byte(original.Auth.BootstrapAdmin.Password)) {
		t.Fatal("plaintext password in system object")
	}
	want := strings.Repeat("live metadata ", 20000)
	if e = s.writeBlob("growth-test", want); e != nil {
		t.Fatal(e)
	}
	if s.system.Size() <= 262144 {
		t.Fatal("admin volume failed to grow")
	}
	size := s.system.Size()
	var got string
	if e = s.readBlob("growth-test", &got); e != nil || got != want {
		t.Fatal("grown object", e)
	}
	path := s.configPath
	s.Close()
	s2, e := openTransportTestNode(Options{ConfigPath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	if s2.cfg.ClusterID != original.ClusterID || s2.cfg.Node.ID != original.Node.ID || s2.cfg.Auth.BootstrapAdmin.Password != original.Auth.BootstrapAdmin.Password || s2.system.Size() != size {
		t.Fatal("identity/credentials/capacity changed on restart")
	}
	if e = s2.readBlob("growth-test", &got); e != nil || got != want {
		t.Fatal("growth lost on restart", e)
	}
	if _, e = openTransportTestNode(Options{ConfigPath: path}); e == nil {
		t.Fatal("configuration lock allowed a duplicate node")
	}
}
func TestFlagsAndJoinNeverForkExistingCluster(t *testing.T) {
	o, e := ParseOptions([]string{"--config", "custom.yaml", "--port", "7520", "--listen", "127.0.0.1:7419", "--ram-bytes", "1048576", "--allow-subnet", "10.0.0.0/8,2001:db8::/32"}, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	c, e := loadConfig(filepath.Join(t.TempDir(), "custom.yaml"), o)
	if e != nil || c.Node.Listen != "127.0.0.1:7520" || c.RAMBytes != 1048576 || len(c.AllowSubnets) != 2 {
		t.Fatalf("flags: %+v %v", c, e)
	}
	o.values["connect"] = "http://127.0.0.1:9999"
	c, e = loadConfig(filepath.Join(t.TempDir(), "join.yaml"), o)
	if e == nil || c.ClusterID != "" || c.Auth.ClusterToken != "" {
		t.Fatal("unimplemented join created a cluster")
	}
	if _, e = ParseOptions([]string{"init"}, io.Discard); e == nil {
		t.Fatal("unexpected CLI subcommand")
	}
}
func addTestVolume(t *testing.T, s *Server, name, class string, size int64) Operation {
	t.Helper()
	op, e := s.AddVolume(newID(), AddVolumeRequest{Name: name, Class: class, SizeBytes: size})
	if e != nil {
		t.Fatal(e)
	}
	return op
}
func waitOperation(t *testing.T, s *Server, id, state string) Operation {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		op, e := s.operation(id)
		if e == nil && op.State == state {
			return op
		}
		time.Sleep(10 * time.Millisecond)
	}
	op, _ := s.operation(id)
	t.Fatalf("operation never became %s: %+v", state, op)
	return op
}
func TestVolumeAddIdempotencyAndRescan(t *testing.T) {
	s := testServer(t)
	id := newID()
	q := AddVolumeRequest{Name: "data-01", Class: "hdd", SizeBytes: 512 << 10}
	op, e := s.AddVolume(id, q)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.AddVolume(id, q)
	if e != nil || again != op {
		t.Fatalf("retry: %+v %v", again, e)
	}
	q.Name = "different"
	if _, e = s.AddVolume(id, q); statusOf(e) != 400 {
		t.Fatal("operation reuse not rejected", e)
	}
	if _, e = s.AddVolume(newID(), AddVolumeRequest{Name: "../bad", Class: "hdd", SizeBytes: 512 << 10}); statusOf(e) != 400 {
		t.Fatal("path traversal not rejected", e)
	}
	m := s.volumes[op.VolumeID]
	if e = os.Truncate(m.config.Path, 640<<10); e != nil {
		t.Fatal(e)
	}
	r, e := s.RescanVolume(newID(), op.VolumeID)
	if e != nil || r.State != "fulfilled" || m.store.Size() != 640<<10 {
		t.Fatalf("rescan: %+v %v", r, e)
	}
	system := s.system.Identity().VolumeID.String()
	if _, e = s.RemoveVolume(newID(), system); statusOf(e) != 409 {
		t.Fatal("system volume removal allowed", e)
	}
}
func TestDrainWaitsAndPreservesSparseTombstonesAndDedupe(t *testing.T) {
	s := testServer(t)
	source := addTestVolume(t, s, "source", "hdd", 2<<20)
	src := s.volumes[source.VolumeID].store
	k := volume.Key{Bucket: volume.ID{1}, Object: volume.ObjectID{1}}
	deleted := volume.Key{Bucket: volume.ID{2}, Object: volume.ObjectID{2}}
	request, _ := volume.NewID()
	h, e := src.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.PutRange(k, 1<<30, []byte("far"), volume.Options{}); e != nil {
		t.Fatal(e)
	}
	result, e := h.Append(k, []byte("!"), volume.Options{RequestID: request})
	if e != nil {
		t.Fatal(e)
	}
	h.Put(deleted, []byte("old"), volume.Options{})
	h.Delete(deleted, volume.Options{})
	h.Close()
	remove, e := s.RemoveVolume(newID(), source.VolumeID)
	if e != nil {
		t.Fatal(e)
	}
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		op, _ := s.operation(remove.ID)
		if strings.Contains(op.Detail, "kein passendes") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.RLock()
	still := s.volumes[source.VolumeID]
	s.mu.RUnlock()
	if still == nil || still.store.State() != volume.Draining {
		t.Fatal("drain detached without a safe target")
	}
	target := addTestVolume(t, s, "target", "ssd", 3<<20)
	waitOperation(t, s, remove.ID, "fulfilled")
	s.mu.RLock()
	dst := s.volumes[target.VolumeID].store
	_, exists := s.volumes[source.VolumeID]
	s.mu.RUnlock()
	if exists {
		t.Fatal("source still configured")
	}
	if _, e = os.Stat(filepath.Join(s.volumesDir, "source")); e != nil {
		t.Fatal("source file was deleted")
	}
	h, e = dst.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	defer h.Close()
	data, info, e := h.GetRange(k, 1<<30, 4)
	if e != nil || string(data) != "far!" || info.Length != result.Info.Length {
		t.Fatal("migration lost object", e)
	}
	again, e := h.Append(k, []byte("!"), volume.Options{RequestID: request})
	if e != nil || again != result {
		t.Fatal("migration lost retry result", e)
	}
	records, e := h.ScanObjects(nil, 10)
	if e != nil || len(records) != 2 || !records[1].Info.Deleted {
		t.Fatal("migration lost tombstone", e)
	}
}
func TestConfigValidationAndNetworkGate(t *testing.T) {
	s := testServer(t)
	bad := map[string]json.RawMessage{"allow_subnets": json.RawMessage(`["nonsense"]`)}
	before, _ := os.ReadFile(s.configPath)
	if e := s.updateConfig(bad); statusOf(e) != 400 {
		t.Fatal(e)
	}
	after, _ := os.ReadFile(s.configPath)
	if !bytes.Equal(before, after) {
		t.Fatal("invalid config was saved")
	}
	if e := s.updateConfig(map[string]json.RawMessage{"name": json.RawMessage(`"review-node"`), "listen": json.RawMessage(`"127.0.0.1:7418"`)}); e != nil {
		t.Fatal(e)
	}
	if s.cfg.Node.Name != "review-node" || s.configView()["restart_required"] != true {
		t.Fatal("config edits not applied")
	}
	r := httptest.NewRequest("GET", "http://localhost/api/health", nil)
	r.RemoteAddr = "192.0.2.1:4321"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("forwarded header bypassed network gate")
	}
	for _, addr := range []string{"127.0.0.1:4321", "[::1]:4321"} {
		r.RemoteAddr = addr
		w = httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal("IPv4/6 gate did not permit loopback")
		}
	}
}
func requestMessage(t *testing.T, c *websocket.Conn, m client.Message) client.Message {
	t.Helper()
	if e := c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(m)); e != nil {
		t.Fatal(e)
	}
	c.SetReadDeadline(time.Now().Add(15 * time.Second))
	for {
		_, b, e := c.ReadMessage()
		if e != nil {
			t.Fatal(e)
		}
		got, e := client.DecodeMessage(b)
		if e != nil {
			t.Fatal(e)
		}
		if got.Kind == client.Response && got.ID == m.ID {
			return got
		}
	}
}
func TestWebSocketOriginalMessagesQueuesAndStats(t *testing.T) {
	s := testServer(t)
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	req, _ := http.NewRequest("GET", httpServer.URL, nil)
	req.SetBasicAuth(s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password)
	dial := websocket.Dialer{Subprotocols: []string{"stqr.v2"}}
	conn, _, e := dial.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http")+"/api/ws", req.Header)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	toID := func() client.ID { id, _ := parseID(newID()); return client.ID(id) }
	ping := client.Encoder{}
	ping.U64(42)
	ff := toID()
	conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.FireAndForget, Opcode: client.Ping, ID: ff, Body: ping.B}))
	ids := []client.ID{toID(), toID(), toID(), toID()}
	for _, id := range ids {
		conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.Ping, ID: id, Body: ping.B}))
	}
	seen := map[client.ID]bool{}
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	for len(seen) < len(ids) {
		_, b, e := conn.ReadMessage()
		if e != nil {
			t.Fatal(e)
		}
		m, e := client.DecodeMessage(b)
		if e != nil || m.Kind != client.Response || m.ID == ff {
			t.Fatal("fire-and-forget generated a response", e)
		}
		seen[m.ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatal("request response missing")
		}
	}
	sub := toID()
	body := client.Encoder{}
	body.ID(sub)
	body.U8(1)
	body.V(0)
	body.V(0)
	body.V(0)
	body.Bool(false)
	body.U32(3000)
	conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.StatsSubscribe, ID: toID(), Body: body.B}))
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	found := false
	for i := 0; i < 3 && !found; i++ {
		_, b, e := conn.ReadMessage()
		if e != nil {
			t.Fatal(e)
		}
		m, e := client.DecodeMessage(b)
		if e != nil {
			t.Fatal(e)
		}
		if m.Kind == client.Push && m.Opcode == client.StatsSnapshotMessage {
			d := client.Decoder{B: m.Body}
			if d.ID() != sub {
				t.Fatal("wrong subscription")
			}
			d.ID()
			d.ID()
			d.U64()
			if d.U32() != 0 || d.U32() != 1 {
				t.Fatal("wrong multipart layout")
			}
			digest := d.Take(32)
			data := d.Blob()
			sum := sha256.Sum256(data)
			if d.Done() != nil || !bytes.Equal(digest, sum[:]) {
				t.Fatal("stats digest invalid")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no immediate statistics push")
	}
	// Mutations use the same binary dispatcher, not a dashboard-only endpoint.
	add := client.Encoder{}
	add.Text("from-ws")
	add.U8(0)
	add.Text("")
	add.U8(0)
	add.U64(512 << 10)
	response := requestMessage(t, conn, client.Message{Kind: client.Request, Opcode: client.VolumeAdd, ID: toID(), Body: add.B})
	d := client.Decoder{B: response.Body}
	if status := d.V(); status < 200 || status >= 300 {
		t.Fatalf("binary add status %d", status)
	}
}
func TestConfiguredVolumeDamageNeverReformats(t *testing.T) {
	s := testServer(t)
	path := s.configPath
	system := s.system.Name()
	s.Close()
	if e := os.Remove(system); e != nil {
		t.Fatal(e)
	}
	if _, e := openTransportTestNode(Options{ConfigPath: path}); e == nil {
		t.Fatal("missing configured system volume was recreated")
	}
	if _, e := os.Stat(system); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("system volume unexpectedly exists")
	}
}

func TestBrowserSessionsCSRFAndOriginalHTTPMessage(t *testing.T) {
	s := testServer(t)
	handler := s.Handler()
	makeRequest := func(method, path string, body []byte) *http.Request {
		r := httptest.NewRequest(method, "http://stqr.test"+path, bytes.NewReader(body))
		r.RemoteAddr = "127.0.0.1:1234"
		return r
	}
	probe := httptest.NewRecorder()
	handler.ServeHTTP(probe, makeRequest("GET", "/api/session", nil))
	if probe.Code != 401 || probe.Header().Get("WWW-Authenticate") != "" {
		t.Fatal("session probe must not trigger a native browser authentication prompt")
	}
	login := makeRequest("POST", "/api/session", nil)
	login.SetBasicAuth(s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, login)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	var sessionImage struct {
		CSRF string `json:"csrf_token"`
	}
	json.Unmarshal(w.Body.Bytes(), &sessionImage)
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].Secure || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("session cookie attributes")
	}
	id, _ := parseID(newID())
	body := client.Encoder{}
	body.Text(`{"name":"browser-config"}`)
	message := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.NodeConfigSet, ID: client.ID(id), Body: body.B})
	for _, test := range []struct {
		token, origin string
		want          int
	}{{"", "http://stqr.test", 403}, {sessionImage.CSRF, "http://evil.test", 403}, {sessionImage.CSRF, "http://stqr.test", 200}} {
		r := makeRequest("POST", "/api/message", message)
		r.AddCookie(cookies[0])
		r.Header.Set("X-CSRF-Token", test.token)
		r.Header.Set("Origin", test.origin)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != test.want {
			t.Fatalf("csrf/origin: %d want %d", w.Code, test.want)
		}
	}
	if s.cfg.Node.Name != "browser-config" {
		t.Fatal("original binary message did not update the config")
	}
	logout := makeRequest("DELETE", "/api/session", nil)
	logout.AddCookie(cookies[0])
	logout.Header.Set("X-CSRF-Token", sessionImage.CSRF)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, logout)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	expired := makeRequest("GET", "/api/health", nil)
	expired.AddCookie(cookies[0])
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, expired)
	if w.Code != 401 {
		t.Fatal("revoked session remains valid")
	}
}
func TestIncompleteLocalOperationsRecoverWithoutLosingVolumes(t *testing.T) {
	s := testServer(t)
	added := addTestVolume(t, s, "recovered", "hdd", 512<<10)
	op, _ := s.operation(added.ID)
	op.State = "requested"
	op.Status = 202
	s.saveOperation(op)
	abortedID := newID()
	_, _, e := s.beginOperation(abortedID, "add", AddVolumeRequest{Name: "unpublished", Class: "hdd", SizeBytes: 512 << 10})
	if e != nil {
		t.Fatal(e)
	}
	path := s.configPath
	s.Close()
	next, e := openTransportTestNode(Options{ConfigPath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	recovered, e := next.operation(added.ID)
	if e != nil || recovered.State != "fulfilled" || recovered.VolumeID != added.VolumeID {
		t.Fatalf("published add not recovered: %+v %v", recovered, e)
	}
	aborted, e := next.operation(abortedID)
	if e != nil || aborted.State != "failed" {
		t.Fatal("unpublished operation was falsely acknowledged", e)
	}
}

func TestBoundObjectSDKAndAdministrativeRoleBoundary(t *testing.T) {
	s := testServer(t)
	var image accountsImage
	if e := s.readBlob("accounts", &image); e != nil {
		t.Fatal(e)
	}
	hash, e := bcrypt.GenerateFromPassword([]byte("user-password"), bcrypt.MinCost)
	if e != nil {
		t.Fatal(e)
	}
	image.Users = append(image.Users, account{ID: newID(), Name: "app-user", Role: "USER", PasswordHash: string(hash)})
	if e = s.writeBlob("accounts", image); e != nil {
		t.Fatal(e)
	}
	// The SDK binds and authorizes its bucket during upgrade, so this test needs
	// an existing bucket and grant; an unknown bucket is rejected at Dial now.
	draft, e := NewBucket(volume.ID{9}, "app-bucket")
	if e != nil {
		t.Fatal(e)
	}
	description := draft.Describe()
	description.Grants = []BucketGrant{{UserID: image.Users[len(image.Users)-1].ID, Permissions: BucketRead}}
	if _, e = s.buckets.Save(description, 0); e != nil {
		t.Fatal(e)
	}

	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, e := client.Dial(ctx, host.URL, "app-user", "user-password", client.BucketID{9})
	if e != nil {
		t.Fatal("ordinary bucket user cannot negotiate the object transport", e)
	}
	defer c.Close()
	if _, _, e = c.Get(ctx, client.ObjectID{1}); !errors.Is(e, client.ErrNotFound) {
		t.Fatal("missing object must return not found", e)
	}
	filter := client.Encoder{}
	filter.U8(0)
	filter.V(0)
	filter.V(0)
	filter.V(0)
	filter.Bool(false)
	id, _ := parseID(newID())
	body := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: client.StatsGet, ID: client.ID(id), Body: filter.B})
	r, _ := http.NewRequest("POST", host.URL+"/api/message", bytes.NewReader(body))
	r.SetBasicAuth("app-user", "user-password")
	response, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatal("ordinary user accessed cluster statistics")
	}
}

func TestNodeIOCountersKeepDetachedVolumeHistory(t *testing.T) {
	s := testServer(t)
	a := addTestVolume(t, s, "retire-source", "hdd", 1<<20)
	b := addTestVolume(t, s, "retire-target", "ssd", 2<<20)
	src, dst := s.volumes[a.VolumeID].store, s.volumes[b.VolumeID].store
	h, e := src.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	_, e = h.Put(volume.Key{Bucket: volume.ID{1}, Object: volume.ObjectID{1}}, bytes.Repeat([]byte("x"), 80<<10), volume.Options{})
	h.Close()
	if e != nil {
		t.Fatal(e)
	}
	op, e := s.RemoveVolume(newID(), a.VolumeID)
	if e != nil {
		t.Fatal(e)
	}
	waitOperation(t, s, op.ID, "fulfilled")
	expected := src.IOStats().WriteBytes + dst.IOStats().WriteBytes + s.system.IOStats().WriteBytes
	got := s.CollectStats(client.StatsFilter{}).IO.PhysicalWriteBytes
	if got < expected {
		t.Fatalf("node counters forgot detached bytes: %d < %d", got, expected)
	}
}
