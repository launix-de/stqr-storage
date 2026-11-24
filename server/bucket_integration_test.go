// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func bucketPolicyBody(t *testing.T, b *Bucket) []byte {
	t.Helper()
	info, err := b.Info(volume.ID{}, true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := info.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return data[:len(data)-4]
}
func adminBucketMessage(t *testing.T, s *Server, opcode uint16, body []byte) (client.Message, int, client.Message) {
	t.Helper()
	m := client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: body}
	admin := s.cfg.Auth.BootstrapAdmin
	status, reply := bucketHTTPReply(t, s, m, admin.Username, admin.Password)
	return m, status, reply
}
func createProtocolBucket(t *testing.T, s *Server, name string) (*Bucket, client.Message) {
	t.Helper()
	draft := newTestBucket(t, name)
	m, status, reply := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatalf("create HTTP %d: %v", status, reply)
	}
	bucketReplyResult(t, reply, 201)
	b, err := s.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	return b, m
}
func TestBucketAdminAndClientObjectsEndToEnd(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 8<<20)
	b, create := createProtocolBucket(t, s, "native")
	// Creating the same logical operation after a lost reply returns the original ID.
	admin := s.cfg.Auth.BootstrapAdmin
	status, reply := bucketHTTPReply(t, s, create, admin.Username, admin.Password)
	if status != 201 {
		t.Fatal(status)
	}
	bucketReplyResult(t, reply, 201)
	user := bucketProtocolUser(t, s)
	grant := client.Encoder{}
	grant.ID(client.ID(b.ID()))
	uid, _ := parseID(user.ID)
	grant.ID(client.ID(uid))
	grant.U32(uint32(BucketAllPermissions))
	_, status, reply = adminBucketMessage(t, s, client.BucketGrant, grant.B)
	if status != 200 {
		t.Fatal(status, reply)
	}
	if len(b.Describe().Grants) != 1 {
		t.Fatal("stable RAM instance did not receive its users sub-array")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{1}
	info, err := c.Put(ctx, oid, []byte("hello"), client.PutOptions{RequestID: client.NewRequestID()})
	if err != nil || info.Length != 5 {
		t.Fatal(info, err)
	}
	appendID := client.NewRequestID()
	app, err := c.Append(ctx, oid, []byte(" world"), client.AppendOptions{RequestID: appendID})
	if err != nil || app.StartOffset != 5 {
		t.Fatal(app, err)
	}
	retry, err := c.Append(ctx, oid, []byte(" world"), client.AppendOptions{RequestID: appendID})
	if err != nil || retry != app {
		t.Fatal("append duplicated", retry, app, err)
	}
	changed, err := c.CAS(ctx, oid, 6, []byte("world"), []byte("STQR!"), client.CASOptions{MutationOptions: client.MutationOptions{RequestID: client.NewRequestID()}})
	if err != nil || changed.Length != app.Length {
		t.Fatal(changed, err)
	}
	if _, err = c.CAS(ctx, oid, 0, []byte("bad"), []byte("new"), client.CASOptions{MutationOptions: client.MutationOptions{RequestID: client.NewRequestID()}}); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("CAS conflict missing", err)
	}
	if _, err = c.PutRange(ctx, oid, 20, []byte("tail"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	data, _, err := c.Get(ctx, oid)
	if err != nil || string(data[:11]) != "hello STQR!" || !bytes.Equal(data[11:20], make([]byte, 9)) || string(data[20:]) != "tail" {
		t.Fatal(data, err)
	}
	if _, err = c.Trunc(ctx, oid, 3, client.TruncOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete(ctx, oid, client.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("delete did not hide object", err)
	}
	// Recreating preserves the prior tombstone's version chain and original retry receipts.
	if _, err = c.Put(ctx, oid, []byte("new"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if again, err := c.Append(ctx, oid, []byte(" world"), client.AppendOptions{RequestID: appendID}); err != nil || again != app {
		t.Fatal("retry lost after delete/recreate", again, err)
	}
	data, _, err = c.Get(ctx, oid)
	if err != nil || string(data) != "new" {
		t.Fatal(data, err)
	}
	// Different connections isolate namespaces even when an OID is identical.
	other, _ := createProtocolBucket(t, s, "other")
	second, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(other.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, _, err = second.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("bucket namespaces overlap", err)
	}
	// Revocation applies to the already-open native client.
	revoke := client.Encoder{}
	revoke.ID(client.ID(b.ID()))
	revoke.ID(client.ID(uid))
	_, status, _ = adminBucketMessage(t, s, client.BucketRevoke, revoke.B)
	if status != 200 {
		t.Fatal(status)
	}
	if _, _, err = c.Get(ctx, oid); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("existing connection retained revoked rights", err)
	}
}
func TestBucketClientStreamingAndFilesystem(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "stream-data", "hdd", 96<<20)
	b, _ := createProtocolBucket(t, s, "filesystem")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte("streaming"), 2100000)
	oid := client.ObjectID{9}
	if _, err = c.PutFrom(ctx, oid, bytes.NewReader(payload), uint64(len(payload)), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err = c.GetTo(ctx, oid, &out); err != nil || !bytes.Equal(out.Bytes(), payload) {
		t.Fatal("stream roundtrip", err, out.Len())
	}
	fs, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	dir, err := fs.Create(ctx, client.FilesystemRootID(), "dir", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create(ctx, dir.ID, "large", 0644, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fs.WriteAt(ctx, file.ID, payload, 0); err != nil {
		t.Fatal(err)
	}
	reopened, err := client.OpenFilesystem(ctx, c, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err = reopened.ReadAt(ctx, file.ID, got, 0); err != nil || !bytes.Equal(got, payload) {
		t.Fatal("filesystem over native protocol", err)
	}
}
func TestBucketConfigurationAndAsynchronousDeletion(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "config-data", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "configurable")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{4}
	if _, err = c.Put(ctx, oid, []byte(strings.Repeat("compressible", 1000)), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	description := b.Describe()
	description.Policy.Version++
	description.Policy.Compression = "gzip"
	draft := &Bucket{description: description}
	body := client.Encoder{B: bucketPolicyBody(t, draft)}
	body.U64(1)
	configOp, status, _ := adminBucketMessage(t, s, client.BucketSetPolicy, body.B)
	if status != 202 {
		t.Fatal(status)
	}
	deadline := time.Now().Add(5 * time.Second)
	for b.Describe().DesiredPolicy != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.Describe().Policy.Compression != "gzip" || b.Describe().DesiredPolicy != nil {
		t.Fatal("configuration worker did not complete", b.Describe())
	}
	if info, err := c.Stat(ctx, oid); err != nil || info.Compression != 1 {
		t.Fatal("recompression not applied", info, err)
	}
	op, err := s.operation(configOp.ID.String())
	if err != nil || op.State != "fulfilled" {
		t.Fatal(op, err)
	}
	rename := client.Encoder{}
	rename.ID(client.ID(b.ID()))
	rename.Text("renamed")
	_, status, _ = adminBucketMessage(t, s, client.BucketRename, rename.B)
	if status != 200 {
		t.Fatal(status)
	}
	same, err := s.BucketByName("renamed")
	if err != nil || same != b {
		t.Fatal("rename replaced stable RAM instance", err)
	}
	list := client.Encoder{}
	list.Text("")
	list.U32(100)
	_, status, reply := adminBucketMessage(t, s, client.BucketList, list.B)
	if status != 200 {
		t.Fatal(status)
	}
	result := bucketReplyResult(t, reply, 200)
	decoder := client.Decoder{B: result}
	if !strings.Contains(decoder.Text(), "renamed") {
		t.Fatal("bucket list missing renamed bucket")
	}
	del := client.Encoder{}
	del.ID(client.ID(b.ID()))
	deleteOp, status, _ := adminBucketMessage(t, s, client.BucketDelete, del.B)
	if status != 202 {
		t.Fatal(status)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !b.Describe().DeletionComplete && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !b.Describe().DeletionComplete {
		t.Fatal("bucket deletion not completed")
	}
	op, err = s.operation(deleteOp.ID.String())
	if err != nil || op.State != "fulfilled" {
		t.Fatal(op, err)
	}
	if _, _, err = c.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("deleted bucket readable", err)
	}
	recreated, _ := createProtocolBucket(t, s, "renamed")
	if recreated.ID() == b.ID() {
		t.Fatal("deleted ID reused")
	}
}

func TestBucketLogicalQuotaAndAppendOnlyUser(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "quota-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "quota")
	user := bucketProtocolUser(t, s)
	uid, _ := parseID(user.ID)
	grant := client.Encoder{}
	grant.ID(client.ID(b.ID()))
	grant.ID(client.ID(uid))
	grant.U32(uint32(BucketAppend))
	_, status, _ := adminBucketMessage(t, s, client.BucketGrant, grant.B)
	if status != 200 {
		t.Fatal(status)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{8}
	if _, err = c.Put(ctx, oid, []byte("forbidden"), client.PutOptions{}); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal(err)
	}
	firstID := client.NewRequestID()
	first, err := c.Append(ctx, oid, []byte("abc"), client.AppendOptions{RequestID: firstID})
	if err != nil || first.StartOffset != 0 {
		t.Fatal(first, err)
	}
	quota := client.Encoder{}
	quota.ID(client.ID(b.ID()))
	quota.U8(0)
	quota.U64(0)
	quota.Bool(false)
	quota.U64(5)
	quota.U32(150000)
	quota.U64(1)
	_, status, _ = adminBucketMessage(t, s, client.BucketSetQuota, quota.B)
	if status != 200 {
		t.Fatal(status)
	}
	if _, err = c.Append(ctx, oid, []byte("def"), client.AppendOptions{}); !errors.Is(err, client.ErrFull) {
		t.Fatal("quota allowed excess growth", err)
	}
	if _, err = c.Append(ctx, oid, []byte("de"), client.AppendOptions{}); err != nil {
		t.Fatal(err)
	}
	again, err := c.Append(ctx, oid, []byte("abc"), client.AppendOptions{RequestID: firstID})
	if err != nil || again != first {
		t.Fatal("retry at quota limit was not deduplicated", again, err)
	}
	if _, _, err = c.Get(ctx, oid); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatal("append permission implied read", err)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	a, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	data, _, err := a.Get(ctx, oid)
	if err != nil || string(data) != "abcde" {
		t.Fatal(data, err)
	}
	// Ordinary users cannot turn their data connection into bucket administration.
	draft := newTestBucket(t, "unauthorized")
	status, _ = bucketHTTPReply(t, s, client.Message{Kind: client.Request, Opcode: client.BucketCreate, ID: client.NewRequestID(), Body: bucketPolicyBody(t, draft)}, user.Name, "bucket-test-password")
	if status != 403 {
		t.Fatal(status)
	}
}
func TestBucketPlacementDrainAndRestartPreserveRetries(t *testing.T) {
	s := testServer(t)
	source := addTestVolume(t, s, "before-drain", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "persistent")
	host := httptest.NewServer(s.Handler())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	oid := client.ObjectID{11}
	request := client.NewRequestID()
	app, err := c.Append(ctx, oid, []byte("durable-log"), client.AppendOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	target := addTestVolume(t, s, "after-drain", "hdd", 8<<20)
	op, err := s.RemoveVolume(newID(), source.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	waitOperation(t, s, op.ID, "fulfilled")
	placements := b.Describe().LocalVolumes
	if len(placements) != 1 || placements[0] != target.VolumeID {
		t.Fatal("drain did not publish replacement placement", placements)
	}
	again, err := c.Append(ctx, oid, []byte("durable-log"), client.AppendOptions{RequestID: request})
	if err != nil || again != app {
		t.Fatal("drain lost retry", again, err)
	}
	path := s.configPath
	id := b.ID()
	c.Close()
	host.Close()
	s.Close()
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	host = httptest.NewServer(reopened.Handler())
	defer host.Close()
	c, err = client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(id))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	data, _, err := c.Get(ctx, oid)
	if err != nil || string(data) != "durable-log" {
		t.Fatal(data, err)
	}
	again, err = c.Append(ctx, oid, []byte("durable-log"), client.AppendOptions{RequestID: request})
	if err != nil || again != app {
		t.Fatal("restart lost retry", again, err)
	}
}
func TestBucketDiskPolicyMovePreservesVersionsAndRetries(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "hdd-source", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "tiered")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	oid := client.ObjectID{12}
	request := client.NewRequestID()
	original, err := c.Append(ctx, oid, bytes.Repeat([]byte("x"), 10000), client.AppendOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	target := addTestVolume(t, s, "ssd-target", "ssd", 8<<20)
	description := b.Describe()
	description.Policy.Version = 2
	description.Policy.StorageStrategy = "ssd"
	description.Policy.Compression = "gzip"
	body := client.Encoder{B: bucketPolicyBody(t, &Bucket{description: description})}
	body.U64(1)
	_, status, _ := adminBucketMessage(t, s, client.BucketSetPolicy, body.B)
	if status != 202 {
		t.Fatal(status)
	}
	deadline := time.Now().Add(5 * time.Second)
	for b.Describe().DesiredPolicy != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	d := b.Describe()
	if d.DesiredPolicy != nil || len(d.LocalVolumes) != 1 || d.LocalVolumes[0] != target.VolumeID {
		t.Fatal("tier migration incomplete", d)
	}
	info, err := c.Stat(ctx, oid)
	if err != nil || info.Length != original.Length || info.Compression != 1 {
		t.Fatal("conversion changed logical version", info, err)
	}
	again, err := c.Append(ctx, oid, bytes.Repeat([]byte("x"), 10000), client.AppendOptions{RequestID: request})
	if err != nil || again != original {
		t.Fatal("migration lost original retry codec/fingerprint", again, err)
	}
	if _, err = c.Append(ctx, oid, []byte("tail"), client.AppendOptions{}); err != nil {
		t.Fatal("new append cannot use target codec", err)
	}
}
func TestBucketDeletionIntentResumesOnRestart(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "delete-data", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "resumable-delete")
	admin, err := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Put(context.Background(), principal{User: admin}, volume.ObjectID{1}, []byte("payload"), volume.Options{RequestID: volume.ID(client.NewRequestID())})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate interruption after the deletion intent committed and before its
	// worker started. This is the durable recovery boundary, not a timing race.
	description := b.Describe()
	description.State = BucketDeleting
	operation := client.NewRequestID()
	description.Receipts = append(description.Receipts, BucketReceipt{ID: operation.String(), Actor: admin.ID, Digest: strings.Repeat("a", 64), Opcode: client.BucketDelete, Status: 202, Running: true})
	if _, err = s.buckets.Save(description, description.Revision); err != nil {
		t.Fatal(err)
	}
	id := b.ID()
	path := s.configPath
	s.Close()
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		op, e := reopened.operation(operation.String())
		if e == nil && op.State == "fulfilled" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = reopened.Bucket(id); !errors.Is(err, ErrBucketNotFound) {
		t.Fatal("deleted bucket still accessible", err)
	}
	op, err := reopened.operation(operation.String())
	if err != nil || op.State != "fulfilled" {
		t.Fatal(op, err)
	}
}

func TestBucketRAMResetRejectsLearnedClientRuntime(t *testing.T) {
	s := testServer(t)
	op, err := s.AddVolume(newID(), AddVolumeRequest{Name: "ram-data", Source: 2, Class: "ram", SizeBytes: 8 << 20})
	if err != nil || op.State == "failed" {
		t.Fatal(op, err)
	}
	draft := newTestBucket(t, "volatile")
	draft.description.Policy.StorageStrategy = "ram"
	_, status, _ := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatal(status)
	}
	host := httptest.NewServer(s.Handler())
	address := strings.TrimPrefix(host.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	request := client.NewRequestID()
	oid := client.ObjectID{44}
	first, err := c.Append(ctx, oid, []byte("before-reset"), client.AppendOptions{RequestID: request})
	if err != nil || first.RuntimeID == (client.ID{}) {
		t.Fatal(first, err)
	}
	path := s.configPath
	s.Close()
	host.Close()
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	next := httptest.NewUnstartedServer(reopened.Handler())
	next.Listener.Close()
	next.Listener = listener
	next.Start()
	defer next.Close()
	if _, err = c.Append(ctx, oid, []byte("before-reset"), client.AppendOptions{RequestID: request}); !errors.Is(err, client.ErrGone) {
		t.Fatal("pre-reset retry was replayed into empty RAM", err)
	}
	fresh, err := client.Dial(ctx, next.URL, admin.Username, admin.Password, client.BucketID(draft.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if _, _, err = fresh.Get(ctx, oid); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("old operation reappeared after RAM reset", err)
	}
}
func TestBucketStreamingDigestFailureDoesNotCommit(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "digest-data", "hdd", 4<<20)
	b, _ := createProtocolBucket(t, s, "checked-upload")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := s.cfg.Auth.BootstrapAdmin
	auth, _ := http.NewRequest(http.MethodGet, host.URL, nil)
	auth.SetBasicAuth(admin.Username, admin.Password)
	auth.Header.Set(client.BucketHeader, b.ID().String())
	dialer := websocket.Dialer{Subprotocols: []string{"stqr.v2"}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(host.URL, "http")+"/api/ws", auth.Header)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	id := client.NewRequestID()
	oid := client.ObjectID{45}
	digest := sha256.Sum256([]byte("abc"))
	body := client.Encoder{}
	body.ID(client.ID(b.ID()))
	body.B = append(body.B, oid[:]...)
	body.U64(0)
	body.U64(0)
	body.ID(client.ID{})
	body.Bool(false)
	body.Bool(false)
	body.Bool(false)
	body.U8(1)
	body.U64(3)
	body.B = append(body.B, digest[:]...)
	body.Blob(nil)
	if err = conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Request, Opcode: 0x0102, ID: id, Body: body.B})); err != nil {
		t.Fatal(err)
	}
	chunk := client.Encoder{}
	chunk.ID(id)
	chunk.U64(0)
	chunk.U64(0)
	chunk.Blob([]byte("xyz"))
	conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.FireAndForget, Opcode: 5, ID: client.NewRequestID(), Body: chunk.B}))
	end := client.Encoder{}
	end.ID(id)
	end.U64(3)
	end.B = append(end.B, digest[:]...)
	conn.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.FireAndForget, Opcode: 6, ID: client.NewRequestID(), Body: end.B}))
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, replyBytes, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	reply, err := client.DecodeMessage(replyBytes)
	if err != nil || reply.ID != id {
		t.Fatal(reply, err)
	}
	bucketReplyResult(t, reply, 400)
	actor, err := s.findAccount(admin.Username)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Get(context.Background(), principal{User: actor}, volume.ObjectID(oid)); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("bad upload became visible", err)
	}
}
