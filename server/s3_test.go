// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const testS3Access = "STQRTESTACCESSKEY0001"
const testS3Secret = "s3-test-secret-not-a-production-credential"

func s3TestSetup(t *testing.T) (*Server, *Bucket, account) {
	t.Helper()
	s := testServer(t)
	addTestVolume(t, s, "s3-test", "hdd", 96<<20)
	b, _ := createProtocolBucket(t, s, "fop-files")
	user := bucketProtocolUser(t, s)
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	uid, _ := parseID(user.ID)
	e.ID(client.ID(uid))
	e.U32(uint32(BucketAllPermissions))
	if _, status, _ := adminBucketMessage(t, s, client.BucketGrant, e.B); status != 200 {
		t.Fatal(status)
	}
	enc := client.Encoder{}
	enc.ID(client.ID(uid))
	enc.Text(testS3Access)
	enc.Text(testS3Secret)
	if _, status := s.dispatchAs(userMessage(client.UserS3KeySet, enc), nil, userPrincipal(t, s)); status != 200 {
		t.Fatal(status)
	}
	return s, b, user
}
func signS3Test(req *http.Request, body []byte, secret string) {
	hash := sha256.Sum256(body)
	payload := hex.EncodeToString(hash[:])
	signS3Payload(req, payload, secret)
}
func signS3Payload(req *http.Request, payload, secret string) {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	day := stamp[:8]
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	names := []string{"host"}
	for k := range req.Header {
		if strings.ToLower(k) != "authorization" {
			names = append(names, strings.ToLower(k))
		}
	}
	sort.Strings(names)
	headers := ""
	for _, name := range names {
		value := req.Host
		if name != "host" {
			value = strings.Join(req.Header.Values(name), ",")
		}
		headers += name + ":" + strings.Join(strings.Fields(value), " ") + "\n"
	}
	signed := strings.Join(names, ";")
	canonical := req.Method + "\n" + req.URL.EscapedPath() + "\n" + strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20") + "\n" + headers + "\n" + signed + "\n" + payload
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/eu-west-1/s3/aws4_request"
	mac := func(k []byte, v string) []byte { h := hmac.New(sha256.New, k); h.Write([]byte(v)); return h.Sum(nil) }
	key := mac([]byte("AWS4"+secret), day)
	key = mac(key, "eu-west-1")
	key = mac(key, "s3")
	key = mac(key, "aws4_request")
	sig := hex.EncodeToString(mac(key, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+hex.EncodeToString(sum[:])))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testS3Access+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
}
func s3TestRequest(t *testing.T, s *Server, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	r.RemoteAddr = "127.0.0.1:1234"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	signS3Test(r, body, testS3Secret)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}
func requireS3Status(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status %d expected %d: %s", w.Code, status, w.Body.String())
	}
}
func TestS3HTTPPublicationRangesAndBinaryDelete(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	p := principal{User: user}
	key := "12: space/ümlaut+%"
	path := "/s3/fop-files/" + s3Escape(key, true)
	put := s3TestRequest(t, s, "PUT", path, []byte("hello world"), map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Owner": "fop", "X-Amz-Acl": "private"})
	requireS3Status(t, put, 200)
	get := s3TestRequest(t, s, "GET", path, nil, nil)
	requireS3Status(t, get, 200)
	if get.Body.String() != "hello world" || get.Header()["x-amz-meta-owner"][0] != "fop" || get.Header().Get("ETag") != put.Header().Get("ETag") {
		t.Fatal(get.Result())
	}
	for rng, want := range map[string]string{"bytes=2-5": "llo ", "bytes=-5": "world", "bytes=6-": "world"} {
		w := s3TestRequest(t, s, "GET", path, nil, map[string]string{"Range": rng})
		requireS3Status(t, w, 206)
		if w.Body.String() != want {
			t.Fatal(rng, w.Body.String())
		}
	}
	requireS3Status(t, s3TestRequest(t, s, "GET", path, nil, map[string]string{"Range": "bytes=99-"}), 416)
	requireS3Status(t, s3TestRequest(t, s, "GET", path, nil, map[string]string{"If-None-Match": put.Header().Get("ETag")}), 304)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, []byte("no"), map[string]string{"If-None-Match": "*"}), 412)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, []byte("no"), map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(make([]byte, 16))}), 400)
	before, _, err := b.Get(ctx, p, volume.ObjectID(client.S3MetadataID(key)))
	if err != nil {
		t.Fatal(err)
	}
	requireS3Status(t, s3TestRequest(t, s, "DELETE", path, nil, nil), 204)
	if _, _, err = b.Get(ctx, p, volume.ObjectID(client.S3MetadataID(key))); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("deleted native content retained", err)
	}
	if string(before[:4]) != "STS3" || len(before) < client.S3HeaderSize {
		t.Fatal("missing binary record")
	}
	requireS3Status(t, s3TestRequest(t, s, "GET", path, nil, nil), 404)
	requireS3Status(t, s3TestRequest(t, s, "DELETE", path, nil, nil), 204)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, []byte("again"), map[string]string{"If-None-Match": "*"}), 200)
}
func TestS3ListingAndConcurrentPublication(t *testing.T) {
	s, _, _ := s3TestSetup(t)
	for _, key := range []string{"dir/a", "dir/b", "dir/sub/c", "other"} {
		requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/"+key, []byte(key), nil), 200)
	}
	w := s3TestRequest(t, s, "GET", "/s3/fop-files?prefix=dir%2F&delimiter=%2F&max-keys=2&list-type=2", nil, nil)
	requireS3Status(t, w, 200)
	var result s3ListResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.IsTruncated || len(result.Contents) != 2 || result.NextContinuationToken == "" {
		t.Fatal(w.Body.String())
	}
	w = s3TestRequest(t, s, "GET", "/s3/fop-files?prefix=dir%2F&delimiter=%2F&list-type=2&continuation-token="+url.QueryEscape(result.NextContinuationToken), nil, nil)
	requireS3Status(t, w, 200)
	result = s3ListResult{}
	xml.Unmarshal(w.Body.Bytes(), &result)
	if result.IsTruncated || len(result.CommonPrefixes) != 1 || result.CommonPrefixes[0].Prefix != "dir/sub/" {
		t.Fatal(w.Body.String())
	}
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/other", nil, nil), 204)
	w = s3TestRequest(t, s, "GET", "/s3/fop-files?max-keys=100", nil, nil)
	requireS3Status(t, w, 200)
	if strings.Contains(w.Body.String(), "<Key>other</Key>") {
		t.Fatal("deleted key listed")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := strings.Repeat(strconv.Itoa(i), 128<<10)
			r := httptest.NewRequest("PUT", "http://localhost/s3/fop-files/race", strings.NewReader(data))
			r.RemoteAddr = "127.0.0.1:1"
			r.Header.Set("X-Amz-Meta-Generation", strconv.Itoa(i))
			signS3Test(r, []byte(data), testS3Secret)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 200 {
				t.Errorf("concurrent upload: %d %s", w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	get := s3TestRequest(t, s, "GET", "/s3/fop-files/race", nil, nil)
	requireS3Status(t, get, 200)
	if get.Body.String() != strings.Repeat(get.Header()["x-amz-meta-generation"][0], 128<<10) {
		t.Fatal("mixed metadata/payload")
	}
	sum := md5.Sum(get.Body.Bytes())
	if get.Header().Get("ETag") != "\""+hex.EncodeToString(sum[:])+"\"" {
		t.Fatal("ETag mismatch")
	}
}
func TestS3AuthenticationGrantsAndKeyPersistence(t *testing.T) {
	s, b, user := s3TestSetup(t)
	for _, permission := range []BucketPermissions{BucketWrite, BucketRead, BucketDelete} {
		e := client.Encoder{}
		e.ID(client.ID(b.ID()))
		uid, _ := parseID(user.ID)
		e.ID(client.ID(uid))
		e.U32(uint32(permission))
		if _, st, _ := adminBucketMessage(t, s, client.BucketGrant, e.B); st != 200 {
			t.Fatal(st)
		}
		if permission == BucketWrite {
			requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/private", []byte("private"), nil), 200)
			requireS3Status(t, s3TestRequest(t, s, "GET", "/s3/fop-files/private", nil, nil), 403)
		}
		if permission == BucketRead {
			requireS3Status(t, s3TestRequest(t, s, "GET", "/s3/fop-files/private", nil, nil), 200)
			requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/private", []byte("no"), nil), 403)
		}
		if permission == BucketDelete {
			requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/private", nil, nil), 204)
			requireS3Status(t, s3TestRequest(t, s, "GET", "/s3/fop-files", nil, nil), 403)
		}
	}
	r := httptest.NewRequest("GET", "http://localhost/s3/fop-files", nil)
	r.RemoteAddr = "127.0.0.1:1"
	signS3Test(r, nil, "wrong secret")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	requireS3Status(t, w, 403)
	var err error
	var image accountsImage
	if err = s.readBlob("accounts", &image); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(image)
	if bytes.Contains(data, []byte(testS3Secret)) {
		t.Fatal("plaintext signing secret persisted")
	}
	config := s.configPath
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{ConfigPath: config})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	found, secret, err := reopened.s3KeyUser(testS3Access)
	if err != nil || found.ID != user.ID || secret != testS3Secret {
		t.Fatal("access key did not survive restart", err)
	}
	e := client.Encoder{}
	uid, _ := parseID(user.ID)
	e.ID(client.ID(uid))
	e.Text(testS3Access)
	if _, st := reopened.dispatchAs(userMessage(client.UserS3KeyDelete, e), nil, userPrincipal(t, reopened)); st != 200 {
		t.Fatal(st)
	}
	requireS3Status(t, s3TestRequest(t, reopened, "GET", "/s3/fop-files", nil, nil), 403)
}
func TestS3PHPClient(t *testing.T) {
	autoload := os.Getenv("STQR_S3_PHP_AUTOLOAD")
	if autoload == "" {
		t.Skip("set STQR_S3_PHP_AUTOLOAD to an installed aws/aws-sdk-php vendor/autoload.php")
	}
	s, _, _ := s3TestSetup(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	cmd := exec.Command("php", "tests/s3.php", autoload, host.URL+"/s3", testS3Access, testS3Secret)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("PHP SDK: %v\n%s", err, out)
	}
}

// Public library and the embedded adapter exchange the exact same records.
func TestS3NativeSDKAndFailedPublication(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx := context.Background()
	a := s.cfg.Auth.BootstrapAdmin
	conn, err := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), "s3")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	api := client.NewS3(conn)
	o, err := api.Put(ctx, "sdk", strings.NewReader("from native SDK"), 15, client.S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	get := s3TestRequest(t, s, "GET", "/s3/fop-files/sdk", nil, nil)
	requireS3Status(t, get, 200)
	if get.Body.String() != "from native SDK" {
		t.Fatal(get.Body.String())
	}
	if err = api.Delete(ctx, o.Key); err != nil {
		t.Fatal(err)
	}
	requireS3Status(t, s3TestRequest(t, s, "GET", "/s3/fop-files/sdk", nil, nil), 404)
	failing := client.NewS3(s3FailPublication{S3Store: conn, target: client.S3MetadataID("failed")})
	if _, err = failing.Put(ctx, "failed", strings.NewReader("unpublished"), 11, client.S3PutOptions{}); err == nil {
		t.Fatal("publication failure swallowed")
	}
	listed, err := api.List(ctx, "")
	if err != nil || len(listed) != 0 {
		t.Fatal("failed publication exposed", listed, err)
	}
}

type s3FailPublication struct {
	client.S3Store
	target client.ObjectID
}

func (s s3FailPublication) PutFrom(ctx context.Context, id client.ObjectID, src io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	if id == s.target {
		return client.ObjectInfo{}, errors.New("injected publication failure")
	}
	return s.S3Store.PutFrom(ctx, id, src, n, opt)
}

func TestS3ChecksumsStagingAndSignatureTampering(t *testing.T) {
	s, _, _ := s3TestSetup(t)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/crc", []byte("original"), nil), 200)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/crc", []byte("no"), map[string]string{"X-Amz-Checksum-Crc32": "AAAAAA=="}), 400)
	r := httptest.NewRequest("PUT", "http://localhost/s3/fop-files/crc", strings.NewReader("tampered"))
	r.RemoteAddr = "127.0.0.1:1"
	signS3Test(r, []byte("original"), testS3Secret)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	requireS3Status(t, w, 400)
	get := s3TestRequest(t, s, "GET", "/s3/fop-files/crc", nil, nil)
	requireS3Status(t, get, 200)
	if get.Body.String() != "original" {
		t.Fatal("bad checksum replaced prior image")
	}
	r = httptest.NewRequest("PUT", "http://localhost/s3/fop-files/staged", strings.NewReader("staged body"))
	r.RemoteAddr = "127.0.0.1:1"
	r.ContentLength = -1
	signS3Test(r, []byte("staged body"), testS3Secret)
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	requireS3Status(t, w, 200)
	get = s3TestRequest(t, s, "GET", "/s3/fop-files/staged", nil, nil)
	if get.Body.String() != "staged body" {
		t.Fatal(get.Body.String())
	}
	r = httptest.NewRequest("GET", "http://localhost/s3/fop-files/crc", nil)
	r.RemoteAddr = "127.0.0.1:1"
	signS3Test(r, nil, testS3Secret)
	r.URL.Path = "/s3/fop-files/staged"
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	requireS3Status(t, w, 403)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/no-sse", []byte("data"), map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}), 501)
}
func TestS3KeyAdminReceiptsAndPrivateListings(t *testing.T) {
	s, _, user := s3TestSetup(t)
	admin := userPrincipal(t, s)
	uid, _ := parseID(user.ID)
	e := client.Encoder{}
	e.ID(client.ID(uid))
	e.Text("STQRNEWACCESSKEY001")
	e.Text("another test secret with enough entropy")
	message := userMessage(client.UserS3KeySet, e)
	for i := 0; i < 2; i++ {
		body, status := s.dispatchAs(message, nil, admin)
		if status != 200 || bytes.Contains(body, []byte("another test secret")) {
			t.Fatal("key retry", status)
		}
	}
	e = client.Encoder{}
	e.ID(client.ID(uid))
	body, status := s.dispatchAs(userMessage(client.UserS3KeyList, e), nil, admin)
	if status != 200 || bytes.Contains(body, []byte(testS3Secret)) || !bytes.Contains(body, []byte("STQRNEWACCESSKEY001")) {
		t.Fatal("key list", status)
	}
	message.Body = append(message.Body, 0)
	if _, st := s.dispatchAs(message, nil, admin); st != 400 {
		t.Fatal("changed retry input accepted", st)
	}
	if _, st := s.dispatchAs(userMessage(client.UserS3KeyList, e), nil, principal{User: user}); st != 403 {
		t.Fatal("ordinary user accessed key administration", st)
	}
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	if err := validateSystemAccounts(accounts); err != nil {
		t.Fatal(err)
	}
}
func TestS3DashboardKeysBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for real dashboard tests")
	}
	s := testServer(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := userPrincipal(t, s)
	input, _ := json.Marshal(map[string]string{"url": host.URL, "username": s.cfg.Auth.BootstrapAdmin.Username, "password": s.cfg.Auth.BootstrapAdmin.Password, "user": admin.User.ID})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "dashboard_s3_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("S3 dashboard: %v\n%s", err, out)
	}
}

func TestS3ReclaimsPhysicalSpaceOnOverwriteDeleteAndFailure(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	ctx := context.Background()
	// More total writes than the entire volume, always replacing the same key.
	// The old detached-payload implementation fills the volume here.
	data := bytes.Repeat([]byte("payload!"), 512<<10)
	var baseline volume.Stats
	volumes := b.allVolumes()
	if len(volumes) != 1 {
		t.Fatal("unexpected test placement", len(volumes))
	}
	v := volumes[0]
	baseline = v.Stats()
	staging := t.TempDir()
	t.Setenv("TMPDIR", staging)
	for i := 0; i < 32; i++ {
		data[0] = byte(i)
		requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/replaced", data, nil), 200)
		stats := v.Stats()
		if stats.DataPhysical > baseline.DataPhysical+uint64(len(data))+(1<<20) {
			t.Fatalf("old content retained on overwrite %d: physical data %d", i, stats.DataPhysical)
		}
	}
	before := v.Stats()
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/replaced", data, map[string]string{"Content-MD5": "AAAAAAAAAAAAAAAAAAAAAA=="}), 400)
	if after := v.Stats(); after.DataPhysical != before.DataPhysical || after.LogicalBytes != before.LogicalBytes {
		t.Fatal("checksum failure changed persistent space", before, after)
	}
	r := httptest.NewRequest("PUT", "http://localhost/s3/fop-files/aborted", io.NopCloser(strings.NewReader("short")))
	r.ContentLength = int64(len(data))
	r.RemoteAddr = "127.0.0.1:1"
	signS3Test(r, data, testS3Secret)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	requireS3Status(t, w, 400)
	if after := v.Stats(); after.DataPhysical != before.DataPhysical {
		t.Fatal("aborted upload retained payload", after.DataPhysical)
	}
	requireS3Status(t, s3TestRequest(t, s, "DELETE", "/s3/fop-files/replaced", nil, nil), 204)
	after := v.Stats()
	if after.DataPhysical != baseline.DataPhysical || after.LogicalBytes != baseline.LogicalBytes {
		t.Fatalf("DELETE retained data: before=%d after=%d, logical=%d", baseline.DataPhysical, after.DataPhysical, after.LogicalBytes)
	}
	live, _, _, err := b.scan(ctx, b.Describe(), nil, 1000)
	if err != nil || len(live) != 0 {
		t.Fatal("detached payload or name index survived", len(live), err)
	}
	files, err := filepath.Glob(filepath.Join(staging, "stqr-s3-*"))
	if err != nil || len(files) != 0 {
		t.Fatal("staging file leaked", files, err)
	}
}

type s3BlockedResponse struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
	once             sync.Once
}

func (w *s3BlockedResponse) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.ResponseRecorder.Write(p)
}
func TestS3DownloadSnapshotSurvivesReclamation(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	old := bytes.Repeat([]byte("old snapshot\n"), 20000)
	put := s3TestRequest(t, s, "PUT", "/s3/fop-files/downloading", old, nil)
	requireS3Status(t, put, 200)
	r := httptest.NewRequest("GET", "http://localhost/s3/fop-files/downloading", nil)
	r.RemoteAddr = "127.0.0.1:1"
	signS3Test(r, nil, testS3Secret)
	w := &s3BlockedResponse{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(w, r) }()
	select {
	case <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	// Reclamation must complete while the network writer remains blocked.
	mutated := make(chan error, 1)
	go func() {
		replacement := s3TestRequest(t, s, "PUT", "/s3/fop-files/downloading", []byte("new"), nil)
		if replacement.Code != 200 {
			mutated <- fmt.Errorf("overwrite: %d", replacement.Code)
			return
		}
		deleted := s3TestRequest(t, s, "DELETE", "/s3/fop-files/downloading", nil, nil)
		if deleted.Code != 204 {
			mutated <- fmt.Errorf("delete: %d", deleted.Code)
			return
		}
		mutated <- nil
	}()
	select {
	case err := <-mutated:
		if err != nil {
			close(w.release)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(w.release)
		t.Fatal("slow network download prevented reclamation")
	}
	if physical := b.allVolumes()[0].Stats().DataPhysical; physical != 0 {
		close(w.release)
		t.Fatal("replaced/deleted native data retained", physical)
	}
	close(w.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not finish")
	}
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), old) || w.Header().Get("ETag") != put.Header().Get("ETag") {
		t.Fatal("snapshot mixed versions")
	}
}
func TestS3NativeSnapshotVersionMismatchEmitsNoBytes(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	a := s.cfg.Auth.BootstrapAdmin
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx := context.Background()
	conn, err := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), "s3")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	api := client.NewS3(conn)
	old, err := api.Put(ctx, "versioned", strings.NewReader("old"), 3, client.S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.Put(ctx, "versioned", strings.NewReader("different generation"), 20, client.S3PutOptions{}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = api.ReadTo(ctx, old, 0, 3, &out); !errors.Is(err, client.ErrPrecondition) || out.Len() != 0 {
		t.Fatal("stale header exposed new data", out.String(), err)
	}
}

// The child is killed without Go defers. Its staging descriptor must already
// be unlinked, so kernel descriptor teardown releases even a partial body.
func TestS3StagingFileReleasedOnProcessKill(t *testing.T) {
	if os.Getenv("STQR_S3_STAGE_CHILD") == "1" {
		api := client.NewS3(nil)
		_, err := api.Put(context.Background(), "killed", &s3StageReadyReader{}, 1<<20, client.S3PutOptions{})
		t.Fatal("child upload unexpectedly returned", err)
	}
	if runtime.GOOS != "linux" {
		t.Skip("checks Linux open-unlinked file lifetime")
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestS3StagingFileReleasedOnProcessKill$")
	cmd.Env = append(os.Environ(), "STQR_S3_STAGE_CHILD=1", "TMPDIR="+dir)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	if _, err = input.Write(make([]byte, 32768)); err != nil {
		t.Fatal(err)
	}
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if scanner.Text() == "S3_STAGE_READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child did not stage data")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child staging timeout")
	}
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", cmd.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, fd := range fds {
		path := fmt.Sprintf("/proc/%d/fd/%s", cmd.Process.Pid, fd.Name())
		name, _ := os.Readlink(path)
		if strings.Contains(name, "stqr-s3-body-") {
			st, e := os.Stat(path)
			if !strings.HasSuffix(name, " (deleted)") || e != nil || st.Size() == 0 {
				t.Fatal("staging file is linked or empty", name, e)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing staged descriptor")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("killed upload left files", entries, err)
	}
}

type s3StageReadyReader struct{ reads int }

func (r *s3StageReadyReader) Read(p []byte) (int, error) {
	if r.reads == 1 {
		fmt.Println("S3_STAGE_READY")
	}
	r.reads++
	return os.Stdin.Read(p)
}
