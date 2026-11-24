// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func publicRequest(s *Server, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://localhost"+path, nil)
	r.RemoteAddr = "127.0.0.1:1000"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}
func TestS3PublicReadObjectACL(t *testing.T) {
	s, _, _ := s3TestSetup(t)
	path := "/s3/fop-files/app.exe"
	data := []byte("public download")
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, data, nil), 200)
	requireS3Status(t, publicRequest(s, "GET", path, nil), 403)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path+"?acl", nil, map[string]string{"X-Amz-Acl": "public-read"}), 200)
	w := publicRequest(s, "GET", path, nil)
	requireS3Status(t, w, 200)
	if !bytes.Equal(w.Body.Bytes(), data) {
		t.Fatal("changed payload")
	}
	etag, modified := w.Header().Get("ETag"), w.Header().Get("Last-Modified")
	requireS3Status(t, publicRequest(s, "HEAD", path, nil), 200)
	w = publicRequest(s, "GET", path, map[string]string{"Range": "bytes=1-3"})
	requireS3Status(t, w, 206)
	if w.Body.String() != string(data[1:4]) {
		t.Fatal(w.Body.String())
	}
	requireS3Status(t, publicRequest(s, "GET", path, map[string]string{"If-None-Match": etag}), 304)
	w = s3TestRequest(t, s, "GET", path+"?acl", nil, nil)
	requireS3Status(t, w, 200)
	if !strings.Contains(w.Body.String(), allUsersGroup) {
		t.Fatal("ACL not reported")
	}
	requireS3Status(t, s3TestRequest(t, s, "PUT", path+"?acl", w.Body.Bytes(), map[string]string{"Content-Type": "application/xml"}), 200)
	requireS3Status(t, s3TestRequest(t, s, "PUT", "/s3/fop-files/copy.exe", nil, map[string]string{"X-Amz-Copy-Source": "/fop-files/app.exe"}), 200)
	requireS3Status(t, publicRequest(s, "GET", "/s3/fop-files/copy.exe", nil), 403)
	for _, method := range []string{"PUT", "POST", "DELETE"} {
		requireS3Status(t, publicRequest(s, method, path, nil), 403)
	}
	for _, p := range []string{"/s3", "/s3/fop-files?list-type=2", path + "?acl", path + "?uploadId=hidden", path + "?X-Amz-Algorithm=invalid"} {
		requireS3Status(t, publicRequest(s, "GET", p, nil), 403)
	}
	requireS3Status(t, publicRequest(s, "GET", path, map[string]string{"Authorization": "invalid"}), 403)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path+"?acl", nil, map[string]string{"X-Amz-Acl": "private"}), 200)
	requireS3Status(t, publicRequest(s, "GET", path, nil), 403)
	w = s3TestRequest(t, s, "GET", path, nil, nil)
	requireS3Status(t, w, 200)
	if w.Header().Get("ETag") != etag || w.Header().Get("Last-Modified") != modified || !bytes.Equal(w.Body.Bytes(), data) {
		t.Fatal("ACL update changed object")
	}
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, data, map[string]string{"X-Amz-Acl": "public-read"}), 200)
	requireS3Status(t, publicRequest(s, "GET", path, nil), 200)
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, data, nil), 200)
	requireS3Status(t, publicRequest(s, "GET", path, nil), 403) // overwrites default to private
}

func TestMultipartPublicACLSurvivesCompletion(t *testing.T) {
	s, b, u := s3TestSetup(t)
	ctx := context.Background()
	api := client.NewS3(s3Store{b: b, p: principal{User: u}})
	upload, err := api.CreateMultipart(ctx, "multipart", u.ID, "", "", client.S3PutOptions{PublicRead: true})
	if err != nil {
		t.Fatal(err)
	}
	part, err := api.UploadPart(ctx, upload.ID, 1, strings.NewReader("part"), 4, client.S3PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.CompleteMultipart(ctx, upload.ID, []client.S3CompletedPart{{Number: 1, ETag: part.ETag}}, client.S3PutOptions{}); err != nil {
		t.Fatal(err)
	}
	w := publicRequest(s, "GET", "/s3/fop-files/multipart", nil)
	requireS3Status(t, w, 200)
	if w.Body.String() != "part" {
		t.Fatal(w.Body.String())
	}
}
func TestClusterPublicS3ACLPersistsAndRoutes(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 16<<20)
	addTestVolume(t, target.node, "data", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, source.node, "public-downloads")
	msg, uid := createUserMessage("publisher", "publisher-test-password")
	if _, status := source.node.dispatchAs(msg, nil, userPrincipal(t, source.node)); status != 201 {
		t.Fatal(status)
	}
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.ID(uid)
	e.U32(uint32(BucketRead | BucketWrite))
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketGrant, e.B); status != 200 {
		t.Fatal(status)
	}
	e = client.Encoder{}
	e.ID(uid)
	e.Text(testS3Access)
	e.Text(testS3Secret)
	if _, status := source.node.dispatchAs(userMessage(client.UserS3KeySet, e), nil, userPrincipal(t, source.node)); status != 200 {
		t.Fatal(status)
	}
	path := "/s3/public-downloads/app.exe"
	data := bytes.Repeat([]byte("download"), 8192)
	requireS3Status(t, s3TestRequest(t, source.node, "PUT", path, data, map[string]string{"X-Amz-Acl": "public-read"}), 200)
	for _, n := range []*daemonTestNode{source, target} {
		w := publicRequest(n.node, "GET", path, nil)
		requireS3Status(t, w, 200)
		if !bytes.Equal(w.Body.Bytes(), data) {
			t.Fatal("cluster bytes")
		}
	}
	target = restartDaemonTestNode(t, target)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		w := publicRequest(target.node, "GET", path, nil)
		if w.Code == 200 {
			break
		}
		if w.Code != 503 || ctx.Err() != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	requireS3Status(t, s3TestRequest(t, source.node, "PUT", path+"?acl", nil, map[string]string{"X-Amz-Acl": "private"}), 200)
	requireS3Status(t, publicRequest(target.node, "GET", path, nil), 403)
}

type revokePublicOnRead struct {
	client.S3Store
	once   sync.Once
	revoke func()
}

func (s *revokePublicOnRead) GetRangeToVersion(ctx context.Context, id client.ObjectID, off, n, version uint64, dst io.Writer) (client.ObjectInfo, error) {
	s.once.Do(s.revoke)
	return s.S3Store.GetRangeToVersion(ctx, id, off, n, version, dst)
}
func TestPublicACLRevocationRechecksReadSnapshot(t *testing.T) {
	s, b, u := s3TestSetup(t)
	ctx := context.Background()
	base := s3Store{b: b, p: principal{User: u, frontendPermission: BucketRead}}
	api := client.NewS3(s3Store{b: b, p: principal{User: u}})
	if _, err := api.Put(ctx, "file", strings.NewReader("private after revocation"), 24, client.S3PutOptions{PublicRead: true}); err != nil {
		t.Fatal(err)
	}
	wrapped := &revokePublicOnRead{S3Store: base, revoke: func() {
		if err := api.SetPublicRead(ctx, "file", false); err != nil {
			t.Fatal(err)
		}
	}}
	r := httptest.NewRequest("GET", "http://localhost/s3/fop-files/file", nil)
	r = r.WithContext(context.WithValue(ctx, publicS3Context{}, true))
	w := httptest.NewRecorder()
	s.serveS3Read(w, r, client.NewS3(wrapped), "file")
	requireS3Status(t, w, 403)
	if bytes.Contains(w.Body.Bytes(), []byte("private after revocation")) {
		t.Fatal("private bytes leaked")
	}
}
