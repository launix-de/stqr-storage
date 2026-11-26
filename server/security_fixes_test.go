// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestSecurityNativeCASRequiresRead(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oid := volume.ObjectID{42}
	if _, err := b.Put(ctx, userPrincipal(t, s), oid, []byte("secret"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketWrite}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.BucketID(b.ID()), "s3")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.Get(ctx, client.ObjectID(oid)); !errors.Is(err, client.ErrUnauthorized) {
		t.Fatalf("direct read: %v", err)
	}
	for _, expected := range []string{"secret", "xxxxxx"} {
		if _, err := b.CAS(ctx, principal{User: user}, oid, 0, []byte(expected), []byte("change"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); !errors.Is(err, ErrBucketForbidden) {
			t.Fatalf("uncached CAS %q: %v", expected, err)
		}
		if _, err := c.CAS(ctx, client.ObjectID(oid), 0, []byte(expected), []byte("change"), client.CASOptions{}); !errors.Is(err, client.ErrUnauthorized) {
			t.Fatalf("socket CAS %q: %v", expected, err)
		}
		e := client.Encoder{}
		e.ID(client.ID(b.ID()))
		e.B = append(e.B, oid[:]...)
		e.U64(0)          // placement revision
		e.U64(0)          // owner epoch
		e.ID(client.ID{}) // RAM runtime
		e.U8(0)           // range comparison
		e.U64(0)          // offset
		e.Bool(false)     // expected version absent
		e.Bool(false)     // expected length absent
		e.Bool(false)     // create only
		e.Blob([]byte(expected))
		e.Blob([]byte("change"))
		wire := client.EncodeMessage(client.Message{Kind: client.Request, Opcode: 0x0108, ID: client.NewRequestID(), Body: e.B})
		r := httptest.NewRequest("POST", "http://localhost/api/message", bytes.NewReader(wire))
		r.RemoteAddr = "127.0.0.1:1"
		r.SetBasicAuth(user.Name, "bucket-test-password")
		r.Header.Set(client.FormatHeader, "s3")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatalf("HTTP CAS %q: %d %s", expected, w.Code, w.Body.String())
		}
	}
	d = b.Describe()
	d.Grants[0].Permissions = BucketRead | BucketWrite
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CAS(ctx, principal{User: user}, oid, 0, []byte("xxxxxx"), []byte("change"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); !errors.Is(err, volume.ErrPrecondition) {
		t.Fatalf("authorized mismatch: %v", err)
	}
	if _, err := b.CAS(ctx, principal{User: user}, oid, 0, []byte("secret"), []byte("change"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
}

func TestSecurityS3ObjectBrowserIsolation(t *testing.T) {
	s, _, _ := s3TestSetup(t)
	path := "/s3/fop-files/active.html"
	data := []byte("<script>document.title='active'</script>")
	requireS3Status(t, s3TestRequest(t, s, "PUT", path, data, map[string]string{"Content-Type": "text/html", "Content-Disposition": "inline"}), 200)
	for _, method := range []string{"GET", "HEAD"} {
		for _, ranged := range []bool{false, true} {
			headers := map[string]string{}
			status := 200
			if ranged {
				headers["Range"] = "bytes=0-7"
				if method == "GET" {
					status = 206
				}
			}
			w := s3TestRequest(t, s, method, path, nil, headers)
			requireS3Status(t, w, status)
			if w.Header().Get("Content-Disposition") != "attachment" || w.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.HasPrefix(w.Header().Get("Content-Security-Policy"), "sandbox;") {
				t.Fatalf("unsafe response: %v", w.Header())
			}
		}
	}
}

type securityCountingBody struct {
	r io.Reader
	n int
}

func (b *securityCountingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += n
	return n, err
}
func (b *securityCountingBody) Close() error { return nil }

func TestSecurityS3UploadAdmission(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	d := b.Describe()
	d.Policy.Limits.Unlimited = false
	d.Policy.Version++
	d.Policy.Limits.Guaranteed = 0
	d.Policy.Limits.Absolute = 64
	d.Policy.Limits.Accounting = "logical"
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 1024)
	for _, mode := range []string{"known", "unknown", "streaming"} {
		t.Run(mode, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "http://localhost/s3/fop-files/large", bytes.NewReader(data))
			r.RemoteAddr = "127.0.0.1:1"
			signS3Test(r, data, testS3Secret)
			if mode == "unknown" {
				r.ContentLength = -1
			}
			if mode == "streaming" {
				r = streamingS3Request(t, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", data, "")
			}
			body := &securityCountingBody{r: r.Body}
			r.Body = body
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			requireS3Status(t, w, 507)
			if mode == "unknown" {
				if body.n > 65 {
					t.Fatalf("read %d bytes", body.n)
				}
			} else if body.n != 0 {
				t.Fatalf("read %d bytes before rejecting declared length", body.n)
			}
		})
	}
}

func TestSecurityS3StagingBoundary(t *testing.T) {
	for _, size := range []int{0, 63, 64, 65} {
		data := bytes.Repeat([]byte("x"), size)
		r := httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(data))
		r.ContentLength = -1
		f, n, err := s3StageRequest(r, "", 64)
		if size > 64 {
			if !errors.Is(err, volume.ErrFull) {
				t.Fatal(err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		f.Close()
		if err != nil || n != int64(size) || !bytes.Equal(got, data) {
			t.Fatalf("size %d: %d, %v", size, n, err)
		}
	}
}
