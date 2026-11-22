// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func s3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string
		Message string
	}{Code: code, Message: message})
}
func s3Failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, client.ErrS3Upload):
		s3Error(w, 404, "NoSuchUpload", "The multipart upload does not exist")
	case errors.Is(err, client.ErrS3Part):
		s3Error(w, 400, "InvalidPart", "A part or ETag does not match")
	case errors.Is(err, client.ErrS3PartOrder):
		s3Error(w, 400, "InvalidPartOrder", "Parts must be ordered")
	case errors.Is(err, client.ErrS3PartSmall):
		s3Error(w, 400, "EntityTooSmall", "All but the last part must contain at least 5 MiB")
	case errors.Is(err, client.ErrS3UploadBusy):
		s3Error(w, 409, "OperationAborted", "Multipart completion is in progress")
	case errors.Is(err, client.ErrNotFound), errors.Is(err, ErrBucketNotFound):
		s3Error(w, 404, "NoSuchKey", "The specified key does not exist")
	case errors.Is(err, client.ErrPrecondition):
		s3Error(w, 412, "PreconditionFailed", "A supplied condition was not met")
	case errors.Is(err, io.ErrUnexpectedEOF):
		s3Error(w, 400, "IncompleteBody", "Upload interrupted")
	case errors.Is(err, client.ErrS3Checksum):
		s3Error(w, 400, "BadDigest", "The supplied checksum does not match")
	case errors.Is(err, client.ErrRange):
		s3Error(w, 400, "InvalidArgument", "Invalid key, metadata or request argument")
	case errors.Is(err, volume.ErrFull), errors.Is(err, client.ErrFull):
		s3Error(w, 507, "InsufficientStorage", err.Error())
	case errors.Is(err, client.ErrUnauthorized):
		s3Error(w, 403, "AccessDenied", "Access denied")
	case errors.Is(err, client.ErrNotImplemented):
		s3Error(w, 501, "NotImplemented", "Storage operation is not implemented")
	default:
		status := objectStatus(err)
		var native *client.StatusError
		if errors.As(err, &native) {
			status = native.Status
		}
		if status == 501 {
			s3Error(w, 501, "NotImplemented", err.Error())
		} else if status == 507 {
			s3Error(w, 507, "InsufficientStorage", err.Error())
		} else if status == 400 {
			s3Error(w, 400, "InvalidArgument", err.Error())
		} else if status == 403 {
			s3Error(w, 403, "AccessDenied", "Access denied")
		} else if status == 409 || status == 410 || status == 503 || status == 504 {
			s3Error(w, 503, "ServiceUnavailable", "Bucket is unavailable")
		} else {
			s3Error(w, 500, "InternalError", "Storage operation failed")
		}
	}
}
func (s *Server) serveS3(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	r = r.WithContext(ctx)
	if s.drainClosing.Load() || s.shortStopping.Load() {
		s3Error(w, 503, "ServiceUnavailable", "Node is finishing shutdown")
		return
	}
	w.Header().Set("x-amz-request-id", client.NewRequestID().String())
	if s.joinPending() {
		s3Error(w, 503, "ServiceUnavailable", "Node admission is pending")
		return
	}
	p, bodySHA, err := s.authenticateS3(r)
	if err != nil {
		status := statusOf(err)
		code := "SignatureDoesNotMatch"
		if status == 501 {
			code = "NotImplemented"
		}
		s3Error(w, status, code, err.Error())
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/s3/")
	if r.URL.Path == "/s3" || path == "" {
		s.s3ListBuckets(w, r, p)
		return
	}
	name, key, _ := strings.Cut(path, "/")
	b, err := s.BucketByName(name)
	if err != nil {
		s3Error(w, 404, "NoSuchBucket", "The specified bucket does not exist")
		return
	}
	q := r.URL.Query()
	permission := BucketRead
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		permission = BucketWrite
	} else if r.Method == http.MethodDelete {
		permission = BucketDelete
		if q.Get("uploadId") != "" {
			permission = BucketWrite
		}
	} else if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3Error(w, 501, "NotImplemented", "Unsupported S3 operation")
		return
	}
	if _, ok := q["delete"]; ok {
		permission = BucketDelete
	}
	if _, err = b.authorize(p, permission); err != nil {
		s3Failure(w, err)
		return
	}
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		staged, n, e := s3StageRequest(r, bodySHA)
		if e != nil {
			s3Failure(w, e)
			return
		}
		defer staged.Close()
		r.Body = io.NopCloser(staged)
		r.ContentLength = n
		bodySHA = ""
	}
	p.frontendPermission = permission
	api, closeAPI, err := s.openS3(r.Context(), b, p)
	if err != nil {
		s3Failure(w, err)
		return
	}
	defer closeAPI()
	if s.serveS3Multipart(w, r, p, api, name, key) {
		return
	}
	if key == "" && s.s3BucketProbe(w, r, name) {
		return
	}
	if _, ok := q["delete"]; ok {
		if r.Method != "POST" || key != "" {
			s3Error(w, 400, "InvalidRequest", "Invalid multi-object delete")
			return
		}
		s.s3DeleteObjects(w, r, api)
		return
	}
	if _, ok := q["tagging"]; ok && key != "" {
		s3EmptyTagging(w, r, api, key)
		return
	}
	for _, unsupported := range []string{"acl", "versionId", "versions", "policy", "lifecycle"} {
		if _, ok := q[unsupported]; ok {
			s3Error(w, 501, "NotImplemented", "Unsupported S3 subresource")
			return
		}
	}

	if key == "" {
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		if r.Method != http.MethodGet {
			s3Error(w, 501, "NotImplemented", "Manage buckets through STQR administration")
			return
		}
		s.serveS3List(w, r, api, name)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			s.s3Copy(w, r, p, api, key, "", 0)
			return
		}
		s.serveS3Put(w, r, api, key, bodySHA)
	case http.MethodDelete:
		if err = api.Delete(r.Context(), key); err != nil {
			s3Failure(w, err)
			return
		}
		w.WriteHeader(204)
	case http.MethodGet, http.MethodHead:
		s.serveS3Read(w, r, api, key)
	}
}

// Delay headers until the version-checked snapshot is available. A racing PUT
// can then be retried before any stale metadata or body bytes reach the client.
type s3DeferredResponse struct {
	dst       http.ResponseWriter
	headers   http.Header
	status    int
	committed bool
}

func (w *s3DeferredResponse) Header() http.Header { return w.headers }
func (w *s3DeferredResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *s3DeferredResponse) commit() {
	if w.committed {
		return
	}
	w.committed = true
	for k := range w.dst.Header() {
		delete(w.dst.Header(), k)
	}
	for k, v := range w.headers {
		w.dst.Header()[k] = v
	}
	if w.status == 0 {
		w.status = 200
	}
	w.dst.WriteHeader(w.status)
}
func (w *s3DeferredResponse) Write(b []byte) (int, error) { w.commit(); return w.dst.Write(b) }
func (s *Server) serveS3Read(w http.ResponseWriter, r *http.Request, api *client.S3, key string) {
	for attempt := 0; attempt < 128; attempt++ {
		response := &s3DeferredResponse{dst: w, headers: w.Header().Clone()}
		err := s.serveS3ReadAttempt(response, r, api, key)
		if !response.committed && (errors.Is(err, client.ErrPrecondition) || errors.Is(err, client.ErrNotFound)) {
			continue
		}
		if err != nil {
			if response.committed {
				panic(http.ErrAbortHandler)
			}
			s3Failure(w, err)
			return
		}
		response.commit()
		return
	}
	s3Error(w, 503, "SlowDown", "Object changed repeatedly; retry the request")
}
func (s *Server) serveS3ReadAttempt(w http.ResponseWriter, r *http.Request, api *client.S3, key string) error {
	o, e := api.Head(r.Context(), key)
	if e != nil {
		s3Failure(w, e)
		return nil
	}
	etag := "\"" + o.ETag + "\""
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", o.Modified.Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	for k, v := range o.Headers {
		if !strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
			w.Header().Set(k, v)
		}
	}
	for k, v := range o.Metadata {
		w.Header()["x-amz-meta-"+k] = []string{v}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if match := r.Header.Get("If-Match"); match != "" && !s3ETagMatch(match, etag, false) {
		s3Error(w, 412, "PreconditionFailed", "ETag mismatch")
		return nil
	}
	if match := r.Header.Get("If-None-Match"); match != "" && s3ETagMatch(match, etag, true) {
		w.WriteHeader(304)
		return nil
	}
	if r.Header.Get("If-None-Match") == "" {
		if t, e := http.ParseTime(r.Header.Get("If-Modified-Since")); e == nil && !o.Modified.Truncate(time.Second).After(t) {
			w.WriteHeader(304)
			return nil
		}
	}
	off, n, status := uint64(0), o.Size, 200
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" && r.Method == http.MethodGet {
		use := true
		if ir := r.Header.Get("If-Range"); ir != "" && ir != etag {
			t, e := http.ParseTime(ir)
			use = e == nil && !o.Modified.Truncate(time.Second).After(t)
		}
		if use {
			off, n, e = s3Range(rangeHeader, o.Size)
			if e != nil {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", o.Size))
				s3Error(w, 416, "InvalidRange", "The requested range is not satisfiable")
				return nil
			}
			status = 206
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, o.Size))
		}
	}
	if status == 200 {
		for k, v := range o.Headers {
			if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
				w.Header().Set(k, v)
			}
		}
	}
	w.Header().Set("Content-Length", strconv.FormatUint(n, 10))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		return api.ReadTo(r.Context(), o, off, n, w)
	}
	return nil
}

func s3ETagMatch(value, etag string, weak bool) bool {
	for _, v := range strings.Split(value, ",") {
		v = strings.TrimSpace(v)
		if weak {
			v = strings.TrimPrefix(v, "W/")
		}
		if v == "*" || v == etag {
			return true
		}
	}
	return false
}
func s3Range(header string, size uint64) (uint64, uint64, error) {
	if !strings.HasPrefix(header, "bytes=") || strings.Contains(header, ",") || size == 0 {
		return 0, 0, client.ErrRange
	}
	a, b, ok := strings.Cut(strings.TrimPrefix(header, "bytes="), "-")
	if !ok {
		return 0, 0, client.ErrRange
	}
	if a == "" {
		n, e := strconv.ParseUint(b, 10, 64)
		if e != nil || n == 0 {
			return 0, 0, client.ErrRange
		}
		n = min(n, size)
		return size - n, n, nil
	}
	start, e := strconv.ParseUint(a, 10, 64)
	if e != nil || start >= size {
		return 0, 0, client.ErrRange
	}
	end := size - 1
	if b != "" {
		end, e = strconv.ParseUint(b, 10, 64)
		if e != nil || end < start {
			return 0, 0, client.ErrRange
		}
		end = min(end, size-1)
	}
	return start, end - start + 1, nil
}
func s3PutOptions(r *http.Request) (client.S3PutOptions, error) {
	opt := client.S3PutOptions{Headers: map[string]string{}, Metadata: map[string]string{}, Checksums: map[string]string{}, IfMatch: r.Header.Get("If-Match")}
	if acl := r.Header.Get("X-Amz-Acl"); acl != "" && acl != "private" {
		return opt, fail(501, "only private ACL is supported")
	}
	for _, h := range []string{"X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Customer-Algorithm", "X-Amz-Tagging", "X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date", "X-Amz-Object-Lock-Legal-Hold"} {
		if r.Header.Get(h) != "" {
			return opt, fail(501, "unsupported S3 header")
		}
	}
	if v := r.Header.Get("X-Amz-Storage-Class"); v != "" && v != "STANDARD" {
		return opt, fail(501, "configure storage strategy through the STQR bucket")
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		if v != "*" {
			return opt, client.ErrRange
		}
		opt.IfNoneMatch = true
	}
	for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Disposition", "Content-Language", "Cache-Control", "Expires"} {
		if v := r.Header.Get(k); v != "" {
			opt.Headers[k] = v
		}
	}
	size := 0
	for k, vs := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			name := strings.ToLower(k[len("x-amz-meta-"):])
			value := strings.Join(vs, ",")
			size += len(name) + len(value)
			opt.Metadata[name] = value
		}
	}
	if size > 2048 {
		return opt, client.ErrRange
	}
	for _, algo := range []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
		header := "X-Amz-Checksum-" + strings.ToLower(algo)
		if v := r.Header.Get(header); v != "" {
			opt.Checksums[algo] = v
			opt.Headers[header] = v
		}
	}
	if value := r.Header.Get("Content-MD5"); value != "" {
		v, e := base64.StdEncoding.DecodeString(value)
		if e != nil || len(v) != 16 {
			return opt, client.ErrS3Checksum
		}
		opt.ContentMD5 = hex.EncodeToString(v)
	}
	return opt, nil
}
func (s *Server) serveS3Put(w http.ResponseWriter, r *http.Request, api *client.S3, key, bodySHA string) {
	opt, e := s3PutOptions(r)
	if e != nil {
		s3Failure(w, e)
		return
	}
	opt.SHA256 = bodySHA
	result, e := api.Put(r.Context(), key, r.Body, uint64(r.ContentLength), opt)
	if e != nil {
		s3Failure(w, e)
		return
	}
	w.Header().Set("ETag", "\""+result.ETag+"\"")
	for k, v := range result.Headers {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(200)
}
