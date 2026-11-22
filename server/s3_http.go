// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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
	case errors.Is(err, volume.ErrFull):
		s3Error(w, 507, "InsufficientStorage", err.Error())
	default:
		status := objectStatus(err)
		if status == 403 {
			s3Error(w, 403, "AccessDenied", "Access denied")
		} else if status == 409 || status == 503 {
			s3Error(w, 503, "ServiceUnavailable", "Bucket is unavailable")
		} else {
			s3Error(w, 500, "InternalError", "Storage operation failed")
		}
	}
}
func (s *Server) serveS3(w http.ResponseWriter, r *http.Request) {
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
		s3Error(w, 501, "NotImplemented", "Use a bucket path")
		return
	}
	name, key, _ := strings.Cut(path, "/")
	b, err := s.BucketByName(name)
	if err != nil {
		s3Error(w, 404, "NoSuchBucket", "The specified bucket does not exist")
		return
	}
	permission := BucketRead
	if r.Method == http.MethodPut {
		permission = BucketWrite
	} else if r.Method == http.MethodDelete {
		permission = BucketDelete
	} else if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3Error(w, 501, "NotImplemented", "Unsupported S3 operation")
		return
	}
	if _, err = b.authorize(p, permission); err != nil {
		s3Failure(w, err)
		return
	}
	// This is an internal frontend capability, not the caller's native grant.
	p.frontendPermission = permission
	api := client.NewS3(s3Store{b: b, p: p})
	q := r.URL.Query()
	for _, unsupported := range []string{"uploads", "uploadId", "partNumber", "acl", "versionId", "versions", "delete", "tagging", "location", "policy", "lifecycle"} {
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
func (s *Server) serveS3Put(w http.ResponseWriter, r *http.Request, api *client.S3, key, bodySHA string) {
	if r.Header.Get("X-Amz-Copy-Source") != "" {
		s3Error(w, 501, "NotImplemented", "CopyObject is not supported")
		return
	}
	if acl := r.Header.Get("X-Amz-Acl"); acl != "" && acl != "private" {
		s3Error(w, 501, "NotImplemented", "Only private ACL is supported")
		return
	}
	if r.Header.Get("Content-Encoding") == "aws-chunked" {
		s3Error(w, 501, "NotImplemented", "aws-chunked is not supported")
		return
	}
	for _, h := range []string{"X-Amz-Server-Side-Encryption", "X-Amz-Server-Side-Encryption-Customer-Algorithm", "X-Amz-Tagging", "X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date", "X-Amz-Object-Lock-Legal-Hold", "X-Amz-Checksum-Crc64nvme"} {
		if r.Header.Get(h) != "" {
			s3Error(w, 501, "NotImplemented", "Unsupported S3 header: "+h)
			return
		}
	}
	if storage := r.Header.Get("X-Amz-Storage-Class"); storage != "" && storage != "STANDARD" {
		s3Error(w, 501, "NotImplemented", "Configure storage strategy through the STQR bucket")
		return
	}
	opt := client.S3PutOptions{Headers: map[string]string{}, Metadata: map[string]string{}, SHA256: bodySHA, IfMatch: r.Header.Get("If-Match")}
	if value := r.Header.Get("If-None-Match"); value != "" {
		if value != "*" {
			s3Error(w, 400, "InvalidArgument", "PUT supports If-None-Match: *")
			return
		}
		opt.IfNoneMatch = true
	}
	for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Disposition", "Content-Language", "Cache-Control", "Expires"} {
		if v := r.Header.Get(k); v != "" {
			opt.Headers[k] = v
		}
	}
	for k, vs := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			opt.Metadata[strings.ToLower(k[len("x-amz-meta-"):])] = strings.Join(vs, ",")
		}
	}
	opt.Checksums = map[string]string{}
	for _, algorithm := range []string{"CRC32", "CRC32C", "SHA1", "SHA256"} {
		header := "X-Amz-Checksum-" + strings.ToLower(algorithm)
		if v := r.Header.Get(header); v != "" {
			opt.Checksums[algorithm] = v
			opt.Headers[header] = v
		}
	}
	if algorithm := r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"); algorithm != "" && opt.Checksums[algorithm] == "" {
		s3Error(w, 501, "NotImplemented", "A supported checksum header is required")
		return
	}
	metadataSize := 0
	for k, v := range opt.Metadata {
		metadataSize += len(k) + len(v)
	}
	if metadataSize > 2048 {
		s3Error(w, 400, "MetadataTooLarge", "User metadata exceeds 2 KiB")
		return
	}
	if md := r.Header.Get("Content-MD5"); md != "" {
		v, e := base64.StdEncoding.DecodeString(md)
		if e != nil || len(v) != 16 {
			s3Error(w, 400, "InvalidDigest", "Invalid Content-MD5")
			return
		}
		opt.ContentMD5 = hex.EncodeToString(v)
	}
	var src io.Reader = r.Body
	length := r.ContentLength
	// Unknown-length HTTP bodies are counted on local disk first. The library
	// verifies and stages the body before atomically publishing its envelope.
	if length < 0 {
		f, e := os.CreateTemp("", "stqr-s3-upload-*")
		if e != nil {
			s3Failure(w, e)
			return
		}
		if e = os.Remove(f.Name()); e != nil {
			f.Close()
			s3Failure(w, e)
			return
		}
		defer f.Close()
		length, e = io.Copy(f, r.Body)
		if e != nil {
			s3Error(w, 400, "IncompleteBody", "Upload interrupted")
			return
		}
		if _, e = f.Seek(0, 0); e != nil {
			s3Failure(w, e)
			return
		}
		src = f
	}
	o, err := api.Put(r.Context(), key, src, uint64(length), opt)
	if err != nil {
		s3Failure(w, err)
		return
	}
	w.Header().Set("ETag", "\""+o.ETag+"\"")
	w.WriteHeader(200)
}
