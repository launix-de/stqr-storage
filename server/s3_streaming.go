// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type s3SigningKey struct{}
type s3Signing struct {
	key               []byte
	date, scope, seed string
}

func s3Temp(prefix string) (*os.File, error) {
	f, e := os.CreateTemp("", prefix)
	if e != nil {
		return nil, e
	}
	if e = os.Remove(f.Name()); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
func s3Line(r *bufio.Reader) (string, error) {
	b, e := r.ReadSlice('\n')
	if e != nil {
		return "", e
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", client.ErrRange
	}
	return string(b[:len(b)-2]), nil
}
func s3DecodeChunks(r *http.Request, dst io.Writer) (uint64, error) {
	mode := r.Header.Get("X-Amz-Content-Sha256")
	signed := mode != "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	trailers := strings.HasSuffix(mode, "-TRAILER")
	signing, _ := r.Context().Value(s3SigningKey{}).(s3Signing)
	if signed && len(signing.key) == 0 {
		return 0, fail(403, "missing streaming signature")
	}
	declared, e := strconv.ParseUint(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 63)
	if e != nil {
		return 0, client.ErrRange
	}
	br := bufio.NewReaderSize(bucketContextReader{ctx: r.Context(), r: r.Body}, 8192)
	var total uint64
	previous := signing.seed
	for {
		line, e := s3Line(br)
		if e != nil {
			return total, e
		}
		sizeText, extension, _ := strings.Cut(line, ";")
		n, e := strconv.ParseUint(sizeText, 16, 63)
		if e != nil || n > declared-total {
			return total, client.ErrRange
		}
		signature := ""
		if signed {
			if !strings.HasPrefix(extension, "chunk-signature=") {
				return total, fail(403, "missing chunk signature")
			}
			signature = strings.TrimPrefix(extension, "chunk-signature=")
			if len(signature) != 64 {
				return total, fail(403, "invalid chunk signature")
			}
		} else if extension != "" {
			return total, client.ErrRange
		}
		digest := sha256.New()
		if n > 0 {
			if _, e = io.CopyN(io.MultiWriter(dst, digest), br, int64(n)); e != nil {
				return total, e
			}
			end := make([]byte, 2)
			if _, e = io.ReadFull(br, end); e != nil || string(end) != "\r\n" {
				return total, client.ErrRange
			}
		}
		if signed {
			toSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + signing.date + "\n" + signing.scope + "\n" + previous + "\n" + s3SHA("") + "\n" + hex.EncodeToString(digest.Sum(nil))
			given, e := hex.DecodeString(signature)
			if e != nil || !hmac.Equal(given, s3HMAC(signing.key, toSign)) {
				return total, fail(403, "invalid chunk signature")
			}
			previous = signature
		}
		total += n
		if n == 0 {
			break
		}
	}
	if total != declared {
		return total, io.ErrUnexpectedEOF
	}
	values := map[string]string{}
	bytes := 0
	for {
		line, e := s3Line(br)
		if e != nil {
			return total, e
		}
		bytes += len(line)
		if bytes > 16384 {
			return total, client.ErrRange
		}
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		name = strings.ToLower(name)
		if !ok || values[name] != "" {
			return total, client.ErrRange
		}
		values[name] = strings.TrimSpace(value)
	}
	expected := []string{}
	if trailers {
		for _, name := range strings.Split(r.Header.Get("X-Amz-Trailer"), ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" || values[name] == "" || !strings.HasPrefix(name, "x-amz-checksum-") {
				return total, client.ErrRange
			}
			if _, e := client.S3Checksum(strings.ToUpper(strings.TrimPrefix(name, "x-amz-checksum-"))); e != nil {
				return total, e
			}
			expected = append(expected, name)
		}
	}
	if len(values) != len(expected)+boolInt(signed && trailers) {
		return total, client.ErrRange
	}
	sort.Strings(expected)
	var canonical strings.Builder
	for _, name := range expected {
		fmt.Fprintf(&canonical, "%s:%s\n", name, values[name])
	}
	if signed && trailers {
		toSign := "AWS4-HMAC-SHA256-TRAILER\n" + signing.date + "\n" + signing.scope + "\n" + previous + "\n" + s3SHA(canonical.String())
		given, e := hex.DecodeString(values["x-amz-trailer-signature"])
		if e != nil || !hmac.Equal(given, s3HMAC(signing.key, toSign)) {
			return total, fail(403, "invalid trailer signature")
		}
	}
	if _, e = br.ReadByte(); e != io.EOF {
		return total, client.ErrRange
	}
	for _, name := range expected {
		if initial := r.Header.Get(name); initial != "" && initial != values[name] {
			return total, client.ErrRange
		}
		r.Header.Set(name, values[name])
	}
	return total, nil
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// No backend mutation starts until signatures, trailers and checksums validate.
func s3StageRequest(r *http.Request, payload string, limit uint64) (*os.File, int64, error) {
	limit = min(limit, uint64(maxObjectStagingBytes))
	streaming := strings.HasPrefix(payload, "STREAMING-")
	if streaming {
		n, err := strconv.ParseUint(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 63)
		if err != nil {
			return nil, 0, client.ErrRange
		}
		if n > limit {
			return nil, 0, volume.ErrFull
		}
	} else if r.ContentLength >= 0 && uint64(r.ContentLength) > limit {
		return nil, 0, volume.ErrFull
	}
	for name := range r.Header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "x-amz-checksum-") {
			continue
		}
		algo := strings.TrimPrefix(lower, "x-amz-checksum-")
		if algo == "algorithm" || algo == "type" || algo == "mode" {
			continue
		}
		if _, e := client.S3Checksum(strings.ToUpper(algo)); e != nil {
			return nil, 0, fail(501, "unsupported checksum algorithm")
		}
	}
	f, e := s3Temp("stqr-s3-request-*")
	if e != nil {
		return nil, 0, e
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	md, sha := md5.New(), sha256.New()
	writers := []io.Writer{f, md, sha}
	checks := map[string]hash.Hash{}
	for _, algo := range []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
		h, _ := client.S3Checksum(algo)
		checks[algo] = h
		writers = append(writers, h)
	}
	var n int64
	dst := &s3StagingWriter{dst: io.MultiWriter(writers...), remaining: limit}
	if streaming {
		var size uint64
		size, e = s3DecodeChunks(r, dst)
		n = int64(size)
	} else {
		n, e = io.Copy(dst, io.LimitReader(bucketContextReader{ctx: r.Context(), r: r.Body}, int64(limit)+1))
		if e == nil && r.ContentLength >= 0 && n != r.ContentLength {
			e = io.ErrUnexpectedEOF
		}
	}
	if e != nil {
		return nil, 0, e
	}
	if payload != "" && !strings.HasPrefix(payload, "STREAMING-") && payload != hex.EncodeToString(sha.Sum(nil)) {
		return nil, 0, client.ErrS3Checksum
	}
	if value := r.Header.Get("Content-MD5"); value != "" {
		given, e := base64.StdEncoding.DecodeString(value)
		if e != nil || !hmac.Equal(given, md.Sum(nil)) {
			return nil, 0, client.ErrS3Checksum
		}
	}
	for algo, h := range checks {
		if r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "" {
			continue
		}
		if value := r.Header.Get("X-Amz-Checksum-" + strings.ToLower(algo)); value != "" {
			given, e := base64.StdEncoding.DecodeString(value)
			if e != nil || !hmac.Equal(given, h.Sum(nil)) {
				return nil, 0, client.ErrS3Checksum
			}
		}
	}
	if algo := r.Header.Get("X-Amz-Sdk-Checksum-Algorithm"); algo != "" && !(r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "") {
		if _, e = client.S3Checksum(algo); e != nil || r.Header.Get("X-Amz-Checksum-"+strings.ToLower(algo)) == "" {
			return nil, 0, client.ErrRange
		}
	}
	encodings := []string{}
	for _, v := range strings.Split(r.Header.Get("Content-Encoding"), ",") {
		v = strings.TrimSpace(v)
		if v != "" && v != "aws-chunked" {
			encodings = append(encodings, v)
		}
	}
	r.Header.Del("Content-Encoding")
	if len(encodings) > 0 {
		r.Header.Set("Content-Encoding", strings.Join(encodings, ","))
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return nil, 0, e
	}
	ok = true
	return f, n, nil
}

type s3StagingWriter struct {
	dst       io.Writer
	remaining uint64
}

func (w *s3StagingWriter) Write(p []byte) (int, error) {
	if uint64(len(p)) > w.remaining {
		return 0, volume.ErrFull
	}
	n, err := w.dst.Write(p)
	w.remaining -= uint64(n)
	return n, err
}
