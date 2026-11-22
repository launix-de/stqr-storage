// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func s3SHA(data string) string { sum := sha256.Sum256([]byte(data)); return hex.EncodeToString(sum[:]) }
func s3HMAC(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
func s3Escape(s string, slash bool) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.~", rune(c)) || slash && c == '/' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}
func s3Query(q url.Values, presigned bool) string {
	pairs := []string{}
	for key, values := range q {
		if presigned && key == "X-Amz-Signature" {
			continue
		}
		for _, v := range values {
			pairs = append(pairs, s3Escape(key, false)+"="+s3Escape(v, false))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}
func (s *Server) authenticateS3(r *http.Request) (principal, string, error) {
	denied := func() (principal, string, error) { return principal{}, "", fail(403, "invalid S3 signature") }
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return denied()
	}
	presigned := q.Get("X-Amz-Algorithm") != ""
	var credential, signed, signature, date string
	payload := r.Header.Get("X-Amz-Content-Sha256")
	if presigned {
		if r.Header.Get("Authorization") != "" || q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
			return denied()
		}
		for _, key := range []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Date", "X-Amz-Expires"} {
			if len(q[key]) != 1 {
				return denied()
			}
		}
		credential, signed, signature, date = q.Get("X-Amz-Credential"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature"), q.Get("X-Amz-Date")
		if payload == "" {
			payload = "UNSIGNED-PAYLOAD"
		}
	} else {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			return denied()
		}
		fields := map[string]string{}
		for _, field := range strings.Split(strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 "), ",") {
			key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
			if !ok || fields[key] != "" {
				return denied()
			}
			fields[key] = value
		}
		if len(fields) != 3 {
			return denied()
		}
		credential, signed, signature = fields["Credential"], fields["SignedHeaders"], fields["Signature"]
		date = r.Header.Get("X-Amz-Date")
	}
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[2] == "" || parts[3] != "s3" || parts[4] != "aws4_request" || !validS3AccessID(parts[0]) {
		return denied()
	}
	stamp, err := time.Parse("20060102T150405Z", date)
	if err != nil || parts[1] != stamp.Format("20060102") {
		return denied()
	}
	now := time.Now()
	if presigned {
		seconds, e := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
		if e != nil || seconds < 1 || seconds > 604800 || now.After(stamp.Add(time.Duration(seconds)*time.Second)) || stamp.After(now.Add(15*time.Minute)) {
			return denied()
		}
	} else if stamp.Before(now.Add(-15*time.Minute)) || stamp.After(now.Add(15*time.Minute)) {
		return denied()
	}
	if strings.HasPrefix(payload, "STREAMING-") {
		return principal{}, "", fail(501, "aws-chunked streaming signatures are not supported")
	}
	if payload != "UNSIGNED-PAYLOAD" {
		v, e := hex.DecodeString(payload)
		if e != nil || len(v) != 32 || payload != strings.ToLower(payload) {
			return denied()
		}
	}
	names := strings.Split(signed, ";")
	if !sort.StringsAreSorted(names) || signed == "" {
		return denied()
	}
	var headers strings.Builder
	host, dateSigned := false, false
	for i, name := range names {
		if name == "" || name != strings.ToLower(name) || i > 0 && names[i-1] == name {
			return denied()
		}
		var values []string
		if name == "host" {
			host = true
			values = []string{r.Host}
		} else {
			values = r.Header.Values(name)
		}
		if name == "x-amz-date" {
			dateSigned = true
		}
		if len(values) == 0 {
			return denied()
		}
		for j := range values {
			values[j] = strings.Join(strings.Fields(values[j]), " ")
		}
		fmt.Fprintf(&headers, "%s:%s\n", name, strings.Join(values, ","))
	}
	if !host || !presigned && !dateSigned {
		return denied()
	}
	// S3 paths are encoded once and never cleaned or slash-collapsed.
	canonical := strings.Join([]string{r.Method, s3Escape(r.URL.Path, true), s3Query(q, presigned), headers.String(), signed, payload}, "\n")
	scope := strings.Join(parts[1:], "/")
	toSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + s3SHA(canonical)
	user, secret, err := s.s3KeyUser(parts[0])
	if err != nil {
		return denied()
	}
	key := s3HMAC([]byte("AWS4"+secret), parts[1])
	key = s3HMAC(key, parts[2])
	key = s3HMAC(key, "s3")
	key = s3HMAC(key, "aws4_request")
	sig, err := hex.DecodeString(signature)
	if err != nil || !hmac.Equal(sig, s3HMAC(key, toSign)) {
		return denied()
	}
	if payload == "UNSIGNED-PAYLOAD" {
		payload = ""
	}
	return principal{User: user}, payload, nil
}
