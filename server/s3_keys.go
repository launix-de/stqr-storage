// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/launix-de/stqr-storage/client"
)

// Access keys live inside their owning account, including a recoverable signing
// secret sealed under the cluster token. Public admin responses expose IDs only.
type s3AccessKey struct {
	ID     string `json:"id"`
	Sealed string `json:"secret_sealed"`
}

func validS3AccessID(id string) bool {
	if len(id) < 16 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func (s *Server) s3Cipher() (cipher.AEAD, error) {
	s.mu.RLock()
	token := s.cfg.Auth.ClusterToken
	s.mu.RUnlock()
	key := sha256.Sum256([]byte("stqr:s3:access-secret:v1:\x00" + token))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (s *Server) sealS3Secret(user, id, secret string) (string, error) {
	c, err := s.s3Cipher()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, c.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(c.Seal(nonce, nonce, []byte(secret), []byte(user+":"+id))), nil
}
func (s *Server) openS3Secret(user string, k s3AccessKey) (string, error) {
	c, err := s.s3Cipher()
	if err != nil {
		return "", err
	}
	b, err := base64.StdEncoding.DecodeString(k.Sealed)
	if err != nil || len(b) < c.NonceSize()+c.Overhead() {
		return "", fail(503, "invalid S3 signing secret")
	}
	out, err := c.Open(nil, b[:c.NonceSize()], b[c.NonceSize():], []byte(user+":"+k.ID))
	return string(out), err
}
func (s *Server) s3KeyUser(id string) (account, string, error) {
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		return account{}, "", err
	}
	for _, u := range image.Users {
		if u.Disabled || u.Role != "USER" && u.Role != "ADMIN" {
			continue
		}
		for _, k := range u.S3Keys {
			if k.ID == id {
				secret, err := s.openS3Secret(u.ID, k)
				return u, secret, err
			}
		}
	}
	return account{}, "", fail(403, "unknown access key")
}

// JSON convenience adapter over the same authenticated, receipt-backed user
// operations as the binary protocol. Secrets are supplied, never returned.
func (s *Server) serveS3Keys(w http.ResponseWriter, r *http.Request, p principal) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/users/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[1] != "s3-keys" {
		httpError(w, 404, fail(404, "endpoint not found"))
		return
	}
	uid, err := parseID(parts[0])
	if err != nil {
		httpError(w, 400, err)
		return
	}
	enc := client.Encoder{}
	enc.ID(client.ID(uid))
	opcode := client.UserS3KeyList
	switch {
	case r.Method == http.MethodGet && len(parts) == 2:
	case r.Method == http.MethodPut && len(parts) == 2:
		var q struct {
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		}
		if err = readJSON(w, r, &q); err != nil {
			httpError(w, 400, err)
			return
		}
		opcode = client.UserS3KeySet
		enc.Text(q.AccessKey)
		enc.Text(q.SecretKey)
	case r.Method == http.MethodDelete && len(parts) == 3:
		opcode = client.UserS3KeyDelete
		enc.Text(parts[2])
	default:
		w.WriteHeader(405)
		return
	}
	id := client.NewRequestID()
	if raw := r.Header.Get("Idempotency-Key"); raw != "" {
		parsed, e := parseID(raw)
		if e != nil {
			httpError(w, 400, e)
			return
		}
		id = client.ID(parsed)
	}
	response, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: opcode, ID: id, Body: enc.B}, nil, p)
	envelope := client.Decoder{B: response}
	envelope.V()
	headers := envelope.Blob()
	if status >= 300 {
		detail := "S3 key administration failed"
		h := client.Decoder{B: headers}
		for n := h.Count(100); n > 0; n-- {
			tag := h.V()
			value := client.Decoder{B: h.Blob()}
			if tag == 3 {
				detail = value.Text()
			}
		}
		httpError(w, status, fail(status, detail))
		return
	}
	result := envelope.B
	if opcode == client.UserS3KeyList {
		d := client.Decoder{B: result}
		n := d.Count(1024)
		ids := make([]string, n)
		for i := range ids {
			ids[i] = d.Text()
		}
		writeJSON(w, 200, map[string]any{"access_keys": ids})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
