// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"golang.org/x/crypto/bcrypt"
)

// Stable system IDs identify ordinary objects. ALL admission publishes their
// immutable system graph through the EVERYWHERE placement group before use.
var systemBucket = volume.ID{0x53, 0x54, 0x51, 0x52, 0x53, 0x59, 0x53, 1}

func systemKey(name string) volume.Key {
	return volume.Key{Bucket: systemBucket, Object: volume.ObjectID(sha256.Sum256([]byte(name)))}
}

type account struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	Role         string        `json:"role"`
	Disabled     bool          `json:"disabled"`
	PasswordHash string        `json:"password_hash"`
	S3Keys       []s3AccessKey `json:"s3_keys,omitempty"`
}
type accountsImage struct {
	Schema   int              `json:"schema"`
	Users    []account        `json:"users"`
	Receipts []accountReceipt `json:"receipts,omitempty"`
}

func (s *Server) readBlob(name string, dst any) error {
	store, e := s.metadataStore()
	if e != nil {
		return e
	}
	h, e := store.Open(false)
	if e != nil {
		return e
	}
	defer h.Close()
	b, _, e := h.Get(systemKey(name))
	if e != nil {
		return e
	}
	return json.Unmarshal(b, dst)
}

// System writes grow only the regular admin file. No fixed maximum is imposed;
// allocation/I/O failure is reported as 507 rather than pretending to have space.
func (s *Server) writeBlob(name string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	return s.metadataWrite(func(h *volume.Handle) error {
		_, e := h.Put(systemKey(name), b, volume.Options{})
		return e
	})
}

func (s *Server) metadataWrite(write func(*volume.Handle) error) error {
	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	store, e := s.metadataStore()
	if e != nil {
		return e
	}
	for {
		h, e := store.Open(true)
		if e != nil {
			return e
		}
		e = write(h)
		h.Close()
		if !errors.Is(e, volume.ErrFull) || errors.Is(e, volume.ErrRecovery) {
			return e
		}
		current := store.Size()
		if current > uint64(^uint64(0)>>1)/2 {
			return volume.ErrFull
		}
		if e = store.Grow(int64(current * 2)); e != nil {
			return e
		}
		s.mu.Lock()
		c := copyConfig(s.cfg)
		for i := range c.Volumes {
			if c.Volumes[i].ID == store.Identity().VolumeID.String() {
				c.Volumes[i].SizeBytes = int64(store.Size())
				if m := s.volumes[c.Volumes[i].ID]; m != nil {
					m.config.SizeBytes = c.Volumes[i].SizeBytes
				}
			}
		}
		s.cfg = c
		if !s.metadataPreview {
			e = saveConfig(s.configPath, c)
		}
		s.mu.Unlock()
		if e != nil {
			return e
		}
	}
}
func (s *Server) initAccounts(allowBootstrap bool) error {
	var users accountsImage
	e := s.readBlob("accounts", &users)
	if errors.Is(e, volume.ErrNotFound) && allowBootstrap {
		// Only an empty newly adopted system volume may bootstrap. A corrupt/missing
		// account blob in an existing configured system volume is a recovery error.
		if s.system.Stats().Objects != 0 {
			return errors.New("system account object missing from nonempty admin volume")
		}
		a := s.cfg.Auth.BootstrapAdmin
		hash, e := bcrypt.GenerateFromPassword([]byte(a.Password), bcrypt.DefaultCost)
		if e != nil {
			return e
		}
		return s.writeBlob("accounts", accountsImage{Schema: 1, Users: []account{{ID: newID(), Name: a.Username, Role: "ADMIN", PasswordHash: string(hash)}}})
	}
	if e != nil {
		return e
	}
	if users.Schema != 1 || len(users.Users) == 0 {
		return errors.New("invalid system account image")
	}
	return nil
}
func (s *Server) findAccount(name string) (account, error) {
	var image accountsImage
	if e := s.readBlob("accounts", &image); e != nil {
		return account{}, e
	}
	for _, u := range image.Users {
		if u.Name == name && !u.Disabled {
			return u, nil
		}
	}
	return account{}, errors.New("unknown account")
}

type session struct {
	Username, UserID, CSRF string
	Expires                time.Time
}
type principal struct {
	// Internal S3 operations retain the original HTTP operation grant, including
	// write-only/delete-only users. Native clients cannot set this field.
	frontendPermission BucketPermissions
	frontendIssuer     client.ID
	connection         *connectionAuthorization
	ClusterPeer        bool
	User               account
	SessionToken       string
	CSRF               string
	Cookie             bool
}

func (s *Server) authenticate(r *http.Request) (principal, error) {
	if name, password, ok := r.BasicAuth(); ok {
		if strings.HasPrefix(name, frontendCredentialPrefix) {
			return s.authenticateFrontend(r, name, password)
		}
		if strings.HasPrefix(name, "node-") {
			id, err := bucketID(strings.TrimPrefix(name, "node-"))
			if err != nil {
				return principal{}, errors.New("invalid peer identity")
			}
			s.mu.RLock()
			token := s.cfg.Auth.ClusterToken
			s.mu.RUnlock()
			if token == "" || !constantEqual(password, token) {
				return principal{}, errors.New("invalid peer credentials")
			}
			return principal{User: account{ID: id.String(), Name: name, Role: "NODE"}, ClusterPeer: true}, nil
		}
		if s.joinPending() {
			return principal{}, fail(503, "node is waiting for committed cluster admission")
		}
		u, e := s.findAccount(name)
		if e != nil || bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
			return principal{}, errors.New("invalid credentials")
		}
		if u.Role != "ADMIN" && u.Role != "USER" {
			return principal{}, errors.New("invalid account role")
		}
		return principal{User: u}, nil
	}
	if s.joinPending() {
		return principal{}, fail(503, "node is waiting for committed cluster admission")
	}
	c, e := r.Cookie("stqr_session")
	if e != nil {
		return principal{}, e
	}
	s.authMu.Lock()
	ss, ok := s.sessions[c.Value]
	if ok && time.Now().After(ss.Expires) {
		delete(s.sessions, c.Value)
		ok = false
	}
	s.authMu.Unlock()
	if !ok {
		return principal{}, errors.New("session expired")
	}
	u, e := s.findAccount(ss.Username)
	if e != nil || u.ID != ss.UserID || u.Role != "ADMIN" {
		return principal{}, errors.New("session revoked")
	}
	return principal{User: u, SessionToken: c.Value, CSRF: ss.CSRF, Cookie: true}, nil
}
func constantEqual(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func (s *Server) createSession(w http.ResponseWriter, p principal) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.requireAdmin(p); err != nil {
		httpError(w, 403, err)
		return
	}
	token, e := randomSecret()
	if e != nil {
		httpError(w, 500, e)
		return
	}
	csrf, e := randomSecret()
	if e != nil {
		httpError(w, 500, e)
		return
	}
	ss := session{Username: p.User.Name, UserID: p.User.ID, CSRF: csrf, Expires: time.Now().Add(12 * time.Hour)}
	s.authMu.Lock()
	for k, v := range s.sessions {
		if time.Now().After(v.Expires) {
			delete(s.sessions, k)
		}
	}
	if len(s.sessions) >= 1024 {
		s.authMu.Unlock()
		httpError(w, 503, errors.New("session limit reached"))
		return
	}
	s.sessions[token] = ss
	s.authMu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "stqr_session", Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: 12 * 3600})
	writeJSON(w, 200, map[string]any{"username": p.User.Name, "csrf_token": csrf})
}
