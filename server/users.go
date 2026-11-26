// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
	"golang.org/x/crypto/bcrypt"
)

// Account mutations and their retry receipts share one atomic system object.
// No plaintext password or unkeyed password digest is persisted. opMu serializes
// read/modify/write with other local administrative commits. The cluster path
// prepares this mutation on a private draft and publishes it through ALL.
type accountReceipt struct {
	ID     string `json:"id"`
	Actor  string `json:"actor"`
	Digest string `json:"digest"`
	Status int    `json:"status"`
}

func userOpcode(op uint16) bool {
	return op == client.UserListPage || op == client.UserS3KeyList || op == client.UserS3KeySet || op == client.UserS3KeyDelete || op == client.UserList || op == client.UserCreate || op == client.UserUpdate || op == client.UserSetPassword || op == client.UserDelete
}
func encodeUser(e *client.Encoder, u account) {
	id, _ := parseID(u.ID)
	e.ID(client.ID(id))
	e.Text(u.Name)
	role := uint8(0)
	if u.Role == "ADMIN" {
		role = 1
	}
	e.U8(role)
	e.Bool(u.Disabled)
}
func (s *Server) executeUsers(m client.Message, p principal) ([]byte, int, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	token := s.cfg.Auth.ClusterToken
	s.mu.RUnlock()
	if closed {
		return nil, 0, fail(503, "server is stopping")
	}
	index, err := s.accountIndex()
	if err != nil {
		return nil, 0, err
	}
	image := index.image
	// Authorization is checked again inside the mutation lock so an already queued
	// request cannot survive deletion, password change or role revocation.
	authorized := p.connection != nil && p.isAdmin()
	if p.connection == nil {
		if i, exists := index.byID[p.User.ID]; exists {
			u := image.Users[i]
			authorized = u.Role == "ADMIN" && !u.Disabled && constantEqual(u.PasswordHash, p.User.PasswordHash)
		}
	}

	if !authorized {
		return nil, 0, fail(403, "administrator required")
	}
	if m.Opcode == client.UserListPage {
		return userListPage(m.Body, image, s)
	}
	d := client.Decoder{B: m.Body}
	if m.Opcode == client.UserList {
		if e := d.Done(); e != nil {
			return nil, 0, fail(400, "USER_LIST requires an empty body")
		}
		users := append([]account(nil), image.Users...)
		sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
		e := client.Encoder{}
		e.V(uint64(len(users)))
		for _, u := range users {
			encodeUser(&e, u)
		}
		return e.B, 200, nil
	}
	if m.Opcode == client.UserS3KeyList {
		target := d.ID()
		if d.Done() != nil {
			return nil, 0, fail(400, "invalid key list request")
		}
		if i, exists := index.byID[target.String()]; exists {
			u := image.Users[i]
			e := client.Encoder{}
			e.V(uint64(len(u.S3Keys)))
			for _, k := range u.S3Keys {
				e.Text(k.ID)
			}
			return e.B, 200, nil
		}
		return nil, 0, fail(404, "user not found")
	}
	if m.ID == (client.ID{}) {
		return nil, 0, fail(400, "a nonzero operation ID is required")
	}
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte("STQR account mutation v1"))
	mac.Write([]byte(p.User.ID))
	mac.Write([]byte{byte(m.Opcode), byte(m.Opcode >> 8)})
	mac.Write(m.Body)
	digest := hex.EncodeToString(mac.Sum(nil))
	if i, exists := index.byReceipt[m.ID.String()]; exists {
		receipt := image.Receipts[i]
		if receipt.Actor != p.User.ID || !constantEqual(receipt.Digest, digest) {
			return nil, 0, fail(400, "operation ID reused with different input")
		}
		return encodeAck(Operation{ID: receipt.ID, State: "fulfilled"}), receipt.Status, nil
	}
	target := d.ID()
	var name, password, accessID, accessSecret string
	var role uint8
	var disabled bool
	var deleteBuckets bool
	if m.Opcode == client.UserCreate || m.Opcode == client.UserUpdate {
		name = d.Text()
		role = d.U8()
		disabled = d.Bool()
		if m.Opcode == client.UserCreate {
			password = d.Text()
		}
	} else if m.Opcode == client.UserDelete && len(m.Body) > 16 {
		deleteBuckets = d.Bool()
	} else if m.Opcode == client.UserSetPassword {
		password = d.Text()
	} else if m.Opcode == client.UserS3KeySet || m.Opcode == client.UserS3KeyDelete {
		accessID = d.Text()
		if m.Opcode == client.UserS3KeySet {
			accessSecret = d.Text()
		}
	}
	if e := d.Done(); e != nil {
		return nil, 0, fail(400, "invalid user message")
	}
	if target == (client.ID{}) {
		return nil, 0, fail(400, "a nonzero user ID is required")
	}
	if m.Opcode == client.UserCreate || m.Opcode == client.UserUpdate {
		if !validUsername(name) || role > 1 {
			return nil, 0, fail(400, "invalid username or role")
		}
		_, duplicateID := index.byID[target.String()]
		nameIndex, duplicateName := index.byName[name]
		if m.Opcode == client.UserCreate && duplicateID || duplicateName && image.Users[nameIndex].ID != target.String() {
			return nil, 0, fail(409, "username or user ID already exists")
		}
	}
	targetIndex, found := index.byID[target.String()]
	if m.Opcode != client.UserCreate {
		if !found {
			return nil, 0, fail(404, "user not found")
		}
		if m.Opcode == client.UserDelete && target.String() == p.User.ID {
			return nil, 0, fail(409, "you cannot delete your own account")
		}
		if m.Opcode == client.UserUpdate && target.String() == p.User.ID && (role != 1 || disabled) {
			return nil, 0, fail(409, "you cannot change your own role or disable your own account")
		}
	}
	var hash []byte
	if m.Opcode == client.UserCreate || m.Opcode == client.UserSetPassword {
		if len(password) == 0 || len(password) > 72 || !utf8.ValidString(password) {
			return nil, 0, fail(400, "password must contain 1 to 72 UTF-8 bytes")
		}
		var e error
		hash, e = bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if e != nil {
			return nil, 0, e
		}
	}
	if m.Opcode == client.UserS3KeySet || m.Opcode == client.UserS3KeyDelete {
		if !validS3AccessID(accessID) {
			return nil, 0, fail(400, "access key must contain 16 to 128 ASCII letters, digits, hyphens or underscores")
		}
		if m.Opcode == client.UserS3KeySet && (len(accessSecret) < 16 || len(accessSecret) > 128 || !utf8.ValidString(accessSecret)) {
			return nil, 0, fail(400, "secret must contain 16 to 128 UTF-8 bytes")
		}
		if key, exists := index.byAccessKey[accessID]; exists && image.Users[key.user].ID != target.String() {
			return nil, 0, fail(409, "access key already belongs to another user")
		}
	}
	// Only mutation preparation needs a writable copy of the account image.
	image = cloneAccountsImage(image)
	status := 200
	switch m.Opcode {
	case client.UserCreate:
		roleName := "USER"
		if role == 1 {
			roleName = "ADMIN"
		}
		image.Users = append(image.Users, account{ID: target.String(), Name: name, Role: roleName, Disabled: disabled, PasswordHash: string(hash)})
		status = 201
	case client.UserUpdate:
		u := &image.Users[targetIndex]
		u.Name, u.Role, u.Disabled = name, "USER", disabled
		if role == 1 {
			u.Role = "ADMIN"
		}
	case client.UserSetPassword:
		image.Users[targetIndex].PasswordHash = string(hash)
	case client.UserS3KeySet, client.UserS3KeyDelete:
		{
			u := &image.Users[targetIndex]
			found := -1
			for j, k := range u.S3Keys {
				if k.ID == accessID {
					found = j
					break
				}
			}
			if m.Opcode == client.UserS3KeyDelete {
				if found >= 0 {
					u.S3Keys = append(u.S3Keys[:found], u.S3Keys[found+1:]...)
				}
			} else {
				if found < 0 && len(u.S3Keys) >= 32 {
					return nil, 0, fail(400, "at most 32 access keys per user")
				}
				sealed, e := s.sealS3Secret(u.ID, accessID, accessSecret)
				if e != nil {
					return nil, 0, e
				}
				key := s3AccessKey{ID: accessID, Sealed: sealed}
				if found < 0 {
					u.S3Keys = append(u.S3Keys, key)
				} else {
					u.S3Keys[found] = key
				}
			}
		}
	case client.UserDelete:
		if deleteBuckets {
			if err := s.deleteExclusiveUserBuckets(target.String()); err != nil {
				return nil, 0, err
			}
		}
		image.Users = append(image.Users[:targetIndex], image.Users[targetIndex+1:]...)
	}
	image.Receipts = append(image.Receipts, accountReceipt{ID: m.ID.String(), Actor: p.User.ID, Digest: digest, Status: status})
	if e := s.writeBlob("accounts", image); e != nil {
		return nil, 0, e
	}
	if m.Opcode != client.UserCreate && m.Opcode != client.UserS3KeySet && m.Opcode != client.UserS3KeyDelete {
		s.invalidateUserConnections(target.String())
		s.authMu.Lock()
		for key, ss := range s.sessions {
			if ss.UserID == target.String() {
				delete(s.sessions, key)
			}
		}
		s.authMu.Unlock()
	}
	return encodeAck(Operation{ID: m.ID.String(), State: "fulfilled"}), status, nil
}
func validUsername(name string) bool {
	if name == "" || len(name) > 128 || !utf8.ValidString(name) || strings.HasPrefix(name, "node-") {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == ':' {
			return false
		}
	}
	return true
}
