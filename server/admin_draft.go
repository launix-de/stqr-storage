// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A draft reuses native admin validation on a private metadata volume. User data
// volumes are borrowed for read-only usage checks, not owned by this Server.
// Never call Server.Close on it; close releases only the private root. Neither
// plaintext requests nor config changes are persisted to the source node.
type adminDraft struct {
	server *Server
	result []byte
	status int
}

func (d *adminDraft) close() error {
	d.server.cancel()
	return d.server.system.Close()
}

func globalAdminMutation(opcode uint16) bool {
	return userOpcode(opcode) && opcode != client.UserList && opcode != client.UserS3KeyList || bucketAdminOpcode(opcode) && opcode != client.BucketList && opcode != client.BucketQuotaCapacity
}

// The caller binds a global round before preparing a draft. opMu gives a
// consistent source snapshot without blocking ordinary metadata reads. The
// resulting draft remains invisible until captured into an immutable system
// image and committed through the ALL participant path.
func (s *Server) prepareAdminDraft(m client.Message, actor principal) (_ *adminDraft, err error) {
	if m.Kind != client.Request || !globalAdminMutation(m.Opcode) {
		return nil, fail(400, "global administrative mutation required")
	}
	s.opMu.Lock()
	preview, err := s.newAdminPreview()
	s.opMu.Unlock()
	if err != nil {
		return nil, err
	}
	draft := &adminDraft{server: preview}
	defer func() {
		if err != nil {
			draft.close()
		}
	}()
	current, err := preview.findAccount(actor.User.Name)
	if err != nil || current.ID != actor.User.ID || current.Role != "ADMIN" || current.Disabled || !constantEqual(current.PasswordHash, actor.User.PasswordHash) {
		return nil, fail(403, "current administrator required")
	}
	// Revalidate the account rather than trusting a cached socket capability or
	// binding the source's live bucket pointers into the private preview.
	identity := principal{User: current}
	if userOpcode(m.Opcode) {
		draft.result, draft.status, err = preview.executeUsers(m, identity)
	} else {
		draft.result, draft.status, err = preview.executeBuckets(m, identity)
	}
	if err != nil {
		return nil, err
	}
	return draft, nil
}

// newAdminPreview runs with source opMu held. This is an isolated preparation
// object, never an independently serving node and never a second replica.
func (s *Server) newAdminPreview() (_ *Server, err error) {
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		return nil, err
	}
	store, err := s.metadataStore()
	if err != nil {
		return nil, err
	}
	identity := store.Identity()
	mem, err := volume.NewMemory(1 << 20)
	if err != nil {
		return nil, err
	}
	root, err := volume.Format(mem, volume.FormatOptions{Class: identity.Class, ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID})
	if err != nil {
		mem.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	preview := &Server{metadataPreview: true, system: root, ctx: ctx, cancel: cancel, volumes: make(map[string]*managedVolume), sessions: make(map[string]session), sockets: make(map[*wsSession]struct{})}
	defer func() {
		if err != nil {
			cancel()
			root.Close()
		}
	}()
	s.mu.RLock()
	preview.cfg, preview.running = copyConfig(s.cfg), copyConfig(s.running)
	for id, managed := range s.volumes {
		copy := *managed
		if id == identity.VolumeID.String() {
			copy.store = root
			copy.config.SizeBytes = int64(root.Size())
		}
		preview.volumes[id] = &copy
	}
	s.mu.RUnlock()
	preview.placementGroups.Store(s.placementGroups.Load()) // immutable shared PG image
	if err = preview.writeBlob("accounts", accounts); err != nil {
		return nil, err
	}
	// Build the same canonical catalog without Save's create/revision increment:
	// the preview must begin with the exact committed description revisions.
	objects := make(map[volume.ObjectID][]byte)
	var hashes []volume.ObjectID
	for _, description := range s.bucketSnapshots() {
		raw, encodeErr := json.Marshal(description)
		if encodeErr != nil {
			return nil, encodeErr
		}
		id := volume.ObjectID(sha256.Sum256(raw))
		objects[id] = raw
		hashes = append(hashes, id)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	list := make([]byte, 0, 32*len(hashes))
	for _, id := range hashes {
		list = append(list, id[:]...)
	}
	err = preview.metadataWrite(func(h *volume.Handle) error {
		for id, raw := range objects {
			if _, err := h.Put(volume.Key{Bucket: systemBucket, Object: id}, raw, volume.Options{}); err != nil {
				return err
			}
		}
		_, err := h.Put(BucketCatalogKey(), list, volume.Options{})
		return err
	})
	if err != nil {
		return nil, err
	}
	if err = preview.initBuckets(); err != nil {
		return nil, err
	}
	// Preserve the maintenance exclusion in the validation snapshot. A private
	// draft cannot bypass a live filesystem-repair owner merely by rebuilding
	// the catalog's RAM bucket objects.
	s.bucketMu.RLock()
	for id, b := range preview.bucketsByID {
		if live := s.bucketsByID[id]; live != nil {
			live.mu.RLock()
			b.maintenance = live.maintenance
			live.mu.RUnlock()
		}
	}
	s.bucketMu.RUnlock()
	return preview, nil
}
