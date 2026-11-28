// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"fmt"
	"github.com/launix-de/stqr-storage/volume"
	"strings"

	"github.com/launix-de/stqr-storage/client"
)

// initBuckets runs during Open, before requests/workers can access the server.
// Version 0 is the explicit migration from the former accounts-only admin volume.
// Syncing the root precedes the config marker: interruption before config save
// is resumed by validating the already written root, not by replacing it.
func (s *Server) initBuckets() error {
	if s.cfg.BucketCatalogVersion > 1 {
		return fmt.Errorf("unsupported bucket catalog version %d", s.cfg.BucketCatalogVersion)
	}
	store, err := s.metadataStore()
	if err != nil {
		return err
	}
	catalog, err := NewBucketCatalog(store)
	if err != nil {
		return err
	}
	loaded, err := catalog.List()
	if errors.Is(err, ErrBucketCatalogMissing) && s.cfg.BucketCatalogVersion == 0 {
		if err = catalog.Initialize(); err != nil {
			return fmt.Errorf("initialize bucket catalog: %w", err)
		}
		loaded, err = catalog.List()
	}
	if err != nil {
		return fmt.Errorf("load bucket catalog: %w", err)
	}
	s.buckets = catalog
	s.bucketsByID = make(map[volume.ID]*Bucket)
	s.bucketsByName = make(map[string]*Bucket)
	for _, b := range loaded {
		if b.ID() == systemBucket && b.Describe().Policy.PlacementGroup == "" {
			d := b.Describe()
			d.Policy.PlacementGroup = systemPlacementGroup.String()
			d.Policy.Version++
			b, err = catalog.Save(d, d.Revision)
			if err != nil {
				return fmt.Errorf("migrate system placement-group reference: %w", err)
			}
		}
		s.publishBucket(b.Describe())
	}
	catalog.onSave = s.publishBucket
	s.cfg.BucketCatalogVersion = 1
	// Bootstrap can grow the admin file. Save its actual size along with the
	// activation marker through Open's existing final atomic config publication.
	size := int64(s.system.Size())
	for i := range s.cfg.Volumes {
		if s.cfg.Volumes[i].System {
			s.cfg.Volumes[i].SizeBytes = size
			if m := s.volumes[s.cfg.Volumes[i].ID]; m != nil {
				m.config.SizeBytes = size
			}
		}
	}
	return nil
}

// bucketInfo implements the complete successful result of opcode 0x010c.
// Name is the spec's Text field; no requested role/permissions are accepted.
func (s *Server) bucketInfo(m client.Message, p principal) ([]byte, error) {
	if m.Kind != client.Request {
		return nil, fail(400, "BUCKET_INFO requires a request")
	}
	d := client.Decoder{B: m.Body}
	name := d.Text()
	if err := d.Done(); err != nil {
		return nil, fail(400, err.Error())
	}
	if len(name) == 0 || len(name) > 255 || strings.ContainsRune(name, 0) {
		return nil, fail(400, "invalid bucket name")
	}
	if p.User.Disabled || (p.User.Role != "ADMIN" && p.User.Role != "USER") {
		return nil, fail(401, "authenticated account required")
	}
	user, err := bucketID(p.User.ID)
	if err != nil {
		return nil, fail(401, "invalid account identity")
	}
	if s.buckets == nil {
		return nil, fail(503, "bucket catalog unavailable")
	}
	if p.connection != nil && p.connection.bucket != nil {
		auth := p.connection
		description := auth.bucket.storageDescription()
		// A bound client may have opened by ID and never learned the name.
		// Both selectors remain confined to this authenticated namespace.
		if auth.invalid.Load() || name != description.Name && name != description.ID {
			return nil, ErrBucketForbidden
		}
		return (BucketInfo{description: description, Permissions: auth.permissions}).MarshalBinary()
	}
	bucket, err := s.BucketByName(name)
	if err != nil {
		return nil, err
	}
	info, err := bucket.Info(user, p.User.Role == "ADMIN")
	if err != nil {
		return nil, err
	}
	return info.MarshalBinary()
}

// publishBucket runs only after the description and replacement catalog synced.
// Existing handles retain the same RAM object across configuration changes.
func (s *Server) publishBucket(d BucketDescription) {
	id, _ := bucketID(d.ID)
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	b := s.bucketsByID[id]
	if b == nil {
		b = bucketSnapshot(d)
		b.server = s
		s.bucketsByID[id] = b
		s.cowTopologyCache.revision.Add(1)
	} else {
		b.mu.Lock()
		old := b.description
		if old.Revision >= d.Revision {
			b.mu.Unlock()
			return
		}
		if s.bucketsByName[old.Name] == b {
			delete(s.bucketsByName, old.Name)
		}
		b.setDescription(d)
		b.mu.Unlock()
	}
	if !d.DeletionComplete {
		s.bucketsByName[d.Name] = b
	}
	s.invalidateBucketConnections(b, d)
	s.publishQuotaPoliciesLocked()
}
func (s *Server) Bucket(id volume.ID) (*Bucket, error) {
	s.bucketMu.RLock()
	b := s.bucketsByID[id]
	s.bucketMu.RUnlock()
	if b == nil || b.storageDescription().DeletionComplete {
		return nil, ErrBucketNotFound
	}
	return b, nil
}
func (s *Server) BucketByName(name string) (*Bucket, error) {
	s.bucketMu.RLock()
	b := s.bucketsByName[name]
	s.bucketMu.RUnlock()
	if b == nil {
		return nil, ErrBucketNotFound
	}
	return b, nil
}
func (s *Server) bucketSnapshots() []BucketDescription {
	s.bucketMu.RLock()
	defer s.bucketMu.RUnlock()
	out := make([]BucketDescription, 0, len(s.bucketsByID))
	for _, b := range s.bucketsByID {
		out = append(out, b.Describe())
	}
	return out
}
