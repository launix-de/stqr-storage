// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestSystemAdminFormatBootstrapAndLegacyReopen(t *testing.T) {
	s := testServer(t)
	system, err := s.Bucket(systemBucket)
	if err != nil {
		t.Fatal(err)
	}
	if d := system.Describe(); d.Format != systemAdminFormat || d.Schema != 2 {
		t.Fatal("new system bucket not tagged admin", d)
	}
	_, descriptions, _, err := s.buckets.load()
	if err != nil {
		t.Fatal(err)
	}
	if descriptions[0].Format != systemAdminFormat || descriptions[0].Schema != 2 {
		t.Fatal("bootstrap did not persist format")
	}
	// Reproduce an existing catalog written before the reserved format label.
	// Keep its original content-addressed bytes intact through reopening.
	legacy := system.Describe()
	legacy.Format = ""
	legacy.Schema = 1
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	id := volume.ObjectID(sha256.Sum256(raw))
	beforeAccounts := accountsImage{}
	if err = s.readBlob("accounts", &beforeAccounts); err != nil {
		t.Fatal(err)
	}
	if err = s.metadataWrite(func(h *volume.Handle) error {
		if _, err := h.Put(volume.Key{Bucket: systemBucket, Object: id}, raw, volume.Options{}); err != nil {
			return err
		}
		_, err := h.Put(BucketCatalogKey(), id[:], volume.Options{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	path := s.configPath
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	system, err = reopened.Bucket(systemBucket)
	if err != nil {
		t.Fatal(err)
	}
	if d := system.Describe(); d.Format != systemAdminFormat || d.Schema != 2 || d.Revision != legacy.Revision {
		t.Fatal("legacy runtime projection missing or revision changed", d)
	}
	if d := system.storageDescription(); d.Format != systemAdminFormat {
		t.Fatal("internal runtime description missing admin label")
	}
	h, err := reopened.system.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	same, _, e1 := h.Get(volume.Key{Bucket: systemBucket, Object: id})
	list, _, e2 := h.Get(BucketCatalogKey())
	h.Close()
	if e1 != nil || e2 != nil || !bytes.Equal(raw, same) || !bytes.Equal(list, id[:]) {
		t.Fatal("legacy hashes/list rewritten during local projection", e1, e2)
	}
	var afterAccounts accountsImage
	if err = reopened.readBlob("accounts", &afterAccounts); err != nil {
		t.Fatal(err)
	}
	beforeJSON, _ := json.Marshal(beforeAccounts)
	afterJSON, _ := json.Marshal(afterAccounts)
	if !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatal("format projection rewrote account data")
	}
}

func TestSystemAdminFormatLegacySystemImageHashesAndNewPublication(t *testing.T) {
	s, err := Open(Options{ConfigPath: filepath.Join(t.TempDir(), "node.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.controller.mu.Lock()
	defer s.controller.mu.Unlock()
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	membership := client.Membership{Cluster: client.ID(cluster), Epoch: 1, Members: []client.Member{{ID: client.ID(node)}}}
	roots := map[client.ID]volume.ID{client.ID(node): s.system.Identity().VolumeID}
	claim, err := s.controller.coordinator.begin(ctx, controlAdministration, []client.ID{client.ID(node)})
	if err != nil {
		t.Fatal(err)
	}
	membership = claim.Base.Membership
	bundle, err := s.captureSystemImage(membership, claim.Base.Sequence+1, claim.Base.Generation, claim.Base.MapRevision, roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	var manifest systemManifest
	if err = decodeControlRecord(bundle.Objects[bundle.Root], &manifest); err != nil {
		t.Fatal(err)
	}
	// Build a hash-correct old system graph, not an unvalidated runtime shortcut.
	for i, id := range manifest.Buckets {
		d, err := strictBucketJSON(bundle.Objects[id])
		if err != nil {
			t.Fatal(err)
		}
		if d.ID == systemBucket.String() {
			d.Format = ""
			d.Schema = 1
			raw, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			manifest.Buckets[i] = bundle.add(raw)
		}
	}
	sort.Slice(manifest.Buckets, func(i, j int) bool { return bytes.Compare(manifest.Buckets[i][:], manifest.Buckets[j][:]) < 0 })
	manifest.Schema = 1
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Root = bundle.add(raw)
	originalRoot := bundle.Root
	originalRootBytes := bytes.Clone(raw)
	if err = s.stageSystemBundle(bundle); err != nil {
		t.Fatal(err)
	}
	view, err := s.loadSystemView(originalRoot)
	if err != nil {
		t.Fatal("legacy graph rejected", err)
	}
	if view.descriptions[0].Format != "" {
		t.Fatal("validation changed hashed legacy description")
	}
	proposal := controlProposal{Claim: claim, Kind: controlAdministration, Image: [32]byte(originalRoot)}
	if err = s.controller.coordinator.prepared(ctx, proposal, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.controller.coordinator.resume(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(systemBucket)
	if err != nil {
		t.Fatal(err)
	}
	if b.Describe().Format != systemAdminFormat {
		t.Fatal("installed legacy view not labeled admin")
	}
	if s.currentSystem.Load().root != originalRoot {
		t.Fatal("projection replaced committed root")
	}
	again, err := s.loadSystemView(originalRoot)
	if err != nil || again.root != originalRoot {
		t.Fatal("legacy graph no longer validates", err)
	}
	h, err := s.system.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	preserved, _, err := h.Get(volume.Key{Bucket: systemBucket, Object: originalRoot})
	h.Close()
	if err != nil || !bytes.Equal(preserved, originalRootBytes) {
		t.Fatal("old manifest was rewritten", err)
	}
	next, err := s.captureSystemImage(membership, claim.Base.Sequence+2, claim.Base.Generation, claim.Base.MapRevision, roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	newView, err := decodeSystemView(next.Root, func(id volume.ObjectID) ([]byte, error) {
		raw, ok := next.Objects[id]
		if !ok {
			return nil, volume.ErrNotFound
		}
		return raw, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if newView.manifest.Schema != 2 || newView.descriptions[0].Format != systemAdminFormat || newView.descriptions[0].Schema != 2 {
		t.Fatal("next publication omitted explicit admin format")
	}
}

func TestSystemAdminFormatCannotBeClaimedByClientsOrCopied(t *testing.T) {
	s := testServer(t)
	ordinary, _ := createProtocolBucket(t, s, "admin-looking-name")
	p := userPrincipal(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []volume.ID{ordinary.ID(), systemBucket} {
		if err := s.openBucketFormat(ctx, id.String(), systemAdminFormat, p); statusOf(err) != 403 {
			t.Fatal("reserved bind accepted", id, err)
		}
		if err := s.setEmptyBucketFormat(id, systemAdminFormat, p); statusOf(err) != 403 {
			t.Fatal("reserved direct bind accepted", id, err)
		}
		if err := s.commitBucketFormat(ctx, id, systemAdminFormat, p); statusOf(err) != 403 {
			t.Fatal("reserved coordinated bind accepted", id, err)
		}
	}
	if ordinary.Describe().Format != "" {
		t.Fatal("failed binding changed ordinary format")
	}
	d := ordinary.Describe()
	d.Format = systemAdminFormat
	d.Schema = 2
	if _, err := s.buckets.Save(d, d.Revision); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("ordinary catalog accepted admin format", err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := s.cfg.Auth.BootstrapAdmin
	for _, id := range []volume.ID{ordinary.ID(), systemBucket} {
		conn, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(id), systemAdminFormat)
		if err == nil {
			conn.Close()
			t.Fatal("native admin-format connection accepted", id)
		}
		var status *client.StatusError
		if !errors.As(err, &status) || status.Status != 403 {
			t.Fatal("unexpected reserved connection rejection", err)
		}
	}
	for _, pair := range [][2]volume.ID{{systemBucket, ordinary.ID()}, {ordinary.ID(), systemBucket}} {
		e := client.Encoder{}
		e.ID(client.ID(pair[0]))
		e.ID(client.ID(pair[1]))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, p); err == nil {
			t.Fatal("system namespace cloned", pair)
		}
	}
	system, _ := s.Bucket(systemBucket)
	wrong := system.Describe()
	wrong.Format = "fs"
	if _, err := normalizeBucketDescription(wrong); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("system bucket accepted application format", err)
	}
	// Arbitrary ordinary application labels stay opaque and case sensitive.
	if err := s.openBucketFormat(ctx, ordinary.ID().String(), "custom<fs-v2>", p); err != nil {
		t.Fatal("ordinary formats narrowed to enum", err)
	}
}
