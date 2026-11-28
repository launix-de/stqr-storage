// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func deletedProofRow(id byte) BucketDescription {
	return BucketDescription{ID: (volume.ID{id}).String(), ReferenceLineageComplete: true, State: BucketDeleting, DeletionComplete: true, Receipts: []BucketReceipt{{ID: client.NewRequestID().String(), Opcode: client.BucketDelete, Status: 200}}}
}
func cloneProofRows(rows []BucketDescription) []BucketDescription {
	out := make([]BucketDescription, len(rows))
	for i, d := range rows {
		out[i] = cloneBucketDescription(d)
	}
	return out
}
func TestDeletedBucketDependencyProofCatalog(t *testing.T) {
	subject := volume.ID{2}
	a, b, c := deletedProofRow(1), deletedProofRow(2), deletedProofRow(3)
	ab := BucketReceipt{ID: client.NewRequestID().String(), Digest: "ab", Actor: "admin", Opcode: client.BucketClone, Status: 200}
	bc := BucketReceipt{ID: client.NewRequestID().String(), Digest: "bc", Actor: "admin", Opcode: client.BucketClone, Status: 200}
	a.Receipts = append(a.Receipts, ab)
	b.Receipts = append(b.Receipts, ab, bc)
	c.Receipts = append(c.Receipts, bc)
	base := []BucketDescription{a, b, c}
	for _, tc := range []struct {
		name string
		edit func([]BucketDescription) []BucketDescription
		safe bool
	}{
		{"all-deleted-transitive", func(d []BucketDescription) []BucketDescription { return d }, true},
		{"committed-deleting-intent", func(d []BucketDescription) []BucketDescription {
			d[1].DeletionComplete = false
			d[1].Receipts[0].Running = true
			d[1].Receipts[0].Status = 202
			return d
		}, true},
		{"unknown-subject", func(d []BucketDescription) []BucketDescription { return append(d[:1], d[2:]...) }, false},
		{"legacy-subject", func(d []BucketDescription) []BucketDescription { d[1].ReferenceLineageComplete = false; return d }, false},
		{"legacy-ancestor", func(d []BucketDescription) []BucketDescription { d[0].ReferenceLineageComplete = false; return d }, false},
		{"live-detached-descendant", func(d []BucketDescription) []BucketDescription {
			d[2].State = BucketActive
			d[2].DeletionComplete = false
			return d
		}, false},
		{"live-ancestor", func(d []BucketDescription) []BucketDescription {
			d[0].State = BucketActive
			d[0].DeletionComplete = false
			return d
		}, false},
		{"missing-endpoint", func(d []BucketDescription) []BucketDescription { return d[1:] }, false},
		{"missing-delete-receipt", func(d []BucketDescription) []BucketDescription { d[1].Receipts = d[1].Receipts[1:]; return d }, false},
		{"incomplete-clone", func(d []BucketDescription) []BucketDescription { d[1].Receipts[1].Running = true; return d }, false},
		{"mismatched-clone", func(d []BucketDescription) []BucketDescription { d[1].Receipts[1].Digest = "different"; return d }, false},
		{"duplicate-clone-end", func(d []BucketDescription) []BucketDescription { d[1].Receipts = append(d[1].Receipts, ab); return d }, false},
		{"linked-clone", func(d []BucketDescription) []BucketDescription {
			d[1].ClonePeers = []BucketClonePeer{{Bucket: a.ID, Link: ab.ID}}
			return d
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deletedBucketDependencyProof(tc.edit(cloneProofRows(base)), subject); got != tc.safe {
				t.Fatalf("safe=%v want %v", got, tc.safe)
			}
		})
	}
	// An unrelated live or legacy bucket cannot veto a proven closed component.
	if !deletedBucketDependencyProof(append(cloneProofRows(base), BucketDescription{ID: (volume.ID{4}).String(), State: BucketActive}), subject) {
		t.Fatal("unrelated namespace vetoed proof")
	}
}
func TestDeletedBucketProofCreationProvenanceAndNoReads(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "proof", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "new-proof")
	if !b.Describe().ReferenceLineageComplete {
		t.Fatal("fresh receipt-aware creation lacks provenance")
	}
	d := b.Describe()
	d.State = BucketDeleting
	receipt := d.Receipts[0]
	receipt.ID = client.NewRequestID().String()
	receipt.Opcode, receipt.Status, receipt.Running = client.BucketDelete, 202, true
	d.Receipts = append(d.Receipts, receipt)
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	before := s.system.IOStats()
	safe, err := s.canRetireDeletedBucketDependencies(context.Background(), b.ID())
	if err != nil || !safe {
		t.Fatal("known deleting namespace", safe, err)
	}
	after := s.system.IOStats()
	if before.ReadOps != after.ReadOps || before.SyncOps != after.SyncOps {
		t.Fatal("metadata-only proof performed volume reads or Sync")
	}
}

func TestDeletedBucketProofWireAndSDK(t *testing.T) {
	f := newCombiningFixture(t)
	subject, _ := createProtocolBucket(t, f.s, "subject-proof")
	var target client.ObjectID
	subjectID := subject.ID()
	copy(target[:16], subjectID[:])
	message := referenceRequest(f.b, target, client.NewRequestID(), nil)
	message.Body[len(message.Body)-17] = 10
	before := f.root(t)
	syncs := f.v.IOStats().SyncOps
	body, err := f.s.executeObjectReference(context.Background(), message, f.ws, f.p)
	if err != nil || !bytes.Equal(body, []byte{0}) {
		t.Fatal("live subject was not refused", body, err)
	}
	ordinary := f.p
	ordinary.User.Role = "USER"
	if _, err = f.s.executeObjectReference(context.Background(), message, f.ws, ordinary); objectStatus(err) != 403 {
		t.Fatal("nonadmin proof allowed", err)
	}
	malformed := message
	malformed.Body = bytes.Clone(message.Body)
	malformed.Body[len(malformed.Body)-18] = 1
	if _, err = f.s.executeObjectReference(context.Background(), malformed, f.ws, f.p); objectStatus(err) != 400 {
		t.Fatal("noncanonical subject accepted", err)
	}
	if !bytes.Equal(before, f.root(t)) || syncs != f.v.IOStats().SyncOps {
		t.Fatal("proof changed reference root or synced")
	}
	host := httptest.NewServer(f.s.Handler())
	defer host.Close()
	auth := f.s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(context.Background(), host.URL, auth.Username, auth.Password, client.BucketID(f.b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	safe, err := c.CanRetireDeletedBucketDependencies(context.Background(), client.BucketID(subject.ID()))
	if err != nil || safe {
		t.Fatal("SDK did not preserve false proof", safe, err)
	}
	d := subject.Describe()
	d.State = BucketDeleting
	receipt := d.Receipts[0]
	receipt.ID = client.NewRequestID().String()
	receipt.Opcode, receipt.Status, receipt.Running = client.BucketDelete, 202, true
	d.Receipts = append(d.Receipts, receipt)
	if _, err = f.s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	safe, err = c.CanRetireDeletedBucketDependencies(context.Background(), client.BucketID(subject.ID()))
	if err != nil || !safe {
		t.Fatal("SDK rejected positive proof", safe, err)
	}
}

// Uses the existing authenticated metadata-only info RPC, with in-memory
// committed cuts. There is no simulated object inventory or authority shrink.
func TestDeletedBucketProofRequiresMatchingEveryMember(t *testing.T) {
	source := testServer(t)
	quiesceProofFixture(source)
	addTestVolume(t, source, "proof-members", "hdd", 8<<20)
	subject, _ := createProtocolBucket(t, source, "member-subject")
	d := subject.Describe()
	d.State = BucketDeleting
	r := d.Receipts[0]
	r.ID = client.NewRequestID().String()
	r.Opcode, r.Status, r.Running = client.BucketDelete, 202, true
	d.Receipts = append(d.Receipts, r)
	if _, err := source.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	target := peerTestNode(t, source.running)
	quiesceProofFixture(target)
	a, _ := parseID(source.running.Node.ID)
	b, _ := parseID(target.running.Node.ID)
	cluster, _ := parseID(source.running.ClusterID)
	members := []client.Member{{ID: client.ID(a)}, {ID: client.ID(b)}}
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i].ID[:], members[j].ID[:]) < 0 })
	base := controlBase{Membership: client.Membership{Cluster: client.ID(cluster), Epoch: 1, Members: members}, Sequence: 1, Generation: 1, MapRevision: 1, Head: [32]byte{2}, SystemImage: [32]byte{3}}
	source.control = &controlRuntime{server: source, ledger: &controlLedger{node: client.ID(a), current: base}}
	target.control = &controlRuntime{server: target, ledger: &controlLedger{node: client.ID(b), current: base}}
	source.control.ledger.activity.Store(&controlActivity{})
	target.control.ledger.activity.Store(&controlActivity{})
	source.controller = &clusterController{server: source}
	source.currentSystem.Store(&systemView{root: volume.ObjectID(base.SystemImage), manifest: systemManifest{Membership: base.Membership}, descriptions: source.bucketSnapshots()})
	host := httptest.NewServer(target.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := source.dialPeer(ctx, host.URL, client.ID(b))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.close()
	if err = source.startPeerObservation(peer); err != nil {
		t.Fatal(err)
	}
	safe, err := source.canRetireDeletedBucketDependencies(ctx, subject.ID())
	if err != nil || !safe {
		t.Fatal("matching committed members rejected", safe, err)
	}
	target.control.ledger.mu.Lock()
	target.control.ledger.current.Head[0]++
	target.control.ledger.mu.Unlock()
	safe, err = source.canRetireDeletedBucketDependencies(ctx, subject.ID())
	if safe || objectStatus(err) != 503 {
		t.Fatal("disagreeing member ignored", safe, err)
	}
	peer.close()
	host.Close()
	short, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	safe, err = source.canRetireDeletedBucketDependencies(short, subject.ID())
	if safe || objectStatus(err) != 503 {
		t.Fatal("offline member ignored", safe, err)
	}
	// Stop normal cleanup from treating the intentionally synthetic ledger as a
	// running coordinator after this independent protocol fixture exits.
	source.cancel()
	target.cancel()
	source.workers.Wait()
	target.workers.Wait()
	source.controller = nil
	source.control = nil
	target.control = nil
}

func TestDeletedBucketProvenanceNeedsEveryDecoder(t *testing.T) {
	s := testServer(t)
	quiesceProofFixture(s)
	remote := client.NewRequestID()
	local, _ := parseID(s.running.Node.ID)
	s.controller = &clusterController{server: s}
	defer func() { s.controller = nil }()
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Members: []client.Member{{ID: client.ID(local)}, {ID: remote}}}}})
	profile := s.protocolVersion()
	old := profile
	old.Operations = make(map[string]operationSemantics, len(profile.Operations))
	for k, v := range profile.Operations {
		old.Operations[k] = v
	}
	old.Operations[fmt.Sprintf("%04x", peerControlInfo)] = operationSemantics{Emit: 3, Min: 3, Max: 3}
	if checkPeerOperation(profile, old, client.BucketCreate) != nil || checkPeerOperation(profile, old, peerControlInfo) != nil {
		t.Fatal("ordinary old-member forwarding regressed")
	}
	s.rememberVersion(remote, old)
	if s.referenceLineageCreationAllowed() {
		t.Fatal("old catalog decoder certified")
	}
	s.opMu.Lock()
	preview, err := s.newAdminPreview()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if preview.lineageCreationProof || preview.referenceLineageCreationAllowed() {
		t.Fatal("isolated preview self-certified old members")
	}
	fresh, _ := createProtocolBucket(t, preview, "old-member-new")
	raw, err := json.Marshal(fresh.Describe())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("reference_lineage_complete")) {
		t.Fatal("mixed cluster emitted unknown JSON field")
	}
	preview.cancel()
	preview.system.Close()
	s.rememberVersion(remote, profile)
	if !s.referenceLineageCreationAllowed() {
		t.Fatal("compatible members rejected")
	}
	s.opMu.Lock()
	preview, err = s.newAdminPreview()
	s.opMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !preview.lineageCreationProof || !preview.referenceLineageCreationAllowed() {
		t.Fatal("preview lost creator admission proof")
	}
	preview.cancel()
	preview.system.Close()
}
func TestDeletedBucketLegacyProvenanceCannotBePromoted(t *testing.T) {
	s := testServer(t)
	b, err := NewBucket(volume.ID{72}, "legacy-proof")
	if err != nil {
		t.Fatal(err)
	}
	saved, err := s.buckets.Save(b.Describe(), 0)
	if err != nil {
		t.Fatal(err)
	}
	d := saved.Describe()
	d.ReferenceLineageComplete = true
	if _, err = s.buckets.Save(d, d.Revision); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("single edit certified legacy history", err)
	}
	if err = s.buckets.saveBatch([]BucketDescription{d}); !errors.Is(err, ErrBucketInvalid) {
		t.Fatal("batch edit certified legacy history", err)
	}
}

// Synthetic authority is installed only after real telemetry workers exit.
// Production installs these immutable pointers before starting serving workers.
func quiesceProofFixture(s *Server) {
	s.cancel()
	s.workers.Wait()
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func TestDeletedBucketProvenanceCannotLoseReceiptHistory(t *testing.T) {
	before := deletedProofRow(1)
	receipt := BucketReceipt{ID: client.NewRequestID().String(), Opcode: client.BucketClone, Status: 200, Digest: "link"}
	before.Receipts = append(before.Receipts, receipt)
	after := cloneBucketDescription(before)
	after.Receipts = after.Receipts[:1]
	if validateReferenceLineageEdit(before, after) == nil {
		t.Fatal("removed lifetime evidence retained trusted marker")
	}
	if validateNewCloneReferenceEvidence([]BucketDescription{before}, []BucketDescription{after}) == nil {
		t.Fatal("system install removed lifetime evidence")
	}
	after.ReferenceLineageComplete = false
	if err := validateReferenceLineageEdit(before, after); err != nil {
		t.Fatal("conservative downgrade rejected", err)
	}
	if validateNewCloneReferenceEvidence([]BucketDescription{after}, []BucketDescription{before}) == nil {
		t.Fatal("system install promoted unknown history")
	}
}

func TestDeletedBucketProofDoesNotWaitForReferenceMutation(t *testing.T) {
	f := newCombiningFixture(t)
	release, err := f.s.referenceCoordination.acquire(context.Background(), f.b.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var target client.ObjectID
	target[0] = 77
	message := referenceRequest(f.b, target, client.NewRequestID(), nil)
	message.Body[len(message.Body)-17] = 10
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	body, err := f.s.executeObjectReference(ctx, message, f.ws, f.p)
	if err != nil || !bytes.Equal(body, []byte{0}) {
		t.Fatal("readonly proof queued behind ORF1 mutation gate", body, err)
	}
}
