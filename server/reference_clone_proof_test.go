// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestTicket32CloneCannotInvalidateExclusiveMaintenance(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "clone-maintenance", "hdd", 8<<20)
	a, _ := createProtocolBucket(t, s, "clone-maintenance-source")
	b, _ := createProtocolBucket(t, s, "clone-maintenance-destination")
	for _, held := range []*Bucket{a, b} {
		held.mu.Lock()
		held.maintenance = &connectionAuthorization{}
		held.mu.Unlock()
		e := client.Encoder{}
		e.ID(client.ID(a.ID()))
		e.ID(client.ID(b.ID()))
		_, err := s.executeBucketCopy(context.Background(), client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s))
		var status *apiError
		if !errors.As(err, &status) || status.status != 409 {
			t.Fatalf("clone during maintenance: %v", err)
		}
		if len(a.Describe().ClonePeers) != 0 || len(b.Describe().ClonePeers) != 0 {
			t.Fatal("rejected clone changed dependencies")
		}
		held.mu.Lock()
		held.maintenance = nil
		held.mu.Unlock()
	}
}

func TestTicket32DependencyProofSurvivesCloneDetach(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "reference-clones", "hdd", 16<<20)
	a, _ := createProtocolBucket(t, s, "ref-original")
	b, _ := createProtocolBucket(t, s, "ref-copy")
	c, _ := createProtocolBucket(t, s, "ref-next-copy")
	ctx := context.Background()
	actor := userPrincipal(t, s)
	if !s.canReclaimDependencyAnchors(a.ID()) {
		t.Fatal("fresh namespace should be sole owner")
	}
	clone := func(from, to *Bucket) {
		e := client.Encoder{}
		e.ID(client.ID(from.ID()))
		e.ID(client.ID(to.ID()))
		if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
			t.Fatal(err)
		}
	}
	detach := func(b *Bucket) {
		for i := 0; i < 10; i++ {
			done, err := s.cowDetachStep(ctx, b.ID())
			if err != nil {
				t.Fatal(err)
			}
			if done {
				return
			}
		}
		t.Fatal("detach did not complete")
	}
	clone(a, b)
	detach(b)
	if len(a.Describe().ClonePeers) != 0 || len(b.Describe().ClonePeers) != 0 {
		t.Fatal("fixture still linked")
	}
	if s.canReclaimDependencyAnchors(a.ID()) || s.canReclaimDependencyAnchors(b.ID()) {
		t.Fatal("detached clone lost shared external-reference lifetime")
	}
	clone(b, c)
	detach(c)
	// A deleted intermediate namespace must still connect a surviving clone
	// to the original owner through their existing administrative clone receipts.
	d := b.Describe()
	d.State = BucketDeleting
	d.DeletionComplete = true
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if s.canReclaimDependencyAnchors(a.ID()) {
		t.Fatal("transitive detached clone was missed")
	}
	d = c.Describe()
	d.State = BucketDeleting
	d.DeletionComplete = true
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	if !s.canReclaimDependencyAnchors(a.ID()) {
		t.Fatal("last clone ended but sole owner remains blocked")
	}
}

func TestTicket32NewCloneRequiresBothEndpointReceipts(t *testing.T) {
	a, b := volume.ID{1}.String(), volume.ID{2}.String()
	link := client.NewRequestID().String()
	before := []BucketDescription{{ID: a}, {ID: b}}
	after := []BucketDescription{{ID: a, ClonePeers: []BucketClonePeer{{Bucket: b, Link: link}}}, {ID: b, ClonePeers: []BucketClonePeer{{Bucket: a, Link: link}}, Receipts: []BucketReceipt{{ID: link, Opcode: client.BucketClone, Status: 200}}}}
	if err := validateNewCloneReferenceEvidence(before, after); err == nil {
		t.Fatal("legacy unproven clone accepted")
	}
	after[0].Receipts = append(after[0].Receipts, after[1].Receipts[0])
	if err := validateNewCloneReferenceEvidence(before, after); err != nil {
		t.Fatal(err)
	}
	// Existing pre-feature COW data remains readable/installable unchanged.
	if err := validateNewCloneReferenceEvidence(after, after); err != nil {
		t.Fatal(err)
	}
}
