// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestSystemImageCopyNeedsCompleteGraphAndAllCommit(t *testing.T) {
	source := testServer(t)
	target := peerTestNode(t, source.running)
	sourceID, _ := parseID(source.running.Node.ID)
	targetID, _ := parseID(target.running.Node.ID)
	cluster, _ := parseID(source.running.ClusterID)
	old := client.Membership{Epoch: 1, Cluster: client.ID(cluster), Members: []client.Member{{ID: client.ID(sourceID), Addresses: []string{"http://source:7419"}}}}
	next := old
	next.Epoch = 2
	next.Members = append(append([]client.Member{}, old.Members...), client.Member{ID: client.ID(targetID), Addresses: []string{"http://target:7419"}})
	base := controlBase{Membership: old, Generation: 1, MapRevision: 1}
	left, err := openControlLedger(filepath.Join(t.TempDir(), "source.stqr"), cluster, sourceID, &base)
	if err != nil {
		t.Fatal(err)
	}
	defer left.journal.close()
	initial := controlBase{Membership: client.Membership{Cluster: client.ID(cluster)}}
	right, err := openControlLedger(filepath.Join(t.TempDir(), "target.stqr"), cluster, targetID, &initial)
	if err != nil {
		t.Fatal(err)
	}
	defer right.journal.close()
	draft := newTestBucket(t, "replicated-config").Describe()
	if _, err := source.buckets.Save(draft, 0); err != nil {
		t.Fatal(err)
	}
	patch := client.MapPatch{Membership: 2, Generation: 1, Revision: 1, Bucket: client.ID(newTestBucketID(t, draft.ID)), ToEnd: true, Addresses: []client.NodeAddress{{Node: client.ID(sourceID), Addresses: []string{"http://source:7419"}}}, Entries: []client.RangeEntry{{PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: client.ID(sourceID)}}}}}
	roots := map[client.ID]volume.ID{client.ID(sourceID): source.system.Identity().VolumeID, client.ID(targetID): target.system.Identity().VolumeID}
	bundle, err := source.captureSystemImage(next, 1, 1, 1, roots, []client.MapPatch{patch})
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range bundle.Objects {
		if bytes.Contains(data, []byte(source.running.Auth.BootstrapAdmin.Password)) {
			t.Fatal("plaintext password copied into system image")
		}
	}
	var oldTarget accountsImage
	if err := target.readBlob("accounts", &oldTarget); err != nil {
		t.Fatal(err)
	}
	if err := source.stageSystemBundle(bundle); err != nil {
		t.Fatal(err)
	}
	var stagedTarget accountsImage
	if err := target.readBlob("accounts", &stagedTarget); err != nil {
		t.Fatal(err)
	}
	if stagedTarget.Users[0].ID != oldTarget.Users[0].ID {
		t.Fatal("staging published accounts before COMMIT")
	}
	required := []client.ID{client.ID(sourceID), client.ID(targetID)}
	sort.Slice(required, func(i, j int) bool { return bytes.Compare(required[i][:], required[j][:]) < 0 })
	proposal := controlProposal{Claim: controlClaim{Round: client.RoundID{Membership: 1, Generation: 1, Initiator: client.ID(sourceID), Attempt: 1}, Base: base, Required: required}, Kind: controlMembership, Image: [32]byte(bundle.Root), NextMembership: &next}
	decision, err := proposal.decision()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.publishSystemImage(right, decision, bundle.Root); err == nil {
		t.Fatal("uncommitted system image published")
	}
	var ready []client.Proof
	for i, ledger := range []*controlLedger{left, right} {
		server := []*Server{source, target}[i]
		if err := ledger.reserve(proposal.Claim); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if err := target.putSystemContent(bundle.Root, bundle.Objects[bundle.Root]); err != nil {
				t.Fatal(err)
			}
			if _, err := target.prepareSystemImage(right, proposal); err == nil || right.slot.Proposal != nil {
				t.Fatal("READY issued before all system objects arrived")
			}
			if err := target.stageSystemBundle(bundle); err != nil {
				t.Fatal(err)
			}
		}
		p, err := server.prepareSystemImage(ledger, proposal)
		if err != nil {
			t.Fatal(err)
		}
		ready = append(ready, p)
	}
	certificate, _ := client.NewCertificate(ready)
	for _, ledger := range []*controlLedger{left, right} {
		if _, err := ledger.commit(decision, certificate); err != nil {
			t.Fatal(err)
		}
	}
	var original accountsImage
	if err := source.readBlob("accounts", &original); err != nil {
		t.Fatal(err)
	}
	for i, server := range []*Server{source, target} {
		ledger := []*controlLedger{left, right}[i]
		view, err := server.publishSystemImage(ledger, decision, bundle.Root)
		if err != nil {
			t.Fatal(err)
		}
		var copied accountsImage
		if err := decodeControlRecord(view.accounts, &copied); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(copied, original) {
			t.Fatal("accounts changed during copy")
		}
		if len(view.descriptions) != 2 || view.manifest.Membership.Epoch != 2 {
			t.Fatal("incomplete system image")
		}
		if _, err := ledger.markApplied(decision); err != nil {
			t.Fatal(err)
		}
		if _, err := server.publishSystemImage(ledger, decision, bundle.Root); err != nil {
			t.Fatal("publication retry", err)
		}
	}
	// A missing or corrupted child never yields a usable view, even if the root
	// itself hashes correctly. It cannot turn into an empty account/catalog copy.
	var missing volume.ObjectID
	for id := range bundle.Objects {
		if id != bundle.Root {
			missing = id
			break
		}
	}
	if _, err := decodeSystemView(bundle.Root, func(id volume.ObjectID) ([]byte, error) {
		if id == missing {
			return nil, volume.ErrNotFound
		}
		return bundle.Objects[id], nil
	}); err == nil {
		t.Fatal("missing system child accepted")
	}
}

func newTestBucketID(t *testing.T, s string) volume.ID {
	t.Helper()
	id, err := bucketID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
