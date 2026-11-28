// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func adminPruneMessage(f *combiningFixture) client.Message {
	m := referenceRequest(f.b, client.ObjectID{}, client.NewRequestID(), nil)
	m.Body[len(m.Body)-17] = 9
	return m
}
func smallPruneRoot(t *testing.T, f *combiningFixture) []objectReferenceEntry {
	t.Helper()
	full := fullReferenceRoot(t, f, 2)
	entries := append(append([]objectReferenceEntry{}, full[:3]...), full[len(full)-3:]...)
	if _, err := f.b.Put(context.Background(), f.p, volume.ObjectID(client.ObjectReferenceRootID()), encodeObjectReferences(entries, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	return entries
}
func TestAdminPruneAbsentPinsSmallRootProtectsLiveClaimsAndDependencies(t *testing.T) {
	f := newCombiningFixture(t)
	before := smallPruneRoot(t, f)
	body, err := f.s.executeObjectReference(context.Background(), adminPruneMessage(f), f.ws, f.p)
	if err != nil || !bytes.Equal(body, []byte{1}) {
		t.Fatal(body, err)
	}
	after, err := decodeObjectReferences(f.root(t), f.b.ID())
	if err != nil || len(after) != len(before)-2 {
		t.Fatal(len(after), err)
	}
	verifyProtectedReferenceEntries(t, before, after, 2)
	raw, mark := f.root(t), f.mark()
	if _, err = f.s.executeObjectReference(context.Background(), adminPruneMessage(f), f.ws, f.p); err != nil || !bytes.Equal(raw, f.root(t)) || mark != f.mark() {
		t.Fatal("repeat repair changed a protected root", err)
	}
}
func TestAdminPruneRejectsNonAdminBeforeRootIO(t *testing.T) {
	f := newCombiningFixture(t)
	smallPruneRoot(t, f)
	_, p := addReferenceGuardUser(t, f)
	before := f.v.IOStats()
	_, err := f.s.executeObjectReference(context.Background(), adminPruneMessage(f), f.ws, p)
	after := f.v.IOStats()
	if !errors.Is(err, ErrBucketForbidden) || before.ReadOps != after.ReadOps || before.ViewReadOps != after.ViewReadOps || before.WriteOps != after.WriteOps {
		t.Fatal("non-admin repair reached root IO", err, before, after)
	}
}
func TestAdminPruneRechecksAccountRecreatedDuringCASAdmission(t *testing.T) {
	f := newCombiningFixture(t)
	smallPruneRoot(t, f)
	ctx := &referencePruneRecreateContext{Context: context.Background(), beforeAdmission: func() {
		f.s.opMu.Lock()
		defer f.s.opMu.Unlock()
		index, err := f.s.accountIndex()
		if err != nil {
			t.Fatal(err)
		}
		image := cloneAccountsImage(index.image)
		image.Users = append(image.Users, account{ID: (volume.ID{0x23}).String(), Name: "recreated-reference-holder", Role: "USER", PasswordHash: "unused"})
		if err := f.s.writeBlob("accounts", image); err != nil {
			t.Fatal(err)
		}
	}}
	raw, mark := f.root(t), f.mark()
	_, err := f.s.executeObjectReference(ctx, adminPruneMessage(f), f.ws, f.p)
	if err != nil || !ctx.changed.Load() || !bytes.Equal(raw, f.root(t)) || mark != f.mark() {
		t.Fatal("stale account-absence proof removed recreated holder", err)
	}
}
func TestAdminPruneRejectsUncertainOrReplicatedAuthority(t *testing.T) {
	for _, scope := range []string{"replicated", "everywhere", "joining"} {
		t.Run(scope, func(t *testing.T) {
			f := newCombiningFixture(t)
			smallPruneRoot(t, f)
			if scope == "joining" {
				f.s.mu.Lock()
				f.s.cfg.JoinPending = true
				f.s.mu.Unlock()
			} else {
				f.b.mu.Lock()
				d := cloneBucketDescription(f.b.description)
				d.Policy.Redundancy.Replicas = 2
				if scope == "everywhere" {
					d.Policy.PlacementMode = "everywhere"
				}
				f.b.setDescription(d)
				f.b.mu.Unlock()
			}
			raw, mark := f.root(t), f.mark()
			_, err := f.s.executeObjectReference(context.Background(), adminPruneMessage(f), f.ws, f.p)
			if objectStatus(err) != 503 || !bytes.Equal(raw, f.root(t)) || mark != f.mark() {
				t.Fatal("unsafe repair changed root", err)
			}
		})
	}
}

func TestAdminPruneCASRetryPreservesExternalClaim(t *testing.T) {
	f := newCombiningFixture(t)
	before := smallPruneRoot(t, f)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	external := objectReferenceEntry{object: volume.ObjectID{0xfd}, proof: [32]byte{42}, account: volume.ID{0x23}, claim: true}
	var armed, once atomic.Bool
	ctx := referenceRetryContext{Context: context.Background(), armed: &armed, update: func() {
		raw := f.root(t)
		entries, err := decodeObjectReferences(raw, f.b.ID())
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.b.CAS(context.Background(), f.p, root, 0, raw, encodeObjectReferences(append(entries, external), f.b.ID()), volume.Options{WholeObject: true, Accepted: true, RequestID: volume.ID(client.NewRequestID())})
		if err != nil {
			t.Fatal(err)
		}
	}}
	if err := f.v.SetAdmission(func(volume.StorageChange) (uint64, error) {
		if once.CompareAndSwap(false, true) {
			armed.Store(true)
			return 0, volume.ErrPrecondition
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.executeObjectReference(ctx, adminPruneMessage(f), f.ws, f.p); err != nil {
		t.Fatal(err)
	}
	after, err := decodeObjectReferences(f.root(t), f.b.ID())
	if err != nil || len(after) != len(before)-2+1 {
		t.Fatal(len(after), err)
	}
	verifyProtectedReferenceEntries(t, before, after, 2)
	found := false
	for _, entry := range after {
		if entry == external {
			found = true
		}
	}
	if !found {
		t.Fatal("concurrent external claim lost")
	}
}
func TestAdminPruneGoClientOverWebSocket(t *testing.T) {
	f := newCombiningFixture(t)
	before := smallPruneRoot(t, f)
	httpServer := httptest.NewServer(f.s.Handler())
	defer httpServer.Close()
	admin := f.s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(context.Background(), httpServer.URL, admin.Username, admin.Password, client.ID(f.b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.PruneAbsentAccountPins(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := decodeObjectReferences(f.root(t), f.b.ID())
	if err != nil {
		t.Fatal(err)
	}
	verifyProtectedReferenceEntries(t, before, after, 2)
}
