// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Full roots model historical/live holders independently of today's per-account
// admission cap. Existing present or disabled identities must remain untouched.
func fullReferenceRoot(t *testing.T, f *combiningFixture, orphans int) []objectReferenceEntry {
	t.Helper()
	index, err := f.s.accountIndex()
	if err != nil {
		t.Fatal(err)
	}
	image := cloneAccountsImage(index.image)
	live := volume.ID{0x21}
	disabled := volume.ID{0x22}
	missing := volume.ID{0x23}
	image.Users = append(image.Users, account{ID: live.String(), Name: "live-reference-holder", Role: "USER", PasswordHash: "unused"}, account{ID: disabled.String(), Name: "disabled-reference-holder", Role: "USER", Disabled: true, PasswordHash: "unused"})
	if err = f.s.writeBlob("accounts", image); err != nil {
		t.Fatal(err)
	}
	entries := make([]objectReferenceEntry, maxObjectReferences)
	for i := range entries {
		var id volume.ObjectID
		binary.LittleEndian.PutUint64(id[:8], uint64(i))
		entries[i] = objectReferenceEntry{object: id, proof: sha256.Sum256(id[:]), account: live}
	}
	for i := 0; i < orphans; i++ {
		entries[i].account = missing
	}
	entries[maxObjectReferences-3].account = missing
	entries[maxObjectReferences-3].claim = true
	entries[maxObjectReferences-2].account = missing
	entries[maxObjectReferences-2].dependency = true
	entries[maxObjectReferences-1].account = disabled
	if _, err = f.b.Put(context.Background(), f.p, volume.ObjectID(client.ObjectReferenceRootID()), encodeObjectReferences(entries, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	return entries
}
func verifyProtectedReferenceEntries(t *testing.T, before, after []objectReferenceEntry, removed int) {
	t.Helper()
	present := make(map[[32]byte]objectReferenceEntry, len(after))
	for _, e := range after {
		present[e.proof] = e
	}
	for i, e := range before {
		got, ok := present[e.proof]
		if i < removed {
			if ok {
				t.Fatal("absent plain PIN retained", i)
			}
			continue
		}
		if !ok || got != e {
			t.Fatal("protected present/disabled/claim/dependency entry changed", i)
		}
	}
}
func TestReferenceOrphanHookSingleAcceptedAndLegacy(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(fmt.Sprint("accepted=", accepted), func(t *testing.T) {
			f := newCombiningFixture(t)
			original := fullReferenceRoot(t, f, 1)
			before := f.mark()
			syncs := f.v.IOStats().SyncOps
			message := f.message(0, client.NewRequestID(), 1)
			message.Body[len(message.Body)-1] = 0
			if accepted {
				message.Body[len(message.Body)-1] = 1
			}
			body, err := f.s.executeObjectReference(context.Background(), message, f.ws, f.p)
			if err != nil || !bytes.Equal(body, []byte{1}) {
				t.Fatal("full root did not reclaim a safe slot", err)
			}
			after, err := decodeObjectReferences(f.root(t), f.b.ID())
			if err != nil || len(after) != maxObjectReferences {
				t.Fatal("changed cap or failed unchanged-count replacement", len(after), err)
			}
			verifyProtectedReferenceEntries(t, original, after, 1)
			uid, _ := bucketID(f.p.User.ID)
			count := 0
			for _, entry := range after {
				if entry.account == uid {
					count++
				}
			}
			if count != 1 {
				t.Fatal("new pin missing", count)
			}
			if accepted {
				if f.mark() != before+1 || f.v.IOStats().SyncOps != syncs {
					t.Fatal("wrong accepted publication or implicit Sync")
				}
			}
		})
	}
}
func TestReferenceOrphanHookCombinedPublishesUnchangedLength(t *testing.T) {
	f := newCombiningFixture(t)
	original := fullReferenceRoot(t, f, 2)
	before := f.mark()
	syncs := f.v.IOStats().SyncOps
	ms := []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}
	contexts := make([]context.Context, 2)
	for i, m := range ms {
		f.ws.admitConnectionMessage(m)
		contexts[i] = f.ws.connectionContext(context.Background(), m.ID)
	}
	for _, err := range f.burst(t, ms, false, contexts) {
		if err != nil {
			t.Fatal(err)
		}
	}
	after, err := decodeObjectReferences(f.root(t), f.b.ID())
	if err != nil || len(after) != maxObjectReferences {
		t.Fatal("combined root count", len(after), err)
	}
	verifyProtectedReferenceEntries(t, original, after, 2)
	if f.mark() != before+1 {
		t.Fatal("combined prune/add did not publish one CAS at unchanged length")
	}
	if f.v.IOStats().SyncOps != syncs {
		t.Fatal("pruning added Sync")
	}
	f.ws.fenceMu.Lock()
	defer f.ws.fenceMu.Unlock()
	var generation uint64
	for _, m := range ms {
		g := f.ws.connectionJobs[m.ID].touches[f.v]
		if g == 0 || generation != 0 && g != generation {
			t.Fatal("filtered CAS dropped follower generation")
		}
		generation = g
	}
}
func TestReferenceOrphanHookNoSafeCandidatesKeeps429(t *testing.T) {
	for _, combined := range []bool{false, true} {
		t.Run(fmt.Sprint("combined=", combined), func(t *testing.T) {
			f := newCombiningFixture(t)
			fullReferenceRoot(t, f, 0)
			raw := f.root(t)
			before := f.mark()
			ms := []client.Message{f.message(0, client.NewRequestID(), 1)}
			if combined {
				ms = append(ms, f.message(0, client.NewRequestID(), 2))
			}
			for _, err := range f.burst(t, ms, false, nil) {
				if objectStatus(err) != 429 {
					t.Fatal("live/disabled or claim/dependency references evicted", err)
				}
			}
			if !bytes.Equal(raw, f.root(t)) || f.mark() != before {
				t.Fatal("full live root was rewritten or cap increased")
			}
		})
	}
}
func TestReferenceOrphanHookOwnAccountCapIsNotEvaded(t *testing.T) {
	f := newCombiningFixture(t)
	entries := fullReferenceRoot(t, f, 1)
	uid, _ := bucketID(f.p.User.ID)
	for i := 1; i <= maxAccountReferences; i++ {
		entries[i].account = uid
	}
	if _, err := f.b.Put(context.Background(), f.p, volume.ObjectID(client.ObjectReferenceRootID()), encodeObjectReferences(entries, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before := f.mark()
	for _, err := range f.burst(t, []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}, false, nil) {
		if objectStatus(err) != 429 {
			t.Fatal("own account cap bypassed", err)
		}
	}
	if f.mark() != before {
		t.Fatal("failed own-quota request published unrelated cleanup")
	}
}

// A test-local context places a real administrative account publication AFTER
// filtering but BEFORE the real mutation's node admission. No production hook
// or publisher bypass is used. Omitting the derived CAS context makes this test
// evict a now-present holder and incorrectly acknowledge the new PIN.
type referencePruneRecreateContext struct {
	context.Context
	armed, changed  atomic.Bool
	beforeAdmission func()
}

func (c *referencePruneRecreateContext) Value(key any) any {
	if _, ok := key.(referencePruneKey); ok {
		c.armed.Store(true)
	}
	return c.Context.Value(key)
}
func (c *referencePruneRecreateContext) Deadline() (time.Time, bool) {
	// mutateUnmetered derives its local timeout BEFORE entering mutateAttempt
	// or taking node admission. valueCtx preserves this parent method.
	if c.armed.Load() && c.changed.CompareAndSwap(false, true) {
		c.beforeAdmission()
	}
	return c.Context.Deadline()
}

func TestReferenceOrphanHookDerivedContextRechecksRecreatedHolder(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(fmt.Sprint("accepted=", accepted), func(t *testing.T) {
			f := newCombiningFixture(t)
			fullReferenceRoot(t, f, 1)
			raw := f.root(t)
			before := f.mark()
			ctx := &referencePruneRecreateContext{Context: context.Background(), beforeAdmission: func() {
				f.s.opMu.Lock()
				defer f.s.opMu.Unlock()
				index, err := f.s.accountIndex()
				if err != nil {
					t.Fatal(err)
				}
				image := cloneAccountsImage(index.image)
				image.Users = append(image.Users, account{ID: (volume.ID{0x23}).String(), Name: "recreated-reference-holder", Role: "USER", PasswordHash: "unused"})
				if err = f.s.writeBlob("accounts", image); err != nil {
					t.Fatal(err)
				}
			}}
			message := f.message(0, client.NewRequestID(), 1)
			message.Body[len(message.Body)-1] = 0
			if accepted {
				message.Body[len(message.Body)-1] = 1
			}
			_, err := f.s.executeObjectReference(ctx, message, f.ws, f.p)
			if !ctx.changed.Load() {
				t.Fatal("test did not race after filtering")
			}
			if objectStatus(err) != 429 {
				t.Fatal("recreated holder was discarded instead of refiltering", err)
			}
			if !bytes.Equal(raw, f.root(t)) || f.mark() != before {
				t.Fatal("stale absence proof changed reference root")
			}
		})
	}
}

func TestReferenceOrphanHookFrontendScopeRejectedNativeInternalAllowed(t *testing.T) {
	f := newCombiningFixture(t)
	message := f.message(0, client.NewRequestID(), 1)
	before := f.mark()
	for _, kind := range []string{"issuer", "connection"} {
		t.Run(kind, func(t *testing.T) {
			p := f.p
			if kind == "issuer" {
				p.frontendIssuer = client.ID{1}
			} else {
				p.connection = &connectionAuthorization{bucket: f.b, bucketID: f.b.ID(), permissions: BucketAllPermissions, frontendPermission: BucketRead}
			}
			if _, err := f.s.executeObjectReference(context.Background(), message, f.ws, p); !errors.Is(err, ErrBucketForbidden) {
				t.Fatal("frontend lifetime operation admitted", err)
			}
			if _, err := f.s.validateCombinedReference(&combinedReferenceRequest{ctx: context.Background(), message: message, ws: f.ws, principal: p}); !errors.Is(err, ErrBucketForbidden) {
				t.Fatal("independent combined preparation admitted frontend", err)
			}
		})
	}
	if f.mark() != before {
		t.Fatal("rejected frontend changed root")
	}
	// Internal native coordination deliberately uses this READ override. It is
	// not a frontend-issued credential and must not be denied by the new guard.
	p := f.p
	p.frontendPermission = BucketRead
	if _, err := f.s.executeObjectReference(context.Background(), message, f.ws, p); err != nil {
		t.Fatal("native internal READ override rejected", err)
	}
}
