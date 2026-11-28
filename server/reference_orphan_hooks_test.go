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

// Roots at the former ceiling model historical/live holders. Dynamic growth
// must preserve present, disabled and absent identities without implicit pruning.
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
	entries := make([]objectReferenceEntry, historicalReferenceRootLimit)
	for i := range entries {
		var id volume.ObjectID
		binary.LittleEndian.PutUint64(id[:8], uint64(i))
		entries[i] = objectReferenceEntry{object: id, proof: sha256.Sum256(id[:]), account: live}
	}
	for i := 0; i < orphans; i++ {
		entries[i].account = missing
	}
	entries[historicalReferenceRootLimit-3].account = missing
	entries[historicalReferenceRootLimit-3].claim = true
	entries[historicalReferenceRootLimit-2].account = missing
	entries[historicalReferenceRootLimit-2].dependency = true
	entries[historicalReferenceRootLimit-1].account = disabled
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
func TestReferenceGrowthSingleAcceptedAndLegacy(t *testing.T) {
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
				t.Fatal("reference root did not grow", err)
			}
			after, err := decodeObjectReferences(f.root(t), f.b.ID())
			if err != nil || len(after) != historicalReferenceRootLimit+1 {
				t.Fatal("failed dynamic growth", len(after), err)
			}
			verifyProtectedReferenceEntries(t, original, after, 0)
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
func TestReferenceGrowthCombinedPublishesOnce(t *testing.T) {
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
	if err != nil || len(after) != historicalReferenceRootLimit+2 {
		t.Fatal("combined root count", len(after), err)
	}
	verifyProtectedReferenceEntries(t, original, after, 0)
	if f.mark() != before+1 {
		t.Fatal("combined growth did not publish one CAS")
	}
	if f.v.IOStats().SyncOps != syncs {
		t.Fatal("dynamic growth added Sync")
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
func TestReferenceGrowthPreservesEveryHolderWithoutReclamation(t *testing.T) {
	for _, combined := range []bool{false, true} {
		t.Run(fmt.Sprint("combined=", combined), func(t *testing.T) {
			f := newCombiningFixture(t)
			original := fullReferenceRoot(t, f, 0)
			before := f.mark()
			ms := []client.Message{f.message(0, client.NewRequestID(), 1)}
			if combined {
				ms = append(ms, f.message(0, client.NewRequestID(), 2))
			}
			for _, err := range f.burst(t, ms, false, nil) {
				if err != nil {
					t.Fatal("reference growth rejected", err)
				}
			}
			after, err := decodeObjectReferences(f.root(t), f.b.ID())
			if err != nil || len(after) != len(original)+len(ms) {
				t.Fatal("wrong dynamic size", len(after), err)
			}
			verifyProtectedReferenceEntries(t, original, after, 0)
			if f.mark() != before+1 {
				t.Fatal("growth did not publish one combined CAS")
			}
		})
	}
}
func TestReferenceGrowthPastAccountAndBucketCeilings(t *testing.T) {
	f := newCombiningFixture(t)
	entries := fullReferenceRoot(t, f, 1)
	uid, _ := bucketID(f.p.User.ID)
	for i := 1; i <= historicalReferenceAccountLimit; i++ {
		entries[i].account = uid
	}
	if _, err := f.b.Put(context.Background(), f.p, volume.ObjectID(client.ObjectReferenceRootID()), encodeObjectReferences(entries, f.b.ID()), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	before := f.mark()
	for _, err := range f.burst(t, []client.Message{f.message(0, client.NewRequestID(), 1), f.message(0, client.NewRequestID(), 2)}, false, nil) {
		if err != nil {
			t.Fatal("dynamic account growth rejected", err)
		}
	}
	after, err := decodeObjectReferences(f.root(t), f.b.ID())
	if err != nil || len(after) != len(entries)+2 {
		t.Fatal("wrong reference count", len(after), err)
	}
	verifyProtectedReferenceEntries(t, entries, after, 0)
	if f.mark() != before+1 {
		t.Fatal("dynamic growth did not publish once")
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

func TestReferenceExplicitPruneRechecksRecreatedHolder(t *testing.T) {
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
			message := adminPruneMessage(f)
			message.Body = append(message.Body, 0)
			if accepted {
				message.Body[len(message.Body)-1] = 1
			}
			_, err := f.s.executeObjectReference(ctx, message, f.ws, f.p)
			if !ctx.changed.Load() {
				t.Fatal("test did not race after filtering")
			}
			if err != nil {
				t.Fatal("explicit repair failed instead of preserving recreated holder", err)
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

// Explicit administrative repair still requires stable authority even though
// ordinary reference growth no longer depends on reclaimable slots.
func TestReferenceExplicitPruneUnsupportedScopeKeepsRoot(t *testing.T) {
	policies := map[string]func(*BucketDescription){
		"replicas-two":   func(d *BucketDescription) { d.Policy.Redundancy.Mode = "replication"; d.Policy.Redundancy.Replicas = 2 },
		"desired-policy": func(d *BucketDescription) { p := d.Policy; d.DesiredPolicy = &p },
		"locked":         func(d *BucketDescription) { d.State = BucketLocked },
		"everywhere": func(d *BucketDescription) {
			d.Policy.PlacementMode = "everywhere"
			d.Policy.Redundancy.Mode = "replication"
			d.Policy.Redundancy.Replicas = 0
		},
	}
	for name, edit := range policies {
		for _, combined := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/combined=%t", name, combined), func(t *testing.T) {
				f := newCombiningFixture(t)
				fullReferenceRoot(t, f, 1)
				raw, mark := f.root(t), f.mark()
				f.s.opMu.Lock()
				f.b.mu.Lock()
				d := cloneBucketDescription(f.b.description)
				edit(&d)
				f.b.setDescription(d)
				f.b.mu.Unlock()
				f.s.opMu.Unlock()
				messages := []client.Message{adminPruneMessage(f)}
				var results []error
				if combined {
					messages = append(messages, adminPruneMessage(f))
					results = f.burst(t, messages, false, nil)
				} else {
					_, err := f.s.executeObjectReference(context.Background(), messages[0], f.ws, f.p)
					results = []error{err}
				}
				for _, err := range results {
					if objectStatus(err) != 503 {
						t.Fatal("unsupported scope admitted explicit repair", err)
					}
				}
				if !bytes.Equal(raw, f.root(t)) || mark != f.mark() {
					t.Fatal("unsupported scope changed reference root")
				}
			})
		}
	}
}

func TestReferenceExplicitPrunePolicyChangesAfterFiltering(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(fmt.Sprint("accepted=", accepted), func(t *testing.T) {
			f := newCombiningFixture(t)
			fullReferenceRoot(t, f, 1)
			raw, mark := f.root(t), f.mark()
			ctx := &referencePruneRecreateContext{Context: context.Background(), beforeAdmission: func() {
				f.s.opMu.Lock()
				defer f.s.opMu.Unlock()
				f.b.mu.Lock()
				defer f.b.mu.Unlock()
				d := cloneBucketDescription(f.b.description)
				d.Policy.Redundancy.Mode = "replication"
				d.Policy.Redundancy.Replicas = 2
				f.b.setDescription(d)
			}}
			message := adminPruneMessage(f)
			message.Body = append(message.Body, 0)
			if accepted {
				message.Body[len(message.Body)-1] = 1
			}
			_, err := f.s.executeObjectReference(ctx, message, f.ws, f.p)
			if !ctx.changed.Load() {
				t.Fatal("test did not change scope after filtering")
			}
			// A scope change may return a retry error or a conservative no-op.
			// Either outcome must preserve every original holder and generation.
			if err != nil && objectStatus(err) != 503 {
				t.Fatal("unexpected repair scope result", err)
			}
			if !bytes.Equal(raw, f.root(t)) || mark != f.mark() {
				t.Fatal("policy transition published unsafe pruning")
			}
		})
	}
}
