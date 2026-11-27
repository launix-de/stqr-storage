// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

func guardIndex(t *testing.T, s *Server, users []account) *accountsIndex {
	t.Helper()
	x, e := newAccountsIndex(accountsImage{Schema: 1, Users: users})
	if e != nil {
		t.Fatal(e)
	}
	s.publishAccountIndex(x)
	return x
}
func guardEntries(owner volume.ID) []objectReferenceEntry {
	es := make([]objectReferenceEntry, maxObjectReferences)
	for i := range es {
		binary.LittleEndian.PutUint64(es[i].object[:8], uint64(i+1))
		es[i].proof = sha256.Sum256(es[i].object[:])
		es[i].account = owner
	}
	return es
}
func TestReferenceAccountContinuityRejectsEqualFieldsABA(t *testing.T) {
	s := &Server{}
	u := account{ID: newID(), Name: "actor", Role: "USER", PasswordHash: "salted-identity"}
	other := account{ID: newID(), Name: "admin", Role: "ADMIN", PasswordHash: "other"}
	x := guardIndex(t, s, []account{u, other})
	p := principal{User: u, referenceAuth: x.referenceAuth[0]}
	old := p.referenceAuth
	other.Name = "unrelated-admin"
	guardIndex(t, s, []account{other, u})
	if _, stamp, e := s.referenceAccountSnapshot(p); e != nil || stamp != old {
		t.Fatal("unrelated user edit revoked actor", e)
	}
	guardIndex(t, s, []account{other})
	guardIndex(t, s, []account{u, other})
	if s.accounts.Load().referenceAuth[0] == old {
		t.Fatal("equal-fields identity reused deleted stamp")
	}
	if _, _, e := s.referenceAccountSnapshot(p); !errors.Is(e, ErrBucketForbidden) {
		t.Fatal("ABA actor accepted", e)
	}
}
func TestReferencePruneGuardPreservesProtectedOwnersAndDetectsImageABA(t *testing.T) {
	s := &Server{}
	live := volume.ID{1}
	disabled := volume.ID{2}
	gone := volume.ID{3}
	user := account{ID: live.String(), Name: "live", Role: "ADMIN", PasswordHash: "hash"}
	off := account{ID: disabled.String(), Name: "disabled", Role: "USER", Disabled: true, PasswordHash: "other"}
	guardIndex(t, s, []account{user, off})
	entries := guardEntries(live)
	entries[0].account = gone
	entries[1].account = gone
	entries[1].claim = true
	entries[2].account = gone
	entries[2].dependency = true
	entries[3].account = disabled
	original := append([]objectReferenceEntry(nil), entries...)
	marker := new(int)
	ctx := context.WithValue(context.Background(), "reference-private-work-marker", marker)
	filtered, guarded, e := s.pruneAbsentPlainReferencePins(ctx, entries)
	if e != nil || len(filtered) != len(entries)-1 {
		t.Fatal("wrong safe subset", len(filtered), e)
	}
	if guarded.Value("reference-private-work-marker") != marker {
		t.Fatal("connection-work context lost")
	}
	if !bytes.Equal(encodeObjectReferences(entries, live), encodeObjectReferences(original, live)) {
		t.Fatal("caller slice mutated")
	}
	for i := 1; i < len(original); i++ {
		if filtered[i-1] != original[i] {
			t.Fatal("protected row lost", i)
		}
	}
	_, stamp, e := s.referenceAccountSnapshot(principal{User: user})
	if e != nil {
		t.Fatal(e)
	}
	actor := principal{User: user, referenceAuth: stamp, internalCoordination: true}
	root := volume.ObjectID(client.ObjectReferenceRootID())
	s.opMu.RLock()
	e = s.validateReferenceMutation(guarded, actor, root)
	s.opMu.RUnlock()
	if e != nil {
		t.Fatal(e)
	}
	// A present disabled recreation is still present, never garbage.
	guardIndex(t, s, []account{user, off, {ID: gone.String(), Name: "recreated", Role: "USER", Disabled: true, PasswordHash: "restored"}})
	guardIndex(t, s, []account{user, off})
	s.opMu.RLock()
	e = s.validateReferenceMutation(guarded, actor, root)
	s.opMu.RUnlock()
	if !errors.Is(e, volume.ErrPrecondition) {
		t.Fatal("absent-ID image ABA not rejected", e)
	}
	s.accounts.Store(nil)
	if _, _, e = s.pruneAbsentPlainReferencePins(ctx, entries); statusOf(e) != 503 {
		t.Fatal("unknown account authority became empty", e)
	}
}
func TestReferencePruneGuardBusyControlFailsClosed(t *testing.T) {
	s := &Server{}
	live := volume.ID{1}
	gone := volume.ID{2}
	u := account{ID: live.String(), Name: "live", Role: "ADMIN", PasswordHash: "hash"}
	guardIndex(t, s, []account{u})
	entries := guardEntries(live)
	entries[0].account = gone
	_, ctx, e := s.pruneAbsentPlainReferencePins(context.Background(), entries)
	if e != nil {
		t.Fatal(e)
	}
	s.control = &controlRuntime{ledger: &controlLedger{}}
	s.control.ledger.activity.Store(&controlActivity{Busy: true})
	_, _, e = s.pruneAbsentPlainReferencePins(context.Background(), entries)
	if statusOf(e) != 503 {
		t.Fatal("busy authority reported permanent capacity", e)
	}
	_, stamp, e := s.referenceAccountSnapshot(principal{User: u})
	if e != nil {
		t.Fatal(e)
	}
	s.opMu.RLock()
	e = s.validateReferenceMutation(ctx, principal{User: u, referenceAuth: stamp, internalCoordination: true}, volume.ObjectID(client.ObjectReferenceRootID()))
	s.opMu.RUnlock()
	if !errors.Is(e, volume.ErrPrecondition) {
		t.Fatal("CAS accepted unstable absence proof", e)
	}
}

func addReferenceGuardUser(t *testing.T, f *combiningFixture) (account, principal) {
	t.Helper()
	x, _ := f.s.accountIndex()
	image := cloneAccountsImage(x.image)
	u := account{ID: newID(), Name: "guard-user", Role: "USER", PasswordHash: f.p.User.PasswordHash}
	image.Users = append(image.Users, u)
	f.s.opMu.Lock()
	e := f.s.writeBlob("accounts", image)
	f.s.opMu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	grant := client.Encoder{}
	grant.ID(client.ID(f.b.ID()))
	uid, _ := bucketID(u.ID)
	grant.ID(client.ID(uid))
	grant.U32(uint32(BucketRead))
	if _, status := f.s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.BucketGrant, ID: client.NewRequestID(), Body: grant.B}, nil, f.p); status != 200 {
		t.Fatal("grant", status)
	}
	_, stamp, e := f.s.referenceAccountSnapshot(principal{User: u})
	if e != nil {
		t.Fatal(e)
	}
	auth := &connectionAuthorization{bucket: f.b, bucketID: f.b.ID(), permissions: BucketRead, accountID: u.ID, accountRevision: frontendAccountRevision(u)}
	// Deliberately not registered: prove final identity checks independently of
	// invalidateUserConnections, including the normal WS password erasure.
	actor := principal{User: u, referenceAuth: stamp, internalCoordination: true, frontendPermission: BucketRead, connection: auth}
	actor.User.PasswordHash = ""
	return u, actor
}
func TestReferenceQueuedActorCannotPublishAfterUserDeleteOrEqualFieldsRecreate(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "deleted", true: "equal-fields-ABA"}[recreate], func(t *testing.T) {
			f := newCombiningFixture(t)
			u, actor := addReferenceGuardUser(t, f)
			raw := f.root(t)
			entries, e := decodeObjectReferences(raw, f.b.ID())
			if e != nil {
				t.Fatal(e)
			}
			uid, _ := bucketID(u.ID)
			entries = append(entries, objectReferenceEntry{object: volume.ObjectID{0xfd}, proof: [32]byte{1}, account: uid})
			next := encodeObjectReferences(entries, f.b.ID())
			key := volume.Key{Bucket: f.b.ID(), Object: volume.ObjectID(client.ObjectReferenceRootID())}
			release := f.s.replicas.lock(key)
			defer func() {
				if release != nil {
					release()
				}
			}()
			result := make(chan error, 1)
			go func() {
				_, e := f.b.CAS(context.Background(), actor, key.Object, 0, raw, next, volume.Options{Accepted: true, WholeObject: true, RequestID: volume.ID(client.NewRequestID())})
				result <- e
			}()
			deadline := time.Now().Add(3 * time.Second)
			for {
				f.s.replicas.mu.Lock()
				waiting := f.s.replicas.locks[key] != nil && f.s.replicas.locks[key].refs == 2
				f.s.replicas.mu.Unlock()
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("CAS did not reach key barrier")
				}
				runtime.Gosched()
			}
			body := client.Encoder{}
			body.ID(client.ID(uid))
			_, status, e := f.s.executeUsers(client.Message{Kind: client.Request, Opcode: client.UserDelete, ID: client.NewRequestID(), Body: body.B}, f.p)
			if e != nil || status != 200 {
				t.Fatal("delete", status, e)
			}
			if recreate {
				image := cloneAccountsImage(f.s.accounts.Load().image)
				image.Users = append(image.Users, u)
				f.s.opMu.Lock()
				e = f.s.writeBlob("accounts", image)
				f.s.opMu.Unlock()
				if e != nil {
					t.Fatal(e)
				}
			}
			if actor.connection.invalid.Load() {
				t.Fatal("fixture accidentally tested only socket invalidation")
			}
			release()
			release = nil
			select {
			case e := <-result:
				if !errors.Is(e, ErrBucketForbidden) {
					t.Fatal("old principal published after deletion", e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("queued CAS did not terminate")
			}
			if !bytes.Equal(raw, f.root(t)) {
				t.Fatal("forbidden CAS changed reference root")
			}
		})
	}
}

func TestReferenceScopedFrontendCannotMutateReservedRoot(t *testing.T) {
	f := newCombiningFixture(t)
	raw := f.root(t)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	for _, connectionOnly := range []bool{false, true} {
		p := f.p
		if connectionOnly {
			p.connection = &connectionAuthorization{frontendPermission: BucketWrite}
		} else {
			p.frontendIssuer = client.ID{1}
		}
		for _, opcode := range []uint16{0x0102, 0x0103, 0x0104, 0x0105, 0x0106, 0x0108} {
			_, err := f.b.mutate(context.Background(), p, root, opcode, 0, 1, bytes.NewReader([]byte{1}), raw, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())})
			if !errors.Is(err, ErrBucketForbidden) {
				t.Fatalf("connectionOnly=%v opcode=%x: %v", connectionOnly, opcode, err)
			}
		}
		ws := connectionTestSession(f.s)
		ws.principal = p
		ws.authorization = &connectionAuthorization{}
		e := client.Encoder{}
		e.U16(2)
		e.U16(2)
		e.U64(client.CapabilityObjectReferences | client.CapabilityAcceptedReferences)
		e.Text("frontend")
		body, status, err := f.s.execute(client.Message{Kind: client.Request, Opcode: client.Hello, Body: e.B}, ws)
		if err != nil || status != 200 {
			t.Fatal(status, err)
		}
		d := client.Decoder{B: body}
		d.U16()
		caps := d.U64()
		if caps&(client.CapabilityObjectReferences|client.CapabilityAcceptedReferences) != 0 || ws.authorization.objectReferences.Load() || ws.acceptedReferencesCap.Load() {
			t.Fatal("frontend advertised lifetime reference support")
		}
	}
	if !bytes.Equal(raw, f.root(t)) {
		t.Fatal("reserved root changed")
	}
	// Ordinary scoped frontend payload publication remains supported.
	p := f.p
	p.frontendIssuer = client.ID{1}
	p.frontendPermission = BucketWrite
	id := volume.ObjectID{0xfe}
	if _, err := f.b.Put(context.Background(), p, id, []byte("ordinary frontend"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	got, _, err := f.b.Get(context.Background(), f.p, id)
	if err != nil || string(got) != "ordinary frontend" {
		t.Fatal(string(got), err)
	}
}

func TestReferenceCookieAndSessionIssuanceRejectEqualFieldsABA(t *testing.T) {
	s := testServer(t)
	r := httptest.NewRequest("POST", "http://stqr.test/api/session", nil)
	r.SetBasicAuth(s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password)
	p, err := s.authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.createSession(w, p)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatal("missing cookie")
	}
	read := httptest.NewRequest("GET", "http://stqr.test/api/health", nil)
	read.AddCookie(cookies[0])
	if _, err = s.authenticate(read); err != nil {
		t.Fatal("positive cookie", err)
	}
	// Keep the old cookie in RAM deliberately: prove identity continuity rather
	// than relying on administrative socket/session invalidation side effects.
	s.opMu.Lock()
	guardIndex(t, s, nil)
	guardIndex(t, s, []account{p.User})
	s.opMu.Unlock()
	if _, err = s.authenticate(read); err == nil {
		t.Fatal("old cookie rebound to recreated identity")
	}
	w = httptest.NewRecorder()
	s.createSession(w, p)
	if w.Code != 403 {
		t.Fatal("stale original Basic principal issued new session", w.Code)
	}
}
