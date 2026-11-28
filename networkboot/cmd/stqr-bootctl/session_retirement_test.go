// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func retirementFixture() (client.ID, client.ID, client.ID, *sessionDependencyRetirement) {
	user, upper, lower := client.ID{1}, client.ID{2}, client.ID{3}
	saved := &sessionDependencyRetirement{Plan: client.OverlayDependencyRetirementPlan{Version: 1, Upper: upper, Lower: lower, Anchors: []client.FilesystemDependencyAnchor{{Owner: client.ObjectID{4}, Target: client.ObjectID{5}, Token: client.ID{6}}}}}
	return user, upper, lower, saved
}
func TestSessionRetirementJournalSurvivesRetryAndPreservesLegacyFields(t *testing.T) {
	user, upper, lower, saved := retirementFixture()
	path := filepath.Join(t.TempDir(), "session.json")
	legacy := map[string]any{"user": user.String(), "upper": upper.String(), "lower": lower.String(), "mode": "overlay", "future_field": map[string]string{"preserved": "yes"}}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := loadSessionRetirement(path, user, upper, lower); err != nil || got != nil {
		t.Fatal(got, err)
	}
	if err := saveSessionRetirement(path, user, upper, lower, saved); err != nil {
		t.Fatal(err)
	}
	restored, err := loadSessionRetirement(path, user, upper, lower)
	if err != nil || restored.Complete || len(restored.Plan.Anchors) != 1 || restored.Plan.Anchors[0].Token != saved.Plan.Anchors[0].Token {
		t.Fatal(restored, err)
	}
	// This is the recovery point after a lost deletion or UNPIN response: same
	// exact token plan remains durable, never inferred from missing bucket data.
	restored.NextAnchor = len(restored.Plan.Anchors)
	restored.Complete = true
	if err = saveSessionRetirement(path, user, upper, lower, restored); err != nil {
		t.Fatal(err)
	}
	restored, err = loadSessionRetirement(path, user, upper, lower)
	if err != nil || !restored.Complete {
		t.Fatal(restored, err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["future_field"]) != `{"preserved":"yes"}` {
		t.Fatal("legacy fields lost", fields)
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal(stat, err)
	}
}
func TestSessionRetirementJournalRejectsForeignTamperedOrPublicRecord(t *testing.T) {
	user, upper, lower, saved := retirementFixture()
	path := filepath.Join(t.TempDir(), "session.json")
	if err := saveSessionRetirement(path, user, upper, lower, saved); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSessionRetirement(path, client.ID{9}, upper, lower); err == nil {
		t.Fatal("foreign user accepted")
	}
	if err := saveSessionRetirement(path, client.ID{9}, upper, lower, saved); err == nil {
		t.Fatal("foreign record overwritten")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(raw, &fields)
	var altered sessionDependencyRetirement
	json.Unmarshal(fields["dependency_retirement"], &altered)
	altered.Plan.Anchors[0].Token = client.ID{99}
	fields["dependency_retirement"], _ = json.Marshal(altered)
	raw, _ = json.Marshal(fields)
	os.WriteFile(path, raw, 0600)
	if _, err = loadSessionRetirement(path, user, upper, lower); err == nil {
		t.Fatal("tampered plan accepted")
	}
	os.Chmod(path, 0644)
	if _, err = loadSessionRetirement(path, user, upper, lower); err == nil {
		t.Fatal("public record accepted")
	}
	if err = saveSessionRetirement("", user, upper, lower, saved); err == nil {
		t.Fatal("nonjournaled plan accepted")
	}
}
func TestSessionRetirementMissingRecordDoesNotInventPlan(t *testing.T) {
	user, upper, lower, _ := retirementFixture()
	path := filepath.Join(t.TempDir(), "missing.json")
	got, err := loadSessionRetirement(path, user, upper, lower)
	if err != nil || got != nil {
		t.Fatal(got, err)
	}
}

type chunkRetirementStore struct {
	lower                   client.ID
	syncs, proofs, failSync int
	tokens                  map[client.ID]int
}

func (s *chunkRetirementStore) BucketID() client.BucketID { return s.lower }
func (s *chunkRetirementStore) CanRetireDeletedBucketDependencies(context.Context, client.BucketID) (bool, error) {
	s.proofs++
	return true, nil
}
func (s *chunkRetirementStore) UnpinObjectDependency(_ context.Context, _ client.ObjectID, token client.ID) error {
	s.tokens[token]++
	return nil
}
func (s *chunkRetirementStore) Sync(context.Context) error {
	s.syncs++
	if s.syncs == s.failSync {
		return errors.New("lost fence reply")
	}
	return nil
}
func TestSessionRetirementResumesOnlyAfterDurableChunk(t *testing.T) {
	user, upper, lower, saved := retirementFixture()
	saved.Plan.Anchors = nil
	for i := 0; i < 260; i++ {
		var owner client.ObjectID
		owner[0] = byte(i >> 8)
		owner[1] = byte(i)
		var token client.ID
		token[0] = byte(i >> 8)
		token[1] = byte(i)
		token[2] = 7
		saved.Plan.Anchors = append(saved.Plan.Anchors, client.FilesystemDependencyAnchor{Owner: owner, Target: client.ObjectID{8}, Token: token})
		saved.Plan.Anchors[i].Owner[2] = 9
	}
	path := filepath.Join(t.TempDir(), "session.json")
	if err := saveSessionRetirement(path, user, upper, lower, saved); err != nil {
		t.Fatal(err)
	}
	store := &chunkRetirementStore{lower: lower, failSync: 2, tokens: map[client.ID]int{}}
	if err := retireSessionDependencies(context.Background(), path, user, upper, lower, saved, store); err == nil {
		t.Fatal("lost fence reported complete")
	}
	restored, err := loadSessionRetirement(path, user, upper, lower)
	if err != nil || restored.NextAnchor != 128 || restored.Complete {
		t.Fatal(restored, err)
	}
	if err = retireSessionDependencies(context.Background(), path, user, upper, lower, restored, store); err != nil {
		t.Fatal(err)
	}
	final, err := loadSessionRetirement(path, user, upper, lower)
	if err != nil || !final.Complete || final.NextAnchor != 260 {
		t.Fatal(final, err)
	}
	for i, a := range saved.Plan.Anchors {
		want := 1
		if i >= 128 && i < 256 {
			want = 2
		}
		if store.tokens[a.Token] != want {
			t.Fatal("incorrect retry cut", i, store.tokens[a.Token])
		}
	}
	if store.syncs != 4 {
		t.Fatal(store.syncs)
	}
}

// Prepare an actual persisted STLA dependency through the public filesystem API.
func imageSessionWithDependency(t *testing.T) (context.Context, config, bootSession, string, *client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	cfg, _, _ := imageTestNode(t)
	if err := publish(ctx, cfg, "ubuntu", imageTestArchive(t)); err != nil {
		t.Fatal(err)
	}
	lowerID, err := resolve(ctx, cfg, "ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	lower, err := client.Dial(ctx, cfg.URI, cfg.Username, cfg.Password, lowerID, "fs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lower.Close() })
	fs, err := client.OpenFilesystem(ctx, lower, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := fs.Lookup(ctx, client.FilesystemRootID(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fs.WriteAt(ctx, entry.ID, make([]byte, (1<<20)+1), 0); err != nil {
		t.Fatal(err)
	}
	if err = lower.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(t.TempDir(), "session.json")
	session, err := newBootSession(ctx, cfg, "ubuntu", 8<<20, record, "ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	upper, err := client.Dial(ctx, cfg.URI, cfg.Username, cfg.Password, session.Upper, "fs")
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := client.OpenOverlayFilesystem(ctx, lower, upper, client.OverlayOptions{LowerBucketID: lowerID})
	if err != nil {
		upper.Close()
		t.Fatal(err)
	}
	mode := uint32(0600)
	_, err = overlay.SetAttributes(ctx, entry.ID, client.FSAttributes{Mode: &mode})
	if err == nil {
		err = upper.Sync(ctx)
	}
	upper.Close()
	if err != nil {
		t.Fatal(err)
	}
	dependencies, err := lower.ReadObjectDependencies(ctx)
	if err != nil || len(dependencies) == 0 {
		t.Fatal("missing actual lower dependency", dependencies, err)
	}
	return ctx, cfg, session, record, lower
}
func retryImageSessionRetirement(t *testing.T, ctx context.Context, cfg config, s bootSession, record string) {
	t.Helper()
	for {
		err := removeImageSession(ctx, cfg, s.User, s.Upper, s.Lower, false, record)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("retirement did not complete", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestBootCleanupRetiresJournaledDependenciesAfterDeletedUpperProof(t *testing.T) {
	ctx, cfg, session, record, lower := imageSessionWithDependency(t)
	retryImageSessionRetirement(t, ctx, cfg, session, record)
	saved, err := loadSessionRetirement(record, session.User, session.Upper, session.Lower)
	if err != nil || !saved.Complete || saved.NextAnchor != len(saved.Plan.Anchors) || len(saved.Plan.Anchors) == 0 {
		t.Fatal(saved, err)
	}
	dependencies, err := lower.ReadObjectDependencies(ctx)
	if err != nil || len(dependencies) != 0 {
		t.Fatal("retired upper still pins lower", dependencies, err)
	}
	if err = removeImageSession(ctx, cfg, session.User, session.Upper, session.Lower, false, record); err != nil {
		t.Fatal("lost-result replay", err)
	}
}
func TestBootCleanupRetainsJournalAcrossCloneBetweenCaptureAndDelete(t *testing.T) {
	ctx, cfg, session, record, lower := imageSessionWithDependency(t)
	upper, err := client.Dial(ctx, cfg.URI, cfg.Username, cfg.Password, session.Upper, "fs")
	if err != nil {
		t.Fatal(err)
	}
	if err = upper.BeginMaintenance(ctx); err != nil {
		upper.Close()
		t.Fatal(err)
	}
	plan, err := client.ReadOverlayDependencyRetirementPlan(ctx, upper, session.Upper, session.Lower)
	if err == nil {
		err = saveSessionRetirement(record, session.User, session.Upper, session.Lower, &sessionDependencyRetirement{Plan: plan})
	}
	upper.Close()
	if err != nil {
		t.Fatal(err)
	}
	cloneID, err := createID(ctx, cfg, "preserved-overlay-clone", client.NewRequestID())
	if err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(session.Upper)
	e.ID(cloneID)
	if _, err = control(ctx, cfg, client.BucketClone, e.B); err != nil {
		t.Fatal(err)
	}
	// This exact race invalidates any pre-delete proof: the new descendant has
	// inherited the journaled tokens even though maintenance was valid earlier.
	if err = removeImageSession(ctx, cfg, session.User, session.Upper, session.Lower, false, record); err == nil {
		t.Fatal("retirement ignored surviving clone")
	}
	saved, err := loadSessionRetirement(record, session.User, session.Upper, session.Lower)
	if err != nil || saved.Complete || saved.NextAnchor != 0 {
		t.Fatal(saved, err)
	}
	dependencies, err := lower.ReadObjectDependencies(ctx)
	if err != nil || len(dependencies) == 0 {
		t.Fatal("live clone lost dependencies", dependencies, err)
	}
	if err = byID(ctx, cfg, client.BucketDelete, cloneID); err != nil {
		t.Fatal(err)
	}
	retryImageSessionRetirement(t, ctx, cfg, session, record)
	dependencies, err = lower.ReadObjectDependencies(ctx)
	if err != nil || len(dependencies) != 0 {
		t.Fatal("deleted clone lineage retained dependencies", dependencies, err)
	}
}
