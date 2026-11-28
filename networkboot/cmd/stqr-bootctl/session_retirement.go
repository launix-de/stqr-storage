// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/launix-de/stqr-storage/client"
)

type sessionDependencyRetirement struct {
	Plan       client.OverlayDependencyRetirementPlan `json:"plan"`
	Digest     string                                 `json:"digest"`
	NextAnchor int                                    `json:"next_anchor"`
	Complete   bool                                   `json:"complete"`
}

func retirementDigest(plan client.OverlayDependencyRetirementPlan) (string, error) {
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func sessionRecordFields(path string, user, upper, lower client.ID) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return fields, nil
	}
	if err != nil {
		return nil, err
	}
	stat, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 {
		return nil, errors.New("session recovery record must be a private regular file")
	}
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for key, want := range map[string]string{"user": user.String(), "upper": upper.String(), "lower": lower.String()} {
		var actual string
		if err = json.Unmarshal(fields[key], &actual); err != nil || actual != want {
			return nil, fmt.Errorf("session recovery record %s does not match cleanup", key)
		}
	}
	return fields, nil
}
func loadSessionRetirement(path string, user, upper, lower client.ID) (*sessionDependencyRetirement, error) {
	if path == "" {
		return nil, nil
	}
	fields, err := sessionRecordFields(path, user, upper, lower)
	if err != nil {
		return nil, err
	}
	raw, ok := fields["dependency_retirement"]
	if !ok {
		return nil, nil
	}
	var saved sessionDependencyRetirement
	if err = json.Unmarshal(raw, &saved); err != nil {
		return nil, err
	}
	digest, err := retirementDigest(saved.Plan)
	if err != nil || saved.Plan.Validate() != nil || saved.Plan.Upper != upper || saved.Plan.Lower != lower || digest != saved.Digest || saved.NextAnchor < 0 || saved.NextAnchor > len(saved.Plan.Anchors) || saved.Complete && saved.NextAnchor != len(saved.Plan.Anchors) {
		return nil, errors.New("invalid dependency retirement journal; retained")
	}
	return &saved, nil
}

// This is an explicit administrative recovery journal. Commit it before upper
// deletion and keep it until token releases have been explicitly fenced. Atomic
// replacement and directory fsync preserve the old record across interruption.
func saveSessionRetirement(path string, user, upper, lower client.ID, saved *sessionDependencyRetirement) error {
	if path == "" {
		return errors.New("dependency retirement requires --session-record before deleting the upper")
	}
	if saved == nil || saved.Plan.Validate() != nil || saved.Plan.Upper != upper || saved.Plan.Lower != lower || saved.NextAnchor < 0 || saved.NextAnchor > len(saved.Plan.Anchors) || saved.Complete && saved.NextAnchor != len(saved.Plan.Anchors) {
		return errors.New("invalid dependency retirement plan")
	}
	fields, err := sessionRecordFields(path, user, upper, lower)
	if err != nil {
		return err
	}
	for key, value := range map[string]string{"user": user.String(), "upper": upper.String(), "lower": lower.String()} {
		fields[key], err = json.Marshal(value)
		if err != nil {
			return err
		}
	}
	if _, ok := fields["mode"]; !ok {
		fields["mode"] = json.RawMessage(`"overlay"`)
	}
	saved.Digest, err = retirementDigest(saved.Plan)
	if err != nil {
		return err
	}
	fields["dependency_retirement"], err = json.Marshal(saved)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".session-retirement-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if err = json.NewEncoder(temporary).Encode(fields); err == nil {
		err = temporary.Sync()
	}
	closed := temporary.Close()
	if err != nil {
		return err
	}
	if closed != nil {
		return closed
	}
	if err = os.Rename(temporary.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	closed = dir.Close()
	if err != nil {
		return err
	}
	return closed
}

// Each bounded chunk is fenced before its cursor is journaled. Cancellation can
// repeat the current chunk's tokens, but never skips an unconfirmed release or
// restarts an arbitrarily long cleanup from its first anchor on every attempt.
func retireSessionDependencies(ctx context.Context, path string, user, upper, lower client.ID, saved *sessionDependencyRetirement, store client.DeletedBucketDependencyStore) error {
	for saved.NextAnchor < len(saved.Plan.Anchors) {
		end := min(saved.NextAnchor+128, len(saved.Plan.Anchors))
		part := saved.Plan
		part.Anchors = saved.Plan.Anchors[saved.NextAnchor:end]
		if err := part.Retire(ctx, store); err != nil {
			return err
		}
		saved.NextAnchor = end
		if err := saveSessionRetirement(path, user, upper, lower, saved); err != nil {
			return err
		}
	}
	saved.Complete = true
	return saveSessionRetirement(path, user, upper, lower, saved)
}
