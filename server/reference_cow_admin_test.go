// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func referenceAdminClone(t *testing.T, s *Server, source, target *Bucket) error {
	t.Helper()
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	_, err := s.executeBucketCopy(context.Background(), client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s))
	return err
}
func referenceAdminPut(t *testing.T, b *Bucket, p principal, id volume.ObjectID, data []byte) {
	t.Helper()
	if _, err := b.Put(context.Background(), p, id, data, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
}
func referenceAdminRoot(owner volume.ID) []byte {
	return encodeObjectReferences([]objectReferenceEntry{{object: volume.ObjectID{42}, proof: [32]byte{43}, account: volume.ID{44}}}, owner)
}
func TestCloneLogicalQuotaExcludesOnlyItsSourceLocalReferenceRoot(t *testing.T) {
	for _, kind := range []string{"live", "legacy-foreign", "deleted", "absent", "ancestor-only"} {
		t.Run(kind, func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "reference-quota", "hdd", 8<<20)
			source := createQuotaBucket(t, s, "reference-source", "logical", 0, 0)
			exact := createQuotaBucket(t, s, "reference-exact", "logical", 0, 8)
			small := createQuotaBucket(t, s, "reference-small", "logical", 0, 7)
			ctx := context.Background()
			p := userPrincipal(t, s)
			root := volume.ObjectID(client.ObjectReferenceRootID())
			payload := volume.ObjectID{17}
			referenceAdminPut(t, source, p, payload, []byte("original"))
			var raw []byte
			switch kind {
			case "live", "deleted":
				raw = referenceAdminRoot(source.ID())
			case "legacy-foreign":
				raw = referenceAdminRoot(volume.ID{99})
			case "ancestor-only":
				ancestor := source
				raw = referenceAdminRoot(ancestor.ID())
				referenceAdminPut(t, ancestor, p, root, raw)
				source = createQuotaBucket(t, s, "reference-intermediate", "logical", 0, 0)
				if err := referenceAdminClone(t, s, ancestor, source); err != nil {
					t.Fatal(err)
				}
				raw = nil
			}
			if raw != nil {
				referenceAdminPut(t, source, p, root, raw)
			}
			if kind == "deleted" {
				if err := source.Delete(ctx, p, root, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := source.logicalUsage()
			if err != nil {
				t.Fatal(err)
			}
			if err := referenceAdminClone(t, s, source, small); !errors.Is(err, volume.ErrFull) && statusOf(err) != 507 {
				t.Fatal("undersized clone admitted", err)
			}
			if len(small.Describe().ClonePeers) != 0 {
				t.Fatal("rejected clone published edges")
			}
			if err := referenceAdminClone(t, s, source, exact); err != nil {
				t.Fatal("exact payload quota rejected", err)
			}
			if used, err := exact.logicalUsage(); err != nil || used != 8 {
				t.Fatal("nontransferable root charged to clone", used, err)
			}
			if _, _, err := exact.Get(ctx, p, root); !errors.Is(err, volume.ErrNotFound) {
				t.Fatal("clone acquired source holders", err)
			}
			if after, err := source.logicalUsage(); err != nil || after != before {
				t.Fatal("source accounting changed", before, after, err)
			}
			if raw != nil && kind != "deleted" {
				got, _, err := source.Get(ctx, p, root)
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatal("source holders changed", err)
				}
			}
		})
	}
}
func TestReferenceOnlyDetachCreatesNoRecordsOrDataFences(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "reference-detach", "hdd", 8<<20)
	source, _ := createProtocolBucket(t, s, "reference-source")
	target, _ := createProtocolBucket(t, s, "reference-target")
	ctx := context.Background()
	p := userPrincipal(t, s)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	raw := referenceAdminRoot(source.ID())
	referenceAdminPut(t, source, p, root, raw)
	if err := referenceAdminClone(t, s, source, target); err != nil {
		t.Fatal(err)
	}
	before := map[*volume.Volume]volume.IOStats{}
	for _, v := range source.allVolumes() {
		before[v] = v.IOStats()
	}
	done, err := s.cowDetachStep(ctx, target.ID())
	if err != nil || !done {
		t.Fatal("reference-only detach failed", done, err)
	}
	if len(source.Describe().ClonePeers) != 0 || len(target.Describe().ClonePeers) != 0 {
		t.Fatal("detach did not remove edges")
	}
	for v, old := range before {
		now := v.IOStats()
		if now.SyncOps != old.SyncOps || now.WriteBytes != old.WriteBytes {
			t.Fatal("detach wrote/fenced a nontransferable record", old, now)
		}
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		info, err := h.RecordInfo(volume.Key{Bucket: target.ID(), Object: root})
		h.Close()
		if !errors.Is(err, volume.ErrNotFound) {
			t.Fatal("phantom root or tombstone materialized", info, err)
		}
	}
	actual, _, err := source.Get(ctx, p, root)
	if err != nil || !bytes.Equal(actual, raw) {
		t.Fatal("source root changed", err)
	}
}
func TestDetachReferenceRootAtPageBoundaryAdvancesWithoutCopy(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "reference-pages", "hdd", 16<<20)
	source, _ := createProtocolBucket(t, s, "reference-page-source")
	target, _ := createProtocolBucket(t, s, "reference-page-target")
	ctx := context.Background()
	p := userPrincipal(t, s)
	root := volume.ObjectID(client.ObjectReferenceRootID())
	for i := byte(1); i <= 31; i++ {
		id := volume.ObjectID{}
		id[31] = i
		if bytes.Compare(id[:], root[:]) >= 0 {
			t.Fatal("fixture requires lower IDs")
		}
		referenceAdminPut(t, source, p, id, []byte{i})
	}
	tail := volume.ObjectID{255}
	if bytes.Compare(tail[:], root[:]) <= 0 {
		t.Fatal("fixture requires upper ID")
	}
	referenceAdminPut(t, source, p, tail, []byte("tail"))
	referenceAdminPut(t, source, p, root, referenceAdminRoot(source.ID()))
	if err := referenceAdminClone(t, s, source, target); err != nil {
		t.Fatal(err)
	}
	done, err := s.cowDetachStep(ctx, target.ID())
	if err != nil || done {
		t.Fatal("detach should yield after first page", done, err)
	}
	s.cow.mu.Lock()
	progress := s.cow.detaches[target.ID()]
	s.cow.mu.Unlock()
	if progress.after == nil || *progress.after != root {
		t.Fatal("reserved last candidate did not advance cursor", progress.after)
	}
	done, err = s.cowDetachStep(ctx, target.ID())
	if err != nil || !done {
		t.Fatal("following page did not finish", done, err)
	}
	data, _, err := target.Get(ctx, p, tail)
	if err != nil || string(data) != "tail" {
		t.Fatal("later payload was skipped", string(data), err)
	}
	for _, v := range target.allVolumes() {
		h, err := v.Open(false)
		if err != nil {
			t.Fatal(err)
		}
		info, err := h.RecordInfo(volume.Key{Bucket: target.ID(), Object: root})
		h.Close()
		if !errors.Is(err, volume.ErrNotFound) {
			t.Fatal("page-boundary root materialized", info, err)
		}
	}
}
func TestDetachPreservesExistingValidAndLegacyPhysicalReferenceRoots(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "legacy-foreign"}[foreign], func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "reference-legacy", "hdd", 8<<20)
			source, _ := createProtocolBucket(t, s, "reference-legacy-source")
			target, _ := createProtocolBucket(t, s, "reference-legacy-target")
			ctx := context.Background()
			p := userPrincipal(t, s)
			root := volume.ObjectID(client.ObjectReferenceRootID())
			sourceRaw := referenceAdminRoot(source.ID())
			referenceAdminPut(t, source, p, root, sourceRaw)
			if err := referenceAdminClone(t, s, source, target); err != nil {
				t.Fatal(err)
			}
			owner := target.ID()
			if foreign {
				owner = source.ID()
			}
			targetRaw := referenceAdminRoot(owner)
			referenceAdminPut(t, target, p, root, targetRaw)
			done, err := s.cowDetachStep(ctx, target.ID())
			if err != nil || !done {
				t.Fatal(done, err)
			}
			for b, expected := range map[*Bucket][]byte{source: sourceRaw, target: targetRaw} {
				raw, _, err := b.Get(ctx, p, root)
				if err != nil || !bytes.Equal(raw, expected) {
					t.Fatal("detach rewrote physical holders", b.ID(), err)
				}
			}
			entries, err := decodeObjectReferences(targetRaw, target.ID())
			if err != nil || foreign && len(entries) != 0 || !foreign && len(entries) != 1 {
				t.Fatal("legacy owner semantics changed", entries, err)
			}
		})
	}
}
