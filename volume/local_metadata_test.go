// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"testing"
)

func TestLocalMetadataUpgradeAndCorruption(t *testing.T) {
	disk := newCrash(1 << 20)
	v, err := Format(disk, FormatOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// A real v2 superblock has the same packed layout and zero reserved roots.
	for _, off := range []int64{0, PageSize} {
		b := make([]byte, PageSize)
		if _, err := disk.ReadAt(b, off); err != nil {
			t.Fatal(err)
		}
		diskOrder.PutUint16(b[8:], 2)
		diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
		if _, err := disk.WriteAt(b, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := disk.Sync(); err != nil {
		t.Fatal(err)
	}
	v, err = Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal("v2 open", err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.SetLocalMetadata([]byte("provenance")); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if v.Header().LocalMetadataRoot == 0 {
		t.Fatal("metadata root not published")
	}
	backend := v.dev.(*meteredIO).VolumeIO.(*crashIO)
	upgraded := backend.crash()
	if _, err := ReadHeader(upgraded); err != nil {
		t.Fatal(err)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
	// Corrupt a byte covered by the management-page checksum.
	root := v.Header().LocalMetadataRoot
	b := make([]byte, PageSize)
	if _, err := upgraded.ReadAt(b, int64(root)); err != nil {
		t.Fatal(err)
	}
	b[40] ^= 1
	if _, err := upgraded.WriteAt(b, int64(root)); err != nil {
		t.Fatal(err)
	}
	if err := upgraded.Sync(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(upgraded.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err = reopened.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.LocalMetadata(); !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupt proof accepted", err)
	}
	h.Close()
	if err := reopened.Scrub(); !errors.Is(err, ErrCorrupt) {
		t.Fatal("scrub accepted corrupt proof", err)
	}
}

func TestLocalMetadataDoesNotBecomePayloadAndSurvivesReassignment(t *testing.T) {
	mem, e := NewMemory(1 << 20)
	if e != nil {
		t.Fatal(e)
	}
	cluster, old, next := ID{1}, ID{2}, ID{3}
	v, e := Format(mem, FormatOptions{ClusterID: cluster, NodeID: old})
	if e != nil {
		t.Fatal(e)
	}
	key := Key{Bucket: ID{4}, Object: ObjectID{5}}
	request := ID{6}
	h, e := v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	info, e := h.Put(key, []byte("unchanged"), Options{RequestID: request})
	if e != nil {
		t.Fatal(e)
	}
	h.Close()
	before := v.Stats()
	metadata := bytes.Repeat([]byte("authority"), 1000)
	h, e = v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.SetLocalMetadata(metadata); e != nil {
		t.Fatal(e)
	}
	records, e := h.ScanObjects(nil, 100)
	if e != nil || len(records) != 1 {
		t.Fatal("private metadata leaked into objects", len(records), e)
	}
	h.Close()
	after := v.Stats()
	if after.Objects != before.Objects || after.LogicalBytes != before.LogicalBytes || after.DataPhysical != before.DataPhysical || after.MetadataPhysical <= before.MetadataPhysical {
		t.Fatal("incorrect metadata accounting")
	}
	identity := v.Identity()
	if e = v.ReassignIdentity(identity, next); e != nil {
		t.Fatal(e)
	}
	if e = v.ReassignIdentity(identity, old); !errors.Is(e, ErrPrecondition) {
		t.Fatal("stale reassignment", e)
	}
	if e = v.Scrub(); e != nil {
		t.Fatal(e)
	}
	var snapshot bytes.Buffer
	if e = v.Export(&snapshot); e != nil {
		t.Fatal(e)
	}
	targetMem, e := NewMemory(1 << 20)
	if e != nil {
		t.Fatal(e)
	}
	target, e := Format(targetMem, FormatOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer target.Close()
	if e = target.Import(bytes.NewReader(snapshot.Bytes()), nil); e != nil {
		t.Fatal(e)
	}
	for _, store := range []*Volume{v, target} {
		h, e = store.Open(false)
		if e != nil {
			t.Fatal(e)
		}
		got, e := h.LocalMetadata()
		if e != nil || !bytes.Equal(got, metadata) {
			t.Fatal("metadata lost", e)
		}
		data, current, e := h.Get(key)
		if e != nil || string(data) != "unchanged" || current.Version != info.Version {
			t.Fatal("data changed", e)
		}
		_, found, e := h.OperationStatus(request)
		if e != nil || !found {
			t.Fatal("retry record lost", e)
		}
		if e = h.SetLocalMetadata(nil); !errors.Is(e, ErrReadOnly) {
			t.Fatal("read handle changed metadata", e)
		}
		h.Close()
	}
	h, e = v.Open(true)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.SetLocalMetadata([]byte("small")); e != nil {
		t.Fatal(e)
	}
	if e = h.SetLocalMetadata(nil); e != nil {
		t.Fatal(e)
	}
	h.Close()
	cleared := v.Stats()
	if cleared.MetadataPhysical != before.MetadataPhysical || cleared.DataPhysical != before.DataPhysical {
		t.Fatal("metadata chain was not reclaimed", cleared.MetadataPhysical, before.MetadataPhysical)
	}
	if e = v.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestAtomicOwnerAndMetadataAtEveryCrashStep(t *testing.T) {
	oldNode, newNode := ID{2}, ID{3}
	oldProof, newProof := bytes.Repeat([]byte("old"), 2000), bytes.Repeat([]byte("new"), 2500)
	base := newCrash(1 << 20)
	v, err := Format(base, FormatOptions{ClusterID: ID{1}, NodeID: oldNode})
	if err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.SetLocalMetadata(oldProof); err != nil {
		t.Fatal(err)
	}
	h.Close()
	for _, through := range []bool{false, true} {
		for _, failedSync := range []bool{false, true} {
			completed := false
			for step := 1; step < 80; step++ {
				disk := base.crash()
				store, err := Open(disk, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				disk.steps = 0
				disk.fail = step
				disk.writeThrough = through
				disk.persistFailedSync = failedSync
				opErr := store.ReassignIdentityWithMetadata(store.Identity(), newNode, newProof)
				reboot, err := Open(disk.crash(), OpenOptions{})
				if err != nil {
					t.Fatalf("step %d reopen: %v", step, err)
				}
				h, err := reboot.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				proof, err := h.LocalMetadata()
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
				node := reboot.Identity().NodeID
				if node == oldNode && !bytes.Equal(proof, oldProof) || node == newNode && !bytes.Equal(proof, newProof) || node != oldNode && node != newNode {
					t.Fatalf("torn owner/provenance pair at step %d", step)
				}
				if opErr == nil {
					if node != newNode {
						t.Fatal("acknowledged takeover was lost")
					}
					completed = true
					break
				}
			}
			if !completed {
				t.Fatal("crash sweep did not reach successful completion")
			}
		}
	}
}

// A handoff must be able to fence a small part of a large authority record
// without borrowing other buckets' reserves for redo of unchanged pages.
func TestLocalMetadataSmallUpdateWithNoUnreservedSpace(t *testing.T) {
	disk := newCrash(2 << 20)
	v, err := Format(disk, FormatOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Repeat([]byte("a"), 60<<10)
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.SetLocalMetadata(original); err != nil {
		t.Fatal(err)
	}
	h.Close()
	free := v.Stats().AdmissionFreePhysical
	if err = v.SetGuaranteedReserve(free); err != nil {
		t.Fatal(err)
	}
	updated := bytes.Clone(original)
	updated[len(updated)-64] = 'b'
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.SetLocalMetadata(updated)
	h.Close()
	if err != nil {
		t.Fatal("small authority update needs additional journal space", err)
	}
	if v.Stats().GuaranteedRemainingPhysical != free {
		t.Fatal("metadata update consumed protected quota")
	}
	reopened, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	h, err = reopened.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.LocalMetadata()
	h.Close()
	if err != nil || !bytes.Equal(got, updated) {
		t.Fatal("metadata update lost after restart", err)
	}
	if err = reopened.Scrub(); err != nil {
		t.Fatal(err)
	}
}
