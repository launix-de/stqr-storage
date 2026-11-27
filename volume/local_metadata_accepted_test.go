// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestAcceptedLocalMetadataExplicitFenceAndCrash(t *testing.T) {
	dev, err := NewMemory(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(dev, FormatOptions{Class: HDD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.SetLocalMetadata([]byte("old authority"))
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	next := bytes.Repeat([]byte("new authority"), 1024)
	err = h.SetLocalMetadataWithOptions(next, Options{Accepted: true})
	cut, gerr := h.Generation()
	h.Close()
	if err != nil || gerr != nil {
		t.Fatal(err, gerr)
	}
	if v.IOStats().SyncOps != before.SyncOps {
		t.Fatal("accepted authority started Sync")
	}
	cold := reopen(t, v, dev)
	oldHandle, err := cold.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	old, err := oldHandle.LocalMetadata()
	oldHandle.Close()
	if err != nil || string(old) != "old authority" {
		t.Fatal("unfenced header replaced durable authority", err)
	}
	if err = v.CheckpointThrough(cut); err != nil {
		t.Fatal(err)
	}
	durable := reopen(t, v, dev)
	durableHandle, err := durable.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := durableHandle.LocalMetadata()
	durableHandle.Close()
	if err != nil || !bytes.Equal(got, next) {
		t.Fatal("explicit authority fence lost metadata", err)
	}
}
func TestAcceptedLocalMetadataProtectsGuaranteedReserve(t *testing.T) {
	dev, err := NewMemory(2 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(dev, FormatOptions{Class: HDD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.SetLocalMetadata([]byte("old"))
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	free := v.Stats().AdmissionFreePhysical
	if err = v.SetGuaranteedReserve(free); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.SetLocalMetadataWithOptions([]byte("new"), Options{Accepted: true})
	cut, gerr := h.Generation()
	h.Close()
	if err != nil || gerr != nil {
		t.Fatal("same-page authority update needed reserved quota", err, gerr)
	}
	if v.IOStats().SyncOps != before.SyncOps {
		t.Fatal("reserve pressure forced Sync")
	}
	if err = v.CheckpointThrough(cut); err != nil {
		t.Fatal(err)
	}
	if v.Stats().GuaranteedRemainingPhysical != free {
		t.Fatal("authority consumed protected guaranteed space")
	}
}

func TestAcceptedLocalMetadataOwnerPairAtEveryCrashStep(t *testing.T) {
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
	err = h.SetLocalMetadata(oldProof)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, through := range []bool{false, true} {
		for _, failedSync := range []bool{false, true} {
			completed := false
			for step := 1; step < 80; step++ {
				disk := base.crash()
				store, err := Open(disk, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				before := store.IOStats()
				if err = store.ReassignIdentityWithMetadataOptions(store.Identity(), newNode, newProof, Options{Accepted: true}); err != nil {
					t.Fatal(err)
				}
				if store.IOStats().SyncOps != before.SyncOps {
					t.Fatal("accepted owner change forced Sync")
				}
				cold, err := Open(disk.crash(), OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if cold.Identity().NodeID != oldNode {
					t.Fatal("unfenced accepted identity became canonical")
				}
				cut := store.Stats().Generation
				disk.steps = 0
				disk.fail = step
				disk.writeThrough = through
				disk.persistFailedSync = failedSync
				opErr := store.CheckpointThrough(cut)
				reboot, err := Open(disk.crash(), OpenOptions{})
				if err != nil {
					t.Fatalf("step%d reopen %v", step, err)
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
					t.Fatalf("torn accepted owner/proof step%d", step)
				}
				if opErr == nil {
					if node != newNode {
						t.Fatal("confirmed takeover lost")
					}
					completed = true
					break
				}
			}
			if !completed {
				t.Fatal("fault sweep never completed")
			}
		}
	}
}
