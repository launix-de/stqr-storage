// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestAcceptedJournalDoesNotConsumeReclaimedPayloadHole(t *testing.T) {
	for _, removed := range []uint64{1, 2} {
		t.Run(string(rune('0'+removed)), func(t *testing.T) {
			disk, err := NewMemory(4 << 20)
			if err != nil {
				t.Fatal(err)
			}
			v, err := Format(disk, FormatOptions{Class: HDD})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { v.Close() }()
			// A complete S3 object is a 1-MiB body plus its 88-byte key/header.
			payload := bytes.Repeat([]byte{0x5c}, (1<<20)+88)
			for i := uint64(1); i <= 3; i++ {
				if _, err = testPut(v, testKey(i), payload, Options{Accepted: true}); err != nil {
					t.Fatal("setup PUT", i, err)
				}
				if err = v.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			h, err := v.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			err = h.Delete(testKey(removed), Options{})
			if err == nil {
				err = h.Purge(testKey(removed))
			}
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err = v.Sync(); err != nil {
				t.Fatal(err)
			}
			before := v.Stats()
			if before.LogicalBytes != 2*uint64(len(payload)) || before.AdmissionFreePhysical < uint64(len(payload))+deferredJournalBytes {
				t.Fatal("fixture does not retain sufficient free capacity", before)
			}
			probe := v.transaction()
			_, _, _, err = probe.findSpace(uint64(len(payload)), PageSize, false, dataStart)
			probe.close()
			if err != nil {
				t.Fatal("positive control: reclaimed contiguous payload hole does not fit", err)
			}
			syncs := v.IOStats().SyncOps
			if _, err = testPut(v, testKey(4), payload, Options{Accepted: true}); err != nil {
				t.Fatal("journal fragmented a reclaimed payload hole", err)
			}
			mustGet(t, v, testKey(4), payload)
			if v.IOStats().SyncOps != syncs {
				t.Fatal("accepted replacement implicitly synchronized")
			}
			if err = v.Sync(); err != nil {
				t.Fatal(err)
			}
			v = reopen(t, v, disk)
			mustGet(t, v, testKey(4), payload)
			for i := uint64(1); i <= 3; i++ {
				if i != removed {
					mustGet(t, v, testKey(i), payload)
				}
			}
			assertFreeTree(t, v)
			if err = v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAcceptedJournalAvoidsScratchOwnedByCheckpoint(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	scratch := allocation{4 * PageSize, scratchPages * PageSize, false}
	// The prior in-flight checkpoint owns scratch until its I/O completes.
	v.fence = &writeFence{reserved: []allocation{scratch}}
	if err := v.beginDeferred(); err != nil {
		t.Fatal(err)
	}
	if overlap(v.deferred.journal, scratch) {
		t.Fatal("later group reused checkpoint scratch")
	}
	v.fence = nil
	if _, err := testPut(v, testKey(1), []byte("later group"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if err := v.Sync(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, testKey(1), []byte("later group"))
}
