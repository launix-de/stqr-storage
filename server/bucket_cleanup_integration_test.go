// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/volume"
)

// Accepted writes may exceed the former fixed group budget without fencing.
// Explicit bucket deletion must still checkpoint its cleanup before reporting
// completion and retain a failed fence obligation for either cleanup lane.
func TestCleanupLargeAcceptedGroupIntegration(t *testing.T) {
	for _, receipts := range []bool{false, true} {
		for _, failFence := range []bool{false, true} {
			name := "objects"
			if receipts {
				name = "receipts"
			}
			if failFence {
				name += "-failed-fence"
			}
			t.Run(name, func(t *testing.T) {
				s, v, probe := cleanupFixture(t, 1)
				doomed := volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = h.Put(doomed, []byte("delete"), volume.Options{RequestID: cleanupID(1)}); err != nil {
					t.Fatal(err)
				}
				if receipts {
					if err = h.Purge(doomed); err != nil {
						t.Fatal(err)
					}
				}
				before := v.IOStats().SyncOps
				keep := volume.Key{Bucket: volume.ID{250}, Object: volume.ObjectID{1}}
				payload := bytes.Repeat([]byte("k"), 1<<20)
				for i := byte(1); i <= 2; i++ {
					keep.Object[0] = i
					if _, err = h.Put(keep, payload, volume.Options{Accepted: true}); err != nil {
						t.Fatal(err)
					}
				}
				generation, err := h.Generation()
				h.Close()
				if err != nil || generation == 0 || v.IsDurableThrough(generation) || v.IOStats().SyncOps != before {
					t.Fatal("positive control: large accepted group must remain unconfirmed without an implicit fence", err)
				}
				s.cleanupReceiptTurn = receipts
				if failFence {
					probe.mu.Lock()
					probe.fail = errors.New("injected full-group checkpoint failure")
					probe.mu.Unlock()
				}
				s.reclaimBucketStep()
				job := s.cleanupJobs[doomed.Bucket]
				if failFence {
					if job.done || job.volumes[0].checkpoint < generation {
						t.Fatal("lost the captured failed-checkpoint obligation", job)
					}
					return
				}
				for i := 0; i < 16 && !job.done; i++ {
					s.reclaimBucketStep()
				}
				if !job.done || !v.IsDurableThrough(generation) {
					t.Fatal("cleanup did not confirm the large accepted group", job)
				}
				h, err = v.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
				if got, _, err := h.Get(keep); err != nil || !bytes.Equal(got, payload) {
					t.Fatal("unrelated accepted payload changed", err)
				}
				if _, found, err := h.OperationStatus(cleanupID(1)); err != nil || found {
					t.Fatal("deleted receipt retained after completion", found, err)
				}
			})
		}
	}
}

func TestCleanupConfirmsPreviouslyAcceptedEmptyBucket(t *testing.T) {
	for _, receiptFirst := range []bool{false, true} {
		for _, failFence := range []bool{false, true} {
			name := "objects"
			if receiptFirst {
				name = "receipts"
			}
			if failFence {
				name += "-failed-fence"
			}
			t.Run(name, func(t *testing.T) {
				s, v, probe := cleanupFixture(t, 1)
				key := volume.Key{Bucket: volume.ID{11}, Object: volume.ObjectID{1}}
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = h.Put(key, []byte("old durable allocation"), volume.Options{}); err != nil {
					t.Fatal(err)
				}
				if err = h.Delete(key, volume.Options{Accepted: true}); err != nil {
					t.Fatal(err)
				}
				generation, err := h.PurgeAccepted(key)
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
				if v.IsDurableThrough(generation) {
					t.Fatal("positive control: accepted purge was already checkpointed")
				}
				s.cleanupReceiptTurn = receiptFirst
				if failFence {
					probe.mu.Lock()
					probe.fail = errors.New("injected inherited-purge checkpoint failure")
					probe.mu.Unlock()
				}
				s.reclaimBucketStep()
				job := s.cleanupJobs[key.Bucket]
				if failFence {
					if job.done || job.volumes[0].checkpoint != generation {
						t.Fatal("volatile absence lost the inherited purge's failed-fence obligation", job)
					}
					return
				}
				if job.done && !v.IsDurableThrough(generation) {
					t.Fatal("volatile absence completed deletion before the inherited accepted purge was durable")
				}
				for i := 0; i < 8 && !job.done; i++ {
					s.reclaimBucketStep()
				}
				if !job.done || !v.IsDurableThrough(generation) {
					t.Fatal("cleanup did not confirm the previously accepted empty view")
				}
			})
		}
	}
}
