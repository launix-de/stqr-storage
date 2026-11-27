// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"errors"
	"testing"

	"github.com/launix-de/stqr-storage/volume"
)

// Spill-backed accepted groups do not force a pressure checkpoint. An explicit,
// committed bucket deletion still fences its cleanup cut outside the handle;
// a failed deletion fence must preserve the exact later-generation obligation.
func TestCleanupFullAcceptedGroupIntegration(t *testing.T) {
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
				keep := volume.Key{Bucket: volume.ID{250}, Object: volume.ObjectID{1}}
				payload := bytes.Repeat([]byte("k"), 1<<20)
				h.Close()
				acceptedBefore := v.IOStats()
				h, err = v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				for i := byte(1); i <= 5; i++ {
					keep.Object[0] = i
					if _, err = h.Put(keep, payload, volume.Options{Accepted: true}); err != nil {
						t.Fatal(err)
					}
				}
				h.Close()
				if v.IOStats().SyncOps != acceptedBefore.SyncOps {
					t.Fatal("accepted burst introduced implicit Sync")
				}
				if generation, pressure := v.PendingCheckpoint(true, 0); pressure || generation != 0 {
					t.Fatal("accepted spill requested a pressure checkpoint", generation, pressure)
				}
				generation, pending := v.PendingCheckpoint(false, 0)
				if !pending || generation == 0 || v.IsDurableThrough(generation) {
					t.Fatal("positive control: accepted generation is not pending", generation, pending)
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
					if job.done || job.volumes[0].checkpoint <= generation || job.volumes[0].checkpoint != v.Stats().Generation {
						t.Fatal("lost the exact accepted cleanup generation after failed fence", job)
					}
					return
				}
				for i := 0; i < 16 && !job.done; i++ {
					s.reclaimBucketStep()
				}
				if !job.done || !v.IsDurableThrough(generation) {
					t.Fatal("cleanup did not resume after full-group checkpoint", job)
				}
				h, err = v.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Close()
				for i := byte(1); i <= 5; i++ {
					keep.Object[0] = i
					if got, _, err := h.Get(keep); err != nil || !bytes.Equal(got, payload) {
						t.Fatal("unrelated accepted payload changed", i, err)
					}
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
