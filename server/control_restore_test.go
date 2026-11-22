// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestControlRuntimeRestoresPublishedAndAppliedImages(t *testing.T) {
	for _, damage := range []string{"none", "head-missing", "head-decision-changed", "child-missing"} {
		t.Run(damage, func(t *testing.T) {
			s := testServer(t)
			node, _ := parseID(s.running.Node.ID)
			cluster, _ := parseID(s.running.ClusterID)
			base := controlBase{Membership: client.Membership{Cluster: client.ID(cluster), Epoch: 1, Members: []client.Member{{ID: client.ID(node)}}}, Generation: 1, MapRevision: 1}
			path := filepath.Join(t.TempDir(), "control.stqr")
			ledger, err := openControlLedger(path, cluster, node, &base)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { ledger.journal.close() }()
			runtime := &controlRuntime{server: s, ledger: ledger, install: func(*systemView) error { return errors.New("interrupted runtime installation") }}
			coordinator := &controlCoordinator{ledger: ledger, participants: peerControlParticipants{runtime: runtime}}
			claim, err := coordinator.begin(context.Background(), controlAdministration, []client.ID{client.ID(node)})
			if err != nil {
				t.Fatal(err)
			}
			bundle, err := s.captureSystemImage(base.Membership, 1, 1, 1, map[client.ID]volume.ID{client.ID(node): s.system.Identity().VolumeID}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.stageSystemBundle(bundle); err != nil {
				t.Fatal(err)
			}
			proposal := controlProposal{Claim: claim, Kind: controlAdministration, Image: [32]byte(bundle.Root)}
			if err := coordinator.prepared(context.Background(), proposal, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.resume(context.Background()); err == nil {
				t.Fatal("installation failure acknowledged")
			}
			if ledger.slot == nil || ledger.slot.Committed.RecordSequence == 0 || ledger.slot.Applied.RecordSequence != 0 {
				t.Fatal("did not reproduce head publication before APPLIED")
			}
			if head, err := s.readSystemHead(); err != nil || head.Manifest != bundle.Root {
				t.Fatal("head was not durably published", err)
			}
			reopen := func() {
				t.Helper()
				if err := ledger.journal.close(); err != nil {
					t.Fatal(err)
				}
				ledger, err = openControlLedger(path, cluster, node, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			reopen()
			var installed *systemView
			runtime = &controlRuntime{server: s, ledger: ledger, install: func(view *systemView) error { installed = view; return nil }}
			if err := runtime.restore(); err != nil {
				t.Fatal("published commit did not recover", err)
			}
			if installed == nil || installed.root != bundle.Root || ledger.slot.Applied.RecordSequence == 0 {
				t.Fatal("restore failed to install and record APPLIED")
			}
			coordinator = &controlCoordinator{ledger: ledger, participants: peerControlParticipants{runtime: runtime}}
			if _, err := coordinator.resume(context.Background()); err != nil {
				t.Fatal(err)
			}
			reopen()
			installed = nil
			if damage != "none" {
				var changedHead []byte
				if damage == "head-decision-changed" {
					head, err := s.readSystemHead()
					if err != nil {
						t.Fatal(err)
					}
					head.Decision.Previous[0] ^= 1
					changedHead, _ = json.Marshal(head)
				}
				key := systemHeadKey
				if damage == "child-missing" {
					key.Object = installedAccountChild(t, bundle)
				}
				if err := s.metadataWrite(func(h *volume.Handle) error {
					if damage == "head-decision-changed" {
						_, err := h.Put(systemHeadKey, changedHead, volume.Options{})
						return err
					}
					return h.Delete(key, volume.Options{})
				}); err != nil {
					t.Fatal(err)
				}
			}
			runtime = &controlRuntime{server: s, ledger: ledger, install: func(view *systemView) error { installed = view; return nil }}
			err = runtime.restore()
			if damage == "none" {
				if err != nil || installed == nil || installed.root != bundle.Root {
					t.Fatal("APPLIED state was not reinstalled in new process memory", err)
				}
			} else if err == nil || installed != nil {
				t.Fatal("damaged acknowledged image silently restored", err)
			}
		})
	}
}

func installedAccountChild(t *testing.T, bundle *systemBundle) volume.ObjectID {
	t.Helper()
	var manifest systemManifest
	if err := decodeControlRecord(bundle.Objects[bundle.Root], &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest.Accounts
}
