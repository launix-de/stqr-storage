// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerForceReplicaState uint16 = 0x1078

type forceReplicaPage struct {
	After   volume.ID
	More    bool
	Records []volume.ReplicaRecord
}

// Exclusion changes membership, not the identity of surviving data. Keep the
// committed copies and let the ordinary placement reconciler fill the deficit.
func replicaSurvivors(before client.RangeEntry, x *controlExclusion) (client.RangeEntry, bool) {
	if x == nil || before.State != 0 || before.Runtime != (client.ID{}) || len(before.Locations) < 2 || before.OwnerEpoch == ^uint64(0) {
		return before, false
	}
	next := before
	next.Locations = nil
	for _, loc := range before.Locations {
		if !hasControlNode(x.Nodes, loc.Node) {
			next.Locations = append(next.Locations, loc)
		}
	}
	if len(next.Locations) == 0 || len(next.Locations) == len(before.Locations) {
		return before, false
	}
	next.OwnerEpoch++
	for i := range next.Locations {
		next.Locations[i].Role = 1
	}
	next.Locations[0].Role = 0
	return next, true
}

func replicaExclusionReplacement(x *controlExclusion, before, after client.RangeEntry) bool {
	next, changed := replicaSurvivors(before, x)
	return changed && reflect.DeepEqual(next, after)
}

// Called under the surviving ALL-members' durable exclusion reservation. Never
// promote a learner or an unresolved PREPARE, and never guess between divergent
// committed copies. The existing global PREPARE/COMMIT persists the new map.
func (s *Server) forceReplicaState(body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	claim, err := decodeControlClaim(d.Blob())
	before, decodeErr := client.DecodeMapPatch(d.Blob())
	action := d.U8()
	var request volume.ID
	var commit bool
	var abortStatus uint16
	if action != 0 {
		request = volume.ID(d.ID())
	}
	if action == 3 {
		commit, abortStatus = d.Bool(), d.U16()
	}
	if err != nil || decodeErr != nil || d.Done() != nil || action > 3 || len(before.Entries) != 1 || claim.Exclusion == nil || claim.Round.Initiator != actor {
		return nil, client.ErrProtocol
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.scopedTransition(&claim); err != nil {
		return nil, err
	}
	entry, known := s.placement.Load().lookup(before.Bucket, before.Begin)
	if !known || !reflect.DeepEqual(entry, before.Entries[0]) {
		return nil, errControlConflict
	}
	next, changed := replicaSurvivors(entry, claim.Exclusion)
	if !changed {
		return nil, errControlConflict
	}
	var store *volume.Volume
	for _, loc := range next.Locations {
		if loc.Node == s.control.ledger.node {
			s.mu.RLock()
			managed := s.volumes[loc.Volume.String()]
			if managed != nil && (managed.config.State == "active" || managed.config.State == "draining") {
				store = managed.store
			}
			s.mu.RUnlock()
		}
	}
	if store == nil {
		return nil, fail(503, "surviving replica volume is unavailable")
	}
	p := rangePlan{Before: before, Moving: entry}
	if action != 0 {
		h, err := store.Open(action == 3)
		if err != nil {
			return nil, err
		}
		defer h.Close()
		if action == 1 {
			var after *volume.ID
			if request != (volume.ID{}) {
				after = &request
			}
			records, err := h.ScanReplicaRecords(after, 64)
			if err != nil {
				return nil, err
			}
			page := forceReplicaPage{More: len(records) == 64}
			for _, record := range records {
				page.After = record.Request.ID
				if client.ID(record.Request.Key.Bucket) == before.Bucket && p.contains(record.Request.Key.Object) && record.Binding.SetHash == replicaSetHash(record.Request.Key.Bucket, entry) {
					page.Records = append(page.Records, record)
				}
			}
			return json.Marshal(page)
		}
		record, found, err := h.ReplicaStatus(request)
		if err != nil {
			return nil, err
		}
		if !found {
			if action == 3 && commit {
				return nil, fail(503, "committed replica preparation is missing")
			}
			return json.Marshal(forceReplicaPage{})
		}
		if client.ID(record.Request.Key.Bucket) != before.Bucket || !p.contains(record.Request.Key.Object) || record.Binding.SetHash != replicaSetHash(record.Request.Key.Bucket, entry) {
			return nil, errControlConflict
		}
		if action == 2 {
			return json.Marshal(forceReplicaPage{Records: []volume.ReplicaRecord{record}})
		}
		if commit {
			if err := h.DecideReplica(request, record.Binding); err != nil {
				return nil, err
			}
			_, err = h.PublishReplica(request, record.Binding)
		} else {
			if abortStatus < 400 || abortStatus > 599 {
				return nil, client.ErrProtocol
			}
			err = h.AbortReplicaPending(request, record.Binding, abortStatus)
			if err == nil {
				err = h.FinishReplicaAbort(request, record.Binding)
			}
		}
		return nil, err
	}
	if err := s.checkPendingReplicas(func(k volume.Key) bool { return client.ID(k.Bucket) == before.Bucket && p.contains(k.Object) }); err != nil {
		return nil, err
	}
	if err := store.Sync(); err != nil {
		return nil, err
	}
	h, err := store.Open(false)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	sum := sha256.New()
	var after *volume.ObjectID
	for {
		records, err := p.scanObjects(h, after, 128)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		for _, record := range records {
			if !p.contains(record.Key.Object) {
				continue
			}
			e := client.Encoder{}
			e.B = append(e.B, record.Key.Object[:]...)
			e.U64(record.Info.Length)
			e.Bool(record.Info.Deleted)
			sum.Write(e.B)
			if !record.Info.Deleted {
				if _, err := h.CopyTo(record.Key, 0, record.Info.Length, bucketContextWriter{ctx: s.ctx, w: sum}); err != nil {
					return nil, err
				}
			}
		}
		last := records[len(records)-1].Key.Object
		after = &last
	}
	return sum.Sum(nil), nil
}

func (c *clusterController) checkReplicaSurvivors(ctx context.Context, claim controlClaim, before client.MapPatch, next client.RangeEntry) error {
	wire, err := marshalControlClaim(claim)
	if err != nil {
		return err
	}
	patch, err := before.MarshalBinary()
	if err != nil {
		return err
	}
	e := client.Encoder{}
	e.Blob(wire)
	e.Blob(patch)
	query := func(loc client.Location, action uint8, request volume.ID, commit bool, status uint16) ([]byte, error) {
		message := client.Encoder{B: append([]byte(nil), e.B...)}
		message.U8(action)
		if action != 0 {
			message.ID(client.ID(request))
		}
		if action == 3 {
			message.Bool(commit)
			message.U16(status)
		}
		if loc.Node == c.server.control.ledger.node {
			return c.server.forceReplicaState(message.B, claim.Round.Initiator)
		}
		return (peerControlParticipants{runtime: c.server.control}).call(ctx, loc.Node, peerForceReplicaState, message.B)
	}
	// Use the retained prepare/decision/publication records, never a payload
	// version or a timeout. A COMMIT on any survivor is irreversible. With all
	// survivors frozen, a prepare without any decision can safely be aborted.
	for _, loc := range next.Locations {
		var cursor volume.ID
		for {
			raw, err := query(loc, 1, cursor, false, 0)
			if err != nil {
				return err
			}
			var page forceReplicaPage
			if err := json.Unmarshal(raw, &page); err != nil {
				return err
			}
			for _, candidate := range page.Records {
				if candidate.State == volume.ReplicaPublished {
					continue
				}
				commit, status := false, uint16(409)
				for _, peer := range next.Locations {
					raw, err := query(peer, 2, candidate.Request.ID, false, 0)
					if err != nil {
						return err
					}
					var state forceReplicaPage
					if err := json.Unmarshal(raw, &state); err != nil {
						return err
					}
					for _, record := range state.Records {
						if record.Request != candidate.Request || record.Binding != candidate.Binding {
							return errControlConflict
						}
						commit = commit || record.State == volume.ReplicaDecided || record.State == volume.ReplicaPublished
						if record.State == volume.ReplicaAborted && record.AbortStatus >= 400 {
							status = record.AbortStatus
						}
					}
				}
				for _, peer := range next.Locations {
					if _, err := query(peer, 3, candidate.Request.ID, commit, status); err != nil {
						return err
					}
				}
			}
			if !page.More {
				break
			}
			if page.After == cursor {
				return client.ErrProtocol
			}
			cursor = page.After
		}
	}
	var digest []byte
	for _, loc := range next.Locations {
		got, err := query(loc, 0, volume.ID{}, false, 0)
		if err != nil {
			return err
		}
		if len(got) != sha256.Size || digest != nil && !reflect.DeepEqual(got, digest) {
			return fail(503, "surviving replica snapshots disagree")
		}
		digest = got
	}
	return nil
}
