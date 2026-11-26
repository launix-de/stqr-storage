// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Dashboard gauges use the already maintained counters, never payload/index
// scans or per-bucket peer RPCs. One page shares one immutable telemetry image.
// Missing/stale owners remain unknown; they are not zero bytes of usage.
type dashboardBucketUsage struct {
	controller bool
	placement  *placementImage
	values     map[client.ID]map[client.ID]client.BucketStats
	local      client.ID
	system     volume.Stats
}

func (s *Server) dashboardBucketUsage() dashboardBucketUsage {
	local := s.CollectStats(client.StatsFilter{})
	image := dashboardBucketUsage{controller: s.controller != nil, placement: s.placement.Load(), values: make(map[client.ID]map[client.ID]client.BucketStats), local: local.Node, system: s.system.Stats()}
	add := func(snapshot client.StatsSnapshot) {
		if snapshot.MembershipEpoch != local.MembershipEpoch || snapshot.Generation != local.Generation || snapshot.MapRevision != local.MapRevision {
			return
		}
		entries := make(map[client.ID]client.BucketStats, len(snapshot.Buckets))
		for _, b := range snapshot.Buckets {
			entries[b.ID] = b
		}
		image.values[snapshot.Node] = entries
	}
	add(local)
	s.peerStatsMu.RLock()
	for _, peer := range s.peerStats {
		if peer.online && peer.snapshot != nil && time.Since(peer.lastSeen) < 10*time.Second {
			add(*peer.snapshot)
		}
	}
	s.peerStatsMu.RUnlock()
	return image
}

func (image dashboardBucketUsage) used(d BucketDescription) (uint64, error) {
	id, err := bucketID(d.ID)
	if err != nil {
		return 0, err
	}
	if id == systemBucket {
		if d.Policy.Limits.Accounting == "physical" {
			return image.system.AllocatedPhysical, nil
		}
		return image.system.LogicalBytes, nil
	}
	if len(d.ClonePeers) > 0 && d.Policy.Limits.Accounting == "logical" {
		return 0, errors.New("Belegung des COW-Verbunds noch nicht als Statistik verfügbar")
	}
	owners := map[client.ID]bool{image.local: true}
	if image.controller {
		owners = map[client.ID]bool{}
		replicated := false
		if image.placement == nil {
			return 0, errors.New("Placement-Statistik noch nicht verfügbar")
		}
		for _, entry := range image.placement.bucketEntries(client.ID(id)) {
			if entry.State != 0 || entry.Runtime != (client.ID{}) {
				return 0, errors.New("Belegung während des Placement-Wechsels noch nicht verfügbar")
			}
			primaries := 0
			for _, location := range entry.Locations {
				if location.Role == 0 {
					primaries++
					owners[location.Node] = true
				}
				if location.Role == 1 {
					replicated = true
					if d.Policy.Limits.Accounting == "physical" {
						owners[location.Node] = true
					}
				}
			}
			if primaries != 1 {
				return 0, errors.New("Primäre Placement-Statistik nicht verfügbar")
			}
		}
		// Per-node logical counters contain every owned replica. A node which is a
		// primary for one range and replica for another cannot be deduplicated by
		// adding whole-bucket counters. Do not silently overstate logical usage.
		if replicated && d.Policy.Limits.Accounting == "logical" && len(owners) > 1 {
			return 0, errors.New("Logische Belegung benötigt getrennte Range-Statistiken")
		}
	}
	var total uint64
	for node := range owners {
		values, ok := image.values[node]
		if !ok {
			return 0, errors.New("Belegung einer zuständigen Node nicht verfügbar")
		}
		value, ok := values[client.ID(id)]
		if !ok {
			return 0, errors.New("Bucket-Statistik einer zuständigen Node fehlt")
		}
		if d.Policy.Limits.Accounting == "physical" {
			total = addCapacity(total, value.AttributedPhysical)
		} else {
			total = addCapacity(total, value.LogicalBytes)
		}
	}
	return total, nil
}

func (image dashboardBucketUsage) annotate(entry *bucketListEntry) {
	used, err := image.used(entry.BucketDescription)
	if err != nil {
		entry.UsageError = err.Error()
	} else {
		entry.UsedBytes = &used
	}
	physical := entry.BucketDescription
	physical.Policy.Limits.Accounting = "physical"
	if occupied, err := image.used(physical); err == nil {
		entry.OccupiedPhysicalBytes = &occupied
	}
}
