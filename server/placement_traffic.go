// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func (s *Server) placementBackgroundVolumes(ids []client.ID, now time.Time) ([]float64, []float64) {
	read, write := make([]float64, len(ids)), make([]float64, len(ids))
	type source struct{ node, boot client.ID }
	sources := make(map[client.ID]source)
	if s.scopes != nil {
		if catalog := s.scopes.pgCatalog.Load(); catalog != nil {
			for _, snap := range catalog.snapshots {
				for _, v := range snap.Volumes {
					sources[v.ID] = source{snap.Node, snap.BootID}
				}
			}
		}
	}
	for i, id := range ids {
		ref, ok := sources[id]
		if !ok {
			ref = source{s.quotaNode, s.boot}
		}
		read[i], write[i], _ = s.statistics.volumeForecast(ref.node, ref.boot, id, now)
	}
	return read, write
}
func (r *rangeManager) placementBackground(destinations []rangeDestination) ([]float64, []float64) {
	ids := make([]client.ID, len(destinations))
	for i, d := range destinations {
		ids[i] = d.volume.ID
	}
	return r.server.placementBackgroundVolumes(ids, time.Now())
}
