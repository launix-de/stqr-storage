// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Cached metadata measurements are estimates for optimization, never authority
// for PREPARE. The existing executor always measures/reserves the actual move.
type placementMeasurement struct {
	epoch, policy uint64
	codec         volume.Compression
	at            time.Time
	cost          uint64
	retained      map[volume.ID]uint64
	growth        float64
}

func (r *rangeManager) measuredPlacement(shape placementShape, entry client.RangeEntry, d BucketDescription, required bool, allowance *int) (placementMeasurement, bool) {
	key := rangeMetricKey{shape.bucket, shape.start, shape.bits}
	codec := bucketCodec(d.Policy)
	var prior placementMeasurement
	if value, ok := r.measurements.Load(key); ok {
		prior = value.(placementMeasurement)
		if prior.epoch == entry.OwnerEpoch && prior.policy == entry.PolicyVersion && prior.codec == codec && time.Since(prior.at) < 30*time.Second {
			return prior, true
		}
	}
	if !required {
		if *allowance == 0 {
			return placementMeasurement{}, false
		}
		*allowance--
	}
	before, ok := r.server.routingPatch(volume.ID(shape.bucket), volume.ObjectID(shape.start))
	if !ok {
		return placementMeasurement{}, false
	}
	measurement := placementMeasurement{epoch: entry.OwnerEpoch, policy: entry.PolicyVersion, codec: codec, at: time.Now(), retained: make(map[volume.ID]uint64)}
	var err error
	measurement.cost, _, err = r.policyPlacementCost(before, entry, client.ID{}, volume.ID{}, codec, measurement.retained)
	if err != nil {
		return placementMeasurement{}, false
	}
	if prior.epoch == entry.OwnerEpoch && prior.policy == entry.PolicyVersion && prior.codec == codec && prior.at.Before(measurement.at) && !prior.at.IsZero() && measurement.cost > prior.cost {
		measurement.growth = float64(measurement.cost-prior.cost) / measurement.at.Sub(prior.at).Seconds()
	}
	r.measurements.Store(key, measurement)
	return measurement, true
}

// Keep discovery linear in the PG's ranges. Four oldest hot measurements win;
// the remainder is a moving cold window, so neither busy nor quiet ranges can
// permanently monopolize the eight metadata refreshes.
func (r *rangeManager) placementSamples(shapes []placementShape, work []placementWorkload, seed uint64) []bool {
	type candidate struct {
		index   int
		at      time.Time
		traffic float64
	}
	var hot [4]candidate
	count := 0
	for i, shape := range shapes {
		key := rangeMetricKey{shape.bucket, shape.start, shape.bits}
		work[i].Read, work[i].Write = r.rangeWorkload(key)
		traffic := work[i].Read + work[i].Write
		if traffic == 0 {
			continue
		}
		item := candidate{index: i, traffic: traffic}
		if value, ok := r.measurements.Load(key); ok {
			item.at = value.(placementMeasurement).at
		}
		pos := count
		for pos > 0 {
			previous := hot[pos-1]
			if !item.at.Before(previous.at) && !(item.at.Equal(previous.at) && item.traffic > previous.traffic) {
				break
			}
			pos--
		}
		if pos == len(hot) {
			continue
		}
		if count < len(hot) {
			count++
		}
		copy(hot[pos+1:count], hot[pos:count-1])
		hot[pos] = item
	}
	selected := make([]bool, len(shapes))
	for _, item := range hot[:count] {
		selected[item.index] = true
	}
	if len(shapes) > 0 {
		start := int(seed % uint64(len(shapes)))
		for offset := 0; offset < len(shapes) && count < 8; offset++ {
			i := (start + offset) % len(shapes)
			if !selected[i] {
				selected[i] = true
				count++
			}
		}
	}
	return selected
}
