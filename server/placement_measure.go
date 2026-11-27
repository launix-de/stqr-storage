// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"math"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Live RAM accounting estimates are inputs for optimization, never authority
// for PREPARE. The existing executor always measures/reserves the actual move.
type placementMeasurement struct {
	epoch, policy uint64
	codec         volume.Compression
	at            time.Time
	cost          uint64
	retained      map[volume.ID]uint64
	growth        float64
	estimateOnly  bool
}

// placementWeight is bounded by map entries and volumes, never object count.
// A split has no exact child byte count until work on those children supplies
// one. Apportion maintained bucket totals by owned hash width, rather than
// reconstructing child totals by reading object/receipt metadata. These are
// optimization estimates only: PREPARE still reserves the actual move.
type placementWeightKey struct {
	bucket client.ID
	volume volume.ID
}

func (r *rangeManager) placementWeights(image *placementImage, buckets map[client.ID]BucketDescription) map[placementWeightKey]float64 {
	weights := make(map[placementWeightKey]float64)
	for bucket := range buckets {
		for _, entry := range image.bucketEntries(bucket) {
			if len(entry.Locations) == 0 || entry.Locations[0].Node != r.node {
				continue
			}
			before := client.MapPatch{Bucket: bucket, Entries: []client.RangeEntry{entry}}
			for _, store := range r.sourceVolumes(rangePlan{Before: before, Moving: entry, Source: r.node}) {
				weights[placementWeightKey{bucket, store.Identity().VolumeID}] += math.Ldexp(1, -int(entry.PrefixBits))
			}
		}
	}
	return weights
}

// Software contract: the periodic planner uses RAM aggregates exclusively,
// even for expired samples, invalid placements, pressure and policy changes.
// Missing statistics mean unknown, not an empty range. No cache miss or timer
// is permission to census object, request or replica indexes. Exact inventory
// belongs to execution of a selected map difference, not optimization scoring.
func (r *rangeManager) measuredPlacement(shape placementShape, entry client.RangeEntry, d BucketDescription, weights map[placementWeightKey]float64) (placementMeasurement, bool) {
	key := rangeMetricKey{shape.bucket, shape.start, shape.bits}
	var prior placementMeasurement
	if value, ok := r.measurements.Load(key); ok {
		prior = value.(placementMeasurement)
	}
	before, ok := r.server.routingPatch(volume.ID(shape.bucket), volume.ObjectID(shape.start))
	if !ok {
		return placementMeasurement{}, false
	}
	usage := r.server.quotaUsage.Load()
	if usage == nil {
		return placementMeasurement{}, false
	}
	measurement := placementMeasurement{epoch: entry.OwnerEpoch, policy: entry.PolicyVersion, codec: bucketCodec(d.Policy), at: time.Now(), retained: make(map[volume.ID]uint64), estimateOnly: true}
	stores := r.sourceVolumes(rangePlan{Before: before, Moving: entry, Source: r.node})
	if len(stores) == 0 {
		return placementMeasurement{}, false
	}
	for _, store := range stores {
		id := store.Identity().VolumeID
		rows, known := usage.volumes[id]
		if !known {
			return placementMeasurement{}, false
		}
		total := sortedStoragePhysical(rows, volume.ID(shape.bucket))
		weight := weights[placementWeightKey{shape.bucket, id}]
		if weight <= 0 {
			return placementMeasurement{}, false
		}
		share := math.Min(1, math.Ldexp(1, -int(shape.bits))/weight)
		n := total
		if share < 1 && total > 0 {
			estimate := math.Ceil(float64(total) * share)
			if estimate < float64(total) {
				n = uint64(estimate)
			}
		}
		measurement.cost = addCapacity(measurement.cost, n)
		measurement.retained[id] = n
	}
	return r.storePlacementMeasurement(key, prior, measurement), true
}

// Local quota images originate in volume.usage and are sorted by bucket. Keep
// this fast path separate from storagePhysical's externally supplied slices.
func sortedStoragePhysical(rows []volume.BucketStorage, bucket volume.ID) uint64 {
	i := sort.Search(len(rows), func(i int) bool {
		return bytes.Compare(rows[i].Bucket[:], bucket[:]) >= 0
	})
	if i < len(rows) && rows[i].Bucket == bucket {
		return rows[i].Physical
	}
	return 0
}

func (r *rangeManager) storePlacementMeasurement(key rangeMetricKey, prior, measurement placementMeasurement) placementMeasurement {
	if prior.epoch == measurement.epoch && prior.policy == measurement.policy && prior.codec == measurement.codec && prior.estimateOnly == measurement.estimateOnly && prior.at.Before(measurement.at) && !prior.at.IsZero() && measurement.cost > prior.cost {
		measurement.growth = float64(measurement.cost-prior.cost) / measurement.at.Sub(prior.at).Seconds()
	}
	r.measurements.Store(key, measurement)
	return measurement
}
