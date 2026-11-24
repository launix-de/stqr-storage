// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type rangeCostRow struct {
	id                        volume.ObjectID
	physical, receipts, count uint64
	volume                    volume.ID
}
type rangeCostEstimate struct {
	rows rangeCostRows
	err  error
}

// One lazy metadata pass per candidate parent, shared by all binary refinements
// and destinations. Retry records have their own index ordering, so scan them
// once rather than once per child prefix. No payload is read for planning.
func (r *rangeManager) rangeCosts(before client.MapPatch) (func(client.RangeEntry, *volume.ID) (uint64, uint64, error), func()) {
	var estimate *rangeCostEstimate
	measure := func(entry client.RangeEntry, onlyVolume *volume.ID) (uint64, uint64, error) {
		if estimate == nil {
			estimate = &rangeCostEstimate{}
			parent := rangePlan{Before: before, Moving: before.Entries[0]}
			for _, v := range r.sourceVolumes(parent) {
				id := v.Identity().VolumeID
				h, e := v.Open(false)
				if e != nil {
					estimate.err = e
					break
				}
				var after *volume.ObjectID
				for {
					records, e := parent.scanObjects(h, after, 128)
					if e != nil {
						estimate.err = e
						break
					}
					if len(records) == 0 {
						break
					}
					for _, record := range records {
						n, e := h.AllocatedPhysical(record.Key)
						if e != nil {
							estimate.err = e
							break
						}
						if e := estimate.rows.add(rangeCostRow{id: record.Key.Object, physical: n, count: 1, volume: id}); e != nil {
							estimate.err = e
							break
						}
					}
					if estimate.err != nil {
						break
					}
					last := records[len(records)-1].Key.Object
					after = &last
				}
				if estimate.err == nil {
					estimate.err = h.ScanRequests(func(record volume.RequestRecord) error {
						if record.Key.Bucket == volume.ID(before.Bucket) && parent.contains(record.Key.Object) {
							return estimate.rows.add(rangeCostRow{id: record.Key.Object, receipts: 512, volume: id})
						}
						return nil
					})
				}
				h.Close()
				if estimate.err != nil {
					break
				}
			}
		}
		if estimate.err != nil {
			return 0, 0, estimate.err
		}
		return estimate.rows.measure(entry, onlyVolume)
	}
	return measure, func() {
		if estimate != nil {
			estimate.rows.close()
		}
	}
}
