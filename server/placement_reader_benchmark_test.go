// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func BenchmarkPlacementReaderTraffic(b *testing.B) {
	for _, hinted := range []bool{false, true} {
		name := "aggregate"
		if hinted {
			name = "reader"
		}
		b.Run(name, func(b *testing.B) {
			s := &Server{}
			r := &rangeManager{server: s, node: client.ID{1}}
			s.scopes = r
			bucket := client.ID{9}
			image, err := buildPlacement([]client.MapPatch{{Membership: 1, Generation: 1, Revision: 1, Bucket: bucket, ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: r.node, Volume: client.ID{2}}}}}, Addresses: []client.NodeAddress{{Node: r.node, Addresses: []string{"http://node:7419"}}}}})
			if err != nil {
				b.Fatal(err)
			}
			s.placement.Store(image)
			store, err := NewBucket(volume.ID(bucket), "traffic")
			if err != nil {
				b.Fatal(err)
			}
			store.server = s
			p := principal{}
			if hinted {
				p.connection = &connectionAuthorization{readerID: client.ID{3}, accountID: "benchmark", bucket: store, bucketID: volume.ID(bucket)}
				p.connection.placementHints.Store(true)
			}
			object := volume.ObjectID{7}
			store.recordRangeTraffic(p, object, 1024, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				store.recordRangeTraffic(p, object, 1024, 0)
			}
		})
	}
}
