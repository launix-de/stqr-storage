// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/internal/decay"
	"github.com/launix-de/stqr-storage/volume"
)

var statisticsLookupSink uint64
var statisticsRateSink float64

func BenchmarkStatisticsStorageLookup(b *testing.B) {
	for _, count := range []int{128, 1024, 10000} {
		rows := make([]volume.BucketStorage, count)
		for i := range rows {
			binary.BigEndian.PutUint64(rows[i].Bucket[8:], uint64(i+1))
			rows[i].Physical = uint64(i + 1)
		}
		id := rows[len(rows)-1].Bucket
		b.Run(fmt.Sprintf("buckets-%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				statisticsLookupSink = sortedStoragePhysical(rows, id)
			}
		})
	}
}

func BenchmarkStatisticsTraffic(b *testing.B) {
	s := &Server{}
	bucket := &Bucket{server: s}
	p := principal{User: account{ID: "warm-user"}}
	bucket.recordTraffic(p, 1, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.recordTraffic(p, 1, 1)
	}
}

func BenchmarkStatisticsRate(b *testing.B) {
	var counter decay.Accumulator
	start := time.Unix(100, 0)
	counter.Tick(start, defaultPlacementStatsHalfLife)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter.Add(200)
		counter.Tick(start.Add(time.Duration(i+1)*time.Second), defaultPlacementStatsHalfLife)
		statisticsRateSink = counter.AveragePerSecond()
	}
}
