// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sync/atomic"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Logical frontend traffic, not physical device work. Replica copies, sparse
// TRUNC sizes and DELETE lengths must not inflate user payload throughput.
// Successful retried RPCs count their delivered payload again, as traffic.
// Counters last for this node incarnation; snapshots bind the BootID.
type bucketIO struct{ read, write atomic.Uint64 }

func (b *Bucket) ioSnapshot() client.IOStats {
	return client.IOStats{LogicalReadBytes: b.io.read.Load(), LogicalWriteBytes: b.io.write.Load()}
}
func (b *Bucket) recordObjectTraffic(p principal, id volume.ObjectID, read, write uint64) {
	b.recordTraffic(p, read, write)
	b.recordRangeTraffic(p, id, read, write)
}
func (b *Bucket) recordTraffic(p principal, read, write uint64) {
	// Reference-control requests transfer tokens, not the root internally read
	// and rewritten to maintain them. Backend volume counters remain intact.
	if p.internalCoordination {
		return
	}
	b.io.read.Add(read)
	b.io.write.Add(write)
	if b.server == nil || p.ClusterPeer || read == 0 && write == 0 {
		return
	}
	id := p.User.ID

	if id == "" {
		return
	}
	value, exists := b.server.userIO.Load(id)
	if !exists {
		value, _ = b.server.userIO.LoadOrStore(id, &bucketIO{})
	}
	counters := value.(*bucketIO)
	counters.read.Add(read)
	counters.write.Add(write)
}
func (s *Server) userTraffic(id string) (uint64, uint64) {
	if value, ok := s.userIO.Load(id); ok {
		c := value.(*bucketIO)
		return c.read.Load(), c.write.Load()
	}
	return 0, 0
}
