// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"io"
	"sync/atomic"
)

// Logical frontend traffic, not physical device work. Replica copies, sparse
// TRUNC sizes and DELETE lengths must not inflate user payload throughput.
// Successful retried RPCs count their delivered payload again, as traffic.
// Counters last for this node incarnation; snapshots bind the BootID.
type bucketIO struct{ read, write atomic.Uint64 }

func (b *Bucket) ioSnapshot() client.IOStats {
	return client.IOStats{LogicalReadBytes: b.io.read.Load(), LogicalWriteBytes: b.io.write.Load()}
}
func (b *Bucket) GetRange(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64) ([]byte, volume.Info, error) {
	data, info, err := b.getRange(ctx, p, id, offset, length)
	b.recordTraffic(p, uint64(len(data)), 0)
	b.recordRangeTraffic(p, id, uint64(len(data)), 0)
	return data, info, err
}
func (b *Bucket) copyToChecked(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer, condition *volume.Options) (volume.Info, uint64, error) {
	info, n, err := b.copyToCheckedUnmetered(ctx, p, id, offset, length, dst, condition)
	b.recordTraffic(p, n, 0)
	b.recordRangeTraffic(p, id, n, 0)
	return info, n, err
}
func (b *Bucket) mutate(ctx context.Context, p principal, id volume.ObjectID, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opt volume.Options) (volume.Result, error) {
	result, err := b.mutateUnmetered(ctx, p, id, opcode, offset, length, src, expected, opt)
	if err == nil && (opcode == 0x0102 || opcode == 0x0103 || opcode == 0x0104 || opcode == 0x0108) {
		b.recordTraffic(p, 0, length)
		b.recordRangeTraffic(p, id, 0, length)
	}
	return result, err
}

func (b *Bucket) recordTraffic(p principal, read, write uint64) {
	b.io.read.Add(read)
	b.io.write.Add(write)
	if b.server == nil || p.ClusterPeer || read == 0 && write == 0 {
		return
	}
	id := p.User.ID

	if id == "" {
		return
	}
	value, _ := b.server.userIO.LoadOrStore(id, &bucketIO{})
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
