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
	b.io.read.Add(uint64(len(data)))
	return data, info, err
}
func (b *Bucket) copyToVersion(ctx context.Context, p principal, id volume.ObjectID, offset, length uint64, dst io.Writer, version *uint64) (volume.Info, uint64, error) {
	info, n, err := b.copyToVersionUnmetered(ctx, p, id, offset, length, dst, version)
	b.io.read.Add(n)
	return info, n, err
}
func (b *Bucket) mutate(ctx context.Context, p principal, id volume.ObjectID, opcode uint16, offset, length uint64, src io.Reader, expected []byte, opt volume.Options) (volume.Result, error) {
	result, err := b.mutateUnmetered(ctx, p, id, opcode, offset, length, src, expected, opt)
	if err == nil && (opcode == 0x0102 || opcode == 0x0103 || opcode == 0x0104 || opcode == 0x0108) {
		b.io.write.Add(length)
	}
	return result, err
}
