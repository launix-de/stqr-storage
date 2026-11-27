// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"io"
	"os"
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
	if src != nil && !localMutationSource(src) && (opcode == 0x0102 || opcode == 0x0103 || opcode == 0x0104 || opcode == 0x0108) {
		permission := BucketWrite
		if opcode == 0x0104 {
			permission = BucketAppend
		}
		if _, err := b.authorize(p, permission); err != nil {
			return volume.Result{}, err
		}
		if opcode == 0x0108 || opt.Compare != nil {
			if _, err := b.authorize(p, BucketRead); err != nil {
				return volume.Result{}, err
			}
		}
		if length > maxObjectStagingBytes {
			return volume.Result{}, volume.ErrFull
		}
		spool, err := newReplicaSpool(b.storageDescription().Policy.StorageStrategy == "ram")
		if err != nil {
			return volume.Result{}, err
		}
		defer spool.Close()
		if _, err = io.CopyN(spool, bucketContextReader{ctx: ctx, r: src}, int64(length)); err != nil {
			return volume.Result{}, err
		}
		if _, err = spool.Seek(0, io.SeekStart); err != nil {
			return volume.Result{}, err
		}
		src = spool
	}
	result, err := b.mutateUnmetered(ctx, p, id, opcode, offset, length, src, expected, opt)
	if err == nil && (opcode == 0x0102 || opcode == 0x0103 || opcode == 0x0104 || opcode == 0x0108) {
		b.recordTraffic(p, 0, length)
		b.recordRangeTraffic(p, id, 0, length)
	}
	return result, err
}

// Only server-owned, locally bounded sources may be read under a volume lock.
// Network handlers stage their inputs before reaching this boundary. Generic
// Reader callbacks are captured outside every mutation/admission/volume lock.
func localMutationSource(src io.Reader) bool {
	switch r := src.(type) {
	case *bytes.Reader, *bytes.Buffer, *replicaSpool:
		return true
	case *os.File:
		info, err := r.Stat()
		return err == nil && info.Mode().IsRegular()
	default:
		return false
	}
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
