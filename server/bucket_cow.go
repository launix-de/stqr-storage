// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"io"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// A clone is a connection between namespaces, not a filesystem or an object
// revision. Each physical record is a materialization boundary, including an
// explicit tombstone. A record must never be purged while a clone depends on it.
type cowGraph map[volume.ID][]volume.ID

// The server's reference root describes holders in exactly one bucket. Its
// embedded bucket ID has always made inherited holders invalid (see ORF1
// decoding). It is coordination state, not clonable user payload. Never read it
// through a clone edge or preserve a source holder list into another bucket:
// doing so adds useless cross-bucket writes and durability fences to open/close.
func bucketLocalReferenceRoot(object volume.ObjectID) bool {
	return object == volume.ObjectID(client.ObjectReferenceRootID())
}

type cowSnapshot struct {
	bucket         volume.ID
	node, transfer client.ID
	Info           volume.Info
	Data           io.ReaderAt
	Extents        []volume.Extent
	Close          func() error
}

func (s *cowSnapshot) close() {
	if s != nil && s.Close != nil {
		_ = s.Close()
	}
}

// capture returns nil only for an ID which has never been materialized. Deleted
// records return a tombstone snapshot. Snapshots retain their read's actual bytes
// independently of subsequent writes. materialize atomically creates a record
// only if NO record exists; a tombstone must never be overwritten by this path.
type cowObjects interface {
	capture(context.Context, volume.ID, volume.ObjectID) (*cowSnapshot, error)
	materialize(context.Context, volume.ID, volume.ObjectID, *cowSnapshot) error
}

// resolve searches only through unmaterialized namespaces. Recheck the local
// record after consulting a peer: a concurrent writer may have installed our
// preimage before changing the peer we read. Returning that local preimage is
// what prevents read-through from exposing the peer's later mutation.
func (g cowGraph) resolve(ctx context.Context, store cowObjects, bucket volume.ID, object volume.ObjectID, seen map[volume.ID]bool) (*cowSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	own, err := store.capture(ctx, bucket, object)
	if err != nil || own != nil || bucketLocalReferenceRoot(object) {
		return own, err
	}
	if seen[bucket] {
		return nil, nil
	}
	if len(seen) >= 4096 {
		return nil, fail(503, "clone graph exceeds traversal budget")
	}
	seen[bucket] = true
	for _, peer := range g[bucket] {
		if seen[peer] {
			continue
		}
		candidate, err := g.resolve(ctx, store, peer, object, seen)
		if err != nil {
			return nil, err
		}
		own, err = store.capture(ctx, bucket, object)
		if err != nil || own != nil {
			candidate.close()
			return own, err
		}
		if candidate != nil {
			return candidate, nil
		}
	}
	return nil, nil
}

// prepareWrite preserves the old logical contents for every direct peer before
// the ordinary mutation may acquire its volume write lock. Connections must be
// revalidated under the server's mutation lock before applying that mutation.
// Concurrent preservation is safe because materialize never replaces a record.
func (g cowGraph) prepareWrite(ctx context.Context, store cowObjects, bucket volume.ID, object volume.ObjectID) error {
	if len(g[bucket]) == 0 || bucketLocalReferenceRoot(object) {
		return nil
	}
	// Positive physical presence is permanent while this graph remains linked.
	// Once every neighbor has its own record, a ranged overwrite need not read
	// or copy the object's payload again.
	var missing []volume.ID
	probe, fast := store.(interface {
		present(context.Context, volume.ID, volume.ObjectID) (bool, error)
	})
	ownPresent := false
	if fast {
		var err error
		ownPresent, err = probe.present(ctx, bucket, object)
		if err != nil {
			return err
		}
		for _, peer := range g[bucket] {
			present, err := probe.present(ctx, peer, object)
			if err != nil {
				return err
			}
			if !present {
				missing = append(missing, peer)
			}
		}
		if ownPresent && len(missing) == 0 {
			return nil
		}
	} else {
		missing = g[bucket]
	}
	if !ownPresent {
		own, err := g.resolve(ctx, store, bucket, object, map[volume.ID]bool{})
		if err != nil {
			return err
		}
		if own == nil {
			own = &cowSnapshot{Info: volume.Info{Deleted: true}}
		}
		err = store.materialize(ctx, bucket, object, own)
		own.close()
		if err != nil {
			return err
		}
	}
	// A competing writer may have won the first materialization. Capture the
	// record actually stored here before preserving it for remaining peers.
	var own *cowSnapshot
	var err error
	own, err = store.capture(ctx, bucket, object)
	if err != nil {
		return err
	}
	if own == nil {
		return errors.New("clone: materialization did not create a record")
	}
	defer own.close()
	for _, peer := range missing {
		if err := store.materialize(ctx, peer, object, own); err != nil {
			return err
		}
	}
	return nil
}

func (g cowGraph) component(start volume.ID) []volume.ID {
	seen := map[volume.ID]bool{}
	todo := []volume.ID{start}
	for len(todo) > 0 {
		id := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		todo = append(todo, g[id]...)
	}
	out := make([]volume.ID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
