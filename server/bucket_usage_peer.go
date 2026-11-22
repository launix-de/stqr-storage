// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketUsage uint16 = 0x1064

// Dashboard aggregation asks owners for their maintained scalar count instead
// of transferring every object's metadata every three seconds. Counts remain
// administrative measurements; they never authorize distributed quota growth.
func (s *Server) receiveBucketUsage(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema, bucket, user, name := d.U16(), volume.ID(d.ID()), d.ID(), d.Text()
	var stamp [32]byte
	copy(stamp[:], d.Take(32))
	if schema != 1 || d.Done() != nil || bucket == systemBucket {
		return nil, client.ErrProtocol
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, ErrBucketForbidden
	}
	account, err := s.findAccount(name)
	if err != nil || account.ID != user.String() || account.Disabled || account.Role != "ADMIN" {
		return nil, ErrBucketForbidden
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	entries := s.placement.Load().bucketEntries(client.ID(bucket))
	if inventoryStamp(entries, s.control.ledger.node) != stamp {
		return nil, fail(503, "usage owner map differs")
	}
	if err := b.inventoryAvailable(entries); err != nil {
		return nil, err
	}
	used, err := b.logicalUsage()
	if err != nil {
		return nil, err
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(bucket)), s.control.ledger.node) != stamp {
		return nil, fail(503, "usage owner changed")
	}
	e := client.Encoder{}
	e.U64(used)
	return e.B, nil
}
