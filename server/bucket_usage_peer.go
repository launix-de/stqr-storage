// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketUsage uint16 = 0x1064

// Dashboard and filesystem-capacity aggregation ask for maintained scalar counts instead
// of transferring every object's metadata every three seconds. Counts remain
// administrative measurements; they never authorize distributed quota growth.
func (s *Server) receiveBucketUsage(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	schema, bucket, user, name := d.U16(), volume.ID(d.ID()), d.ID(), d.Text()
	var stamp [32]byte
	copy(stamp[:], d.Take(32))
	accounting := "logical"
	internal := schema == 2 && len(d.B) == 0 || schema == 3
	if schema == 2 && !internal || schema == 3 {
		if d.U8() != 1 {
			return nil, client.ErrProtocol
		}
		accounting = "physical"
	}
	if (schema != 1 && schema != 2 && schema != 3) || d.Done() != nil || bucket == systemBucket {
		return nil, client.ErrProtocol
	}
	view := s.currentSystem.Load()
	if view == nil || !hasControlNode(membershipIDs(view.manifest.Membership), actor) {
		return nil, ErrBucketForbidden
	}
	if !internal {
		account, err := s.findAccount(name)
		if err != nil || account.ID != user.String() || account.Disabled || account.Role != "ADMIN" {
			return nil, ErrBucketForbidden
		}
	} else if user != (client.ID{}) || name != "" {
		return nil, client.ErrProtocol
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	entries := s.placement.Load().bucketEntries(client.ID(bucket))
	stampNode := s.control.ledger.node
	if accounting == "physical" {
		stampNode = client.ID{}
	}
	if inventoryStamp(entries, stampNode) != stamp {
		return nil, fail(503, "usage owner map differs")
	}
	if err := b.inventoryAvailable(entries); err != nil {
		return nil, err
	}
	policy := b.storageDescription().Policy
	policy.Limits.Accounting = accounting
	used, err := b.accountedUsageFor(policy)
	if err != nil {
		return nil, err
	}
	if inventoryStamp(s.placement.Load().bucketEntries(client.ID(bucket)), stampNode) != stamp {
		return nil, fail(503, "usage owner changed")
	}
	e := client.Encoder{}
	e.U64(used)
	return e.B, nil
}
