// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketCopy uint16 = 0x1089

// Route copy receipts through one admitted node. Their global catalog commit
// makes replay protection survive restart and later coordinator/member changes.
func (s *Server) clusterBucketCopy(ctx context.Context, m client.Message, p principal) (out []byte, err error) {
	if m.Opcode == client.BucketClone {
		return s.cloneCow(ctx, m, p)
	}
	ids, _, err := s.leaseMembers()
	if err != nil {
		return nil, err
	}
	if ids[0] != s.control.ledger.node {
		actor, e := parseID(p.User.ID)
		if e != nil {
			return nil, e
		}
		envelope := client.Encoder{}
		envelope.ID(client.ID(actor))
		envelope.Text(p.User.Name)
		envelope.ID(m.ID)
		envelope.U16(m.Opcode)
		envelope.Blob(m.Body)
		body := envelope.B
		return (peerControlParticipants{runtime: s.control}).call(ctx, ids[0], peerBucketCopy, body)
	}
	s.copyCoordinator.Lock()
	defer s.copyCoordinator.Unlock()
	defer func() {
		var native *client.StatusError
		if errors.As(err, &native) {
			err = fail(native.Status, err.Error())
		}
	}()
	s.opMu.Lock()
	receipt, previous, e := s.bucketReceipt(p, m)
	s.opMu.Unlock()
	if e != nil {
		return nil, e
	}
	encode := func(n uint64) []byte { e := client.Encoder{}; e.U64(n); return e.B }
	if previous != nil {
		if receipt.Running || receipt.Status != 200 {
			return nil, fail(409, "previous copy/clear was interrupted; inspect destination before retry")
		}
		return encode(receipt.Objects), nil
	}
	d := client.Decoder{B: m.Body}
	src, dst := volume.ID(d.ID()), volume.ID{}
	dst = src
	if m.Opcode == client.BucketClone {
		dst = volume.ID(d.ID())
	}
	if d.Done() != nil || src == systemBucket || dst == systemBucket || m.Opcode == client.BucketClone && src == dst {
		return nil, fail(400, "invalid copy buckets")
	}
	for _, id := range []volume.ID{src, dst} {
		if _, e = s.Bucket(id); e != nil {
			return nil, e
		}
	}
	receipt.Running = true
	if e = s.saveClusterCopyReceipt(ctx, dst, receipt); e != nil {
		return nil, e
	}
	count := uint64(0)
	// Keep leases until the final receipt is durable; no other writer can enter
	// between the last data operation and publication of the operation result.
	clients := map[volume.ID]*frontendClient{}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()
	defer func() {
		receipt.Running = false
		receipt.Objects = count
		if err != nil {
			receipt.Status = 500
		}
		err = errors.Join(err, s.saveClusterCopyReceipt(ctx, dst, receipt))
	}()
	ordered := []volume.ID{src}
	if dst != src {
		ordered = append(ordered, dst)
	}
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i][:], ordered[j][:]) < 0 })
	for _, id := range ordered {
		actor := p
		actor.frontendPermission = BucketWrite
		if id == src && src != dst {
			actor.frontendPermission = BucketRead
		}
		if src == dst {
			actor.frontendPermission = BucketDelete
		}
		c, e := s.openFrontend(ctx, id, actor)
		if e != nil {
			return nil, fmt.Errorf("open copy connection: %w", e)
		}
		clients[id] = c
		if e = c.BeginBlockMaintenance(ctx, src != dst && id == src); e != nil {
			return nil, fmt.Errorf("copy lease: %w", e)
		}
	}
	if src != dst {
		page, _, e := clients[dst].ScanObjects(ctx, nil, 1)
		if e != nil {
			return nil, e
		}
		if len(page) != 0 {
			return nil, fail(409, "clone destination must be empty")
		}
	}
	var after *client.ObjectID
	for {
		page, next, e := clients[src].ScanObjects(ctx, after, 100)
		if e != nil {
			return nil, e
		}
		for _, r := range page {
			if src == dst {
				e = clients[src].Delete(ctx, r.ID, client.DeleteOptions{RequestID: client.NewRequestID()})
			} else {
				// Bound memory to one stripe independently of native object length.
				for off := uint64(0); off < r.Info.Length || off == 0; {
					n := min(uint64(1<<20), r.Info.Length-off)
					data, info, readErr := clients[src].GetRange(ctx, r.ID, off, n)
					if readErr != nil {
						return nil, fmt.Errorf("copy source read: %w", readErr)
					}
					if info.Length != r.Info.Length || uint64(len(data)) != n {
						return nil, fail(409, "source changed during clone")
					}
					opt := client.PutOptions{RequestID: client.NewRequestID(), Absent: off == 0}
					if off == 0 {
						_, e = clients[dst].Put(ctx, r.ID, data, opt)
					} else {
						_, e = clients[dst].PutRange(ctx, r.ID, off, data, opt)
					}
					if e != nil {
						break
					}
					off += n
					if off >= r.Info.Length {
						break
					}
				}
			}
			if e != nil {
				return nil, e
			}
			count++
		}
		if next == nil {
			break
		}
		after = next
	}
	if e = clients[dst].Sync(ctx); e != nil {
		return nil, e
	}
	return encode(count), nil
}
func (s *Server) receiveBucketCopy(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	d := client.Decoder{B: body}
	request := client.PeerAdmin{Actor: d.ID(), Username: d.Text(), Operation: d.ID(), Opcode: d.U16(), Body: d.Blob()}
	if d.Done() != nil || request.Actor == (client.ID{}) || request.Operation == (client.ID{}) || request.Username == "" || len(request.Username) > 128 || request.Opcode != client.BucketClear && request.Opcode != client.BucketClone {
		return nil, client.ErrProtocol
	}
	ids, _, e := s.leaseMembers()
	if e != nil || !hasControlNode(ids, actor) {
		return nil, ErrBucketForbidden
	}
	user, e := s.findAccount(request.Username)
	if e != nil || user.ID != request.Actor.String() || user.Role != "ADMIN" || user.Disabled {
		return nil, ErrBucketForbidden
	}
	return s.clusterBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: request.Opcode, ID: request.Operation, Body: request.Body}, principal{User: user})
}
func (s *Server) saveClusterCopyReceipt(ctx context.Context, destination volume.ID, receipt BucketReceipt) error {
	c := s.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, e := c.resume(ctx); e != nil {
		return e
	}
	_, _, e := c.change(ctx, nil, func() (*adminDraft, error) {
		s.opMu.Lock()
		preview, e := s.newAdminPreview()
		s.opMu.Unlock()
		if e != nil {
			return nil, e
		}
		draft := &adminDraft{server: preview, status: 200}
		b, e := preview.Bucket(destination)
		if e != nil {
			draft.close()
			return nil, e
		}
		desc := b.Describe()
		found := false
		for i, r := range desc.Receipts {
			if r.ID == receipt.ID {
				desc.Receipts[i] = receipt
				found = true
				break
			}
		}
		if !found {
			desc.Receipts = append(desc.Receipts, receipt)
		}
		if _, e = preview.buckets.Save(desc, desc.Revision); e != nil {
			draft.close()
			return nil, e
		}
		return draft, nil
	})
	return e
}
