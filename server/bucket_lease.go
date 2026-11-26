// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const peerBucketLease uint16 = 0x1088
const bucketLeaseHeader = "X-STQR-Maintenance"

// Epoch and Sequence fence delayed prepare/release messages. Records, including
// released high-water marks, survive participant restart in the admin volume.
// Leases do not expire on a timer or peer disconnect. Only the owner releases
// them; a restarted owner can release its preceding incarnation's orphan lease.
type bucketLease struct {
	ReferenceAware     bool        `json:",omitempty"`
	Group              client.ID   `json:",omitempty"`
	Mask               uint64      `json:",omitempty"`
	Members            []client.ID `json:",omitempty"`
	Bucket             volume.ID
	Epoch, Sequence    uint64
	Request            client.ID
	Token, Owner, Boot client.ID
	User, Username     string
	ObjectIO           bool `json:"Block"` // legacy persisted/wire field name; generic object I/O lease
	ReadOnly, Released bool
	// Opt-in so leases persisted by older nodes retain their placement fence.
	AllowHandoff bool
}
type leaseRequest struct {
	Action uint8
	Lease  bucketLease
}

func (s *Server) leaseMembers(buckets ...*Bucket) ([]client.ID, uint64, error) {
	v := s.currentSystem.Load()
	if v == nil || s.control == nil {
		return nil, 0, fail(503, "membership unavailable")
	}
	ids := membershipIDs(v.manifest.Membership)
	if len(buckets) > 0 && buckets[0] != nil && buckets[0].Describe().Policy.PlacementGroup != "" {
		var err error
		ids, _, err = s.pgMembers(buckets[0].Describe())
		if err != nil {
			return nil, 0, err
		}
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	if len(ids) == 0 {
		return nil, 0, fail(503, "empty membership")
	}
	return ids, v.manifest.Membership.Epoch, nil
}
func leaseName(id volume.ID) string { return "bucket-lease/" + id.String() }
func (s *Server) leaseRecord(b *Bucket) *bucketLease {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.lease == nil {
		return nil
	}
	copy := *b.lease
	copy.Members = append([]client.ID(nil), copy.Members...)
	return &copy
}
func (s *Server) storeLease(b *Bucket, l bucketLease) error {
	if e := s.writeBlob(leaseName(l.Bucket), l); e != nil {
		return e
	}
	b.mu.Lock()
	b.lease = &l
	if l.Released {
		b.maintenance = nil
	} else {
		a := &connectionAuthorization{accountID: l.User, bucket: b, bucketID: l.Bucket, permissions: BucketAllPermissions}
		a.lease.Store(&l)
		a.objectLease.Store(l.ObjectIO)
		a.leaseReadOnly.Store(l.ReadOnly)
		b.maintenance = a
	}
	b.mu.Unlock()
	s.leaseMu.Lock()
	if l.Released {
		delete(s.activeLeases, l.Bucket)
	} else {
		if s.activeLeases == nil {
			s.activeLeases = map[volume.ID]*Bucket{}
		}
		s.activeLeases[l.Bucket] = b
	}
	s.leaseMu.Unlock()
	if l.Released {
		s.wsMu.Lock()
		for ws := range s.sockets {
			if ws.authorization != nil {
				claim := ws.authorization.lease.Load()
				if claim != nil && claim.Token == l.Token && ws.authorization.bucketID == l.Bucket {
					ws.authorization.invalid.Store(true)
				}
			}
		}
		s.wsMu.Unlock()
	}
	return nil
}
func (s *Server) loadBucketLeases() error {
	for _, b := range s.leaseBuckets(false) {
		id := b.ID()
		var l bucketLease
		e := s.readBlob(leaseName(id), &l)
		if errors.Is(e, volume.ErrNotFound) {
			continue
		}
		if e != nil {
			return e
		}
		if l.Bucket != id || l.Token == (client.ID{}) || l.Request == (client.ID{}) || l.Boot == (client.ID{}) || l.Epoch == 0 || l.Sequence == 0 {
			return volume.ErrCorrupt
		}
		if view := s.currentSystem.Load(); view != nil && l.Epoch != view.manifest.Membership.Epoch {
			if l.Epoch > view.manifest.Membership.Epoch {
				return volume.ErrRecovery
			}
			l.Released = true
			if e = s.storeLease(b, l); e != nil {
				return e
			}
			continue
		}
		b.mu.Lock()
		b.lease = &l
		if !l.Released {
			a := &connectionAuthorization{accountID: l.User, bucket: b, bucketID: id, permissions: BucketAllPermissions}
			a.lease.Store(&l)
			a.objectLease.Store(l.ObjectIO)
			a.leaseReadOnly.Store(l.ReadOnly)
			b.maintenance = a
		}
		b.mu.Unlock()
		if !l.Released {
			s.leaseMu.Lock()
			if s.activeLeases == nil {
				s.activeLeases = map[volume.ID]*Bucket{}
			}
			s.activeLeases[id] = b
			s.leaseMu.Unlock()
		}
	}
	return nil
}
func (s *Server) callLease(ctx context.Context, node client.ID, action uint8, l bucketLease) (bucketLease, error) {
	wire, e := json.Marshal(leaseRequest{action, l})
	if e != nil {
		return l, e
	}
	raw, e := (peerControlParticipants{runtime: s.control}).call(ctx, node, peerBucketLease, wire)
	if e != nil {
		return l, e
	}
	var result bucketLease
	if e = json.Unmarshal(raw, &result); e != nil {
		return l, fmt.Errorf("lease reply decode from %s action %d: %w (bytes %d)", node.String(), action, e, len(raw))
	}
	return result, nil
}
func (s *Server) receiveBucketLease(ctx context.Context, body []byte, actor client.ID) ([]byte, error) {
	var req leaseRequest
	if len(body) > 8192 || json.Unmarshal(body, &req) != nil {
		return nil, fmt.Errorf("invalid lease request JSON")
	}
	l := req.Lease
	// Action 4 probes the cluster lease capability before a bucket exists.
	// It binds only membership and authenticates the peer; it acquires no lease.
	if req.Action == 4 {
		ids, epoch, err := s.leaseMembers()
		if err != nil {
			return nil, err
		}
		if !hasControlNode(ids, actor) || l.Token == (client.ID{}) || l.Epoch != epoch || l.Bucket != (volume.ID{}) || l.Group != (client.ID{}) || l.Mask != 0 || len(l.Members) != 0 {
			return nil, ErrBucketForbidden
		}
		return json.Marshal(l)
	}
	b, e := s.Bucket(l.Bucket)
	if e != nil {
		return nil, e
	}
	ids, epoch, e := s.leaseMembers()
	continuing := false
	if l.Group != (client.ID{}) && (req.Action == 2 || req.Action == 3) && len(l.Members) > 0 {
		prior := s.leaseRecord(b)
		continuing = prior != nil && prior.Token == l.Token && prior.Group == l.Group && prior.Mask == l.Mask && equalIDs(prior.Members, l.Members)
		if req.Action == 3 && prior == nil {
			continuing = true
		}
		if continuing {
			for i, node := range l.Members {
				if !hasControlNode(ids, node) || i > 0 && bytes.Compare(l.Members[i-1][:], node[:]) >= 0 {
					return nil, ErrBucketForbidden
				}
			}
			ids = append([]client.ID(nil), l.Members...)
		}
	}
	if l.Group != (client.ID{}) && !continuing {
		ids, epoch, e = s.leaseMembers(b)
		if e == nil {
			members, mask, err := s.pgMembers(b.Describe())
			if err != nil {
				return nil, err
			}
			if b.Describe().Policy.PlacementGroup != l.Group.String() || mask != l.Mask || !equalIDs(members, l.Members) {
				return nil, ErrBucketForbidden
			}
		}
	}
	if e != nil {
		return nil, e
	}
	if !hasControlNode(ids, actor) || l.Bucket == systemBucket || l.Token == (client.ID{}) || l.Epoch != epoch {
		return nil, ErrBucketForbidden
	}
	if req.Action == 0 || req.Action == 2 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if ids[0] != s.control.ledger.node || actor != l.Owner {
			return nil, ErrBucketForbidden
		}
		s.leaseMu.Lock()
		if s.leaseGate == nil {
			s.leaseGate = make(chan struct{}, 1)
		}
		gate := s.leaseGate
		s.leaseMu.Unlock()
		if l.Group != (client.ID{}) && s.scopes != nil {
			value, _ := s.scopes.pgRuntime.leaseGates.LoadOrStore(l.Bucket, make(chan struct{}, 1))
			gate = value.(chan struct{})
		}
		select {
		case gate <- struct{}{}:
			defer func() { <-gate }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if req.Action == 0 {
			s.opMu.Lock()
			prior := s.leaseRecord(b)
			if prior != nil && (!prior.Released || prior.Token == l.Token) {
				s.opMu.Unlock()
				return nil, fail(409, "bucket has an exclusive owner")
			}
			l.Token = client.NewRequestID()
			l.Sequence = 1
			if prior != nil && prior.Epoch == epoch {
				l.Sequence = prior.Sequence + 1
				if l.Sequence == 0 {
					s.opMu.Unlock()
					return nil, fail(409, "lease sequence exhausted")
				}
			}
			l.Released = false
			e = s.reserveLeaseLocked(b, l)
			s.opMu.Unlock()
			if e != nil {
				return nil, e
			}
			for _, node := range ids {
				if node == s.control.ledger.node {
					continue
				}
				if _, e = s.callLease(ctx, node, 1, l); e != nil {
					cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = s.releaseLeaseParticipants(cleanup, ids, l)
					cancel()
					return nil, e
				}
			}
			// A membership proposal must not have crossed the local lease reserve.
			_, current, e := s.leaseMembers()
			if e != nil || current != epoch {
				return nil, fail(409, "membership changed during acquisition")
			}
			return json.Marshal(l)
		}
		current := s.leaseRecord(b)
		if current == nil || current.Token != l.Token || current.Sequence != l.Sequence || current.Epoch != l.Epoch || current.Released {
			return json.Marshal(l)
		}
		l = *current
		if e = s.releaseLeaseParticipants(ctx, ids, l); e != nil {
			return nil, e
		}
		l.Released = true
		return json.Marshal(l)
	}
	if req.Action == 5 || req.Action == 6 {
		s.opMu.Lock()
		defer s.opMu.Unlock()
		current := s.leaseRecord(b)
		if current == nil || current.Released || current.ObjectIO || !current.ReferenceAware || current.Token != l.Token || current.Sequence != l.Sequence || current.Epoch != l.Epoch || current.Owner != l.Owner || current.User != l.User {
			return nil, ErrBucketForbidden
		}
		user, err := s.findAccount(current.Username)
		if err != nil || user.ID != current.User {
			return nil, ErrBucketForbidden
		}
		uid, err := bucketID(user.ID)
		if err != nil {
			return nil, ErrBucketForbidden
		}
		info, err := b.Info(uid, user.Role == "ADMIN")
		if err != nil || info.Permissions&(BucketRead|BucketDelete) != (BucketRead|BucketDelete) {
			return nil, ErrBucketForbidden
		}
		if err := s.reclaimBucketMetadataLocked(ctx, b, req.Action); err != nil {
			return nil, err
		}
		return json.Marshal(*current)
	}
	if actor != ids[0] || l.Sequence == 0 {
		return nil, ErrBucketForbidden
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if req.Action == 1 {
		e = s.reserveLeaseLocked(b, l)
	} else if req.Action == 3 {
		prior := s.leaseRecord(b)
		// Remember an abort even if the corresponding reserve was delayed/lost.
		if prior == nil || prior.Epoch < l.Epoch || prior.Epoch == l.Epoch && prior.Sequence <= l.Sequence {
			l.Released = true
			e = s.storeLease(b, l)
		}
	} else {
		return nil, client.ErrProtocol
	}
	if e != nil {
		return nil, e
	}
	return json.Marshal(l)
}
func (s *Server) releaseLeaseParticipants(ctx context.Context, ids []client.ID, l bucketLease) error {
	// Coordinator last: a new grant cannot race an incomplete release.
	for _, node := range ids {
		if node == s.control.ledger.node {
			continue
		}
		if _, e := s.callLease(ctx, node, 3, l); e != nil {
			return e
		}
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	b, e := s.Bucket(l.Bucket)
	if e != nil {
		return e
	}
	prior := s.leaseRecord(b)
	if prior == nil || prior.Token != l.Token {
		return nil
	}
	l.Released = true
	return s.storeLease(b, l)
}
func (s *Server) reserveLeaseLocked(b *Bucket, l bucketLease) error {
	if err := s.checkPendingReplicas(func(k volume.Key) bool { return k.Bucket == l.Bucket }); err != nil {
		return err
	}
	_, epoch, e := s.leaseMembers()
	if e != nil {
		return e
	}
	s.control.ledger.mu.Lock()
	transitioning := s.control.ledger.slot != nil
	if l.Group != (client.ID{}) {
		transitioning = false
	}
	s.control.ledger.mu.Unlock()
	if epoch != l.Epoch || transitioning || s.controlBlocksPG(client.ID(l.Bucket)) || s.bucketFrozen(l.Bucket) {
		return fail(503, "bucket control transition in progress")
	}
	if prior := s.leaseRecord(b); prior != nil {
		if prior.Epoch > l.Epoch || prior.Epoch == l.Epoch && prior.Sequence > l.Sequence {
			return errControlConflict
		}
		if prior.Epoch == l.Epoch && prior.Sequence == l.Sequence {
			if prior.Token != l.Token || prior.Released {
				return errControlConflict
			}
			return nil
		}
	}
	d := b.Describe()
	if d.State != BucketActive || d.DesiredPolicy != nil {
		return fail(409, "bucket lifecycle transition in progress")
	}
	user, e := s.findAccount(l.Username)
	if e != nil || user.ID != l.User || user.Disabled {
		return ErrBucketForbidden
	}
	uid, e := bucketID(l.User)
	if e != nil {
		return e
	}
	info, e := b.Info(uid, user.Role == "ADMIN")
	if e != nil {
		return e
	}
	required := BucketRead | BucketDelete
	if l.ObjectIO {
		required = BucketRead
		if !l.ReadOnly {
			required |= BucketWrite
		}
	}
	if info.Permissions&required != required {
		return ErrBucketForbidden
	}
	if !l.ObjectIO {
		if err := s.checkReferenceAwareMaintenance(b, l.ReferenceAware); err != nil {
			return err
		}
	}
	if s.scopes != nil {
		s.scopes.mu.Lock()
		busy := false
		for _, t := range s.scopes.txns {
			if t.plan.Before.Bucket == client.ID(l.Bucket) && s.scopes.active(t) {
				busy = true
				break
			}
		}
		s.scopes.mu.Unlock()
		if busy {
			return fail(503, "bucket handoff must finish before maintenance")
		}
	}
	s.wsMu.Lock()
	busy := false
	for ws := range s.sockets {
		a := ws.authorization
		if a == nil || a.bucket != b || a.invalid.Load() {
			continue
		}
		claim := a.lease.Load()
		if claim != nil && (claim.Token == l.Token || a.leaseOwner.Load() && claim.Request == l.Request && l.Owner == s.control.ledger.node && claim.Boot == l.Boot) {
			continue
		}
		busy = true
		break
	}
	s.wsMu.Unlock()
	if busy {
		return fail(409, "bucket is connected elsewhere; disconnect clients before maintenance")
	}
	return s.storeLease(b, l)
}
func (b *Bucket) leaseAllows(a *connectionAuthorization) bool {
	if a == nil {
		return false
	}
	claim := a.lease.Load()
	if b.server != nil {
		if view := b.server.currentSystem.Load(); view != nil && claim != nil && claim.Epoch != view.manifest.Membership.Epoch {
			return false
		}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return claim != nil && b.lease != nil && !b.lease.Released && claim.Token == b.lease.Token && claim.Epoch == b.lease.Epoch && claim.Sequence == b.lease.Sequence && claim.User == b.lease.User && a.accountID == b.lease.User
}
func (s *Server) leaseBuckets(active bool) []*Bucket {
	var out []*Bucket
	if active {
		s.leaseMu.Lock()
		defer s.leaseMu.Unlock()
		for _, b := range s.activeLeases {
			out = append(out, b)
		}
	} else {
		s.bucketMu.RLock()
		defer s.bucketMu.RUnlock()
		for _, b := range s.bucketsByID {
			out = append(out, b)
		}
	}
	return out
}
func (s *Server) anyBucketLease() bool {
	for _, b := range s.leaseBuckets(false) {
		if b.underMaintenance() {
			return true
		}
	}
	return false
}
func (s *Server) retirePreviousEpochLeases(epoch uint64) error {
	for _, b := range s.leaseBuckets(true) {
		l := s.leaseRecord(b)
		if l != nil && !l.Released && l.Epoch < epoch {
			l.Released = true
			if e := s.storeLease(b, *l); e != nil {
				return e
			}
		}
	}
	return nil
}

func (s *Server) beginClusterLease(ctx context.Context, b *Bucket, ws *wsSession, p principal, id client.ID, objectIO, readonly bool) error {
	ids, epoch, e := s.leaseMembers(b)
	if e != nil {
		return e
	}
	l := bucketLease{ReferenceAware: ws.authorization.objectReferences.Load(), Bucket: b.ID(), Epoch: epoch, Token: id, Request: id, Owner: s.control.ledger.node, Boot: s.boot, User: p.User.ID, Username: p.User.Name, ObjectIO: objectIO, ReadOnly: readonly}
	// Private clear/clone inventories pin their namespace. General object
	// leases exclude competing clients while allowing durable placement handoffs.
	l.AllowHandoff = objectIO && p.frontendPermission == 0 && ws.authorization.frontendPermission == 0
	if b.Describe().Policy.PlacementGroup != "" {
		group, _ := bucketID(b.Describe().Policy.PlacementGroup)
		_, mask, err := s.pgMembers(b.Describe())
		if err != nil {
			return err
		}
		l.Group, l.Mask, l.Members = client.ID(group), mask, append([]client.ID(nil), ids...)
	}
	ws.authorization.leaseOwner.Store(true)
	ws.authorization.lease.Store(&l)
	granted, e := s.callLease(ctx, ids[0], 0, l)
	if e != nil {
		ws.authorization.leaseOwner.Store(false)
		ws.authorization.lease.Store(nil)
		return e
	}
	ws.authorization.lease.Store(&granted)
	ws.authorization.objectLease.Store(objectIO)
	ws.authorization.leaseReadOnly.Store(readonly)
	return nil
}

// Retry orphan release without expiring a live lease on network silence. The
// owner is local, so loss of its original socket or boot is definitive.
func (s *Server) startLeaseRecovery() {
	if s.control == nil {
		return
	}
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
			}
			for _, b := range s.leaseBuckets(true) {
				l := s.leaseRecord(b)
				if l == nil || l.Released || l.Owner != s.control.ledger.node {
					continue
				}
				live := false
				s.wsMu.Lock()
				for ws := range s.sockets {
					a := ws.authorization
					if a != nil && a.bucket == b && !a.invalid.Load() && ws.ctx.Err() == nil {
						claim := a.lease.Load()
						if claim != nil && a.leaseOwner.Load() && (claim.Token == l.Token || claim.Sequence == 0 && claim.Request == l.Request && claim.Boot == l.Boot) {
							live = true
							break
						}
					}
				}
				s.wsMu.Unlock()
				if live && l.Boot == s.boot {
					continue
				}
				ids, _, e := s.leaseMembers()
				if l.Group != (client.ID{}) {
					ids = append([]client.ID(nil), l.Members...)
				}
				if e != nil {
					continue
				}
				ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
				_, _ = s.callLease(ctx, ids[0], 2, *l)
				cancel()
			}
		}
	}()
}

func bytesEqualLeaseToken(raw []byte, id client.ID) bool { return bytes.Equal(raw, id[:]) }
