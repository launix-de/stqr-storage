// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"math/big"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Allocations are part of the ALL-committed system image, not the client range
// map. Independent range commits and private grant journals overlay this base.
// Their two watermarks prevent replay from applying a checkpointed delta twice.
type quotaNodeAllocation struct {
	Node                  client.ID
	Bytes, Grants, Ranges uint64
}
type quotaAllocation struct {
	Bucket, Epoch client.ID
	Limit         uint64
	Nodes         []quotaNodeAllocation
}

type quotaDemand struct {
	bucket    volume.ID
	object    volume.ObjectID
	growth    uint64
	until     time.Time
	exhausted bool
}
type quotaManager struct {
	ledger   *quotaLedger
	mu       sync.Mutex
	demands  []quotaDemand
	wake     chan struct{}
	balances map[client.ID]quotaBalance
}

type quotaBalance struct {
	view                  *systemView
	grants, ranges, bytes uint64
}

func (s *Server) initQuota(dataDir string) error {
	if s.cfg.QuotaVersion > 1 {
		return volume.ErrUnsupported
	}
	node, _ := parseID(s.cfg.Node.ID)
	cluster, _ := parseID(s.cfg.ClusterID)
	l, err := openQuotaLedger(filepath.Join(dataDir, "quota.stqr"), cluster, node, s.cfg.QuotaVersion == 0)
	if err != nil {
		return err
	}
	s.quotas = &quotaManager{ledger: l, wake: make(chan struct{}, 1)}
	s.cfg.QuotaVersion = 1
	return saveConfig(s.configPath, s.cfg)
}

func findQuota(view *systemView, bucket client.ID) *quotaAllocation {
	if view == nil {
		return nil
	}
	for i := range view.manifest.Quotas {
		if view.manifest.Quotas[i].Bucket == bucket {
			return &view.manifest.Quotas[i]
		}
	}
	return nil
}

// Caller holds opMu. A handoff debits exactly the live bytes in its sealed
// snapshot at source COMMIT, and credits them at target COMMIT. The period
// between those commits can reduce availability, never increase the total.
func (s *Server) quotaAllowance(bucket client.ID) (uint64, error) {
	view := s.currentSystem.Load()
	a := findQuota(view, bucket)
	if a == nil {
		return 0, fail(503, "bucket quota allocation is not committed")
	}
	if s.quotas == nil {
		return 0, fail(503, "quota ledger is not ready")
	}
	node := s.quotas.ledger.node
	l := s.quotas.ledger
	l.mu.Lock()
	grantSequence := l.sequence
	l.mu.Unlock()
	var rangeSequence uint64
	if r := s.scopes; r != nil {
		r.mu.Lock()
		rangeSequence = r.journal.sequence
		r.mu.Unlock()
	}
	q := s.quotas
	q.mu.Lock()
	cached, known := q.balances[bucket]
	q.mu.Unlock()
	if known && cached.view == view && cached.grants == grantSequence && cached.ranges == rangeSequence {
		return cached.bytes, nil
	}
	var base quotaNodeAllocation
	for _, n := range a.Nodes {
		if n.Node == node {
			base = n
			break
		}
	}
	// Combine signed deltas before checking the bound: a node can receive
	// credits and subsequently hand their objects to a third node.
	value := new(big.Int).SetUint64(base.Bytes)
	l.mu.Lock()
	delta, err := l.deltaLocked(bucket, a.Epoch, base.Grants)
	l.mu.Unlock()
	if err != nil {
		return 0, err
	}
	value.Add(value, delta)
	if r := s.scopes; r != nil {
		r.mu.Lock()
		for _, t := range r.txns {
			if t.plan.Before.Bucket == bucket && r.active(t) && t.plan.Source == node && t.plan.ram() && t.sourceBoot != s.boot && t.decision == nil {
				r.mu.Unlock()
				return 0, fail(503, "RAM handoff must recover before quota admission")
			}
			if t.decision == nil || t.plan.Before.Bucket != bucket || t.commitProof.RecordSequence <= base.Ranges || t.plan.Source == t.plan.Target {
				continue
			}
			// Only participant commits change this node's allowance. Observing
			// another pair's historical decision grants no local capacity.
			n := new(big.Int).SetUint64(t.decision.LogicalUsed)
			if t.plan.Source == node {
				value.Sub(value, n)
			} else if t.plan.Target == node {
				value.Add(value, n)
			}
		}
		r.mu.Unlock()
	}
	if value.Sign() < 0 || !value.IsUint64() || value.Uint64() > a.Limit {
		return 0, volume.ErrCorrupt
	}
	q.mu.Lock()
	if q.balances == nil {
		q.balances = make(map[client.ID]quotaBalance)
	}
	q.balances[bucket] = quotaBalance{view: view, grants: grantSequence, ranges: rangeSequence, bytes: value.Uint64()}
	q.mu.Unlock()
	return value.Uint64(), nil
}

// Demand hints reserve no durable authority. Their short lifetime only avoids
// lending freshly obtained credits away before the original client retries;
// it never expires a grant, a peer, or an ownership promise.
func (q *quotaManager) reserveHints(bucket volume.ID, except *volume.ObjectID) uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	var total uint64
	for _, h := range q.demands {
		if h.bucket == bucket && (except == nil || h.object != *except) && time.Now().Before(h.until) {
			total = addCapacity(total, h.growth)
		}
	}
	return total
}
func (q *quotaManager) request(bucket volume.ID, object volume.ObjectID, growth uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, h := range q.demands {
		if h.bucket == bucket && h.object == object {
			if h.exhausted && time.Now().Before(h.until) && h.growth == growth {
				return false
			}
			q.demands[i] = quotaDemand{bucket: bucket, object: object, growth: growth, until: time.Now().Add(10 * time.Second)}
			select {
			case q.wake <- struct{}{}:
			default:
			}
			return true
		}
	}
	if len(q.demands) >= 128 {
		q.demands = q.demands[1:]
	}
	q.demands = append(q.demands, quotaDemand{bucket: bucket, object: object, growth: growth, until: time.Now().Add(10 * time.Second)})
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}
func (q *quotaManager) fulfilled(bucket volume.ID, object volume.ObjectID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, h := range q.demands {
		if h.bucket == bucket && h.object == object {
			q.demands = append(q.demands[:i], q.demands[i+1:]...)
			return
		}
	}
}

func sortQuotaNodes(nodes []quotaNodeAllocation) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Node.String() < nodes[j].Node.String() })
}
