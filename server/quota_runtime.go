// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
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
	Accounting    string `json:",omitempty"`
	Nodes         []quotaNodeAllocation
}

type quotaDemand struct {
	bucket     volume.ID
	object     volume.ObjectID
	growth     uint64
	until      time.Time
	activation bool
	exhausted  bool
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
	rangeImage := s.quotaRanges.Load()
	var rangeSequence uint64
	if rangeImage != nil {
		rangeSequence = rangeImage.sequence
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
	if rangeImage != nil {
		if rangeImage.blocked[bucket] {
			return 0, fail(503, "RAM handoff must recover before quota admission")
		}
		for _, d := range rangeImage.deltas {
			if d.bucket != bucket || d.sequence <= base.Ranges {
				continue
			}
			used := d.logical
			if a.Accounting == "physical" {
				used = d.physical
			}
			n := new(big.Int).SetUint64(used)
			if d.source {
				value.Sub(value, n)
			} else {
				value.Add(value, n)
			}
		}
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
func (q *quotaManager) reserveHints(bucket volume.ID, except *volume.ObjectID, used ...uint64) uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	var total uint64
	for _, h := range q.demands {
		if h.bucket == bucket && !h.exhausted && (except == nil || h.object != *except) && time.Now().Before(h.until) {
			growth := h.growth
			if h.activation && len(used) > 0 {
				growth = subtractCapacity(growth, used[0])
			}
			total = addCapacity(total, growth)
		}
	}
	return total
}
func (q *quotaManager) request(bucket volume.ID, object volume.ObjectID, growth uint64) bool {
	return q.requestDemand(quotaDemand{bucket: bucket, object: object, growth: growth})
}
func (q *quotaManager) requestDemand(next quotaDemand) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	next.until = time.Now().Add(10 * time.Second)
	for i, h := range q.demands {
		if h.bucket == next.bucket && h.object == next.object {
			if h.exhausted && time.Now().Before(h.until) && h.growth == next.growth && h.activation == next.activation {
				return false
			}
			q.demands[i] = next
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
	q.demands = append(q.demands, next)
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

func (q *quotaManager) requestActivation(bucket volume.ID, object volume.ObjectID, allowance uint64) bool {
	// Publish the kind together with its value before waking the worker. An
	// activation value is a total allowance, not incremental object growth.
	return q.requestDemand(quotaDemand{bucket: bucket, object: object, growth: allowance, activation: true})
}

// Hints are temporary preferences, never spent quota. A total node order breaks
// circular waits when two owners each have less free credit than one write but
// their combined headroom suffices. The lower ID may borrow unused credits even
// against a donor's hints; the higher ID preserves the lower donor's preference.
// Durable debit/receive still transfer authority before any credit is spent.
func (q *quotaManager) lendable(bucket volume.ID, peer client.ID, allowance, used uint64) uint64 {
	free := subtractCapacity(allowance, used)
	if bytes.Compare(peer[:], q.ledger.node[:]) >= 0 {
		free = subtractCapacity(free, q.reserveHints(bucket, nil, used))
	}
	return free
}
