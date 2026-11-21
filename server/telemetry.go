// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sync"

	"github.com/launix-de/stqr-storage/client"
)

// Telemetry may coalesce superseded snapshots of the same node. It must never
// replace one node's snapshot with another's or discard only part of a batch.
// The ordered source list also prevents a frequently updating node starving peers.
type statsSendQueue struct {
	mu      sync.Mutex
	pending map[client.ID][][]byte
	order   []client.ID
	bytes   int
	wake    chan struct{}
}

const maxQueuedTelemetry = client.MaxStatsBytes + 64*128

func newStatsSendQueue() *statsSendQueue {
	return &statsSendQueue{pending: make(map[client.ID][][]byte), wake: make(chan struct{}, 1)}
}

func batchBytes(batch [][]byte) (n int) {
	for _, b := range batch {
		n += len(b)
	}
	return
}

func (q *statsSendQueue) put(source client.ID, batch [][]byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	old, exists := q.pending[source]
	size := q.bytes - batchBytes(old) + batchBytes(batch)
	if len(batch) == 0 || size > maxQueuedTelemetry || (!exists && len(q.pending) >= 10000) {
		return false
	}
	if !exists {
		q.order = append(q.order, source)
	}
	q.pending[source] = batch
	q.bytes = size
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

func (q *statsSendQueue) take() [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return nil
	}
	source := q.order[0]
	q.order = q.order[1:]
	batch := q.pending[source]
	delete(q.pending, source)
	q.bytes -= batchBytes(batch)
	if len(q.order) > 0 {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
	return batch
}

func (q *statsSendQueue) clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	clear(q.pending)
	q.order = nil
	q.bytes = 0
}
