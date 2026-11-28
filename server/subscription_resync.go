// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"sync"
	"sync/atomic"

	"github.com/launix-de/stqr-storage/client"
)

// A saturated response queue invalidates only the affected subscription, not
// the connection carrying independent object writes. The descriptor is retired
// under the registry lock, and its existing wire-format RESYNC is retained here
// until the sole socket writer sends it. Entries contain no object payload.
// Active descriptors and retained notices share the session subscription limit.
// Lock order is registry -> mailbox; the writer never takes the registry lock.
type subscriptionResyncMailbox struct {
	mu      sync.Mutex
	wake    chan struct{}
	entries map[client.ID]*subscriptionResyncNotice
	closed  bool
}

type subscriptionResyncNotice struct {
	id       client.ID
	wire     []byte
	canceled bool // mailbox.mu, including notices already selected by the writer
	selected bool
	credit   *atomic.Int64
}

func (q *subscriptionResyncMailbox) ready() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.wake == nil {
		q.wake = make(chan struct{}, 1)
	}
	return q.wake
}

func (q *subscriptionResyncMailbox) registration(id client.ID) (int, bool, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries), q.entries[id] != nil, q.closed
}

func (q *subscriptionResyncMailbox) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

func (q *subscriptionResyncMailbox) put(id client.ID, wire []byte, credit *atomic.Int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if q.entries[id] != nil {
		return true
	}
	if len(q.entries) >= maxSessionObjectSubscriptions {
		return false
	}
	if q.entries == nil {
		q.entries = make(map[client.ID]*subscriptionResyncNotice)
	}
	q.entries[id] = &subscriptionResyncNotice{id: id, wire: wire, credit: credit}
	credit.Add(1)
	if q.wake == nil {
		q.wake = make(chan struct{}, 1)
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

func (q *subscriptionResyncMailbox) cancel(id client.ID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if entry := q.entries[id]; entry != nil {
		entry.canceled = true
		if !entry.selected {
			entry.release()
		}
		delete(q.entries, id)
		if len(q.entries) == 0 {
			q.entries = nil
		}
	}
}

func (q *subscriptionResyncMailbox) clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for _, entry := range q.entries {
		entry.canceled = true
		if !entry.selected {
			entry.release()
		}
	}
	q.entries = nil
}

func (q *subscriptionResyncMailbox) take() []*subscriptionResyncNotice {
	q.mu.Lock()
	defer q.mu.Unlock()
	batch := make([]*subscriptionResyncNotice, 0, len(q.entries))
	for _, entry := range q.entries {
		entry.selected = true
		batch = append(batch, entry)
	}
	// Retain selected entries until output completes: registration, UNSUBSCRIBE
	// and revocation must still see and be able to cancel their exact identity.
	return batch
}

func (q *subscriptionResyncMailbox) live(entry *subscriptionResyncNotice) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return !entry.canceled && q.entries[entry.id] == entry
}

func (q *subscriptionResyncMailbox) finish(entry *subscriptionResyncNotice) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.entries[entry.id] == entry {
		delete(q.entries, entry.id)
		if len(q.entries) == 0 {
			q.entries = nil
		}
	}
	entry.release()
}

// mailbox.mu protects this one-shot release. Atomic accounting lets admission
// preserve the global retention budget without a network writer taking the
// registry mutex or publishers retaining any lock across socket output.
func (entry *subscriptionResyncNotice) release() {
	if entry.credit != nil {
		entry.credit.Add(-1)
		entry.credit = nil
	}
}

func (ws *wsSession) writeSubscriptionResync(write func([]byte) bool) bool {
	pending := ws.subscriptionResync.take()
	defer func() {
		// Includes canceled selected notices and all remaining entries if an
		// actual socket error interrupts output. Neither path leaks credits.
		for _, notice := range pending {
			if notice != nil {
				ws.subscriptionResync.finish(notice)
			}
		}
	}()
	// Drain only the prefix already queued when these notices were selected.
	// New traffic must not postpone invalidation forever.
	for n := len(ws.responses); n > 0; n-- {
		select {
		case <-ws.done:
			return false
		case wire := <-ws.responses:
			if !write(wire) {
				return false
			}
		}
	}
	for i, notice := range pending {
		// UNSUBSCRIBE can cancel after take() while queue draining is
		// blocked. Recheck immediately before output. If it cancels after
		// this check, the sole writer still orders the old notice before
		// its ACK and any subsequently reused subscription ID.
		if ws.subscriptionResync.live(notice) && !write(notice.wire) {
			return false
		}
		ws.subscriptionResync.finish(notice)
		pending[i] = nil // Sent bytes must not remain retained after credit release.
	}
	return true
}

// Caller owns objectSubscriptionRegistry.mu. Any previously queued updates are
// older than this gap; writeLoop emits that finite queue prefix before RESYNC.
func (r *objectSubscriptionRegistry) deferResyncLocked(sub *objectSubscription, u client.ObjectUpdate) bool {
	defer r.removeLocked(sub)
	u.Change = client.UpdateResyncRequired
	u.Data = nil
	u.Part, u.Parts = 0, 1
	body, err := u.MarshalBinary()
	if err != nil {
		sub.ws.close()
		return false
	}
	wire := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: client.ObjectUpdateMessage, ID: client.NewRequestID(), Body: body})
	if !sub.ws.subscriptionResync.put(sub.id, wire, &r.resyncCount) {
		// The active + pending admission limit makes this unreachable for a
		// valid session. Never silently discard an invalidation if corrupted
		// accounting or an unbounded fixture violates that invariant.
		sub.ws.close()
		return false
	}
	return false
}
