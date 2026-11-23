// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/binary"
	"github.com/launix-de/stqr-storage/client"
)

// Immutable telemetry avoids taking the range lock under CollectStats' node
// lock. It describes durable participant state, not a speculative election.
type rangeActivity struct {
	Active      uint64
	Coordinator client.ID
	Round       client.RoundID
}

func (r *rangeManager) publishActivityLocked() {
	status := &rangeActivity{}
	for _, id := range r.order {
		txn := r.txns[id]
		if !r.active(txn) {
			continue
		}
		status.Active++
		if status.Active != 1 {
			continue
		}
		attempt := binary.LittleEndian.Uint64(txn.plan.Attempt[:8])
		if attempt == 0 {
			attempt = binary.LittleEndian.Uint64(id[:8])
		}
		status.Coordinator = txn.plan.Source
		status.Round = client.RoundID{Membership: txn.plan.Before.Membership, Generation: txn.plan.Before.Generation, Initiator: txn.plan.Source, Attempt: max(1, attempt)}
	}
	r.activity.Store(status)
	r.server.notifyStats()
}
