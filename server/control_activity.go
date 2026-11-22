// SPDX-License-Identifier: GPL-3.0-only
package server

import "github.com/launix-de/stqr-storage/client"

// Stats must not take the ledger mutex under the node/config mutex. Journal
// replay publishes this tiny immutable value; it does not call into the server.
type controlActivity struct {
	Busy        bool
	Coordinator client.ID
	Round       client.RoundID
}

func (l *controlLedger) publishControlActivity() {
	activity := &controlActivity{}
	var claim *controlClaim
	if l.slot != nil {
		claim = &l.slot.Claim
	} else if l.request != nil {
		claim = &l.request.Claim
	}
	if claim != nil {
		activity.Busy = true
		activity.Coordinator = claim.Round.Initiator
		activity.Round = claim.Round
	}
	l.activity.Store(activity)
}
