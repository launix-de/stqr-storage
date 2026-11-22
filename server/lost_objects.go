// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func (b *Bucket) lostRange(object volume.ObjectID) bool {
	s := b.server
	if s.controller == nil {
		return false
	}
	entry, known := s.placement.Load().lookup(client.ID(b.ID()), client.ObjectID(object))
	return known && entry.State == 5
}

// Missing records in a permanently lost range are unknown data, rather than
// proof of absence. A full PUT restores one complete object; an explicit DELETE
// establishes known absence. Their new canonical records allow normal access.
// Partial create operations must not turn a lost object into a short fragment.
func (b *Bucket) absenceError(object volume.ObjectID, info volume.Info, err error) error {
	if errors.Is(err, volume.ErrNotFound) && info.Version == 0 && b.lostRange(object) {
		return fail(410, "object state was lost")
	}
	return err
}
