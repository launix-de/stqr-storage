// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"errors"
	"math"
	"sort"

	"github.com/launix-de/stqr-storage/volume"
)

// saveBatch replaces existing descriptions with one catalog CAS. PG fanout must
// not re-read/rewrite an entire thousand-bucket catalog for every bucket. Staged
// descriptions remain unreachable until the new catalog root is durable.
// Callers pass the expected revision in each input description.
func (c *BucketCatalog) saveBatch(changes []BucketDescription) error {
	if len(changes) == 0 {
		return nil
	}
	for attempt := 0; attempt < 64; attempt++ {
		hashes, current, before, err := c.load()
		if err != nil {
			return err
		}
		indices := make(map[string]int, len(current))
		for i, d := range current {
			indices[d.ID] = i
		}
		replacements := make([]BucketDescription, len(changes))
		seen := make(map[string]bool, len(changes))
		for i, d := range changes {
			index, ok := indices[d.ID]
			if !ok {
				return ErrBucketNotFound
			}
			old := current[index]
			if seen[d.ID] || d.Revision != old.Revision {
				return ErrBucketConflict
			}
			seen[d.ID] = true
			if d.Revision == math.MaxUint64 {
				return ErrBucketInvalid
			}
			if !c.upgrading && (d.Policy.Version < old.Policy.Version || d.Policy != old.Policy && d.Policy.Version == old.Policy.Version) {
				return ErrBucketInvalid
			}
			d.Schema = 1
			d.Revision++
			d, err = normalizeBucketDescription(d)
			if err != nil {
				return err
			}
			current[index] = d
			replacements[i] = d
		}
		names := make(map[string]bool, len(current))
		for _, d := range current {
			if !d.DeletionComplete {
				if names[d.Name] {
					return ErrBucketConflict
				}
				names[d.Name] = true
			}
		}
		for _, d := range replacements {
			hash, err := c.persistDescription(d)
			if err != nil {
				return err
			}
			hashes[indices[d.ID]] = hash
		}
		sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
		if len(hashes) > volume.MaxBufferedRead/32 {
			return volume.ErrRange
		}
		list := make([]byte, 0, 32*len(hashes))
		for _, hash := range hashes {
			list = append(list, hash[:]...)
		}
		err = c.write(func(h *volume.Handle) error {
			_, err := h.CAS(BucketCatalogKey(), 0, before, list, volume.Options{WholeObject: true})
			return err
		})
		if errors.Is(err, volume.ErrPrecondition) {
			continue
		}
		if err != nil {
			return err
		}
		if c.onSave != nil {
			for _, d := range replacements {
				c.onSave(cloneBucketDescription(d))
			}
		}
		return nil
	}
	return ErrBucketConflict
}
