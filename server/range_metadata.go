// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/volume"
)

// Inventory cursors are not a volume-wide snapshot. Callers needing a closed
// write set retain their range/control fence. Release the volume between pages
// and before callbacks so background measurement cannot pin readers behind a
// queued writer for an entire volume scan.
func scanRangeRequests(ctx context.Context, store *volume.Volume, visit func(volume.RequestRecord) error) error {
	var after *volume.ID
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		records, err := h.ScanRequestRecords(after, 128)
		h.Close()
		if err != nil || len(records) == 0 {
			return err
		}
		for _, record := range records {
			if err := visit(record); err != nil {
				return err
			}
		}
		last := records[len(records)-1].ID
		after = &last
	}
}

func scanRangeReplicas(ctx context.Context, store *volume.Volume, visit func(volume.ReplicaRecord) error) error {
	var after *volume.ID
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := store.Open(false)
		if err != nil {
			return err
		}
		records, err := h.ScanReplicaRecords(after, 128)
		h.Close()
		if err != nil || len(records) == 0 {
			return err
		}
		for _, record := range records {
			if err := visit(record); err != nil {
				return err
			}
		}
		last := records[len(records)-1].Request.ID
		after = &last
	}
}
