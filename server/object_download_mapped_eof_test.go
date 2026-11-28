// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestMappedDownloadRejectsOffsetPastEOF(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{127}
	if _, err := writer.Put(ctx, id, []byte("end"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	// Nil socket ensures an invalid range cannot begin a success frame.
	ws := &wsSession{server: s}
	op := &objectMutation{bucket: b, object: volume.ObjectID(id), offset: 4}
	if sent, _, err := ws.writeMappedObject(ctx, client.Message{}, userPrincipal(t, s), op, 1, nil, client.ID{}); sent || !errors.Is(err, volume.ErrRange) {
		t.Fatal("out-of-range object read became empty success", sent, err)
	}
	_, _, rangeErr := reader.GetRange(ctx, id, 4, 1)
	var status *client.StatusError
	if !errors.As(rangeErr, &status) || status.Status != 400 {
		t.Fatal("RPC lost range error", rangeErr)
	}
	if got, _, err := reader.GetRange(ctx, id, 3, 1); err != nil || len(got) != 0 {
		t.Fatal("exact EOF must remain an empty read", len(got), err)
	}
}
