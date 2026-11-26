// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestLargeMutationAfterRejectedBufferedWrite(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "empty-buffer", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "empty-buffer")
	p := quotaPrincipal(t, s)
	id := volume.ObjectID{31}
	if _, err := b.Put(context.Background(), p, id, []byte("seed"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	v := b.allVolumes()[0]
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	// A rejected conditional buffered write reserves a group but publishes no
	// new generation. It is valid for its current generation to be durable.
	_, err = h.Put(volume.Key{Bucket: b.ID(), Object: id}, []byte("rejected"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: true, CreateOnly: true})
	h.Close()
	if !errors.Is(err, volume.ErrPrecondition) {
		t.Fatal("expected rejected buffered write", err)
	}
	payload := bytes.Repeat([]byte{0x72}, 2<<20)
	g, pending := v.PendingCheckpoint(false, uint64(len(payload)))
	if !pending || g == 0 || !v.IsDurableThrough(g) {
		t.Fatal("fixture lacks empty group on durable generation", g, pending)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err = b.Put(ctx, p, id, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("large write did not drain empty buffered group", err)
	}
	got, _, err := b.Get(ctx, p, id)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("large write payload", len(got), err)
	}
}
