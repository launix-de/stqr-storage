// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestAcceptedRecompressNeedsExplicitFence(t *testing.T) {
	dev, err := NewMemory(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(dev, FormatOptions{Class: HDD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	key := testKey(1)
	payload := bytes.Repeat([]byte("recompress"), 32<<10)
	if _, err = testPut(v, key, payload, Options{}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.RecompressWithOptions(key, Gzip, Options{Accepted: true})
	generation, genErr := h.Generation()
	h.Close()
	if err != nil || genErr != nil {
		t.Fatal(err, genErr)
	}
	if v.IOStats().SyncOps != before.SyncOps {
		t.Fatal("accepted recompression forced Sync")
	}
	mustGet(t, v, key, payload)
	live, err := v.Stat(key)
	if err != nil || live.Compression != Gzip {
		t.Fatal(live, err)
	}
	cold := reopen(t, v, dev)
	old, err := cold.Stat(key)
	if err != nil || old.Compression != Raw {
		t.Fatal("unfenced codec overwrote durable checkpoint", old, err)
	}
	if err = v.CheckpointThrough(generation); err != nil {
		t.Fatal(err)
	}
	durable := reopen(t, v, dev)
	current, err := durable.Stat(key)
	if err != nil || current.Compression != Gzip {
		t.Fatal("explicit fence lost encoding", current, err)
	}
	mustGet(t, durable, key, payload)
	if v.IOStats().SyncOps-before.SyncOps != 4 {
		t.Fatal("expected one explicit four-barrier checkpoint")
	}
}
