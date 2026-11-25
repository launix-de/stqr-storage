// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/launix-de/stqr-storage/client"
	"testing"
	"time"
)

// Check planning after each write so the result is independent of the worker
// ticker and host speed. Both nodes, Sync and complete readback are real.
func TestCapacityReserveAvoidsRepeatedSmallHandoffs(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addReplicaDataVolume(t, source.node, "overflow-small", "hdd", 8<<20)
	addReplicaDataVolume(t, target.node, "overflow-large", "hdd", 38<<20)
	reviewWaitDest(t, source.node, 2)
	b, _ := createProtocolBucket(t, source.node, "overflow-import")
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(b.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte("bulk"), 32<<10)
	for i := 0; i < 256; i++ {
		id := client.ObjectID(sha256.Sum256([]byte{byte(i), byte(i >> 8)}))
		if _, err = c.CAS(ctx, id, 0, nil, payload, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID(), Absent: true, Accepted: true}}); err != nil {
			logDaemonRangeState(t, source, target)
			t.Fatal("overflow object", i, err)
		}
		for {
			p, ok := source.node.scopes.planNext()
			if !ok {
				break
			}
			if err := source.node.scopes.move(ctx, p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		id := client.ObjectID(sha256.Sum256([]byte{byte(i), byte(i >> 8)}))
		got, _, err := c.Get(ctx, id)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("overflow readback", i, err)
		}
	}
	source.node.scopes.mu.Lock()
	moves := 0
	for _, tx := range source.node.scopes.txns {
		if tx.plan.Before.Bucket == client.ID(b.ID()) && tx.plan.Source == source.node.control.ledger.node {
			moves++
		}
	}
	source.node.scopes.mu.Unlock()
	t.Logf("32 MiB import including Sync/readback: elapsed=%s handoffs=%d", time.Since(start), moves)
	if moves > 6 {
		t.Fatalf("reserve recovery churned through %d small handoffs", moves)
	}
}
