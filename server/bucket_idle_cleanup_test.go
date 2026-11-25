// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
	"time"
)

func TestDeletingBucketDoesNotHoldGenerationAndRespectsIOPriorities(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	src := addReplicaDataVolume(t, source.node, "idle-delete-data", "hdd", 8<<20)
	addReplicaDataVolume(t, target.node, "idle-peer-data", "hdd", 16<<20)
	doomed, _ := createProtocolBucket(t, source.node, "idle-doomed")
	live, _ := createProtocolBucket(t, source.node, "idle-live")
	actor := userPrincipal(t, source.node)
	ctx := context.Background()
	for i := 0; i < 24; i++ {
		if _, err := doomed.Put(ctx, actor, volume.ObjectID{byte(i)}, bytes.Repeat([]byte("x"), 4096), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []*Server{source.node, target.node} {
		n.ioPriority.rebuilds.Store(1)
		defer n.ioPriority.rebuilds.Store(0)
		defer n.ioPriority.users.Store(0)
	}
	e := client.Encoder{}
	e.ID(client.ID(doomed.ID()))
	if _, status, _ := adminBucketMessage(t, source.node, client.BucketDelete, e.B); status != 202 {
		t.Fatal("delete intent", status)
	}
	if _, _, err := doomed.Get(ctx, actor, volume.ObjectID{0}); err == nil {
		t.Fatal("deleted namespace remains reachable")
	}
	for _, n := range []*Server{source.node, target.node} {
		l := n.control.ledger
		l.mu.Lock()
		busy := l.slot != nil || l.request != nil
		l.mu.Unlock()
		if busy {
			t.Fatal("delete cleanup retained generation slot")
		}
		if n.reclaimBucketStep() {
			t.Fatal("priority 3 ran during priority 2")
		}
		n.ioPriority.users.Store(1)
		n.ioPriority.rebuilds.Store(0)
		if n.reclaimBucketStep() {
			t.Fatal("priority 3 ran during priority 1")
		}
	}
	// A new catalog generation is allowed while the previous delete remains
	// physically incomplete; its immutable catalog carries DELETING forward.
	createProtocolBucket(t, source.node, "idle-next-generation")
	d, err := source.node.Bucket(doomed.ID())
	if err != nil || d.Describe().DeletionComplete {
		t.Fatal("pending delete lost from next generation", err)
	}
	if _, err := live.Put(ctx, actor, volume.ObjectID{77}, []byte("foreground"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("unrelated write blocked", err)
	}
	got, _, err := live.Get(ctx, actor, volume.ObjectID{77})
	if err != nil || string(got) != "foreground" {
		t.Fatal("unrelated read", err)
	}
	// Even an idle reclaimer must skip a read-locked volume rather than wait.
	source.node.ioPriority.users.Store(0)
	h, err := src.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() { done <- source.node.reclaimBucketStep() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		h.Close()
		t.Fatal("idle cleanup queued behind foreground read lock")
	}
	h.Close()
	target.node.ioPriority.users.Store(0)
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := source.node.Bucket(doomed.ID())
		if err == ErrBucketNotFound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle cleanup did not finish", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
