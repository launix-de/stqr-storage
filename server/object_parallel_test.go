// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestObjectRequestsOnOneSocketUseIndependentVolumeLocks(t *testing.T) {
	for _, size := range []int{5, 2 << 20} {
		name := "inline"
		if size > 1<<20 {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) { testIndependentVolumeQueries(t, bytes.Repeat([]byte{'x'}, size)) })
	}
}

func testIndependentVolumeQueries(t *testing.T, payload []byte) {
	node := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil, false)
	s := node.node
	leftVolume := addReplicaDataVolume(t, s, "left", "hdd", 8<<20)
	rightVolume := addReplicaDataVolume(t, s, "right", "hdd", 8<<20)
	bucket, _ := createProtocolBucket(t, s, "parallel-volumes")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	left, right := client.ObjectID{1}, client.ObjectID{0x81}
	for _, id := range []client.ObjectID{left, right} {
		if _, err := c.Put(ctx, id, []byte("before"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := s.routingPatch(bucket.ID(), volume.ObjectID(right))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	p, err := planRangeMove(before, moving, s.control.ledger.node, rightVolume.Identity().VolumeID, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.scopes.move(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, id := range []client.ObjectID{left, right} {
		if _, _, err := c.Get(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	// The left query blocks on an actual volume lock, not a test-only sleep.
	hold, err := leftVolume.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	done := make(chan error, 1)
	go func() { _, err := c.Put(ctx, left, payload, client.PutOptions{}); done <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for s.ioPriority.users.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("blocked write did not reach storage")
		}
		time.Sleep(time.Millisecond)
	}
	read, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	data, _, err := c.Get(read, right)
	if err != nil || string(data) != "before" {
		t.Fatal("independent query queued behind another volume", string(data), err)
	}
	if err := hold.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
