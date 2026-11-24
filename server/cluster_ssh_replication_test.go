// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func runSSHReplication(t *testing.T, f sshRecoveryFixture) {
	t.Helper()
	s, ctx := f.source, f.ctx
	deadline := time.Now().Add(15 * time.Second)
	for {
		dest, _ := s.scopes.destinations(BucketDescription{Policy: DefaultBucketPolicy()})
		if len(dest) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("remote volume statistics missing")
		}
		time.Sleep(25 * time.Millisecond)
	}
	group := volume.ID(client.NewRequestID())
	storage := storagePolicy(DefaultBucketPolicy())
	storage.Redundancy = BucketRedundancy{Mode: "replication", Replicas: 2, ACK: "all"}
	centralPGRequest(t, s, group, 0, PlacementMask{NodeSelection: "all", Storage: &storage})
	b := replicaTestBucket(t, s, group, "ssh-replicated")
	localGroup := volume.ID(client.NewRequestID())
	localStorage := storagePolicy(DefaultBucketPolicy())
	centralPGRequest(t, s, localGroup, 0, PlacementMask{NodeSelection: "list", Nodes: []PlacementNode{{NodeID: s.cfg.Node.ID, Mode: VolumesAll}}, Storage: &localStorage})
	other := replicaTestBucket(t, s, localGroup, "ssh-independent")
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := client.ObjectID{9}
	payload := bytes.Repeat([]byte("cluster-replica"), 32<<10)
	info, err := c.Put(ctx, id, payload, client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	checkRemote := func(want uint64) {
		t.Helper()
		e := client.Encoder{}
		e.ID(client.ID(b.ID()))
		e.B = append(e.B, id[:]...)
		e.U64(0)
		e.U64(0)
		e.ID(client.ID{})
		body, status, err := f.request(f.remote.Node.Advertise, client.Message{Kind: client.Request, Opcode: 0x0107, ID: client.NewRequestID(), Body: e.B})
		if err != nil || status != 200 {
			t.Fatalf("direct remote STAT %d %v", status, err)
		}
		d := client.Decoder{B: body}
		version, length := d.U64(), d.U64()
		if d.Err != nil || version == 0 || length != want {
			t.Fatalf("remote copy version=%d length=%d expected=%d", version, length, want)
		}
	}
	checkRemote(uint64(len(payload)))
	t.Log("ACKed PUT is directly readable on both hosts", info.Version)
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	script := "import os,signal\np=" + strconv.Quote(f.remoteDir) + "\npid=int(open(p+'/pid').read())\nassert os.readlink('/proc/%d/exe'%pid)==p+'/stqr'\nos.kill(pid,signal.SIGKILL)\n"
	if _, err = f.ssh(nil, "python3 -c "+quote(script)); err != nil {
		t.Fatal("crash isolated remote fixture", err)
	}
	request := client.NewRequestID()
	short, stop := context.WithTimeout(ctx, 400*time.Millisecond)
	_, err = c.Append(short, id, []byte("!"), client.AppendOptions{RequestID: request})
	stop()
	if err == nil {
		t.Fatal("write ACKed with required host offline")
	}
	data, _, err := c.Get(ctx, id)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatal("prepared write became visible", err)
	}
	if _, err = other.Put(ctx, userPrincipal(t, s), volume.ObjectID{1}, []byte("online"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("unrelated bucket blocked", err)
	}
	f.start()
	result, err := c.Append(ctx, id, []byte("!"), client.AppendOptions{RequestID: request})
	if err != nil || result.StartOffset != uint64(len(payload)) {
		t.Fatal("reconnect/retry", result, err)
	}
	retry, err := c.Append(ctx, id, []byte("!"), client.AppendOptions{RequestID: request})
	if err != nil || retry != result {
		t.Fatal("duplicate append after reconnect", err)
	}
	checkRemote(uint64(len(payload) + 1))
	t.Log("remote crash/restart preserved bytes; pending APPEND committed exactly once")
	// Stop the primary process locally. The existing SDK must use its learned
	// committed replica location for reads without assigning it write authority.
	f.http.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	readCtx, done := context.WithTimeout(ctx, 10*time.Second)
	defer done()
	data, _, err = c.Get(readCtx, id)
	if err != nil || !bytes.Equal(data, append(payload, '!')) {
		t.Fatal("read from replica with primary offline", err)
	}
	t.Log("SDK reads the remote replica while the primary is offline")
}
