// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type sshRecoveryFixture struct {
	ctx                               context.Context
	source                            *Server
	http                              *http.Server
	remote                            Config
	remoteDir, sshTarget, localVolume string
	remoteVolume                      client.ID
	bucket                            *Bucket
	request                           func(string, client.Message) ([]byte, int, error)
	ssh                               func([]byte, string) ([]byte, error)
	start                             func()
}

// Move only the stopped source's data file to the other machine. No config,
// admin volume or private source journal follows it. The remote process must
// validate its own persisted target cancellation and complete the takeover.
func runSSHRecoveryFence(t *testing.T, f sshRecoveryFixture) {
	t.Helper()
	s, ctx := f.source, f.ctx
	admin := s.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(f.bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	object := client.ObjectID{0x81}
	payload := bytes.Repeat([]byte{0x57}, 1<<20)
	if _, err := c.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.routingPatch(f.bucket.ID(), volume.ObjectID(object))
	_, moving, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	remoteID, _ := parseID(f.remote.Node.ID)
	plan, err := planRangeMove(before, moving, client.ID(remoteID), volume.ID(f.remoteVolume), uint8(volume.HDD), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := s.scopes
	if err := r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, err := r.call(ctx, plan.Target, peerRangeReserve, mustRangeWire(plan)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal(err)
	}
	retryID := client.NewRequestID()
	info, err := c.Append(ctx, object, []byte("acknowledged after bulk"), client.AppendOptions{RequestID: retryID})
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, []byte("acknowledged after bulk")...)
	c.Close()
	if err := r.fence(plan.id()); err != nil {
		t.Fatal(err)
	}
	epoch := s.currentSystem.Load().manifest.Membership.Epoch
	path := s.volumes[f.localVolume].config.Path
	sourceID := s.control.ledger.node
	f.http.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("local source stopped after FENCE; remote target has no READY")
	e := client.Encoder{}
	e.V(1)
	e.ID(sourceID)
	e.Bool(false)
	e.Bool(true)
	e.U64(epoch)
	if _, status, err := f.request(f.remote.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.NodeRecoveryBegin, ID: client.NewRequestID(), Body: e.B}); err != nil || status != 200 {
		t.Fatal("remote recovery begin", status, err)
	}
	recoveredPath := f.remoteDir + "/rescued-source"
	if out, err := exec.CommandContext(ctx, "scp", path, f.sshTarget+":"+recoveredPath).CombinedOutput(); err != nil {
		t.Fatalf("move stopped data volume: %v %s", err, out)
	}
	e = client.Encoder{}
	e.Text("rescued-source")
	e.Text(recoveredPath)
	if _, status, err := f.request(f.remote.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.VolumeRecoveryStage, ID: client.NewRequestID(), Body: e.B}); err != nil || status != 201 {
		t.Fatal("remote stage", status, err)
	}
	e = client.Encoder{}
	e.U64(epoch + 1)
	e.V(1)
	volumeID, _ := parseID(f.localVolume)
	e.ID(client.ID(volumeID))
	if _, status, err := f.request(f.remote.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.VolumeRecoveryCommit, ID: client.NewRequestID(), Body: e.B}); err != nil || status != 200 {
		t.Fatal("remote restore", status, err)
	}
	check := func() {
		t.Helper()
		c, err := client.Dial(ctx, f.remote.Node.Advertise, admin.Username, admin.Password, client.BucketID(f.bucket.ID()))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		got, current, err := c.Get(ctx, object)
		if err != nil || !bytes.Equal(got, payload) || current.Version != info.Version {
			t.Fatal("remote recovered bytes/version", err)
		}
		if result, err := c.Append(ctx, object, []byte("acknowledged after bulk"), client.AppendOptions{RequestID: retryID}); err != nil || result.Version != info.Version {
			t.Fatal("remote retry receipt", err)
		}
	}
	check()
	// Restart with only the survivor's config and journals, then verify again.
	script := "import os,signal,time\np=" + strconv.Quote(f.remoteDir) + "\npid=int(open(p+'/pid').read())\nos.kill(pid,signal.SIGTERM)\nfor i in range(200):\n if not os.path.exists('/proc/%d/exe'%pid): break\n time.sleep(.05)\nelse: raise RuntimeError('fixture did not stop')\n"
	if _, err := f.ssh([]byte(script), "python3 -"); err != nil {
		t.Fatal("remote recovery restart", err)
	}
	f.start()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.remote.Node.Advertise+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			response.Body.Close()
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	check()
	// Source remains physically stopped and absent from survivor membership.
	body, status, err := f.request(f.remote.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.NodeConfigGet, ID: client.NewRequestID()})
	if err != nil || status != 200 {
		t.Fatal("missing persisted source exclusion", status, err)
	}
	d := client.Decoder{B: body}
	var state struct {
		Members  []string `json:"member_nodes"`
		Excluded []string `json:"excluded_nodes"`
	}
	if json.Unmarshal([]byte(d.Text()), &state) != nil || d.Done() != nil || len(state.Members) != 1 || state.Members[0] != f.remote.Node.ID {
		t.Fatal("survivor membership changed during recovery restart")
	}
	found := false
	for _, id := range state.Excluded {
		found = found || id == sourceID.String()
	}
	if !found {
		t.Fatal("source exclusion lost on restart")
	}
	t.Log("cross-host rescue preserved final APPEND/version/retry across restart using only the moved data volume")
}
