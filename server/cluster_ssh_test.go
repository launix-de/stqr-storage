// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"gopkg.in/yaml.v3"
)

// Opt-in acceptance against a second Linux host. Both nodes and all volumes
// belong to this test; no existing daemon, PM2 entry or user data is touched.
// STQR_TEST_BINARY must be built from the same source as this test binary.
func TestSSHClusterAcceptance(t *testing.T) {
	runSSHClusterAcceptance(t, false)
}

func TestSSHRecoveryFenceClearance(t *testing.T) {
	runSSHClusterAcceptance(t, false, true)
}

func TestSSHPM2ClusterAcceptance(t *testing.T) {
	if os.Getenv("STQR_TEST_PM2") != "1" {
		t.Skip("set STQR_TEST_PM2=1 to use a disposable PM2 entry on the SSH host")
	}
	runSSHClusterAcceptance(t, true)
}

func runSSHClusterAcceptance(t *testing.T, usePM2 bool, recovery ...bool) {
	t.Helper()
	sshTarget := os.Getenv("STQR_TEST_SSH")
	if sshTarget == "" {
		t.Skip("set STQR_TEST_SSH, STQR_TEST_LOCAL_IP, STQR_TEST_REMOTE_IP and STQR_TEST_BINARY")
	}
	localIP, remoteIP, binary := os.Getenv("STQR_TEST_LOCAL_IP"), os.Getenv("STQR_TEST_REMOTE_IP"), os.Getenv("STQR_TEST_BINARY")
	if net.ParseIP(localIP) == nil || net.ParseIP(remoteIP) == nil || binary == "" || strings.HasPrefix(sshTarget, "-") {
		t.Fatal("invalid SSH acceptance configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	ssh := func(input []byte, script string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", sshTarget, script)
		cmd.Stdin = bytes.NewReader(input)
		return cmd.Output()
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	remoteDir := "/tmp/stqr-acceptance-" + newID()
	pm2Name := filepath.Base(remoteDir)
	if _, err := ssh(nil, "umask 077; mkdir "+quote(remoteDir)); err != nil {
		t.Fatal("remote fixture directory", err)
	}
	// Cleanup uses a fresh context even when the test's own deadline expires.
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		script := "import os,signal,shutil,time\np=" + strconv.Quote(remoteDir) + "\nf=p+'/pid'\nif os.path.exists(f):\n pid=int(open(f).read())\n try:\n  if os.readlink('/proc/%d/exe'%pid)==p+'/stqr': os.kill(pid,signal.SIGTERM)\n except FileNotFoundError: pass\n time.sleep(1)\n try:\n  if os.readlink('/proc/%d/exe'%pid)==p+'/stqr': os.kill(pid,signal.SIGKILL)\n except FileNotFoundError: pass\nshutil.rmtree(p)\n"
		command := "python3 -c " + quote(script)
		if usePM2 {
			// Delete only this test's unique entry, without saving the process list.
			command = "pm2 delete " + quote(pm2Name) + " >/dev/null\n" + command
		}
		cmd := exec.CommandContext(cleanupCtx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", sshTarget, command)
		if err := cmd.Run(); err != nil {
			t.Errorf("remote fixture cleanup: %v", err)
		}
	})
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ssh(data, "cat > "+quote(remoteDir+"/stqr")+" && chmod 700 "+quote(remoteDir+"/stqr")); err != nil {
		t.Fatal("copy candidate binary", err)
	}
	portRaw, err := ssh(nil, "python3 -c 'import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1])'")
	if err != nil {
		t.Fatal(err)
	}
	remotePort, err := strconv.Atoi(strings.TrimSpace(string(portRaw)))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "local.yaml")
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path, Options{})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	cfg.Node.Listen = ln.Addr().String()
	cfg.Node.Advertise = "http://" + net.JoinHostPort(localIP, strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	cfg.AllowSubnets = []string{"127.0.0.0/8", "::1/128", remoteIP + "/32", localIP + "/32"}
	if err := saveConfig(path, cfg); err != nil {
		ln.Close()
		t.Fatal(err)
	}
	s, err := Open(Options{ConfigPath: path, manualRanges: len(recovery) > 0 && recovery[0]})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	h := &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	t.Cleanup(func() { h.Close(); s.Close() })
	peer := copyConfig(s.cfg)
	peer.Node = NodeConfig{ID: newID(), Name: "ssh-acceptance", Listen: "0.0.0.0:" + strconv.Itoa(remotePort), Advertise: "http://" + net.JoinHostPort(remoteIP, strconv.Itoa(remotePort)), DataDir: "."}
	peer.VolumesDir = "volumes"
	peer.Volumes = nil
	peer.Peers = []PeerConfig{{NodeID: s.cfg.Node.ID, URL: s.cfg.Node.Advertise}}
	peer.ControlVersion, peer.RangeVersion, peer.QuotaVersion, peer.PeerDirectoryVersion, peer.PlacementGroupsVersion, peer.BucketCatalogVersion = 0, 0, 0, 0, 0, 0
	peer.JoinPending = true
	peer.AllowSubnets = cfg.AllowSubnets
	raw, err := yaml.Marshal(peer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ssh(raw, "cat > "+quote(remoteDir+"/stqr.yaml")); err != nil {
		t.Fatal("remote config", err)
	}
	start := func() {
		t.Helper()
		if usePM2 {
			config, err := json.Marshal(map[string]any{"apps": []any{map[string]any{
				"name": pm2Name, "script": remoteDir + "/stqr", "cwd": remoteDir,
				"args": []string{"--config", remoteDir + "/stqr.yaml"}, "interpreter": "none",
				"autorestart": true, "stop_exit_codes": []int{0}, "kill_timeout": 30000,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			pid := "import json,sys\napps=json.load(sys.stdin)\na=next(a for a in apps if a['name']==" + strconv.Quote(pm2Name) + ")\nassert a['pid']>0\nopen(" + strconv.Quote(remoteDir+"/pid") + ",'w').write(str(a['pid']))\n"
			command := "set -eu\ncat > " + quote(remoteDir+"/ecosystem.json") + "\npm2 start " + quote(remoteDir+"/ecosystem.json") + " --only " + quote(pm2Name) + " > " + quote(remoteDir+"/pm2.log") + "\npm2 jlist | python3 -c " + quote(pid)
			if _, err := ssh(config, command); err != nil {
				t.Fatal("temporary PM2 start", err)
			}
			return
		}
		// Explicitly close SSH's descriptors. Backgrounding a compound shell
		// command can retain the SSH channel until the daemon itself exits.
		script := "import subprocess,os\np=" + strconv.Quote(remoteDir) + "\nwith open(p+'/server.log','ab') as log:\n child=subprocess.Popen([p+'/stqr','--config',p+'/stqr.yaml'],cwd=p,stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True,close_fds=True)\nwith open(p+'/pid','w') as f: f.write(str(child.pid))\n"
		if _, err := ssh(nil, "python3 -c "+quote(script)); err != nil {
			t.Fatal("remote start", err)
		}
	}
	start()
	peerID, _ := parseID(peer.Node.ID)
	joinDeadline := time.Now().Add(30 * time.Second)
	for {
		view := s.currentSystem.Load()
		if view != nil && hasControlNode(membershipIDs(view.manifest.Membership), client.ID(peerID)) {
			break
		}
		if ctx.Err() != nil || time.Now().After(joinDeadline) {
			s.peerStatsMu.RLock()
			for id, o := range s.peerStats {
				t.Log("peer", id, "online", o.online)
			}
			s.peerStatsMu.RUnlock()
			if s.control != nil {
				s.control.ledger.mu.Lock()
				t.Log("control slot", s.control.ledger.slot != nil, "request", s.control.ledger.request != nil)
				s.control.ledger.mu.Unlock()
			}
			t.Fatal("remote join did not complete", ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Log("remote node joined the committed membership")
	admin := s.cfg.Auth.BootstrapAdmin
	request := func(endpoint string, m client.Message) ([]byte, int, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/message", bytes.NewReader(client.EncodeMessage(m)))
		if err != nil {
			return nil, 0, err
		}
		r.SetBasicAuth(admin.Username, admin.Password)
		r.Header.Set("Content-Type", "application/octet-stream")
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			return nil, 0, err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, client.MaxMessage))
		if err != nil {
			return nil, 0, err
		}
		reply, err := client.DecodeMessage(raw)
		if err != nil {
			return nil, response.StatusCode, err
		}
		d := client.Decoder{B: reply.Body}
		status := int(d.V())
		d.Blob()
		if d.Err != nil {
			return nil, status, d.Err
		}
		return d.B, status, nil
	}
	localVolume := addTestVolume(t, s, "local-data", "hdd", 4<<20)
	e := client.Encoder{}
	e.Text("remote-data")
	e.U8(0)
	e.Text("")
	e.U8(0)
	e.U64(4 << 20)
	wire, err := (client.NodeAdmin{Target: client.ID(peerID), Opcode: client.VolumeAdd, Body: e.B}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	added, status, err := request(s.cfg.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: client.NewRequestID(), Body: wire})
	if err != nil || status >= 300 {
		t.Fatal("remote volume add through local node", status, err)
	}
	t.Log("remote volume added through local node")
	bucket, _ := createProtocolBucket(t, s, "ssh-extension")
	if len(recovery) > 0 && recovery[0] {
		d := client.Decoder{B: added}
		d.ID()
		d.U8()
		if !d.Bool() {
			t.Fatal("remote volume identity missing")
		}
		remoteVolume := d.ID()
		if d.Done() != nil {
			t.Fatal("invalid remote volume reply")
		}
		runSSHRecoveryFence(t, sshRecoveryFixture{ctx: ctx, source: s, http: h, remote: peer, remoteDir: remoteDir, sshTarget: sshTarget, bucket: bucket, localVolume: localVolume.VolumeID, remoteVolume: remoteVolume, request: request, ssh: ssh, start: start})
		return
	}
	c, err := client.Dial(ctx, s.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{0x56}, 1<<20)
	versions := make([]uint64, 6)
	for i := 0; i < 6; i++ {
		info, err := c.Put(ctx, client.ObjectID{byte(i*41 + 1)}, payload, client.PutOptions{})
		if err != nil {
			t.Fatal("cross-host extension", i, err)
		}
		versions[i] = info.Version
	}
	t.Log("extension wrote six MiB")
	remoteClient, err := client.Dial(ctx, peer.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer remoteClient.Close()
	for i := 0; i < 6; i++ {
		data, info, err := remoteClient.Get(ctx, client.ObjectID{byte(i*41 + 1)})
		if err != nil || !bytes.Equal(data, payload) || info.Version != versions[i] {
			t.Fatal("cross-host read", i, err)
		}
	}
	// Confirm both real hosts hold payload, not just two advertised identities.
	s.peerStatsMu.RLock()
	observed := s.peerStats[client.ID(peerID)]
	var remotePayload uint64
	if observed != nil && observed.snapshot != nil {
		for _, v := range observed.snapshot.Volumes {
			remotePayload += v.PayloadPhysical
		}
	}
	s.peerStatsMu.RUnlock()
	if remotePayload < 1<<20 {
		t.Fatal("remote payload was not observed", remotePayload)
	}
	q := client.Encoder{}
	q.ID(client.ID(bucket.ID()))
	q.U8(0)
	q.U64(0)
	q.Bool(false)
	q.U64(6 << 20)
	q.U32(0)
	q.U64(bucket.Describe().Policy.Version)
	quotaMessage := client.Message{Kind: client.Request, Opcode: client.BucketSetQuota, ID: client.NewRequestID(), Body: q.B}
	for {
		_, status, err := request(peer.Node.Advertise, quotaMessage)
		if err != nil {
			t.Fatal("cross-host quota", err)
		}
		if status == 200 {
			break
		}
		if status != 503 || ctx.Err() != nil {
			t.Fatal("cross-host quota", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := c.Append(ctx, client.ObjectID{1}, []byte{1}, client.AppendOptions{}); !errors.Is(err, client.ErrFull) {
		t.Fatal("cross-host hard quota", err)
	}
	t.Log("finite quota counts both hosts and rejects excess growth")
	// SIGTERM exercises the production daemon's SHORT path. Restart the remote
	// process with the same directory and verify unchanged versions and bytes.
	if _, err := ssh(nil, "kill -TERM $(cat "+quote(remoteDir+"/pid")+")"); err != nil {
		t.Fatal("remote SIGTERM", err)
	}
	for {
		out, err := ssh(nil, "python3 -c "+quote("import os; p=int(open("+strconv.Quote(remoteDir+"/pid")+").read()); print(os.path.exists('/proc/%d/exe'%p))"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(out)) == "False" {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	start()
	for i := 0; i < 6; i++ {
		data, info, err := c.Get(ctx, client.ObjectID{byte(i*41 + 1)})
		if err != nil || !bytes.Equal(data, payload) || info.Version != versions[i] {
			t.Fatal("read after remote restart", i, err)
		}
	}
	// Stop both hosts through the native CLUSTER command. Start the former
	// joining node first; --connect is not required again and no new cluster
	// may bootstrap while the original initiator is still offline.
	stop := client.Encoder{}
	stop.U8(2)
	if _, status, err := request(cfg.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.ClusterShutdown, ID: client.NewRequestID(), Body: stop.B}); err != nil || status != 202 {
		t.Fatal("two-host cluster shutdown", status, err)
	}
	select {
	case <-s.stopRequest:
	case <-ctx.Done():
		t.Fatal("local cluster STOP", ctx.Err())
	}
	for {
		out, err := ssh(nil, "python3 -c "+quote("import os; p=int(open("+strconv.Quote(remoteDir+"/pid")+").read()); print(os.path.exists('/proc/%d/exe'%p))"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(out)) == "False" {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("remote cluster STOP", ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	start()
	// Confirm the returning former joiner is serving before its old seed.
	for {
		probe, err := http.NewRequestWithContext(ctx, http.MethodGet, peer.Node.Advertise+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(probe)
		if err == nil {
			response.Body.Close()
			break
		}
		if ctx.Err() != nil {
			t.Fatal("reverse restart", ctx.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	ln, err = net.Listen("tcp", cfg.Node.Listen)
	if err != nil {
		t.Fatal(err)
	}
	s, err = Open(Options{ConfigPath: path})
	if err != nil {
		ln.Close()
		t.Fatal("local cluster restart", err)
	}
	h = &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	for i := 0; i < 6; i++ {
		data, info, err := c.Get(ctx, client.ObjectID{byte(i*41 + 1)})
		if err != nil || !bytes.Equal(data, payload) || info.Version != versions[i] {
			t.Fatal("data after complete cluster restart", i, err)
		}
	}
	t.Log("complete cluster STOP and reverse startup preserved membership, quota and objects")
	// The remote config owns only its local volumes; validate it without
	// printing the credentials stored beside those records.
	configRaw, err := ssh(nil, "cat "+quote(remoteDir+"/stqr.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Config
	if err := yaml.Unmarshal(configRaw, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Volumes) != 2 {
		t.Fatal("remote local-only volume config", len(persisted.Volumes))
	}
	for _, v := range persisted.Volumes {
		if !strings.HasPrefix(v.Path, remoteDir+"/") {
			t.Fatal("foreign volume path in remote config")
		}
	}
	// Grow the surviving file, then evacuate and detach the remote volume.
	// Full quota does not require extra logical credits to move existing bytes.
	if op, err := s.GrowVolume(newID(), localVolume.VolumeID, 8<<20); err != nil || op.State != "fulfilled" {
		t.Fatal("survivor growth", err)
	}
	var remoteVolume VolumeConfig
	for _, v := range persisted.Volumes {
		if !v.System {
			remoteVolume = v
		}
	}
	remoteVolumeID, err := parseID(remoteVolume.ID)
	if err != nil {
		t.Fatal(err)
	}
	remove := client.Encoder{}
	remove.ID(client.ID(remoteVolumeID))
	wire, err = (client.NodeAdmin{Target: client.ID(peerID), Opcode: client.VolumeRemove, Body: remove.B}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, status, err := request(cfg.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: client.NewRequestID(), Body: wire}); err != nil || status >= 300 {
		t.Fatal("remote volume drain", status, err)
	}
	for {
		raw, err := ssh(nil, "cat "+quote(remoteDir+"/stqr.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var latest Config
		if err := yaml.Unmarshal(raw, &latest); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, v := range latest.Volumes {
			if v.ID == remoteVolume.ID {
				found = true
			}
		}
		if !found {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("remote volume did not drain", ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("remote volume evacuated and detached")
	leave := client.Encoder{}
	leave.ID(client.ID(peerID))
	wire, err = (client.NodeAdmin{Target: client.ID(peerID), Opcode: client.NodeDrain, Body: leave.B}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, status, err := request(cfg.Node.Advertise, client.Message{Kind: client.Request, Opcode: client.NodeAdminRequest, ID: client.NewRequestID(), Body: wire}); err != nil || status >= 300 {
		t.Fatal("remote node drain", status, err)
	}
	for {
		view := s.currentSystem.Load()
		if view != nil && len(view.manifest.Membership.Members) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("remote membership leave", ctx.Err())
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < 6; i++ {
		data, info, err := remoteClient.Get(ctx, client.ObjectID{byte(i*41 + 1)})
		if err != nil || !bytes.Equal(data, payload) || info.Version != versions[i] {
			t.Fatal("read through old seed after node drain", i, err)
		}
	}
	t.Log("two-host acceptance passed: extension, finite quota, remote administration, SHORT/CLUSTER restart, volume evacuation, node departure and existing client rerouting")
}
