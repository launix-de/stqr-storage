// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

func joinMessage(t *testing.T, address, user, password string, opcode uint16, body []byte) (int, []byte) {
	t.Helper()
	m := client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: body}
	req, err := http.NewRequest(http.MethodPost, address+"/api/message", bytes.NewReader(client.EncodeMessage(m)))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, password)
	hc := &http.Client{Timeout: 20 * time.Second}
	res, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return res.StatusCode, raw
	}
	reply, err := client.DecodeMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	d := client.Decoder{B: reply.Body}
	status := int(d.V())
	d.Blob()
	return status, d.B
}
func readJoinConfig(t *testing.T, path string) Config {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c Config
	if err = yaml.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}
func startJoinRun(t *testing.T, volumes int) (string, Config, string, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	s, err := Open(Options{ConfigPath: path, values: map[string]string{"listen": address, "advertise": "http://" + address, "volumes-dir": filepath.Join(t.TempDir(), "data-volumes")}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < volumes; i++ {
		addTestVolume(t, s, "prepared-"+string(rune('a'+i)), "hdd", 8<<20)
	}
	c := copyConfig(s.cfg)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Options{ConfigPath: path}, io.Discard) }()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(25 * time.Second):
			t.Error("Run did not stop")
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, e := http.Get("http://" + address + "/")
		if e == nil {
			res.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return path, c, "http://" + address, func() { cancel() }
}
func awaitJoinedLogin(t *testing.T, address, user, password string) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodPost, address+"/api/session", nil)
		req.SetBasicAuth(user, password)
		res, err := (&http.Client{Timeout: time.Second}).Do(req)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("joined account did not become available", err)
		}
		time.Sleep(40 * time.Millisecond)
	}
}
func TestDashboardClusterJoinPreparedVolumes(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			source := startDaemonTestNode(t, filepath.Join(t.TempDir(), "source.yaml"), nil)
			addTestVolume(t, source.node, "source-data", "hdd", 16<<20)
			bucket, _ := createProtocolBucket(t, source.node, "existing-data")
			actor := userPrincipal(t, source.node)
			oid := volume.ObjectID{9}
			if _, err := bucket.Put(context.Background(), actor, oid, []byte("existing cluster content"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			path, before, address, _ := startJoinRun(t, count)
			local := before.Auth.BootstrapAdmin
			remote := source.node.cfg.Auth.BootstrapAdmin
			status, raw := joinMessage(t, address, local.Username, local.Password, clusterJoinStatus, nil)
			d := client.Decoder{B: raw}
			if status != 200 || !d.Bool() {
				t.Fatal("not eligible", status, string(raw))
			}
			encode := func(password string) []byte {
				e := client.Encoder{}
				e.Text(source.node.cfg.Node.Advertise)
				e.Text(remote.Username)
				e.Text(password)
				return e.B
			}
			status, _ = joinMessage(t, address, local.Username, local.Password, clusterJoinPrepare, encode("incorrect"))
			if status != 401 {
				t.Fatal("wrong credentials", status)
			}
			if c := readJoinConfig(t, path); c.ClusterID != before.ClusterID {
				t.Fatal("failed credentials changed cluster")
			}
			legacy := source.node.protocolVersion()
			legacy.Operations = nil
			source.node.compatibility.Store(&legacy)
			status, _ = joinMessage(t, address, local.Username, local.Password, clusterJoinPrepare, encode(remote.Password))
			source.node.compatibility.Store(nil)
			if status != 426 {
				t.Fatal("unadvertised join operation accepted", status)
			}
			status, _ = joinMessage(t, address, local.Username, local.Password, clusterJoinPrepare, encode(remote.Password))
			if status != 202 {
				t.Fatal("join prepare", status)
			}
			awaitJoinedLogin(t, address, remote.Username, remote.Password)
			after := readJoinConfig(t, path)
			if after.VolumesDir != resolvePath(resolvePath(filepath.Dir(path), before.Node.DataDir), before.VolumesDir) {
				t.Fatal("data volume directory changed", after.VolumesDir)
			}
			if after.ClusterID != source.node.cfg.ClusterID || after.Node.ID != before.Node.ID || after.JoinPending {
				t.Fatal("identity/join state", after.Node, after.JoinPending)
			}
			for _, old := range before.Volumes {
				if old.System {
					continue
				}
				found := false
				for _, v := range after.Volumes {
					if v.ID == old.ID && v.Path == old.Path && v.SizeBytes == old.SizeBytes {
						found = true
					}
				}
				if !found {
					t.Fatal("prepared volume lost", old)
				}
			}
			for _, suffix := range []string{"", ".join"} {
				raw, err := os.ReadFile(path + suffix)
				if err == nil && bytes.Contains(raw, []byte(remote.Password)) {
					t.Fatal("remote password persisted")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, err := client.Dial(ctx, address, remote.Username, remote.Password, client.BucketID(bucket.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			data, _, err := c.Get(ctx, client.ObjectID(oid))
			if err != nil || string(data) != "existing cluster content" {
				t.Fatal("joined SDK", string(data), err)
			}
			status, raw = joinMessage(t, address, remote.Username, remote.Password, clusterJoinStatus, nil)
			d = client.Decoder{B: raw}
			if status != 200 || d.Bool() {
				t.Fatal("joined node still eligible")
			}
		})
	}
}
func TestClusterJoinRejectsExistingBucketsAndPrivilegeEscalation(t *testing.T) {
	s := testServer(t)
	s.joinHosted = true
	// Transport fixtures lack a controller; use a daemon for the eligibility rule.
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	n.node.joinHosted = true
	createProtocolBucket(t, n.node, "already-used")
	n.node.opMu.Lock()
	err := n.node.joinEligibility()
	n.node.opMu.Unlock()
	if statusOf(err) != 409 {
		t.Fatal("existing bucket allowed", err)
	}
	for _, actor := range []principal{{User: account{Role: "USER"}}, {User: account{Role: "NODE"}, ClusterPeer: true}} {
		_, status := n.node.dispatchAs(client.Message{Kind: client.Request, Opcode: clusterJoinInfo, ID: client.NewRequestID()}, nil, actor)
		if status != 403 {
			t.Fatal("join credentials disclosed", status)
		}
	}
	actor := userPrincipal(t, n.node)
	actor.connection = &connectionAuthorization{bucket: &Bucket{}}
	_, status := n.node.dispatchAs(client.Message{Kind: client.Request, Opcode: clusterJoinInfo, ID: client.NewRequestID()}, nil, actor)
	if status != 403 {
		t.Fatal("bucket-bound admin disclosed join credentials", status)
	}
}
func TestDashboardClusterJoinBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE")
	}
	source := startDaemonTestNode(t, filepath.Join(t.TempDir(), "source.yaml"), nil)
	path, before, address, _ := startJoinRun(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	input, _ := json.Marshal(map[string]string{"url": address, "username": before.Auth.BootstrapAdmin.Username, "password": before.Auth.BootstrapAdmin.Password, "target": source.node.cfg.Node.Advertise, "targetUser": source.node.cfg.Auth.BootstrapAdmin.Username, "targetPassword": source.node.cfg.Auth.BootstrapAdmin.Password})
	cmd := exec.CommandContext(ctx, "node", "dashboard_join_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("browser: %v\n%s", err, out)
	}
	t.Log(string(out))
	if readJoinConfig(t, path).ClusterID != source.node.cfg.ClusterID {
		t.Fatal("browser did not join")
	}
}

func TestClusterJoinResumesPartialVolumeRebind(t *testing.T) {
	source := startDaemonTestNode(t, filepath.Join(t.TempDir(), "seed.yaml"), nil)
	path := filepath.Join(t.TempDir(), "candidate.yaml")
	candidate := startDaemonTestNode(t, path, nil)
	s := candidate.node
	var err error
	s.joinHosted = true
	addTestVolume(t, s, "first", "hdd", 4<<20)
	addTestVolume(t, s, "second", "hdd", 4<<20)
	before := copyConfig(s.cfg)
	remote := source.node.cfg.Auth.BootstrapAdmin
	e := client.Encoder{}
	e.Text(source.node.cfg.Node.Advertise)
	e.Text(remote.Username)
	e.Text(remote.Password)
	_, status, err := s.executeClusterJoin(client.Message{Kind: client.Request, Opcode: clusterJoinPrepare, ID: client.NewRequestID(), Body: e.B}, userPrincipal(t, s))
	if err != nil || status != 202 {
		s.Close()
		t.Fatal("prepare", status, err)
	}
	if _, status = s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.UserCreate, ID: client.NewRequestID()}, nil, userPrincipal(t, s)); status != 503 {
		s.Close()
		t.Fatal("administration was not fenced", status)
	}
	candidate.http.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	intent := readJoinConfig(t, path+".join")
	raw, _ := os.ReadFile(path + ".join")
	if bytes.Contains(raw, []byte(remote.Password)) {
		t.Fatal("password stored in intent")
	}
	if info, err := os.Stat(path + ".join"); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("intent permissions", err)
	}
	// Model a process loss after the first of two persistent identity changes.
	first := intent.Volumes[0]
	dev, err := volume.OpenFile(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := volume.Open(dev, volume.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	old, _ := parseID(before.ClusterID)
	cluster, _ := parseID(intent.ClusterID)
	node, _ := parseID(intent.Node.ID)
	id, _ := parseID(first.ID)
	if err = store.JoinEmptyCluster(old, cluster, node, id); err != nil {
		t.Fatal(err)
	}
	store.Close()
	resumed, err := Open(Options{ConfigPath: path})
	if err != nil {
		t.Fatal("resume", err)
	}
	defer resumed.Close()
	if resumed.cfg.ClusterID != intent.ClusterID {
		t.Fatal("wrong resumed cluster")
	}
	for _, v := range resumed.volumes {
		identity := v.store.Identity()
		if identity.ClusterID != cluster || identity.NodeID != node {
			t.Fatal("incomplete identity transition")
		}
	}
	if _, err = os.Stat(path + ".join"); !os.IsNotExist(err) {
		t.Fatal("completed intent retained", err)
	}
}

func TestClusterJoinAddressSelection(t *testing.T) {
	for _, c := range []struct{ listen, advertise, route, wantListen, wantAdvertise string }{
		{"127.0.0.1:7419", "http://127.0.0.1:7419", "192.0.2.10", ":7419", "http://192.0.2.10:7419"},
		{"localhost:7419", "", "192.0.2.10", ":7419", "http://192.0.2.10:7419"},
		{"[::1]:7419", "http://[::1]:7419", "2001:db8::10", ":7419", "http://[2001:db8::10]:7419"},
		{"127.0.0.1:7419", "http://storage.internal:80", "192.0.2.10", "127.0.0.1:7419", "http://storage.internal:80"},
		{"127.0.0.1:7419", "http://127.0.0.1:7419", "127.0.0.1", "127.0.0.1:7419", "http://127.0.0.1:7419"},
	} {
		listen, advertise, err := joinAddresses(c.listen, c.advertise, net.ParseIP(c.route))
		if err != nil || listen != c.wantListen || advertise != c.wantAdvertise {
			t.Fatal(c, listen, advertise, err)
		}
	}
}
