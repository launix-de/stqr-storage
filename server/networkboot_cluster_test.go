// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestClusterNetworkbootNBDWithQEMU(t *testing.T) {
	if os.Getenv("STQR_TEST_NETWORKBOOT") != "1" {
		t.Skip("set STQR_TEST_NETWORKBOOT=1 for real QEMU with two daemon nodes")
	}
	if _, e := exec.LookPath("qemu-io"); e != nil {
		t.Skip(e)
	}
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "images", "hdd", 32<<20)
	addTestVolume(t, target.node, "images", "hdd", 32<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dir := t.TempDir()
	binary := filepath.Join(dir, "bootctl")
	build := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-race", "-o", binary, "./cmd/stqr-bootctl")
	build.Dir = "../networkboot"
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, out)
	}
	admin := source.node.cfg.Auth.BootstrapAdmin
	cfg := filepath.Join(dir, "credentials.json")
	raw, _ := json.Marshal(map[string]string{"uri": target.node.cfg.Node.Advertise, "username": admin.Username, "password": admin.Password})
	if e := os.WriteFile(cfg, raw, 0600); e != nil {
		t.Fatal(e)
	}
	run := func(args ...string) {
		t.Helper()
		out, e := exec.CommandContext(ctx, binary, append([]string{"--config", cfg}, args...)...).CombinedOutput()
		if e != nil {
			t.Fatalf("%v: %v %s", args, e, out)
		}
	}
	run("check-exclusive", "image")
	run("create", "image")
	run("--size", "4194304", "init-disk", "image")
	socket := filepath.Join(dir, "disk.sock")
	cmd := exec.CommandContext(ctx, binary, "--config", cfg, "--socket", socket, "nbd", "image")
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			<-done
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, e := os.Stat(socket); e == nil {
			break
		}
		select {
		case e := <-done:
			t.Fatalf("NBD start: %v %s", e, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("NBD startup timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	args := []string{"-f", "raw", "-c", "write -P 0x5a 1048064 4096", "-c", "flush", "-c", "read -P 0x5a 1048064 4096", "nbd+unix:///?socket=" + socket}
	if out, e := exec.CommandContext(ctx, "qemu-io", args...).CombinedOutput(); e != nil {
		t.Fatalf("QEMU: %v %s", e, out)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("NBD stop: %v %s", e, logs.String())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	artifacts := filepath.Join(dir, "artifacts")
	os.Mkdir(artifacts, 0700)
	for _, name := range []string{"rootfs.tar", "vmlinuz", "initrd.img", "release.json"} {
		if e := os.WriteFile(filepath.Join(artifacts, name), []byte("fixture"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	run("publish", "image", artifacts)
	run("gc", "image")
	run("fetch", "image", filepath.Join(dir, "fetched"))
	run("create", "copy")
	run("clone", "image", "copy")
	run("clear", "copy")
	run("delete", "copy")
}
