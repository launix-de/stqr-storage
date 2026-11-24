// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"archive/tar"
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
	run("--size", "4194304", "check-install-disk", "image")
	if out, e := exec.CommandContext(ctx, binary, "--config", cfg, "--size", "8388608", "check-install-disk", "image").CombinedOutput(); e == nil || !bytes.Contains(out, []byte("requested")) {
		t.Fatal("resume must reject a different disk size", e, string(out))
	}
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
	args := []string{"-f", "raw", "-d", "unmap", "-c", "write -P 0x5a 1048064 4096", "-c", "flush", "-c", "read -P 0x5a 1048064 4096", "-c", "write -P 0x77 2097152 1048576", "-c", "discard 2097152 1048576", "-c", "discard 1048576 512", "-c", "flush", "-c", "read -P 0 2097152 1048576", "-c", "read -P 0 1048576 512", "-c", "read -P 0x5a 1048064 512", "nbd+unix:///?socket=" + socket}
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
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if e := writer.WriteHeader(&tar.Header{Name: "marker", Mode: 0644, Size: 7}); e != nil {
		t.Fatal(e)
	}
	if _, e := writer.Write([]byte("fixture")); e != nil {
		t.Fatal(e)
	}
	if e := writer.Close(); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(artifacts, "rootfs.tar"), archive.Bytes(), 0600); e != nil {
		t.Fatal(e)
	}
	run("publish", "image", artifacts)
	if out, e := exec.CommandContext(ctx, binary, "--config", cfg, "--size", "4194304", "check-install-disk", "image").CombinedOutput(); e == nil || !bytes.Contains(out, []byte("already published")) {
		t.Fatal("resume must reject a published image", e, string(out))
	}
	run("gc", "image")
	run("fetch", "image", filepath.Join(dir, "fetched"))
	run("create", "copy")
	run("clone", "image", "copy")
	run("clear", "copy")
	run("delete", "copy")
}
