// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"net"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	binary := filepath.Join(dir, "bootctl")
	buildCtx, cancelBuild := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-buildvcs=false", "-race", "-o", binary, "./cmd/stqr-bootctl")
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
	// Bound each operation independently. Compilation, race-runtime shutdowns
	// and preceding commands must not consume a later command's deadline.
	invoke := func(args ...string) ([]byte, error) {
		t.Helper()
		commandCtx, cancelCommand := context.WithTimeout(ctx, 15*time.Second)
		defer cancelCommand()
		started := time.Now()
		out, err := exec.CommandContext(commandCtx, binary, append([]string{"--config", cfg}, args...)...).CombinedOutput()
		t.Logf("bootctl %v finished in %s", args, time.Since(started))
		if commandCtx.Err() != nil {
			t.Fatalf("%v: %v: %s", args, commandCtx.Err(), out)
		}
		return out, err
	}
	run := func(args ...string) {
		t.Helper()
		out, e := invoke(args...)
		if e != nil {
			t.Fatalf("%v: %v %s", args, e, out)
		}
	}
	run("check-exclusive", "image-bd")
	run("create", "image-bd")
	run("--size", "4194304", "init-disk", "image-bd")
	run("--size", "4194304", "check-install-disk", "image-bd")
	if out, e := invoke("--size", "8388608", "check-install-disk", "image-bd"); e == nil || !bytes.Contains(out, []byte("requested")) {
		t.Fatal("resume must reject a different disk size", e, string(out))
	}
	socket := filepath.Join(dir, "disk.sock")
	cmd := exec.CommandContext(ctx, binary, "--config", cfg, "--socket", socket, "nbd", "image-bd")
	var logs bytes.Buffer
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	// Only this goroutine owns running. ProcessState is written by Wait and
	// cannot be inspected concurrently, including on an early test failure.
	running := true
	defer func() {
		if running {
			cmd.Process.Kill()
			<-done
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if probe, e := net.DialTimeout("unix", socket, 100*time.Millisecond); e == nil {
			probe.Close()
			break
		}
		select {
		case e := <-done:
			running = false
			t.Fatalf("NBD start: %v %s", e, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("NBD startup timeout")
		}
		time.Sleep(20 * time.Millisecond)
	}
	args := []string{"-f", "raw", "-d", "unmap", "-c", "write -P 0x5a 1048064 4096", "-c", "flush", "-c", "read -P 0x5a 1048064 4096", "-c", "write -P 0x77 2097152 1048576", "-c", "discard 2097152 1048576", "-c", "discard 1048576 512", "-c", "flush", "-c", "read -P 0 2097152 1048576", "-c", "read -P 0 1048576 512", "-c", "read -P 0x5a 1048064 512", "nbd+unix:///?socket=" + socket}
	ioCtx, cancelIO := context.WithTimeout(ctx, 15*time.Second)
	defer cancelIO()
	if out, e := exec.CommandContext(ioCtx, "qemu-io", args...).CombinedOutput(); e != nil {
		t.Fatalf("QEMU: %v %s", e, out)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	stop := time.NewTimer(10 * time.Second)
	defer stop.Stop()
	select {
	case e := <-done:
		running = false
		if e != nil {
			t.Fatalf("NBD stop: %v %s", e, logs.String())
		}
	case <-stop.C:
		t.Fatal("NBD shutdown did not complete")
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
	run("check-image", "image")
	// Publishing uses a separate FS namespace; the installer remains a BD and
	// can still be inspected until prepare-system performs its explicit cleanup.
	run("--size", "4194304", "check-install-disk", "image-bd")
	if out, e := invoke("nbd", "image"); e == nil || !bytes.Contains(out, []byte("format mismatch")) {
		t.Fatal("published FS accepted as installation BD", e, string(out))
	}
	if out, e := invoke("--size", "4194304", "check-install-disk", "image"); e == nil || !bytes.Contains(out, []byte("format")) {
		t.Fatal("resume must reject a filesystem image as an installation BD", e, string(out))
	}

	run("gc", "image")
	run("fetch", "image", filepath.Join(dir, "fetched"))
	run("clone-image", "image", "copy")
	run("check-image", "copy")
	run("create", "bd-copy")
	run("clone", "image-bd", "bd-copy")
	run("clear", "bd-copy")
	run("delete", "bd-copy")
	run("clear", "copy")
	run("delete", "copy")
	run("delete", "image-bd")
	run("check-image", "image")
}
