// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"archive/tar"
	"bytes"
	"context"
	wire "encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestBucketClearCloneOfflineAndReplay(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "copy", "hdd", 16<<20)
	src, _ := createProtocolBucket(t, s, "copy-source")
	dst, _ := createProtocolBucket(t, s, "copy-target")
	actor := userPrincipal(t, s)
	ctx := context.Background()
	oid := client.ObjectID{42}
	want := bytes.Repeat([]byte("test"), 1<<16)
	if _, err := src.Put(ctx, actor, [32]byte(oid), want, volumeOptionsForCopyTest()); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(src.ID()))
	e.ID(client.ID(dst.ID()))
	m := client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}
	if _, err := s.executeBucketCopy(ctx, m, actor); err != nil {
		t.Fatal("clone", err)
	}
	got, _, err := dst.Get(ctx, actor, [32]byte(oid))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("clone contents", err)
	}
	if _, status := s.dispatchAs(m, nil, actor); status != 200 {
		t.Fatal("clone replay", status)
	}
	m.ID = client.NewRequestID()
	if _, status := s.dispatchAs(m, nil, actor); status != 409 {
		t.Fatal("nonempty clone destination", status)
	}
	e = client.Encoder{}
	e.ID(client.ID(dst.ID()))
	clear := client.Message{Kind: client.Request, Opcode: client.BucketClear, ID: client.NewRequestID(), Body: e.B}
	if _, status := s.dispatchAs(clear, nil, actor); status != 200 {
		t.Fatal("clear", status)
	}
	if _, err := dst.Put(ctx, actor, [32]byte(oid), []byte("later"), volumeOptionsForCopyTest()); err != nil {
		t.Fatal(err)
	}
	if _, status := s.dispatchAs(clear, nil, actor); status != 200 {
		t.Fatal("clear replay", status)
	}
	got, _, err = dst.Get(ctx, actor, [32]byte(oid))
	if err != nil || string(got) != "later" {
		t.Fatal("replayed clear removed later writes", err)
	}
	user := bucketProtocolUser(t, s)
	actor.User = user
	if _, status := s.dispatchAs(clear, nil, actor); status != 403 {
		t.Fatal("nonadmin clear", status)
	}
}

func TestNetworkbootNBDWithQEMU(t *testing.T) {
	if os.Getenv("STQR_TEST_NETWORKBOOT") != "1" {
		t.Skip("set STQR_TEST_NETWORKBOOT=1 for QEMU NBD interoperability")
	}
	if _, err := exec.LookPath("qemu-io"); err != nil {
		t.Skip(err)
	}
	s := testServer(t)
	addTestVolume(t, s, "nbd", "hdd", 64<<20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	dir := t.TempDir()
	binary := filepath.Join(dir, "stqr-bootctl")
	cmd := exec.Command("go", "build", "-buildvcs=false", "-race", "-o", binary, "./cmd/stqr-bootctl")
	cmd.Dir = "../networkboot"
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	cfg := filepath.Join(dir, "credentials.json")
	a := s.cfg.Auth.BootstrapAdmin
	raw, _ := json.Marshal(map[string]string{"uri": host.URL, "username": a.Username, "password": a.Password})
	if err := os.WriteFile(cfg, raw, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		out, err := exec.Command(binary, append([]string{"--config", cfg}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v %s", args, err, out)
		}
	}
	run("create", "nbd-image-bd")
	run("--size", "4194304", "init-disk", "nbd-image-bd")
	nbdBinary := filepath.Join(dir, "stqr-nbd")
	buildNBD := exec.Command("go", "build", "-buildvcs=false", "-race", "-o", nbdBinary, "./qemu-plugin-nbd/cmd/stqr-nbd")
	buildNBD.Dir = ".."
	if out, err := buildNBD.CombinedOutput(); err != nil {
		t.Fatalf("build NBD: %v %s", err, out)
	}
	nbdCLI := func(args ...string) {
		t.Helper()
		out, err := exec.Command(nbdBinary, append([]string{"--config", cfg, "--bucket", "nbd-image-bd"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("NBD CLI: %v %s", err, out)
		}
	}
	nbdCLI("--size", "8388608", "create", "data")
	nbdCLI("--readonly", "list")
	nbdCLI("--readonly", "info", "data")
	serve := func(bucket string, write bool) {
		t.Helper()
		socket := filepath.Join(dir, bucket+".sock")
		cmd := exec.Command(binary, "--config", cfg, "--socket", socket, "nbd", bucket)
		var logs bytes.Buffer
		cmd.Stdout = &logs
		cmd.Stderr = &logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		defer func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
			}
		}()
		deadline := time.Now().Add(15 * time.Second)
		for {
			if _, err := os.Stat(socket); err == nil {
				break
			}
			select {
			case err := <-done:
				t.Fatalf("NBD start: %v %s", err, logs.String())
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("NBD timeout")
			}
			time.Sleep(20 * time.Millisecond)
		}
		if bucket == "nbd-image-bd" && write {
			// Keep one device connected while QEMU opens a different named export.
			hold, err := net.DialTimeout("unix", socket, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			hold.SetDeadline(time.Now().Add(10 * time.Second))
			hello := make([]byte, 18)
			if _, err = io.ReadFull(hold, hello); err != nil {
				t.Fatal(err)
			}
			for _, v := range []any{uint32(3), uint64(0x49484156454f5054), uint32(1), uint32(8), []byte("disk.raw")} {
				if err = wire.Write(hold, wire.BigEndian, v); err != nil {
					t.Fatal(err)
				}
			}
			info := make([]byte, 10)
			if _, err = io.ReadFull(hold, info); err != nil {
				t.Fatal(err)
			}
			if wire.BigEndian.Uint64(info) != 4194304 {
				t.Fatal("wrong default geometry")
			}
			ioCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			out, err := exec.CommandContext(ioCtx, "qemu-io", "-f", "raw", "-c", "write -P 0xa5 1048064 4096", "-c", "flush", "-c", "read -P 0xa5 1048064 4096", "nbd+unix:///data?socket="+socket).CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("parallel named QEMU export: %v %s", err, out)
			}
			// DISC closes the connection only after flushing and releasing the writer.
			for _, v := range []any{uint32(0x25609513), uint16(0), uint16(2), uint64(0), uint64(0), uint32(0)} {
				if err = wire.Write(hold, wire.BigEndian, v); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = hold.Read(make([]byte, 1)); err != io.EOF {
				t.Fatal("disconnect", err)
			}
			hold.Close()
		}
		args := []string{"-f", "raw", "-c", "read -P 0x5a 0 4096"}
		if write {
			args = []string{"-f", "raw", "-c", "write -P 0x5a 0 4096", "-c", "flush", "-c", "read -P 0x5a 0 4096"}
		}
		args = append(args, "nbd+unix:///?socket="+socket)
		ioCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ioCtx, "qemu-io", args...).CombinedOutput(); err != nil {
			t.Fatalf("qemu-io: %v %s", err, out)
		}

		if write && os.Getenv("STQR_TEST_GUESTFISH") == "1" {
			guestCtx, cancelGuest := context.WithTimeout(context.Background(), 90*time.Second)
			out, err := exec.CommandContext(guestCtx, "guestfish", "--ro", "--format=raw", "-a", "nbd:///?socket="+socket, "run", ":", "list-devices").CombinedOutput()
			cancelGuest()
			if err != nil || !bytes.Contains(out, []byte("/dev/sda")) {
				t.Fatalf("libguestfs NBD: %v %s", err, out)
			}
		}
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("NBD stop", err, logs.String())
			}
		case <-time.After(15 * time.Second):
			t.Fatal("NBD stop timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}

	serve("nbd-image-bd", true)

	b, err := s.BucketByName("nbd-image-bd")
	if err != nil {
		t.Fatal(err)
	}
	s.durability.mu.Lock()
	state := s.durability.buckets[b.ID()]
	accepted := state != nil && state.sequence > 0
	s.durability.mu.Unlock()
	if !accepted {
		t.Fatal("exclusive NBD path did not use accepted writes")
	}

	artifacts := filepath.Join(dir, "artifacts")
	if err := os.Mkdir(artifacts, 0700); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	payload := bytes.Repeat([]byte("root archive"), 100000)
	if err := writer.WriteHeader(&tar.Header{Name: "etc/marker", Mode: 0644, Size: int64(len(payload)), Uid: 0, Gid: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	rootArchive := archive.Bytes()
	for name, data := range map[string][]byte{"rootfs.tar": rootArchive, "vmlinuz": []byte("kernel"), "initrd.img": []byte("initrd"), "release.json": []byte(`{"schema":1}`)} {
		if err := os.WriteFile(filepath.Join(artifacts, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run("publish", "nbd-image", artifacts)
	run("publish", "nbd-image", artifacts) // immutable upload retries are verified
	fetched := filepath.Join(dir, "fetched")
	run("gc", "nbd-image")
	run("fetch", "nbd-image", fetched)

	{
		gateway := exec.Command("python3", "-c", `
import os,re,signal,socket,subprocess,sys,tempfile,time,urllib.request
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from common import ctl
def port():
    s=socket.socket();s.bind(('127.0.0.1',0));n=s.getsockname()[1];s.close();return n
with tempfile.TemporaryDirectory(prefix='stqr-gateway-') as work:
    hp=port()
    p=subprocess.Popen([sys.executable,str(Path(sys.argv[1])/'serve'),'--config',sys.argv[2],'--base-bucket','nbd-image','--server','127.0.0.1','--interface','lo','--clients','127.0.0.0/8','--no-dhcp','--cache',work,'--http-port',str(hp)])
    try:
        url=f'http://127.0.0.1:{hp}/session.ipxe?mac=001122334455'
        deadline=time.monotonic()+20
        while True:
            try:old=urllib.request.urlopen(url,timeout=15).read().decode();break
            except OSError:
                assert p.poll() is None,'gateway exited'
                if time.monotonic()>deadline:raise
                time.sleep(.1)
        oldurl=re.search(r'kernel (http://\S+)',old).group(1)
        assert urllib.request.urlopen(oldurl).read()==b'kernel'
        (Path(sys.argv[3])/'vmlinuz').write_bytes(b'kernel2')
        ctl(sys.argv[2],'publish','nbd-image',sys.argv[3])
        os.kill(p.pid,signal.SIGHUP)
        deadline=time.monotonic()+20
        next_mac=0x1122334466
        while True:
            assert urllib.request.urlopen(url).read().decode()==old,'pending boot changed its release'
            new=urllib.request.urlopen(url.replace('001122334455',f'{next_mac:012x}')).read().decode()
            next_mac+=1
            if re.search(r'kernel (http://\S+)',new).group(1)!=oldurl:break
            assert time.monotonic()<deadline,'release reload timeout'
            time.sleep(.1)
        newurl=re.search(r'kernel (http://\S+)',new).group(1)
        assert urllib.request.urlopen(newurl).read()==b'kernel2'
        assert urllib.request.urlopen(oldurl).read()==b'kernel','old boot artifacts changed'
    finally:
        p.terminate();p.wait(timeout=20)
`, mustAbsNetworkboot(t), cfg, artifacts)
		if out, err := gateway.CombinedOutput(); err != nil {
			t.Fatalf("gateway reload: %v %s", err, out)
		}
	}

	manifestRaw, err := os.ReadFile(filepath.Join(fetched, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var nativeManifest struct {
		Schema     int       `json:"schema"`
		RootBucket client.ID `json:"root_bucket"`
	}
	if err := json.Unmarshal(manifestRaw, &nativeManifest); err != nil || nativeManifest.Schema != 2 {
		t.Fatal("native manifest", err)
	}
	image, err := s.BucketByName("nbd-image")
	if err != nil || nativeManifest.RootBucket != client.ID(image.ID()) || image.ID() == b.ID() {
		t.Fatal("published filesystem must own its root independently of the installation BD", err)
	}
	ctx := context.Background()
	lower, err := client.Dial(ctx, host.URL, a.Username, a.Password, nativeManifest.RootBucket, "fs")
	if err != nil {
		t.Fatal(err)
	}
	lowerFS, err := client.OpenFilesystem(ctx, lower, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	etc, err := lowerFS.Lookup(ctx, client.FilesystemRootID(), "etc")
	if err != nil {
		t.Fatal(err)
	}
	marker, err := lowerFS.Lookup(ctx, etc.ID, "marker")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if n, err := lowerFS.ReadAt(ctx, marker.ID, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatal("native root roundtrip", n, err)
	}
	lower.Close()

	run("create", "nbd-clone")
	run("clone", "nbd-image-bd", "nbd-clone")
	serve("nbd-clone", false)
	run("clear", "nbd-clone")
	run("delete", "nbd-image-bd")
	run("check-image", "nbd-image") // Temporary disk cleanup preserves the image.

	// Exercise per-MAC allocation, the scoped TCP proxy and disconnect cleanup
	// without exposing DHCP or an NBD listener outside loopback.
	py := exec.Command("python3", "-c", `
import sys,tempfile,subprocess,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from sessions import Sessions
with tempfile.TemporaryDirectory(prefix='stqr-sessions-') as work:
    manager=Sessions(sys.argv[2],'127.0.0.1',work,1)
    try:
        port=manager.allocate('001122334455','127.0.0.1')
        subprocess.run(['qemu-io','-f','raw','-c','write -P 0x55 0 4096','-c','flush','-c','read -P 0x55 0 4096',f'nbd://127.0.0.1:{port}/'],check=True)
        deadline=time.monotonic()+20
        while list(Path(work).glob('*.json')) and time.monotonic()<deadline:time.sleep(.1)
        assert not list(Path(work).glob('*.json')), 'ephemeral bucket cleanup failed'
    finally:manager.close()
`, mustAbsNetworkboot(t), cfg)
	if out, err := py.CombinedOutput(); err != nil {
		t.Fatalf("per-MAC upper: %v %s", err, out)
	}

}

func volumeOptionsForCopyTest() volume.Options {
	return volume.Options{RequestID: volume.ID(client.NewRequestID())}
}

func mustAbsNetworkboot(t *testing.T) string {
	t.Helper()
	p, e := filepath.Abs("../networkboot")
	if e != nil {
		t.Fatal(e)
	}
	return p
}
