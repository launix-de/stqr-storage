// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// The module is never loaded into the host. The VM uses only a disposable server.
func TestKSTQRVirtualMachine(t *testing.T) {
	if os.Getenv("STQR_TEST_KERNEL") != "1" {
		t.Skip("set STQR_TEST_KERNEL=1 and STQR_KERNEL_IMAGE to run isolated kernel tests")
	}
	image := os.Getenv("STQR_KERNEL_IMAGE")
	if image == "" {
		t.Fatal("STQR_KERNEL_IMAGE is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	root, err := filepath.Abs("../kernel/kstqr")
	if err != nil {
		t.Fatal(err)
	}
	run := func(name string, args ...string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, name, args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
	makeArgs := []string{"-C", root, "-j2"}
	if kdir := os.Getenv("STQR_KERNEL_HEADERS"); kdir != "" {
		makeArgs = append(makeArgs, "KDIR="+kdir)
	}
	run("make", makeArgs...)
	initrdRoot := t.TempDir()
	for _, dir := range []string{"bin", "dev", "proc", "sys", "mnt", "readonly", "other", "fresh", "tmp"} {
		if err := os.Mkdir(filepath.Join(initrdRoot, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	copyFile := func(from, to string, mode os.FileMode) {
		t.Helper()
		data, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(initrdRoot, to), data, mode); err != nil {
			t.Fatal(err)
		}
	}
	busybox, err := exec.LookPath("busybox")
	if err != nil {
		t.Fatal(err)
	}
	copyFile(busybox, "bin/busybox", 0755)
	if err := os.Symlink("busybox", filepath.Join(initrdRoot, "bin/sh")); err != nil {
		t.Fatal(err)
	}
	module := os.Getenv("STQR_KERNEL_MODULE")
	if module == "" {
		module = filepath.Join(root, "kstqr.ko")
	}
	copyFile(module, "kstqr.ko", 0600)
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "mount.kstqr"), filepath.Join(root, "mount.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "smoke"), filepath.Join(root, "tests/smoke.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "coherence"), filepath.Join(root, "tests/coherence.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "throughput"), filepath.Join(root, "tests/throughput.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "writeback"), filepath.Join(root, "tests/writeback.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "crash-reconnect"), filepath.Join(root, "tests/crash_reconnect.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "mmap-test"), filepath.Join(root, "tests/mmap.c"), "-lsqlite3", "-lm", "-ldl", "-lpthread")
	s := testServer(t)
	addTestVolume(t, s, "kernel-vm", "hdd", 192<<20)
	b, _ := createProtocolBucket(t, s, "kernel-vm")
	empty, _ := createProtocolBucket(t, s, "kernel-empty")
	corrupt, _ := createProtocolBucket(t, s, "kernel-corrupt")
	user := bucketProtocolUser(t, s)
	grant := client.Encoder{}
	grant.ID(client.ID(b.ID()))
	uid, _ := parseID(user.ID)
	grant.ID(client.ID(uid))
	grant.U32(uint32(BucketRead))
	if _, status, _ := adminBucketMessage(t, s, client.BucketGrant, grant.B); status != 200 {
		t.Fatal(status)
	}
	admin := s.cfg.Auth.BootstrapAdmin
	handler := s.Handler()
	var current atomic.Pointer[Server]
	current.Store(s)
	checkpointImages := map[string][]byte{}
	var checkpointMu sync.Mutex
	var subscriptionFS *client.Filesystem
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__kstqr_test/checkpoint-image" {
			checkpointMu.Lock()
			defer checkpointMu.Unlock()
			s.mu.RLock()
			stores := []*volume.Volume{}
			for _, v := range s.volumes {
				stores = append(stores, v.store)
			}
			s.mu.RUnlock()
			for _, v := range stores {
				path := v.Name()
				h, err := v.Open(false)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				data, err := os.ReadFile(path)
				h.Close()
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				checkpointImages[path] = data
			}
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/__kstqr_test/restart-checkpoint-image" {
			checkpointMu.Lock()
			defer checkpointMu.Unlock()
			if len(checkpointImages) == 0 {
				http.Error(w, "checkpoint image missing", 500)
				return
			}
			if err := s.Close(); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			// Restore the earlier durable image to model loss of the unconfirmed
			// tail. Volume crash-at-every-step tests separately validate real WAL
			// recovery. Only this test's closed, disposable data files are touched.
			for path, data := range checkpointImages {
				if err := os.WriteFile(path, data, 0600); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
			}
			restarted, err := openTransportTestNode(Options{ConfigPath: s.configPath})
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			t.Cleanup(func() { restarted.Close() })
			current.Store(restarted)
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/__kstqr_test/io-stats" {
			s.mu.RLock()
			stores := []*volume.Volume{}
			for _, v := range s.volumes {
				stores = append(stores, v.store)
			}
			s.mu.RUnlock()
			total := volume.IOStats{}
			for _, v := range stores {
				io := v.IOStats()
				total.SyncOps += io.SyncOps
				total.WriteBytes += io.WriteBytes
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(total)
			return
		}
		if r.URL.Path == "/__kstqr_test/lock" || r.URL.Path == "/__kstqr_test/unlock" {
			body := client.Encoder{}
			body.ID(client.ID(b.ID()))
			body.Bool(r.URL.Path == "/__kstqr_test/lock")
			message := client.Message{Kind: client.Request, Opcode: client.BucketLock, ID: client.NewRequestID(), Body: body.B}
			req := httptest.NewRequest(http.MethodPost, "http://localhost/api/message", bytes.NewReader(client.EncodeMessage(message)))
			req.RemoteAddr = "127.0.0.1:12345"
			req.SetBasicAuth(admin.Username, admin.Password)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			w.WriteHeader(response.Code)
			return
		}
		if r.URL.Path == "/__kstqr_test/subscription-reconnect" || r.URL.Path == "/__kstqr_test/subscription-resync" {
			if strings.HasSuffix(r.URL.Path, "reconnect") {
				s.objectSubscriptions.mu.Lock()
				sockets := make([]*wsSession, 0, len(s.objectSubscriptions.sessions))
				for ws := range s.objectSubscriptions.sessions {
					sockets = append(sockets, ws)
				}
				s.objectSubscriptions.mu.Unlock()
				for _, ws := range sockets {
					ws.close()
				}
			} else {
				s.objectSubscriptions.resyncWhere(func(volume.Key) bool { return true })
			}
			// Allow the guest to refault an old page before the new write. The
			// reconnect path must discard those temporarily uncovered pages.
			time.Sleep(50 * time.Millisecond)
			text := "again!"
			if strings.HasSuffix(r.URL.Path, "resync") {
				text = "resync"
			}
			file, err := subscriptionFS.Lookup(r.Context(), client.FilesystemRootID(), "subscription.txt")
			if err == nil {
				_, err = subscriptionFS.WriteAt(r.Context(), file.ID, []byte(text), 0)
			}
			if err != nil {
				http.Error(w, err.Error(), 500)
			} else {
				w.WriteHeader(200)
			}
			return
		}
		if r.URL.Path == "/__kstqr_test/subscription-update" || r.URL.Path == "/__kstqr_test/subscription-delete" {
			file, err := subscriptionFS.Lookup(r.Context(), client.FilesystemRootID(), "subscription.txt")
			if err == nil && r.URL.Path == "/__kstqr_test/subscription-update" {
				_, err = subscriptionFS.WriteAt(r.Context(), file.ID, []byte("after!"), 0)
				mode := uint32(0600)
				if err == nil {
					_, err = subscriptionFS.SetAttributes(r.Context(), file.ID, client.FSAttributes{Mode: &mode})
				}
				if err == nil {
					_, err = subscriptionFS.Create(r.Context(), client.FilesystemRootID(), "subscription-created", 0644, 0, 0, false)
				}
			} else if err == nil {
				err = subscriptionFS.Unlink(r.Context(), client.FilesystemRootID(), "subscription.txt")
			}
			if err != nil {
				http.Error(w, err.Error(), 500)
			} else {
				w.WriteHeader(200)
			}
			return
		}
		current.Load().Handler().ServeHTTP(w, r)
	}))
	defer host.Close()
	conn, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	corruptConn, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(corrupt.ID()))
	if err != nil {
		t.Fatal(err)
	}
	bad := client.Encoder{B: []byte("STFS")}
	bad.U16(5)
	bad.U8(2)
	bad.U32(0755)
	bad.U32(0)
	bad.U32(0)
	bad.U64(1)
	bad.U64(14)
	bad.U8(0)
	dir := client.Encoder{B: []byte("STDR")}
	dir.U16(1)
	dir.U64(0)
	bad.Blob(dir.B)
	bad.Bool(false)
	bad.Text("")
	bad.U64(1)
	bad.U64(1)
	bad.U32(0)
	bad.U32(0)
	// A live directory with zero links is corrupt; --init must not repair it
	// by silently overwriting the existing root.
	malformed := bad.B
	if _, err = corruptConn.Put(ctx, client.FilesystemRootID(), malformed, client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	corruptConn.Close()
	fs, err := client.OpenFilesystem(ctx, conn, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	subscriptionFS = fs
	readBenchmark := os.Getenv("STQR_KERNEL_BENCHMARK") == "1"
	writeBenchmark := os.Getenv("STQR_KERNEL_WRITE_BENCHMARK") == "1"
	benchmark := readBenchmark || writeBenchmark
	if readBenchmark {
		for i := 0; i < 32; i++ {
			f, err := fs.Create(ctx, client.FilesystemRootID(), fmt.Sprintf("bench-%03d.php", i), 0644, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = fs.WriteAt(ctx, f.ID, bytes.Repeat([]byte("x"), 4096), 0); err != nil {
				t.Fatal(err)
			}
		}
		f, err := fs.Create(ctx, client.FilesystemRootID(), "bench-sequential", 0644, 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		for off := int64(0); off < 8<<20; off += 1 << 20 {
			if _, err = fs.WriteAt(ctx, f.ID, bytes.Repeat([]byte("s"), 1<<20), off); err != nil {
				t.Fatal(err)
			}
		}
	}
	file, err := fs.Create(ctx, client.FilesystemRootID(), "go.txt", 0644, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fs.WriteAt(ctx, file.ID, []byte("hello from Go"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err = fs.Link(ctx, client.FilesystemRootID(), "go-alias", file.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = fs.Symlink(ctx, client.FilesystemRootID(), "go-link", "go.txt", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err = fs.SetXattr(ctx, file.ID, "user.from-go", []byte("Go metadata"), 0); err != nil {
		t.Fatal(err)
	}
	if !benchmark {
		// Leave a valid interrupted multi-inode operation for the kernel to read
		// without writes on the RO mount, and then finish on the RW mount.
		type participant struct {
			id      client.ObjectID
			raw     []byte
			version uint64
		}
		members := []participant{{id: client.FilesystemRootID()}, {id: file.ID}}
		sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i].id[:], members[j].id[:]) < 0 })
		var transaction client.ObjectID
		for i := 0; i < 2; i++ {
			id := client.NewRequestID()
			copy(transaction[i*16:], id[:])
		}
		descriptor := client.Encoder{B: []byte("STFT")}
		descriptor.U16(1)
		descriptor.U8(0)
		descriptor.U32(uint32(len(members)))
		var rootVersion uint64
		for i := range members {
			raw, info, e := conn.Get(ctx, members[i].id)
			if e != nil {
				t.Fatal(e)
			}
			members[i].raw = raw
			members[i].version = info.Version
			descriptor.B = append(descriptor.B, members[i].id[:]...)
			descriptor.U64(info.Version)
			descriptor.Blob(raw)
			descriptor.Blob(raw)
			if members[i].id == client.FilesystemRootID() {
				rootVersion = info.Version
			}
		}
		if _, err = conn.CAS(ctx, transaction, 0, nil, descriptor.B, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID(), Absent: true}}); err != nil {
			t.Fatal(err)
		}
		marker := append([]byte("STFL\x01\x00"), transaction[:]...)
		if _, err = conn.CAS(ctx, client.FilesystemRootID(), 0, nil, marker, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID(), ExpectedVersion: &rootVersion}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(initrdRoot, "readonly-credentials"), []byte("username="+user.Name+"\npassword=bucket-test-password\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(initrdRoot, "wrong-credentials"), []byte("username="+admin.Username+"\npassword=wrong-password\n"), 0600); err != nil {
		t.Fatal(err)
	}
	credentials := fmt.Sprintf("username=%s\npassword=%s\n", admin.Username, admin.Password)
	if err = os.WriteFile(filepath.Join(initrdRoot, "credentials"), []byte(credentials), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(initrdRoot, "insecure-credentials"), []byte(credentials), 0644); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(host.URL)
	endpoint := "http://10.0.2.2:" + u.Port()
	init := strings.NewReplacer("@@URL@@", endpoint, "@@BUCKET@@", client.ID(b.ID()).String(), "@@EMPTY@@", client.ID(empty.ID()).String(), "@@CORRUPT@@", client.ID(corrupt.ID()).String()).Replace(`#!/bin/sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
trap 'echo KSTQR_VM_FAIL; poweroff -f' EXIT
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
ip link set lo up
ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
ip route add default via 10.0.2.2
insmod /kstqr.ko
for attempt in 1 2 3 4; do
  if /mount.kstqr --uri @@URL@@ --bucket @@CORRUPT@@ --credentials /credentials --init /mnt; then exit 1; fi
done
if /mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /insecure-credentials /mnt; then exit 1; fi
if /mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /wrong-credentials /mnt; then exit 1; fi
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /readonly-credentials --readonly /readonly
test "$(cat /readonly/go.txt)" = 'hello from Go'
umount /readonly
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /credentials /mnt
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /credentials /other
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /readonly-credentials --readonly /readonly
/mount.kstqr --uri @@URL@@ --bucket @@EMPTY@@ --credentials /credentials --init /fresh
echo initialized-by-kernel > /fresh/new.txt
umount /fresh
if pidof mount.kstqr; then exit 1; fi
/smoke
/mmap-test @@URL@@
/writeback
/mount.kstqr --uri @@URL@@ --bucket @@EMPTY@@ --credentials /credentials --metadata-cache-bytes 2048 /fresh
/coherence @@URL@@
umount /fresh
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /readonly-credentials --readonly --metadata-cache-bytes 0 /fresh
test "$(cat /fresh/go.txt)" = 'Hello from Go'
umount /fresh
cat /proc/self/mountstats
/crash-reconnect @@URL@@
ls -l /mnt
if grep 'password=' /proc/mounts; then exit 1; fi
umount /readonly
umount /other
umount /mnt
rmmod kstqr
insmod /kstqr.ko
/mount.kstqr --uri @@URL@@ --bucket @@BUCKET@@ --credentials /readonly-credentials --readonly /readonly
test "$(cat /readonly/kernel.txt)" = 'written by kstqr'
/mmap-test verify
umount /readonly
rmmod kstqr
if dmesg | grep -E 'BUG:|WARNING: CPU:|Oops:|KASAN:'; then exit 1; fi
echo KSTQR_VM_PASS
trap - EXIT
poweroff -f
`)

	if benchmark {
		begin := strings.Index(init, "for attempt in 1 2 3 4; do")
		end := strings.Index(init, "if dmesg |")
		script := `/mount.kstqr --uri ` + endpoint + ` --bucket ` + client.ID(b.ID()).String() + ` --credentials /readonly-credentials --readonly /readonly
/throughput
cat /proc/self/mountstats
umount /readonly
rmmod kstqr
`
		if writeBenchmark {
			script = `/mount.kstqr --uri ` + endpoint + ` --bucket ` + client.ID(b.ID()).String() + ` --credentials /credentials /mnt
/writeback benchmark ` + endpoint + `
cat /proc/self/mountstats
umount /mnt
rmmod kstqr
`
		}
		init = init[:begin] + script + init[end:]
	}

	if err = os.WriteFile(filepath.Join(initrdRoot, "init"), []byte(init), 0755); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "initrd.cpio")
	out, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	pack := exec.CommandContext(ctx, "sh", "-c", "find . -print0 | cpio --null -o -H newc --owner=0:0")
	pack.Dir = initrdRoot
	pack.Stdout = out
	var packErr bytes.Buffer
	pack.Stderr = &packErr
	err = pack.Run()
	out.Close()
	if err != nil {
		t.Fatalf("cpio: %v %s", err, packErr.String())
	}
	args := []string{"-machine", "q35", "-m", "512M", "-smp", "2", "-display", "none", "-serial", "stdio", "-monitor", "none", "-no-reboot", "-kernel", image, "-initrd", archive, "-append", "console=ttyS0 panic=1 oops=panic rdinit=/init loglevel=6", "-nic", "user,model=virtio-net-pci"}
	if os.Getenv("STQR_KERNEL_TCG") != "1" {
		args = append(args, "-enable-kvm", "-cpu", "host")
	}
	vm := exec.CommandContext(ctx, "qemu-system-x86_64", args...)
	output, err := vm.CombinedOutput()
	if logPath := os.Getenv("STQR_KERNEL_LOG"); logPath != "" {
		os.WriteFile(logPath, output, 0600)
	}
	if err != nil || !strings.Contains(string(output), "KSTQR_VM_PASS") || strings.Contains(string(output), "KSTQR_VM_FAIL") {
		t.Fatalf("kernel VM: %v\n%s", err, output)
	}
	if benchmark {
		for _, line := range strings.Split(string(output), "\n") {
			if strings.Contains(line, "KSTQR_BENCH ") || strings.Contains(line, "KSTQR_WRITE_BENCH ") || strings.Contains(line, "stqr: cache_hits=") {
				t.Log(strings.TrimSpace(line))
			}
		}
		if writeBenchmark {
			for rep := 0; rep < 3; rep++ {
				for _, test := range []struct {
					name   string
					length int
					data   byte
				}{{"direct", 8 << 20, 'D'}, {"dense", 1 << 20, 'M'}, {"commit4k", 4096, 'f'}} {
					file, err := fs.Lookup(ctx, client.FilesystemRootID(), fmt.Sprintf("wb-%s-%d", test.name, rep))
					if err != nil {
						t.Fatal(err)
					}
					data := make([]byte, test.length)
					n, err := fs.ReadAt(ctx, file.ID, data, 0)
					if err != nil || n != len(data) || !bytes.Equal(data, bytes.Repeat([]byte{test.data}, len(data))) {
						t.Fatalf("post-exit writeback validation %s: %d %v", test.name, n, err)
					}
				}
			}
			if _, err := conn.CheckFilesystem(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	t.Log("Standalone kernel mount, Go interoperability, filesystem operations and clean unload passed")
	entries, err := fs.ReadDir(ctx, client.FilesystemRootID())
	if err != nil {
		t.Fatal(err)
	}
	var kernel client.ObjectID
	for _, entry := range entries {
		if entry.Name == "kernel.txt" {
			kernel = entry.ID
		}
	}
	if kernel == (client.ObjectID{}) {
		t.Fatal("kernel-created file is missing")
	}
	data := make([]byte, 16)
	n, err := fs.ReadAt(ctx, kernel, data, 0)
	if err != nil || n != 16 || string(data) != "written by kstqr" {
		t.Fatalf("Go read: %q %d %v", data, n, err)
	}
	recovered, err := fs.Lookup(ctx, client.FilesystemRootID(), "mmap-error")
	if err != nil {
		t.Fatal(err)
	}
	recoveredData := make([]byte, (1<<20)+37)
	n, err = fs.ReadAt(ctx, recovered.ID, recoveredData, 0)
	expectedRecovery := bytes.Repeat([]byte("r"), len(recoveredData))
	copy(expectedRecovery, []byte("recovered"))
	if err != nil || n != len(recoveredData) || !bytes.Equal(recoveredData, expectedRecovery) {
		t.Fatalf("multi-batch failed-writeback recovery did not persist every page: %d %v", n, err)
	}
	link, err := fs.Lookup(ctx, client.FilesystemRootID(), "kernel-link")
	if err != nil || link.Kind != client.FSSymlink {
		t.Fatal(link, err)
	}
	if target, err := fs.Readlink(ctx, link.ID); err != nil || string(target) != "kernel.txt" {
		t.Fatal(string(target), err)
	}
	if value, err := fs.GetXattr(ctx, file.ID, "user.kernel"); err != nil || string(value) != "kernel metadata" {
		t.Fatal(string(value), err)
	}
	if info, err := fs.Stat(ctx, file.ID); err != nil || info.Nlink != 2 {
		t.Fatal(info, err)
	}
	initialized, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(empty.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer initialized.Close()
	initializedFS, err := client.OpenFilesystem(ctx, initialized, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initializedFile, err := initializedFS.Lookup(ctx, client.FilesystemRootID(), "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	expected := []byte("initialized-by-kernel\n")
	actual := make([]byte, len(expected))
	if n, err := initializedFS.ReadAt(ctx, initializedFile.ID, actual, 0); err != nil || n != len(expected) || !bytes.Equal(actual, expected) {
		t.Fatalf("kernel initialized filesystem: %q %v", actual, err)
	}
	if _, err = initialized.CheckFilesystem(ctx); err != nil {
		t.Fatal(err)
	}

	// No kernel connection remains, so the normal Go collector can validate every
	// published inode, segment index and namespace intent produced by the module.
	result, err := conn.CheckFilesystem(ctx)
	if err != nil {
		t.Fatalf("post-kernel fsck: %+v %v", result, err)
	}
}
