// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
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
	copyFile(filepath.Join(root, "kstqr.ko"), "kstqr.ko", 0600)
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "mount.kstqr"), filepath.Join(root, "mount.c"))
	run("cc", "-static", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(initrdRoot, "smoke"), filepath.Join(root, "tests/smoke.c"))
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
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		handler.ServeHTTP(w, r)
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
