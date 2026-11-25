// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

// The test only loads the module inside QEMU and uses disposable volumes.
func TestKSTQRBlockVirtualMachine(t *testing.T) {
	if os.Getenv("STQR_TEST_KERNEL") != "1" {
		t.Skip("set STQR_TEST_KERNEL=1 and STQR_KERNEL_IMAGE")
	}
	image := os.Getenv("STQR_KERNEL_IMAGE")
	if image == "" {
		t.Fatal("STQR_KERNEL_IMAGE required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	root, err := filepath.Abs("../kernel/kstqr")
	if err != nil {
		t.Fatal(err)
	}
	run := func(name string, args ...string) {
		t.Helper()
		if out, err := exec.CommandContext(ctx, name, args...).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
	}
	args := []string{"-C", root, "-j2"}
	if h := os.Getenv("STQR_KERNEL_HEADERS"); h != "" {
		args = append(args, "KDIR="+h)
	}
	run("make", args...)
	s := testServer(t)
	addTestVolume(t, s, "kernel-bd", "hdd", 96<<20)
	bucket, _ := createProtocolBucket(t, s, "kernel-blocks")
	empty, _ := createProtocolBucket(t, s, "kernel-block-empty")
	bad, _ := createProtocolBucket(t, s, "kernel-block-bad")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := s.cfg.Auth.BootstrapAdmin
	disk, err := nbd.Open(ctx, host.URL, admin.Username, admin.Password, client.ID(bucket.ID()), false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := disk.Create(ctx, nbd.Metadata{Name: "data", Size: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = disk.Create(ctx, nbd.Metadata{Name: "system", Size: 16 << 20}); err != nil {
		t.Fatal(err)
	}
	if err = disk.WriteAt(ctx, data, []byte("from Go"), 0); err != nil {
		t.Fatal(err)
	}
	if err = disk.Close(); err != nil {
		t.Fatal(err)
	}
	// One malformed descriptor must reject the whole export, without leaving disks/leases.
	c, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(bad.ID()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Put(ctx, nbd.MetadataID("bad"), []byte("STNBD001"), client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	dir := t.TempDir()
	for _, d := range []string{"bin", "dev", "proc", "sys", "exports", "selected", "fs", "tmp"} {
		if err = os.Mkdir(filepath.Join(dir, d), 0755); err != nil {
			t.Fatal(err)
		}
	}
	copyFile := func(src, dst string, mode os.FileMode) {
		t.Helper()
		raw, e := os.ReadFile(src)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, dst), raw, mode); e != nil {
			t.Fatal(e)
		}
	}
	busybox, err := exec.LookPath("busybox")
	if err != nil {
		t.Fatal(err)
	}
	copyFile(busybox, "bin/busybox", 0755)
	if err = os.Symlink("busybox", filepath.Join(dir, "bin/sh")); err != nil {
		t.Fatal(err)
	}
	copyFile(filepath.Join(root, "kstqr.ko"), "kstqr.ko", 0600)
	run("cc", "-static", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(dir, "mount.kstqr"), filepath.Join(root, "mount.c"))
	run("cc", "-static", "-Wall", "-Wextra", "-Werror", "-O2", "-o", filepath.Join(dir, "block-test"), filepath.Join(root, "tests/block.c"))
	if err = os.WriteFile(filepath.Join(dir, "credentials"), []byte("username="+admin.Username+"\npassword="+admin.Password+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(host.URL)
	guest := "http://10.0.2.2:" + u.Port()
	init := fmt.Sprintf(`#!/bin/sh
set -eu
export PATH=/bin
/bin/busybox --install -s /bin
trap 'echo KSTQR_BLOCK_VM_FAIL; poweroff -f' EXIT
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
ip link set lo up
ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
ip route add default via 10.0.2.2
insmod /kstqr.ko
if /mount.kstqr %[1]s /exports -o bd,bucket=%[3]s,credentials=/credentials; then exit 1; fi
if /mount.kstqr %[1]s /exports -o bd,bucket=%[4]s,credentials=/credentials; then exit 1; fi
/mount.kstqr %[1]s /exports -o bd,bucket=%[2]s,credentials=/credentials
test -b /exports/data
test -b /exports/system
test "$(ls /exports | wc -l)" = 2
/block-test /exports/data rw
if /mount.kstqr %[1]s /selected -o bd,bucket=%[2]s,credentials=/credentials; then exit 1; fi
mke2fs -F -b 4096 /exports/system
mount -t ext4 /exports/system /fs
echo persistent-ext4 > /fs/hello
sync
umount /exports # Detached namespace must not destroy an in-use block device.
test "$(cat /fs/hello)" = persistent-ext4
umount /fs
for attempt in 1 2 3 4 5; do
 if /mount.kstqr %[1]s /selected -o bd,device=data,ro,bucket=%[2]s,credentials=/credentials; then break; fi
 sleep 1
done
test -b /selected/bd
test "$(ls /selected | wc -l)" = 1
/block-test /selected/bd ro
umount /selected
for attempt in 1 2 3 4 5; do
 if /mount.kstqr %[1]s /exports -o bd,bucket=%[2]s,credentials=/credentials; then break; fi
 sleep 1
done
mount -t ext4 /exports/system /fs
test "$(cat /fs/hello)" = persistent-ext4
umount /fs
umount /exports
rmmod kstqr
if dmesg | grep -E 'BUG:|WARNING:|Oops:|KASAN:'; then exit 1; fi
echo KSTQR_BLOCK_VM_PASS
trap - EXIT
poweroff -f
`, guest, client.ID(bucket.ID()).String(), client.ID(empty.ID()).String(), client.ID(bad.ID()).String())
	if err = os.WriteFile(filepath.Join(dir, "init"), []byte(init), 0755); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "initrd.cpio")
	out, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	pack := exec.CommandContext(ctx, "sh", "-c", "find . -print0 | cpio --null -o -H newc --owner=0:0")
	pack.Dir = dir
	pack.Stdout = out
	var packErrors bytes.Buffer
	pack.Stderr = &packErrors
	if e := pack.Run(); e != nil {
		out.Close()
		t.Fatalf("cpio: %v %s", e, packErrors.String())
	}
	out.Close()
	args = []string{"-machine", "q35", "-m", "512M", "-smp", "2", "-display", "none", "-serial", "stdio", "-monitor", "none", "-no-reboot", "-kernel", image, "-initrd", archive, "-append", "console=ttyS0 panic=1 oops=panic panic_on_warn=1 rdinit=/init loglevel=6", "-nic", "user,model=virtio-net-pci"}
	if os.Getenv("STQR_KERNEL_TCG") != "1" {
		args = append(args, "-enable-kvm", "-cpu", "host")
	}
	output, err := exec.CommandContext(ctx, "qemu-system-x86_64", args...).CombinedOutput()
	if p := os.Getenv("STQR_KERNEL_LOG"); p != "" {
		os.WriteFile(p, output, 0600)
	}
	if err != nil || !strings.Contains(string(output), "KSTQR_BLOCK_VM_PASS") || strings.Contains(string(output), "KSTQR_BLOCK_VM_FAIL") {
		t.Fatalf("kernel block VM: %v\n%s", err, output)
	}
	// Read the resulting stripes with the existing Go/NBD implementation.
	disk, err = nbd.Open(ctx, host.URL, admin.Username, admin.Password, client.ID(bucket.ID()), true)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	raw := make([]byte, 1024)
	if err = disk.ReadAt(ctx, data, raw, (1<<20)-512); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[:512], bytes.Repeat([]byte("X"), 512)) || !bytes.Equal(raw[512:], make([]byte, 512)) {
		t.Fatal("kernel trim changed unrelated bytes")
	}
	if _, _, err = disk.Client.Get(ctx, nbd.StripeID(data.ID, 2)); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("discard left full stripe allocated", err)
	}
	t.Log("native named block nodes, sparse/ranged IO, flush, discard, ext4 mount chaining, readonly, lease exclusion and teardown verified")
}
