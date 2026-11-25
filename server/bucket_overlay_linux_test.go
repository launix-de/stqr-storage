// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"golang.org/x/sys/unix"
)

// Two independent daemons, two readonly lower mounts and two private uppers.
// With STQR_FUSE_BINARY set, mount through the actual CLI instead of the adapter.
func TestBucketNativeOverlayPHPCluster(t *testing.T) {
	if os.Getenv("STQR_TEST_FUSE") != "1" {
		t.Skip("set STQR_TEST_FUSE=1 for kernel integration")
	}
	php, err := exec.LookPath("php")
	if err != nil {
		t.Skip("PHP CLI required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lowerServer := testServer(t)
	lowerVolume := addTestVolume(t, lowerServer, "shared-code", "hdd", 32<<20)
	baseBucket, _ := createProtocolBucket(t, lowerServer, "shared-code")
	upperServer := testServer(t)
	addTestVolume(t, upperServer, "local-uppers", "hdd", 32<<20)
	upper1, _ := createProtocolBucket(t, upperServer, "php-node-1")
	upper2, _ := createProtocolBucket(t, upperServer, "php-node-2")
	lowerHost, upperHost := httptest.NewServer(lowerServer.Handler()), httptest.NewServer(upperServer.Handler())
	t.Cleanup(lowerHost.Close)
	t.Cleanup(upperHost.Close)
	la, ua := lowerServer.cfg.Auth.BootstrapAdmin, upperServer.cfg.Auth.BootstrapAdmin
	dial := func(uri, user, password string, id client.ID) *client.Client {
		t.Helper()
		c, e := client.Dial(ctx, uri, user, password, id, "fs")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	publisher := dial(lowerHost.URL, la.Username, la.Password, client.ID(baseBucket.ID()))
	base, err := client.OpenFilesystem(ctx, publisher, client.FSOptions{Create: true, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	if err != nil {
		t.Fatal(err)
	}
	root := client.FilesystemRootID()
	create := func(parent client.ObjectID, name string, data []byte, directory bool) client.FSInfo {
		t.Helper()
		mode := uint32(0644)
		if directory {
			mode = 0755
		}
		info, e := base.Create(ctx, parent, name, mode, uint32(os.Getuid()), uint32(os.Getgid()), directory)
		if e != nil {
			t.Fatal(e)
		}
		if !directory {
			if _, e = base.WriteAt(ctx, info.ID, data, 0); e != nil {
				t.Fatal(e)
			}
		}
		return info
	}
	conf := create(root, "conf.json", []byte(`{"value":"base"}`), false)
	create(root, "code.php", []byte(`<?php $c=json_decode(file_get_contents(__DIR__.'/conf.json'),true); echo $c['value'];`), false)
	dir := create(root, "fop-files", nil, true)
	create(dir.ID, "retired", []byte("will be deleted in both layers"), false)
	bigData := bytes.Repeat([]byte("base-file-"), 250000)
	big := create(dir.ID, "big", bigData, false)
	reader := bucketProtocolUser(t, lowerServer)
	uid, _ := parseID(reader.ID)
	e := client.Encoder{}
	e.ID(client.ID(baseBucket.ID()))
	e.ID(client.ID(uid))
	e.U32(uint32(BucketRead))
	_, status, _ := adminBucketMessage(t, lowerServer, client.BucketGrant, e.B)
	if status != 200 {
		t.Fatal(status)
	}
	before := lowerServer.volumes[lowerVolume.VolumeID].store.Stats().IO.WriteBytes
	readerConnection := func() *client.Client {
		return dial(lowerHost.URL, reader.Name, "bucket-test-password", client.ID(baseBucket.ID()))
	}
	l1, l2, ol1, ol2 := readerConnection(), readerConnection(), readerConnection(), readerConnection()
	u1, u2 := dial(upperHost.URL, ua.Username, ua.Password, client.ID(upper1.ID())), dial(upperHost.URL, ua.Username, ua.Password, client.ID(upper2.ID()))
	ro1, err := client.OpenFilesystem(ctx, l1, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	ro2, err := client.OpenFilesystem(ctx, l2, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	o1, err := client.OpenOverlayFilesystem(ctx, ol1, u1, client.OverlayOptions{Create: true, LowerBucketID: client.ID(baseBucket.ID()), Whitelist: []string{"conf.json", "fop-files/"}})
	if err != nil {
		t.Fatal(err)
	}
	o2, err := client.OpenOverlayFilesystem(ctx, ol2, u2, client.OverlayOptions{Create: true, LowerBucketID: client.ID(baseBucket.ID()), Whitelist: []string{"conf.json", "fop-files/"}})
	if err != nil {
		t.Fatal(err)
	}
	mount := func(view client.FilesystemView, readonly bool, extra []string) (string, func()) {
		t.Helper()
		point := t.TempDir()
		binary := os.Getenv("STQR_FUSE_BINARY")
		if binary == "" {
			mounted, e := client.MountFilesystem(view, point, client.FSMountOptions{ReadOnly: readonly})
			if e != nil {
				t.Fatal(e)
			}
			stopped := false
			stop := func() {
				if stopped {
					return
				}
				stopped = true
				if e := mounted.Unmount(); e != nil {
					t.Error(e)
					return
				}
				mounted.Wait()
			}
			t.Cleanup(stop)
			return point, stop
		}
		log, e := os.Create(filepath.Join(t.TempDir(), "client.log"))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { log.Close() })
		command := exec.CommandContext(ctx, binary, append(extra, point)...)
		command.Stdout, command.Stderr = log, log
		if e = command.Start(); e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			if output, e := exec.Command("fusermount3", "-u", point).CombinedOutput(); e != nil {
				t.Errorf("unmount: %v %s", e, output)
				command.Process.Kill()
			}
			select {
			case e := <-done:
				if e != nil {
					t.Error(e)
				}
			case <-time.After(5 * time.Second):
				command.Process.Kill()
				t.Error("client did not stop")
			}
		}
		t.Cleanup(stop)
		deadline := time.Now().Add(10 * time.Second)
		for {
			var st unix.Statfs_t
			if unix.Statfs(point, &st) == nil && st.Type == 0x65735546 {
				break
			}
			select {
			case e := <-done:
				stopped = true
				output, _ := os.ReadFile(log.Name())
				t.Fatalf("mount process: %v %s", e, output)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("mount startup timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
		return point, stop
	}
	roArgs := []string{"-uri", lowerHost.URL, "-username", reader.Name, "-password", "bucket-test-password", "--readonly-bucket", baseBucket.ID().String()}
	upperArgs := func(id string) []string {
		return []string{"-uri", upperHost.URL, "-username", ua.Username, "-password", ua.Password, "--bucket", id, "--readonly-bucket", baseBucket.ID().String(), "--readonly-uri", lowerHost.URL, "--readonly-username", reader.Name, "--readonly-password", "bucket-test-password", "--whitelist=conf.json,fop-files/"}
	}

	// A finite upper has a distinguishable df size from the larger lower.
	quota := client.Encoder{}
	quota.ID(client.ID(upper1.ID()))
	quota.U8(0)
	quota.U64(0)
	quota.Bool(false)
	quota.U64(16 << 20)
	quota.U32(150000)
	quota.U64(upper1.Describe().Policy.Version)
	if _, status, _ := adminBucketMessage(t, upperServer, client.BucketSetQuota, quota.B); status != 200 {
		t.Fatal(status)
	}
	p1, stopRO1 := mount(ro1, true, roArgs)
	p2, stopRO2 := mount(ro2, true, roArgs)
	p3, stop1 := mount(o1, false, upperArgs(upper1.ID().String()))
	p4, stop2 := mount(o2, false, upperArgs(upper2.ID().String()))

	var lowerSpace, upperSpace syscall.Statfs_t
	if err := syscall.Statfs(p1, &lowerSpace); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Statfs(p3, &upperSpace); err != nil {
		t.Fatal(err)
	}
	if upperSpace.Blocks*uint64(upperSpace.Bsize) != 16<<20 || lowerSpace.Blocks <= upperSpace.Blocks {
		t.Fatalf("overlay statfs must report upper quota: lower=%+v upper=%+v", lowerSpace, upperSpace)
	}
	for _, point := range []string{p1, p2} {
		if e := os.WriteFile(filepath.Join(point, "conf.json"), []byte("bad"), 0600); !errors.Is(e, syscall.EROFS) {
			t.Fatal("readonly mount permits mutation", e)
		}
	}
	// Fault mapped pages in a separate process: a Go page fault must not wait
	// for a server in the same Go runtime while GC is stopping that runtime.
	mmapOutput, e2 := exec.CommandContext(ctx, "python3", "-c", `import mmap,sys; f=open(sys.argv[1],'rb'); m=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ); assert m[:5]==b'<?php'; m.close(); f.close()`, filepath.Join(p1, "code.php")).CombinedOutput()
	if e2 != nil {
		t.Fatalf("readonly mmap: %v %s", e2, mmapOutput)
	}
	runPHP := func(point, want string) {
		t.Helper()
		output, e := exec.CommandContext(ctx, php, "-d", "opcache.enable_cli=1", filepath.Join(point, "code.php")).CombinedOutput()
		if e != nil || string(output) != want {
			t.Fatalf("PHP: %v %q", e, output)
		}
	}
	for _, point := range []string{p1, p2, p3, p4} {
		runPHP(point, "base")
	}
	if err = os.Chmod(filepath.Join(p3, "conf.json"), 0600); err != nil {
		t.Fatal(err)
	}
	output, e2 := exec.CommandContext(ctx, php, "-r", `if(file_put_contents($argv[1],'{"value":"node-1"}',LOCK_EX)===false)exit(1); if(!touch($argv[1],1700000000,1700000001))exit(2);`, filepath.Join(p3, "conf.json")).CombinedOutput()
	if e2 != nil {
		t.Fatalf("PHP LOCK_EX/touch: %v %s", e2, output)
	}
	if info, e := os.Stat(filepath.Join(p3, "conf.json")); e != nil || info.ModTime().Unix() != 1700000000 {
		t.Fatal("touch timestamp lost", info, e)
	}
	lockFile, e2 := os.OpenFile(filepath.Join(p3, "conf.json"), os.O_RDWR, 0)
	if e2 != nil {
		t.Fatal(e2)
	}
	defer lockFile.Close()
	if e2 = unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); e2 != nil {
		t.Fatal(e2)
	}
	lockOutput, e2 := exec.CommandContext(ctx, php, "-r", `$f=fopen($argv[1],'r+'); echo flock($f,LOCK_EX|LOCK_NB)?'acquired':'blocked';`, filepath.Join(p3, "conf.json")).CombinedOutput()
	if e2 != nil || string(lockOutput) != "blocked" {
		t.Fatalf("PHP lock exclusion: %v %q", e2, lockOutput)
	}
	if e2 = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); e2 != nil {
		t.Fatal(e2)
	}
	lockFile.Close()
	readBefore, e2 := u1.Stat(ctx, conf.ID)
	if e2 != nil {
		t.Fatal(e2)
	}
	runPHP(p3, "node-1")
	runPHP(p3, "node-1")
	readAfter, e2 := u1.Stat(ctx, conf.ID)
	if e2 != nil || readAfter.Length != readBefore.Length {
		t.Fatal("reading persisted a metadata update", readBefore, readAfter, e2)
	}
	runPHP(p3, "node-1")
	runPHP(p4, "base")
	runPHP(p1, "base")
	runPHP(p2, "base")
	if err = os.WriteFile(filepath.Join(p3, "code.php"), []byte("bad"), 0600); !errors.Is(err, syscall.EROFS) {
		t.Fatal("protected PHP code writable", err)
	}
	if err = os.WriteFile(filepath.Join(p3, "evil.php"), []byte("bad"), 0600); !errors.Is(err, syscall.EROFS) {
		t.Fatal("protected root writable", err)
	}
	patch, e2 := os.OpenFile(filepath.Join(p3, "fop-files", "big"), os.O_RDWR, 0)
	if e2 != nil {
		t.Fatal(e2)
	}
	if _, e2 = patch.WriteAt([]byte("XX"), 123456); e2 != nil {
		t.Fatal(e2)
	}
	patch.Close()
	if used, e := upper1.logicalUsage(); e != nil || used > 64<<10 {
		t.Fatal("partial write copied base payload", used, e)
	}
	got, e2 := os.ReadFile(filepath.Join(p3, "fop-files", "big"))
	if e2 != nil {
		t.Fatal(e2)
	}
	want := append([]byte(nil), bigData...)
	copy(want[123456:], "XX")
	if !bytes.Equal(got, want) {
		t.Fatal("partial merge differs")
	}
	if original, e := os.ReadFile(filepath.Join(p4, "fop-files", "big")); e != nil || !bytes.Equal(original, bigData) {
		t.Fatal("private upper leaked", e)
	}
	if err = os.Remove(filepath.Join(p3, "conf.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(p3, "conf.json")); !os.IsNotExist(err) {
		t.Fatal("whiteout failed", err)
	}
	tmp := filepath.Join(p3, "fop-files", "next.json")
	if err = os.WriteFile(tmp, []byte(`{"value":"replacement"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(tmp, filepath.Join(p3, "conf.json")); err != nil {
		t.Fatal(err)
	}
	runPHP(p3, "replacement")
	if err = os.Remove(filepath.Join(p3, "fop-files", "retired")); err != nil {
		t.Fatal(err)
	}
	stop1()
	stop2()
	if after := lowerServer.volumes[lowerVolume.VolumeID].store.Stats().IO.WriteBytes; after != before {
		t.Fatal("readonly phase wrote lower", before, after)
	}
	maintenanceArgs := []string{"-uri", lowerHost.URL, "-username", la.Username, "-password", la.Password, "--bucket", baseBucket.ID().String()}
	maintenancePoint, stopMaintenance := mount(base, false, maintenanceArgs)
	if err = os.Remove(filepath.Join(maintenancePoint, "fop-files", "retired")); err != nil {
		t.Fatal("lower maintenance delete", err)
	}
	stopMaintenance()
	before = lowerServer.volumes[lowerVolume.VolumeID].store.Stats().IO.WriteBytes
	// Remount and run upper fsck with actual cross-bucket references in place.
	reopened, err := client.OpenOverlayFilesystem(ctx, ol1, u1, client.OverlayOptions{LowerBucketID: client.ID(baseBucket.ID())})
	if err != nil {
		t.Fatal(err)
	}
	point, stopAgain := mount(reopened, false, upperArgs(upper1.ID().String()))
	runPHP(point, "replacement")
	stopAgain()
	result, err := u1.CheckOverlayFilesystem(ctx, ol1, client.ID(baseBucket.ID()))
	if err != nil || result.Deleted == 0 || result.WhiteoutsRemoved < 2 {
		t.Fatal(result, err)
	}
	buf := make([]byte, 2)
	if _, err = reopened.ReadAt(ctx, big.ID, buf, 123456); err != nil || string(buf) != "XX" {
		t.Fatal("upper fsck lost referenced data", string(buf), err)
	}
	if binary := os.Getenv("STQR_FUSE_BINARY"); binary != "" {
		u1.Close()
		deadline := time.Now().Add(5 * time.Second)
		for {
			busy := false
			upperServer.wsMu.Lock()
			for ws := range upperServer.sockets {
				if ws.authorization != nil && ws.authorization.bucket == upper1 {
					busy = true
				}
			}
			upperServer.wsMu.Unlock()
			if !busy {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("upper connection did not close")
			}
			time.Sleep(time.Millisecond)
		}
		output, e := exec.CommandContext(ctx, binary, append(upperArgs(upper1.ID().String()), "--fsck")...).CombinedOutput()
		if e != nil || !strings.Contains(string(output), "fsck OK") || !strings.Contains(string(output), "deleted 0") {
			t.Fatalf("overlay fsck CLI: %v %s", e, output)
		}
	}
	if after := lowerServer.volumes[lowerVolume.VolumeID].store.Stats().IO.WriteBytes; after != before {
		t.Fatal("readonly lower received physical writes", before, after)
	}
	stopRO1()
	stopRO2()
	if !strings.Contains(string(got), "XX") {
		t.Fatal("patch missing")
	}
}
