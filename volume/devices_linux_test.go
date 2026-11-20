// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixtureFile(t *testing.T, path, data string) {
	t.Helper()
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
}
func TestDeviceDiscoveryMountedPartitionsLVMAndAliases(t *testing.T) {
	root := t.TempDir()
	p := linuxDevicePaths{filepath.Join(root, "sys"), filepath.Join(root, "dev"), filepath.Join(root, "mapper"), filepath.Join(root, "mountinfo"), filepath.Join(root, "swaps")}
	for _, x := range []struct{ name, id, size, rot string }{{"sda", "8:0", "10000", "1"}, {"sda1", "8:1", "5000", "1"}, {"sdb", "8:16", "10000", "0"}, {"dm-0", "253:0", "8000", "0"}, {"dm-1", "253:1", "8000", "0"}} {
		base := filepath.Join(p.sys, x.name)
		fixtureFile(t, filepath.Join(base, "dev"), x.id)
		fixtureFile(t, filepath.Join(base, "size"), x.size)
		fixtureFile(t, filepath.Join(base, "ro"), "0")
		fixtureFile(t, filepath.Join(base, "queue/rotational"), x.rot)
		fixtureFile(t, filepath.Join(p.dev, x.name), "")
	}
	fixtureFile(t, filepath.Join(p.sys, "sda1/partition"), "1")
	fixtureFile(t, filepath.Join(p.sys, "sda/sda1/partition"), "1")
	fixtureFile(t, filepath.Join(p.sys, "sda/sda1/dev"), "8:1")
	fixtureFile(t, filepath.Join(p.sys, "dm-0/dm/uuid"), "LVM-abc")
	fixtureFile(t, p.mountinfo, "23 1 8:1 / /mnt rw - ext4 /dev/sda1 rw\n24 1 253:1 / /lv rw - ext4 /dev/dm-1 rw\n")
	fixtureFile(t, p.swaps, "Filename Type Size Used Priority\n")
	if e := os.MkdirAll(p.mapper, 0700); e != nil {
		t.Fatal(e)
	}
	alias := filepath.Join(p.mapper, "data-lv")
	if e := os.Symlink(filepath.Join(p.dev, "dm-0"), alias); e != nil {
		t.Fatal(e)
	}
	got, e := availableDevicePaths(p)
	if e != nil {
		t.Fatal(e)
	}
	if len(got) != 2 {
		t.Fatalf("expected sdb + available LVM: %+v", got)
	}
	found := false
	for _, d := range got {
		if d.Path == alias {
			found = true
			if d.Type != "lvm" || d.Class != "ssd" || d.SizeBytes != 8000*512 || len(d.Aliases) != 1 {
				t.Fatalf("bad LVM %+v", d)
			}
		}
	}
	if !found {
		t.Fatal("mapper alias absent")
	}
	// Even an unmounted mapper holder claims its underlying physical volume.
	fixtureFile(t, filepath.Join(p.sys, "sdb/holders/dm-0"), "")
	got, e = availableDevicePaths(p)
	if e != nil || len(got) != 1 {
		t.Fatalf("holder filtering %+v %v", got, e)
	}
}
func TestDiscoveryUnclaimedFilesAndJSON(t *testing.T) {
	dir := t.TempDir()
	free := filepath.Join(dir, "unformatted")
	fixtureFile(t, free, "unformatted data")
	owned := filepath.Join(dir, "owned")
	v, e := New(owned, 1<<20, HDD)
	if e != nil {
		t.Fatal(e)
	}
	list, e := ListBlockdevices(dir)
	if e != nil {
		t.Fatal(e)
	}
	seen := false
	for _, d := range list {
		if d.Path == owned {
			t.Fatal("owned file offered")
		}
		if d.Path == free {
			seen = true
			b, e := json.Marshal(d)
			if e != nil || !strings.Contains(string(b), `"size_bytes":16`) || !strings.Contains(string(b), `"type":"file"`) {
				t.Fatalf("%s %v", b, e)
			}
		}
	}
	if !seen {
		t.Fatal("unclaimed file missing")
	}
	v.Close()
	list, e = ListBlockdevices(dir)
	if e != nil {
		t.Fatal(e)
	}
	seen = false
	for _, d := range list {
		if d.Path == owned {
			seen = true
		}
	}
	if !seen {
		t.Fatal("closed volume not offered")
	}
	t.Setenv("VOLUMES_DIR", dir)
	if _, e = ListBlockdevices(); e != nil {
		t.Fatal(e)
	}
}
func TestMountedInfoMalformedFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mountinfo")
	fixtureFile(t, path, "bad entry\n")
	if _, e := mountIDs(path); e == nil {
		t.Fatal("malformed mount table accepted")
	}
}
