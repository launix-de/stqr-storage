// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestVolumeCapacityUsesDFAndAvailableRAM(t *testing.T) {
	s := testServer(t)
	c, e := s.volumeCapacity()
	if e != nil {
		t.Fatal(e)
	}
	var stat syscall.Statfs_t
	if e = syscall.Statfs(s.volumesDir, &stat); e != nil {
		t.Fatal(e)
	}
	// Availability can change between calls; compare the upper bound and accounting
	// rule rather than asserting an exact free-space snapshot on a shared host.
	if c.FileAvailable > stat.Blocks*uint64(stat.Bsize) || c.RAMAvailable == 0 || c.RAMLimit != c.RAMAvailable {
		t.Fatalf("invalid capacities: %+v", c)
	}
	body, status := s.dispatch(client.Message{Opcode: client.VolumeCapacityGet}, nil)
	d := client.Decoder{B: body}
	if status != 200 || d.V() != 200 {
		t.Fatal("capacity command failed")
	}
	d.Blob()
	if d.U64() == 0 || d.U64() == 0 || d.U64() == 0 || d.Done() != nil {
		t.Fatal("capacity wire layout")
	}
}
func TestNewFileAndRAMCapacityValidation(t *testing.T) {
	s := testServer(t)
	if _, e := s.AddVolume(newID(), AddVolumeRequest{Name: "too-large", Class: "hdd", SizeBytes: 1 << 62}); statusOf(e) != 507 {
		t.Fatalf("file capacity not enforced: %v", e)
	}
	if _, e := os.Stat(filepath.Join(s.volumesDir, "too-large")); !os.IsNotExist(e) {
		t.Fatal("oversized file was created")
	}
	// With no explicit RAM cap, actual available RAM is the limit, not a disabled
	// zero budget. Small RAM volumes can be created without changing config first.
	first, e := s.AddVolume(newID(), AddVolumeRequest{Name: "ram-auto", Source: 2, Class: "ram", SizeBytes: volume.MinVolumeSize})
	if e != nil {
		t.Fatal(e)
	}
	if s.volumes[first.VolumeID].store.Identity().Class != volume.RAM {
		t.Fatal("RAM source did not create RAM backend")
	}
	s.mu.Lock()
	s.running.RAMBytes = volume.MinVolumeSize
	s.mu.Unlock()
	c, e := s.volumeCapacity()
	if e != nil || c.RAMLimit != 0 {
		t.Fatalf("assigned RAM budget ignored: %+v %v", c, e)
	}
	if _, e = s.AddVolume(newID(), AddVolumeRequest{Name: "ram-over-budget", Source: 2, Class: "ram", SizeBytes: volume.MinVolumeSize}); statusOf(e) != 507 {
		t.Fatal("explicit RAM budget ignored", e)
	}
}
