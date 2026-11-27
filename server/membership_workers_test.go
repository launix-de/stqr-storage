// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/volume"
)

func TestMembershipAdminAttachStartsOptionalWarm(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(s.volumesDir, "membership-existing")
	v, err := volume.New(path, 8<<20, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	key := volume.Key{Bucket: volume.ID{0x44}, Object: volume.ObjectID{1}}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(key, []byte("existing data"), volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	op, err := s.AddVolume(newID(), AddVolumeRequest{Name: "membership-existing", Class: "hdd"})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	v = s.volumes[op.VolumeID].store
	s.mu.RUnlock()
	deadline := time.Now().Add(3 * time.Second)
	absent := volume.Key{Bucket: volume.ID{0x44}, Object: volume.ObjectID{2}}
	for v.MayContainObject(absent) {
		if time.Now().After(deadline) {
			t.Fatal("admin-attached volume never warmed")
		}
		time.Sleep(time.Millisecond)
	}
	if !v.MayContainObject(key) {
		t.Fatal("warm excluded existing object")
	}
}

func TestMembershipWorkerRegistrationCoalescesAndCloseJoins(t *testing.T) {
	s := testServer(t)
	op := addTestVolume(t, s, "membership-workers", "hdd", 8<<20)
	s.mu.RLock()
	v := s.volumes[op.VolumeID].store
	s.mu.RUnlock()
	// Hold the native handle so a new optional warm attempt cannot finish until
	// we explicitly release it. Existing completed Bloom data remains harmless.
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	s.startVolumeMembership(v)
	for i := 0; i < 32; i++ {
		s.startVolumeMembership(v)
	}
	s.mu.RLock()
	jobs := len(s.membershipJobs)
	s.mu.RUnlock()
	if jobs != 1 {
		h.Close()
		t.Fatal("warm attempts were not coalesced", jobs)
	}
	var callers sync.WaitGroup
	for i := 0; i < 32; i++ {
		callers.Add(1)
		go func() { defer callers.Done(); s.startVolumeMembership(v) }()
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	callers.Wait()
	h.Close()
	awaitParallel(t, closed)
	s.mu.RLock()
	jobs = len(s.membershipJobs)
	s.mu.RUnlock()
	if jobs != 0 {
		t.Fatal("closed volume retained by warm registry", jobs)
	}
	if !v.MayContainObject(volume.Key{}) {
		t.Fatal("closed volume returned definite absence")
	}
}
