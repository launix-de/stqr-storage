// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"math"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestManualGrowthAndBounds(t *testing.T) {
	s := testServer(t)
	s.mu.Lock()
	s.running.RAMBytes = 4 << 20
	s.mu.Unlock()
	for _, source := range []uint8{0, 2} {
		class, name := "hdd", "file-grow"
		if source == 2 {
			class, name = "ram", "ram-grow"
		}
		op, e := s.AddVolume(newID(), AddVolumeRequest{Name: name, Source: source, Class: class, SizeBytes: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		id := op.VolumeID
		info, e := s.growthInfo(id)
		if e != nil || info.Current != 1<<20 || info.Maximum != growthMaximum(info.Current, info.Available) {
			t.Fatalf("bounds: %+v %v", info, e)
		}
		if source == 2 && info.Maximum != 4<<20 {
			t.Fatalf("RAM maximum counts existing size twice or omits it: %+v", info)
		}
		for _, size := range []int64{512 << 10, (1 << 20) + 1, int64(info.Maximum) + volume.PageSize} {
			bad, e := s.GrowVolume(newID(), id, size)
			if e == nil && bad.State != "failed" {
				t.Fatal("invalid growth accepted", size)
			}
			if s.volumes[id].store.Size() != 1<<20 {
				t.Fatal("invalid request changed volume")
			}
		}
		request := newID()
		grown, e := s.GrowVolume(request, id, 2<<20)
		if e != nil || grown.State != "fulfilled" {
			t.Fatalf("grow: %+v %v", grown, e)
		}
		again, e := s.GrowVolume(request, id, 2<<20)
		if e != nil || again.ID != grown.ID {
			t.Fatal("growth retry not idempotent", e)
		}
		if s.volumes[id].store.Size() != 2<<20 || s.volumes[id].config.SizeBytes != 2<<20 {
			t.Fatal("growth not published")
		}
		enc := client.Encoder{}
		parsed, _ := parseID(id)
		enc.ID(client.ID(parsed))
		body, status := s.dispatch(client.Message{Opcode: client.VolumeGrowInfo, Body: enc.B}, nil)
		d := client.Decoder{B: body}
		if status != 200 || d.V() != 200 {
			t.Fatal("growth preview failed")
		}
		d.Blob()
		if d.U8() != source || d.U64() != 2<<20 {
			t.Fatal("growth preview wire layout")
		}
		available, maximum := d.U64(), d.U64()
		if d.Done() != nil || maximum != growthMaximum(2<<20, available) {
			t.Fatal("growth preview maximum")
		}
	}
	path := s.configPath
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, e := openTransportTestNode(Options{ConfigPath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	for _, v := range reopened.volumes {
		if !v.config.System && v.store.Size() != 2<<20 {
			t.Fatal("size lost at restart")
		}
	}
}

func TestStorageGrowthRecoveryAfterCommittedLayout(t *testing.T) {
	s := testServer(t)
	added := addTestVolume(t, s, "growth-recovery", "hdd", 1<<20)
	b, _ := createProtocolBucket(t, s, "growth-recovery-data")
	oid := volume.ObjectID{0x71}
	payload := []byte("confirmed before storage-volume growth")
	if _, err := b.Put(context.Background(), userPrincipal(t, s), oid, payload, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	// Model interruption after committing a full-backend growth but before
	// publishing config/ACK. Raw-device wire requests use zero, whereas the
	// operation records the resolved physical target before touching the layout.
	op, _, err := s.beginOperation(newID(), "grow", struct {
		Volume    string
		SizeBytes int64
	}{added.VolumeID, 0})
	if err != nil {
		t.Fatal(err)
	}
	op.VolumeID, op.SizeBytes = added.VolumeID, 2<<20
	if err = s.saveOperation(op); err != nil {
		t.Fatal(err)
	}
	if err = s.volumes[added.VolumeID].store.Grow(op.SizeBytes); err != nil {
		t.Fatal(err)
	}
	path := s.configPath
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTransportTestNode(Options{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.operation(op.ID)
	if err != nil || recovered.State != "fulfilled" || reopened.volumes[added.VolumeID].config.SizeBytes != 2<<20 {
		t.Fatal("growth not recovered", recovered, err)
	}
	old, err := reopened.Bucket(b.ID())
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := old.Get(context.Background(), userPrincipal(t, reopened), oid)
	if err != nil || string(got) != string(payload) {
		t.Fatal("recovery lost committed objects", err)
	}
}
func TestGrowthMaximumOverflowAndPageAlignment(t *testing.T) {
	if got := growthMaximum(1<<20, 3<<20); got != 4<<20 {
		t.Fatal(got)
	}
	if got := growthMaximum(1<<20, 4095); got != 1<<20 {
		t.Fatal(got)
	}
	if got := growthMaximum(1<<20, math.MaxUint64); got > math.MaxInt64 || got%volume.PageSize != 0 {
		t.Fatal(got)
	}
}
