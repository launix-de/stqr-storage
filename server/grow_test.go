// SPDX-License-Identifier: GPL-3.0-only
package server

import (
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
		for _, size := range []int64{512 << 10, int64(info.Maximum) + volume.PageSize} {
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
