// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestQuotaCapacityCombinesNodesAndProtectsGuarantees(t *testing.T) {
	nodes := []quotaNodeCapacity{{Free: [3]uint64{100, 50, 20}}, {Free: [3]uint64{80, 70, 30}, Reserved: [3]uint64{0, 10, 0}}}
	for _, strategy := range []uint8{0, 1, 2} {
		c := quotaCapacity(nodes, strategy, 5, [3]uint64{200, 30, 10})
		expected := uint64(70)
		if strategy == 2 {
			expected = 40
		}
		if c.Available != expected || c.Maximum != expected+5 || c.Nodes != 2 {
			t.Fatalf("strategy %d: %+v", strategy, c)
		}
	}
}
func TestBucketQuotaCapacityExcludesAdminAndUsesProfile(t *testing.T) {
	s := testServer(t)
	p := userPrincipal(t, s)
	request := func(strategy, profile uint8) client.Message {
		e := client.Encoder{}
		e.ID(client.ID{})
		e.U8(strategy)
		e.U8(profile)
		e.U32(0)
		return userMessage(client.BucketQuotaCapacity, e)
	}
	body, status := s.dispatchAs(request(0, 5), nil, p)
	if status != 200 {
		t.Fatal(status)
	}
	d := client.Decoder{B: body}
	d.V()
	d.Blob()
	d.U64()
	if d.U64() != 0 {
		t.Fatal("admin volume counted as bucket capacity")
	}
	addTestVolume(t, s, "data-capacity", "hdd", 2<<20)
	body, status = s.dispatchAs(request(0, 5), nil, p)
	if status != 200 {
		t.Fatal(status)
	}
	d = client.Decoder{B: body}
	d.V()
	d.Blob()
	used, free, reserved, maximum := d.U64(), d.U64(), d.U64(), d.U64()
	if used != 0 || free == 0 || reserved != 0 || maximum != logicalCapacity(free, DefaultBucketPolicy().Limits.MetadataReservePPM) {
		t.Fatal("incorrect logical maximum")
	}
	if d.U32() != 1 || d.U32() != DefaultBucketPolicy().Limits.MetadataReservePPM {
		t.Fatal("profile mismatch")
	}
	n := d.Count(32)
	found := false
	for i := 0; i < n; i++ {
		key, label, size, ppm := d.U8(), d.Text(), d.U64(), d.U32()
		expected, e := volume.EstimatePutOverheadPPM(size)
		if e != nil || expected != ppm {
			t.Fatal("profile not derived from format")
		}
		if key == 5 {
			found = label == "1M (Blockdevices)" && size == 1<<20
		}
	}
	if !found || d.Done() != nil {
		t.Fatal("profile list")
	}
	_, status = s.dispatchAs(request(1, 5), nil, p)
	if status != 200 {
		t.Fatal(status)
	}
	ordinary := p
	ordinary.User.Role = "USER"
	if _, status = s.dispatchAs(request(0, 5), nil, ordinary); status != 403 {
		t.Fatal("non-admin capacity access")
	}
}
func TestUnusedGuaranteeSubtractsUsedBytes(t *testing.T) {
	p := DefaultBucketPolicy()
	p.Limits.Guaranteed = 100
	p.Limits.MetadataReservePPM = 0
	if unusedGuarantee(p, 40) != 60 || unusedGuarantee(p, 120) != 0 {
		t.Fatal("used guarantee was deducted again")
	}
}
