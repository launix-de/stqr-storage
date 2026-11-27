// SPDX-License-Identifier: GPL-3.0-only
package volume

import "testing"

func TestAdmissionFreePhysicalMatchesStats(t *testing.T) {
	v, e := NewRAM(4 << 20)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	initial := v.Stats().AdmissionFreePhysical
	for _, reserves := range [][2]uint64{{0, 0}, {123, 456}, {initial, 0}, {initial - 1, 1}, {0, initial}, {0, 0}} {
		if e = v.SetInFlightReserve(0); e != nil {
			t.Fatal(e)
		}
		if e = v.SetGuaranteedReserve(reserves[0]); e != nil {
			t.Fatal(e)
		}
		if e = v.SetInFlightReserve(reserves[1]); e != nil {
			t.Fatal(e)
		}
		stats := v.Stats()
		got := v.AdmissionFreePhysical()
		if got != stats.AdmissionFreePhysical || got != initial-reserves[0]-reserves[1] {
			t.Fatalf("reserves%v free%d stats%d", reserves, got, stats.AdmissionFreePhysical)
		}
	}
	before := v.IOStats()
	for i := 0; i < 100; i++ {
		v.AdmissionFreePhysical()
	}
	if v.IOStats() != before {
		t.Fatal("capacity observation performed I/O")
	}
}
