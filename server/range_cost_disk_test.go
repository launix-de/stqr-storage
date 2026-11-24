// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/binary"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"os"
	"testing"
)

// Force multiple spilled runs and compare arbitrary range queries with a direct
// oracle. Exact physical costs matter: underestimates can promise unavailable
// capacity, while overestimates can reject a feasible small migration.
func TestRangeCostSpillIsExactAndBounded(t *testing.T) {
	var index rangeCostRows
	defer index.close()
	var rows []rangeCostRow
	for i := 0; i < 3*rangeCostMemoryRows+29; i++ {
		var id volume.ObjectID
		binary.BigEndian.PutUint32(id[:], uint32((i*7919)%65536))
		row := rangeCostRow{id: id, volume: volume.ID{byte(i % 3)}, physical: uint64(i % 173), receipts: uint64(i%2) * 512, count: 1}
		rows = append(rows, row)
		if err := index.add(row); err != nil {
			t.Fatal(err)
		}
		if len(index.rows) >= rangeCostMemoryRows {
			t.Fatal("unbounded planning memory")
		}
	}
	if index.file == nil {
		t.Fatal("large range did not spill")
	}
	path := index.file.Name()
	for _, bits := range []uint16{0, 8, 16, 24, 32, 256} {
		var start client.ObjectID
		binary.BigEndian.PutUint32(start[:], 32768)
		if bits < 32 {
			for i := int(bits); i < 256; i++ {
				start[i/8] &= ^(byte(1) << uint(7-i%8))
			}
		}
		entry := client.RangeEntry{Start: start, PrefixBits: bits}
		end, toEnd, err := client.RangeEnd(start, bits)
		if err != nil {
			t.Fatal(err)
		}
		for _, only := range []*volume.ID{nil, &(volume.ID{1})} {
			want, count := uint64(0), uint64(0)
			for _, row := range rows {
				if only != nil && row.volume != *only || bytes.Compare(row.id[:], start[:]) < 0 || !toEnd && bytes.Compare(row.id[:], end[:]) >= 0 {
					continue
				}
				want += row.physical
				if only == nil {
					want += row.receipts + row.count*256
				}
				count += row.count
			}
			if only == nil {
				want += 64 << 10
			}
			got, n, e := index.measure(entry, only)
			if e != nil || got != want || n != count {
				t.Fatalf("prefix=%d got=%d/%d want=%d/%d err=%v", bits, got, n, want, count, e)
			}
		}
	}
	index.close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("planning scratch file retained", err)
	}
}
