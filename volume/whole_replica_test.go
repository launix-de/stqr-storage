// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"testing"
)

func prepareWholeReplica(t *testing.T, v *Volume, k Key, op uint8, old, next []byte, id ID, binding ReplicaBinding) Result {
	t.Helper()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	var expected []byte
	if op == opCAS {
		expected = old
	}
	result, err := h.PrepareMutation(op, k, 0, uint64(len(next)), bytes.NewReader(next), expected, Options{RequestID: id, WholeObject: op == opCAS}, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func decideWholeReplica(t *testing.T, v *Volume, id ID, binding ReplicaBinding) {
	t.Helper()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.DecideReplica(id, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
}
func publishWholeReplica(t *testing.T, v *Volume, id ID, binding ReplicaBinding) Result {
	t.Helper()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	result, err := h.PublishReplica(id, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestWholeReplicaGrowthStaysPacked(t *testing.T) {
	for _, op := range []uint8{opPut, opCAS} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			v, _ := memoryVolume(t, 8<<20)
			defer v.Close()
			k := testKey(1)
			raw := bytes.Repeat([]byte("r"), 8<<10)
			if _, err := testPut(v, k, raw, Options{}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				next := append(bytes.Clone(raw), bytes.Repeat([]byte{byte(i)}, 81)...)
				id := testID(uint64(i + 100))
				binding := ReplicaBinding{Epoch: 1, Sequence: uint64(i + 1), SetHash: [32]byte{7}}
				prepared := prepareWholeReplica(t, v, k, op, raw, next, id, binding)
				mustGet(t, v, k, raw)
				decideWholeReplica(t, v, id, binding)
				mustGet(t, v, k, raw)
				if published := publishWholeReplica(t, v, id, binding); published != prepared {
					t.Fatal("publication changed prepared result", published, prepared)
				}
				assertPackedRaw(t, v, k, uint64(len(next)))
				mustGet(t, v, k, next)
				if repeated := publishWholeReplica(t, v, id, binding); repeated != prepared {
					t.Fatal("retry changed result", repeated, prepared)
				}
				if v.Stats().Objects != 1 {
					t.Fatal("whole mutation changed object identity/count", v.Stats())
				}
				raw = next
			}
			assertFreeTree(t, v)
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestWholeReplicaGrowthRecoversEveryPhase(t *testing.T) {
	for _, op := range []uint8{opPut, opCAS} {
		t.Run(fmt.Sprint(op), func(t *testing.T) {
			disk := newCrash(8 << 20)
			v, err := Format(disk, FormatOptions{Class: SSD})
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			key := testKey(1)
			old := bytes.Repeat([]byte("old"), 3000)
			next := append(bytes.Clone(old), []byte("replicated whole growth")...)
			if _, err := testPut(v, key, old, Options{}); err != nil {
				t.Fatal(err)
			}
			id := testID(800)
			binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{8}}
			want := prepareWholeReplica(t, v, key, op, old, next, id, binding)
			crashOpen := func(d *crashIO) (*Volume, *crashIO) {
				t.Helper()
				copy := d.crash()
				opened, err := Open(copy, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { opened.Close() })
				return opened, copy
			}
			prepared, pdisk := crashOpen(disk)
			mustGet(t, prepared, key, old)
			h, err := prepared.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			record, found, err := h.ReplicaStatus(id)
			h.Close()
			if err != nil || !found || record.State != ReplicaPrepared {
				t.Fatal("prepare did not recover", record, found, err)
			}
			decideWholeReplica(t, prepared, id, binding)
			decided, ddisk := crashOpen(pdisk)
			mustGet(t, decided, key, old)
			h, err = decided.Open(false)
			if err != nil {
				t.Fatal(err)
			}
			record, found, err = h.ReplicaStatus(id)
			h.Close()
			if err != nil || !found || record.State != ReplicaDecided {
				t.Fatal("decision did not recover", record, found, err)
			}
			if got := publishWholeReplica(t, decided, id, binding); got != want {
				t.Fatal("published result changed", got, want)
			}
			published, _ := crashOpen(ddisk)
			mustGet(t, published, key, next)
			assertPackedRaw(t, published, key, uint64(len(next)))
			if got := publishWholeReplica(t, published, id, binding); got != want {
				t.Fatal("recovered retry changed result", got, want)
			}
			assertFreeTree(t, published)
			if err := published.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
