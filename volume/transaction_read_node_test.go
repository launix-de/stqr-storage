// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func readNodeFixture(t testing.TB, count int) (*Volume, *transaction, treeNode) {
	t.Helper()
	v, e := NewRAM(16 << 20)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { v.Close() })
	v.mu.Lock()
	t.Cleanup(v.mu.Unlock)
	n := treeNode{kind: treeFree}
	for i := 0; i < count; i++ {
		n.entries = append(n.entries, treeEntry{key: spaceKey(4<<20 + uint64(i)*4*PageSize), value: 2 * PageSize})
	}
	if _, e = v.dev.WriteAt(encodeNode(n, v.sb.Identity.VolumeID, 1), int64(v.sb.FreeRoot)); e != nil {
		t.Fatal(e)
	}
	v.nodes.clear()
	if _, e = v.node(v.sb.FreeRoot); e != nil {
		t.Fatal(e)
	}
	tx := v.transaction()
	t.Cleanup(tx.close)
	return v, tx, n
}
func TestTransactionReadNodeAllocation(t *testing.T) {
	_, tx, _ := readNodeFixture(t, 200)
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, _, e := tx.findSpace(1, 1, false); e != nil {
			panic(e)
		}
	})
	if allocs != 0 {
		t.Fatalf("read-only hot search allocated %.0f times", allocs)
	}
}
func TestTransactionReadNodeEditIsolation(t *testing.T) {
	for _, op := range []string{"insert", "insert-new", "replace", "delete", "claim", "coalesce", "merge-children"} {
		t.Run(op, func(t *testing.T) {
			v, tx, n := readNodeFixture(t, 3)
			root := tx.sb.FreeRoot
			if op == "merge-children" {
				left, right := uint64(2<<20), uint64(2<<20+PageSize)
				a, b := n, n
				a.entries = append([]treeEntry(nil), n.entries[:2]...)
				b.entries = append([]treeEntry(nil), n.entries[2:]...)
				n = treeNode{kind: treeFree, level: 1, entries: []treeEntry{tx.summary(left, a), tx.summary(right, b)}}
				for off, node := range map[uint64]treeNode{root: n, left: a, right: b} {
					if _, e := v.dev.WriteAt(encodeNode(node, v.sb.Identity.VolumeID, 1), int64(off)); e != nil {
						t.Fatal(e)
					}
				}
				v.nodes.clear()
				v.node(left)
				v.node(right)
				v.node(root)
			}
			snapshots := map[uint64][]byte{}
			v.nodes.mu.Lock()
			for off, entry := range v.nodes.items {
				snapshots[off] = encodeNode(entry.Value.(cachedNode).node, v.sb.Identity.VolumeID, 1)
			}
			v.nodes.mu.Unlock()
			var e error
			switch op {
			case "insert":
				e = tx.set(&tx.sb.FreeRoot, n.entries[0].key, PageSize)
			case "insert-new":
				e = tx.set(&tx.sb.FreeRoot, spaceKey(4<<20+3*PageSize), 1)
			case "replace":
				_, e = tx.replaceKeyAt(root, n.entries[0].key, spaceKey(4<<20+1), PageSize)
			case "delete":
				e = tx.remove(&tx.sb.FreeRoot, n.entries[0].key)
			case "claim":
				e = tx.claim(4<<20, PageSize)
			case "coalesce":
				e = tx.freeSpan(4<<20+2*PageSize, 2*PageSize)
			case "merge-children":
				e = tx.remove(&tx.sb.FreeRoot, spaceKey(4<<20))
			}
			if e != nil {
				t.Fatal(e)
			}
			for off, before := range snapshots {
				cached, ok := v.nodes.get(off)
				if !ok {
					t.Fatal("unexpected cache eviction")
				}
				if !bytes.Equal(before, encodeNode(cached, v.sb.Identity.VolumeID, 1)) {
					t.Fatalf("%s modified committed cached entries at%d", op, off)
				}
			}
			original, e := v.node(root)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(original, n) {
				t.Fatal("committed root changed")
			}
		})
	}
}
func TestTransactionReadNodeOverlayPrecedence(t *testing.T) {
	for _, mode := range []string{"dirty", "spilled", "accepted", "accepted-hot"} {
		t.Run(mode, func(t *testing.T) {
			v, tx, n := readNodeFixture(t, 3)
			off := tx.sb.FreeRoot
			n.entries[0].value = 99
			page := encodeNode(n, v.sb.Identity.VolumeID, 1)
			switch mode {
			case "dirty":
				tx.pages[off] = page
			case "spilled":
				f, e := os.CreateTemp(t.TempDir(), "pages")
				if e != nil {
					t.Fatal(e)
				}
				tx.spool = &encodedSpool{file: f}
				if _, e = tx.spool.Write(page); e != nil {
					t.Fatal(e)
				}
				tx.spilled = map[uint64]int64{off: 0}
			case "accepted", "accepted-hot":
				tx.acceptedPages = newDirtyStore(1, true)
				if e := tx.acceptedPages.put(off, page, 1); e != nil {
					t.Fatal(e)
				}
				if mode == "accepted-hot" {
					n.entries[0].value = 123
					tx.pages[off] = encodeNode(n, v.sb.Identity.VolumeID, 1)
				}
			}
			got, e := tx.readNode(off)
			if e != nil || !reflect.DeepEqual(got, n) {
				t.Fatalf("overlay: %v", e)
			}
			// Editable views may change without modifying either the read view or base.
			edit, e := tx.node(off)
			if e != nil {
				t.Fatal(e)
			}
			edit.entries[0].value = 777
			again, e := tx.readNode(off)
			if e != nil || again.entries[0].value != n.entries[0].value {
				t.Fatal("edit escaped into page overlay")
			}
			base, _ := v.node(off)
			if base.entries[0].value != 2*PageSize {
				t.Fatal("overlay changed committed node")
			}
			corrupt := bytes.Clone(page)
			corrupt[33] ^= 1
			tx.pages[off] = corrupt
			if _, e = tx.readNode(off); e != ErrCorrupt {
				t.Fatalf("corrupt overlay accepted: %v", e)
			}
		})
	}
}
func BenchmarkTransactionReadNode(b *testing.B) {
	for _, mode := range []string{"committed", "dirty", "spilled"} {
		b.Run(mode, func(b *testing.B) {
			v, tx, n := readNodeFixture(b, 200)
			off := v.sb.FreeRoot
			page := encodeNode(n, v.sb.Identity.VolumeID, 1)
			if mode == "dirty" {
				tx.pages[off] = page
			}
			if mode == "spilled" {
				f, e := os.CreateTemp(b.TempDir(), "pages")
				if e != nil {
					b.Fatal(e)
				}
				tx.spool = &encodedSpool{file: f}
				if _, e = tx.spool.Write(page); e != nil {
					b.Fatal(e)
				}
				tx.spilled = map[uint64]int64{off: 0}
			}
			for _, mutable := range []bool{true, false} {
				b.Run(fmt.Sprintf("mutable=%v", mutable), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var e error
						if mutable {
							_, _, e = treeNeighbor(tx.node, off, spaceKey(4<<20), false)
						} else {
							_, _, e = treeNeighbor(tx.readNode, off, spaceKey(4<<20), false)
						}
						if e != nil {
							b.Fatal(e)
						}
					}
				})
			}
		})
	}
}
