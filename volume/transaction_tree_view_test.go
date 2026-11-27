// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

func installSearchPages(t testing.TB, tx *transaction, pages map[uint64]treeNode, mode string) {
	t.Helper()
	if mode == "spilled" {
		f, e := os.CreateTemp(t.TempDir(), "tree-pages")
		if e != nil {
			t.Fatal(e)
		}
		tx.spool = &encodedSpool{file: f}
		tx.spilled = make(map[uint64]int64)
	}
	if mode == "accepted" {
		tx.acceptedPages = newDirtyStore(16, true)
	}
	for off, n := range pages {
		page := encodeNode(n, tx.sb.Identity.VolumeID, 1)
		switch mode {
		case "committed":
			if _, e := tx.v.dev.WriteAt(page, int64(off)); e != nil {
				t.Fatal(e)
			}
		case "dirty":
			tx.pages[off] = page
		case "accepted":
			if e := tx.acceptedPages.put(off, page, 1); e != nil {
				t.Fatal(e)
			}
		case "spilled":
			tx.spilled[off] = tx.spool.size
			if _, e := tx.spool.Write(page); e != nil {
				t.Fatal(e)
			}
		default:
			t.Fatal(mode)
		}
	}
	if mode == "committed" {
		tx.v.nodes.clear()
		for off := range pages {
			if _, e := tx.v.node(off); e != nil {
				t.Fatal(e)
			}
		}
	}
}
func TestTransactionTreeViewNeighbors(t *testing.T) {
	for _, mode := range []string{"committed", "dirty", "accepted", "spilled"} {
		t.Run(mode, func(t *testing.T) {
			v, tx, _ := readNodeFixture(t, 1)
			left, middle, right := uint64(2<<20), uint64(2<<20+PageSize), uint64(2<<20+2*PageSize)
			base := uint64(4 << 20)
			entries := []treeEntry{{key: spaceKey(base), value: 10}, {key: spaceKey(base + 20), value: 20}, {key: spaceKey(base + 60), value: 30}, {key: spaceKey(base + 80), value: 40}}
			root := treeNode{kind: treeFree, level: 1, entries: []treeEntry{{key: spaceKey(base), value: left, maximum: 20}, {key: spaceKey(base + 40), value: middle, maximum: 0}, {key: spaceKey(base + 60), value: right, maximum: 40}}}
			installSearchPages(t, tx, map[uint64]treeNode{tx.sb.FreeRoot: root, left: {kind: treeFree, entries: entries[:2]}, middle: {kind: treeFree}, right: {kind: treeFree, entries: entries[2:]}}, mode)
			for _, off := range []uint64{0, base - 1, base, base + 1, base + 19, base + 20, base + 21, base + 39, base + 40, base + 59, base + 60, base + 79, base + 80, base + 81, ^uint64(0)} {
				for _, suffix := range []bool{false, true} {
					key := spaceKey(off)
					if suffix {
						key[48] = 1
					}
					for _, ceil := range []bool{false, true} {
						var want treeEntry
						found := false
						for _, e := range entries {
							if ceil {
								if !lessKey(e.key, key) {
									want = e
									found = true
									break
								}
							} else if !lessKey(key, e.key) {
								want = e
								found = true
							}
						}
						got, ok, e := tx.neighbor(tx.sb.FreeRoot, key, ceil)
						if e != nil || ok != found || ok && got != want {
							t.Fatalf("offset%d suffix%v ceil%v: got%v/%v/%v want%v/%v", off, suffix, ceil, got, ok, e, want, found)
						}
					}
				}
			}
			before := v.ioStatsLocked().SyncOps
			contains, e := tx.freeContains(tx.sb.FreeRoot, base+5, 5)
			if e != nil || !contains {
				t.Fatal("contained span", contains, e)
			}
			contains, e = tx.freeContains(tx.sb.FreeRoot, base+5, 6)
			if e != nil || contains {
				t.Fatal("outside span", contains, e)
			}
			if v.ioStatsLocked().SyncOps != before {
				t.Fatal("search synced")
			}
		})
	}
	for _, mode := range []string{"committed", "dirty", "spilled"} {
		t.Run("empty/"+mode, func(t *testing.T) {
			_, tx, _ := readNodeFixture(t, 1)
			installSearchPages(t, tx, map[uint64]treeNode{tx.sb.FreeRoot: {kind: treeFree}}, mode)
			for _, ceil := range []bool{false, true} {
				if _, ok, e := tx.neighbor(tx.sb.FreeRoot, indexKey{}, ceil); e != nil || ok {
					t.Fatal(ok, e)
				}
			}
			if _, _, _, e := tx.findSpace(1, 1, false); e != ErrFull {
				t.Fatal(e)
			}
		})
	}
}
func TestTransactionTreeViewCorruptionAndErrors(t *testing.T) {
	for _, shape := range []string{"bad-crc", "self", "cycle", "wrong-kind", "wrong-level", "missing-spill", "closed-accepted", "device-error"} {
		t.Run(shape, func(t *testing.T) {
			v, tx, n := readNodeFixture(t, 1)
			root, child := tx.sb.FreeRoot, uint64(2<<20)
			parent := treeNode{kind: treeFree, level: 1, entries: []treeEntry{{key: n.entries[0].key, value: child, maximum: 1 << 20, aligned: 1 << 20}}}
			pages := map[uint64]treeNode{root: parent, child: n}
			switch shape {
			case "self":
				parent.entries[0].value = root
				pages[root] = parent
			case "cycle":
				parent.level = 2
				pages[root] = parent
				pages[child] = treeNode{kind: treeFree, level: 1, entries: []treeEntry{{key: n.entries[0].key, value: root, maximum: 1 << 20}}}
			case "wrong-kind":
				n.kind = treeObjects
				pages[child] = n
			case "wrong-level":
				n.level = 1
				pages[child] = n
			}
			installSearchPages(t, tx, pages, "dirty")
			expected := ErrCorrupt
			if shape == "bad-crc" {
				tx.pages[child] = bytes.Clone(tx.pages[child])
				tx.pages[child][40] ^= 1
			}
			if shape == "missing-spill" {
				tx.pages = nil
				installSearchPages(t, tx, map[uint64]treeNode{root: parent}, "spilled")
				tx.spilled[child] = PageSize
				expected = io.EOF
			}
			if shape == "closed-accepted" {
				tx.pages = nil
				tx.acceptedPages = newDirtyStore(1, true)
				tx.acceptedPages.close()
				expected = ErrClosed
			}
			if shape == "device-error" {
				tx.pages = nil
				v.nodes.clear()
				expected = errors.New("injected tree read failure")
				meter := v.dev.(*meteredIO)
				meter.VolumeIO = treeViewReadError{VolumeIO: meter.VolumeIO, err: expected}
			}
			checks := []func() error{func() error { _, _, e := tx.neighbor(root, n.entries[0].key, true); return e }, func() error { _, _, _, e := tx.findSpace(1, 1, false); return e }, func() error { _, e := tx.freeContains(root, 4<<20, 1); return e }}
			for _, check := range checks {
				if e := check(); !errors.Is(e, expected) {
					t.Fatalf("got%v want%v", e, expected)
				}
			}
		})
	}
}
func TestTransactionTreeViewOverlayPriority(t *testing.T) {
	for _, mode := range []string{"dirty", "accepted", "spilled"} {
		t.Run(mode, func(t *testing.T) {
			v, tx, n := readNodeFixture(t, 3)
			root := tx.sb.FreeRoot
			n.entries[0].value = 99
			installSearchPages(t, tx, map[uint64]treeNode{root: n}, mode)
			got, e := tx.readTreeView(root)
			if e != nil || got.entry(0).value != 99 {
				t.Fatal("overlay ignored", e)
			}
			if mode == "accepted" {
				n.entries[0].value = 123
				tx.pages[root] = encodeNode(n, v.sb.Identity.VolumeID, 1)
				got, e = tx.readTreeView(root)
				if e != nil || got.entry(0).value != 123 {
					t.Fatal("hot overlay ignored", e)
				}
			}
			original, _ := v.node(root)
			if original.entries[0].value != 2*PageSize {
				t.Fatal("committed cache modified")
			}
		})
	}
}
func TestTransactionTreeViewAllocations(t *testing.T) {
	for _, mode := range []string{"committed", "dirty", "accepted", "spilled"} {
		t.Run(mode, func(t *testing.T) {
			_, tx, n := readNodeFixture(t, 200)
			installSearchPages(t, tx, map[uint64]treeNode{tx.sb.FreeRoot: n}, mode)
			allocs := testing.AllocsPerRun(100, func() {
				if _, _, _, e := tx.findSpace(1, 1, false); e != nil {
					panic(e)
				}
			})
			limit := float64(0)
			if mode == "spilled" {
				limit = 1
			}
			if allocs > limit {
				t.Fatalf("search allocated%.0f want<=%.0f", allocs, limit)
			}
		})
	}
}
func BenchmarkTransactionTreeView(b *testing.B) {
	for _, mode := range []string{"committed", "dirty", "accepted", "spilled"} {
		b.Run(mode, func(b *testing.B) {
			_, tx, n := readNodeFixture(b, 200)
			installSearchPages(b, tx, map[uint64]treeNode{tx.sb.FreeRoot: n}, mode)
			for _, raw := range []bool{false, true} {
				b.Run(fmt.Sprintf("raw=%v", raw), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var e error
						if raw {
							_, _, e = tx.neighbor(tx.sb.FreeRoot, spaceKey(4<<20), false)
						} else {
							_, _, e = treeNeighbor(tx.readNode, tx.sb.FreeRoot, spaceKey(4<<20), false)
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

type treeViewReadError struct {
	VolumeIO
	err error
}

func (d treeViewReadError) ReadAt([]byte, int64) (int, error) { return 0, d.err }
