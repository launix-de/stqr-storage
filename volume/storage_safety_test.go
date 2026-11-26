// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"math/rand"
	"testing"
)

// These pages are deliberately injected, not produced by ordinary mutations.
// The contract is safe rejection, never a successful reproduction of a panic.
func TestStorageSafetyTreeCorruption(t *testing.T) {
	for _, kind := range []uint8{treeObjects, treeFree} {
		for _, shape := range []string{"empty", "self", "cycle", "wrong-kind", "wrong-level"} {
			t.Run(string(rune('0'+kind))+"/"+shape, func(t *testing.T) {
				v, d := memoryVolume(t, 4<<20)
				root := v.sb.ObjectRoot
				if kind == treeFree {
					root = v.sb.FreeRoot
				}
				n := treeNode{kind: kind, level: 1}
				child := uint64(2 << 20)
				if shape != "empty" {
					n.entries = []treeEntry{{value: root, maximum: 1 << 20, aligned: 1 << 20}}
				}
				if shape == "cycle" || shape == "wrong-kind" || shape == "wrong-level" {
					n.level = 2
					n.entries[0].value = child
					other := treeNode{kind: kind, level: 1, entries: []treeEntry{{value: root, maximum: 1 << 20, aligned: 1 << 20}}}
					if shape == "wrong-kind" {
						other.kind = 3 - kind
						other.level = 0
					}
					if shape == "wrong-level" {
						other.level = 2
					}
					if _, err := d.WriteAt(encodeNode(other, v.Identity().VolumeID, 1), int64(child)); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := d.WriteAt(encodeNode(n, v.Identity().VolumeID, 1), int64(root)); err != nil {
					t.Fatal(err)
				}
				w, err := Open(d, OpenOptions{})
				if err != nil {
					if !errors.Is(err, ErrCorrupt) {
						t.Fatal(err)
					}
					return
				}
				checks := []struct {
					name string
					run  func() error
				}{
					{"walk", func() error { return treeWalk(w.node, root, func(treeEntry) error { return nil }) }},
					{"lookup", func() error { _, e := treeLookup(w.node, root, indexKey{}); return e }},
					{"neighbor", func() error { _, _, e := treeNeighbor(w.node, root, indexKey{}, true); return e }},
					{"bounds", func() error {
						return treeWalkBounds(w.node, root, indexKey{}, indexKey{255}, 0, func(treeEntry) error { return nil })
					}},
					{"insert", func() error {
						tx := w.transaction()
						defer tx.close()
						_, _, e := tx.insertAt(root, indexKey{}, 1)
						return e
					}},
					{"delete", func() error { tx := w.transaction(); defer tx.close(); _, e := tx.deleteAt(root, indexKey{}); return e }},
				}
				if kind == treeFree {
					checks = append(checks, struct {
						name string
						run  func() error
					}{"allocate", func() error {
						tx := w.transaction()
						defer tx.close()
						_, _, _, e := tx.findSpace(PageSize, PageSize, false)
						return e
					}})
				}
				for _, check := range checks {
					if err := check.run(); !errors.Is(err, ErrCorrupt) {
						t.Fatalf("%s: %v", check.name, err)
					}
				}
				if err := w.Scrub(); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("scrub: %v", err)
				}
			})
		}
	}
}

// Validate topology independently of the production traversal and its checks.
func assertSafetyTree(t *testing.T, v *Volume, root uint64, kind uint8) {
	t.Helper()
	seen := map[uint64]bool{}
	var walk func(uint64, int)
	walk = func(off uint64, level int) {
		if seen[off] {
			t.Fatalf("reused tree page/cycle at %d", off)
		}
		seen[off] = true
		n, err := v.node(off)
		if err != nil {
			t.Fatal(err)
		}
		if n.kind != kind || level >= 0 && int(n.level) != level || n.level > 0 && len(n.entries) == 0 {
			t.Fatalf("invalid node at %d: %+v", off, n)
		}
		if n.level > 0 {
			for _, child := range n.entries {
				walk(child.value, int(n.level)-1)
			}
		}
	}
	walk(root, -1)
}
func TestStorageSafetyTreeSplitMergeReuse(t *testing.T) {
	v, d := memoryVolume(t, 32<<20)
	// More than a leaf and a full first-level directory; randomized insertion
	// and removal exercise root promotion, sibling merges and metadata reuse.
	rng := rand.New(rand.NewSource(8172))
	const count = 5200
	for round := 0; round < 2; round++ {
		order := rng.Perm(count)
		for start := 0; start < count; start += 100 {
			v.mu.Lock()
			tx := v.transaction()
			for _, i := range order[start:min(start+100, count)] {
				if err := tx.set(&tx.sb.ObjectRoot, objectKey(testKey(uint64(i))), uint64(i+1)); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.commit(false); err != nil {
				t.Fatal(err)
			}
			v.mu.Unlock()
			assertSafetyTree(t, v, v.sb.ObjectRoot, treeObjects)
			assertSafetyTree(t, v, v.sb.FreeRoot, treeFree)
		}
		v = reopen(t, v, d)
		n, err := v.node(v.sb.ObjectRoot)
		if err != nil || n.level < 2 {
			t.Fatalf("expected multi-level tree: %+v %v", n, err)
		}
		for i := 0; i < count; i++ {
			got, err := v.lookup(objectKey(testKey(uint64(i))))
			if err != nil || got != uint64(i+1) {
				t.Fatal(i, got, err)
			}
		}
		order = rng.Perm(count)
		for start := 0; start < count; start += 100 {
			v.mu.Lock()
			tx := v.transaction()
			for _, i := range order[start:min(start+100, count)] {
				if err := tx.remove(&tx.sb.ObjectRoot, objectKey(testKey(uint64(i)))); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.commit(false); err != nil {
				t.Fatal(err)
			}
			v.mu.Unlock()
			assertSafetyTree(t, v, v.sb.ObjectRoot, treeObjects)
			assertSafetyTree(t, v, v.sb.FreeRoot, treeFree)
		}
		v = reopen(t, v, d)
		n, err = v.node(v.sb.ObjectRoot)
		if err != nil || n.level != 0 || len(n.entries) != 0 {
			t.Fatalf("empty root: %+v %v", n, err)
		}
	}
}
