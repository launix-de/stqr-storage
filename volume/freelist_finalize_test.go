// SPDX-License-Identifier: GPL-3.0-only
package volume

import "testing"

// Construct the boundary shape directly: ordinary workloads reach it after
// hundreds of mixed-size allocations, but the regression needs only one edit.
func fragmentedTransaction(t *testing.T, count int) (*Volume, *transaction, uint64) {
	t.Helper()
	v, err := NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	tx := v.transaction()
	t.Cleanup(tx.close)
	var left, right uint64
	capacity := (treeNode{kind: treeFree}).capacity()
	if count > capacity {
		left, err = tx.alloc(PageSize, true)
		if err != nil {
			t.Fatal(err)
		}
		right, err = tx.alloc(PageSize, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := treeNeighbor(tx.node, tx.sb.FreeRoot, indexKey{}, true)
	if err != nil || !ok {
		t.Fatal(err)
	}
	base := align(keyOffset(first.key)) + 4*PageSize
	entries := make([]treeEntry, count)
	for i := range entries {
		entries[i] = treeEntry{key: spaceKey(base + uint64(i)*5*PageSize), value: 2 * PageSize}
	}
	if count <= capacity {
		tx.putNode(tx.sb.FreeRoot, treeNode{kind: treeFree, entries: entries})
	} else {
		a := treeNode{kind: treeFree, entries: entries[:count/2]}
		b := treeNode{kind: treeFree, entries: entries[count/2:]}
		tx.putNode(left, a)
		tx.putNode(right, b)
		tx.putNode(tx.sb.FreeRoot, treeNode{kind: treeFree, level: 1, entries: []treeEntry{tx.summary(left, a), tx.summary(right, b)}})
	}
	tx.sb.FreeBytes = uint64(count) * 2 * PageSize
	tx.sb.PayloadBytes = tx.sb.Identity.Size - tx.sb.MetadataBytes - tx.sb.FreeBytes
	return v, tx, base
}
func validateTransactionFreeTree(t *testing.T, tx *transaction) {
	t.Helper()
	var total, end uint64
	err := treeWalk(tx.node, tx.sb.FreeRoot, func(e treeEntry) error {
		start := keyOffset(e.key)
		if start < end {
			t.Fatalf("overlapping free ranges at %x", start)
		}
		end = start + e.value
		total += e.value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != tx.sb.FreeBytes {
		t.Fatalf("free accounting %d != %d", total, tx.sb.FreeBytes)
	}
	var visit func(uint64)
	visit = func(off uint64) {
		e, ok, err := treeNeighbor(tx.node, tx.sb.FreeRoot, spaceKey(off), false)
		if err != nil {
			t.Fatal(err)
		}
		if ok && keyOffset(e.key) <= off && off-keyOffset(e.key) < e.value {
			t.Fatalf("live tree page %x is free", off)
		}
		node, err := tx.node(off)
		if err != nil {
			t.Fatal(err)
		}
		if node.level > 0 {
			for _, child := range node.entries {
				visit(child.value)
			}
		}
	}
	visit(tx.sb.FreeRoot)
}
func TestFreeRangeBoundaryUpdatesAvoidTreeChurn(t *testing.T) {
	for _, operation := range []string{"claim-start", "extend-left", "extend-right"} {
		t.Run(operation, func(t *testing.T) {
			_, tx, base := fragmentedTransaction(t, (treeNode{kind: treeFree}).capacity()+1)
			switch operation {
			case "claim-start":
				if _, err := tx.alloc(PageSize, false); err != nil {
					t.Fatal(err)
				}
			case "extend-left":
				tx.release(base+2*PageSize, PageSize, false)
			case "extend-right":
				tx.release(base+4*PageSize, PageSize, false)
			}
			if err := tx.finalize(); err != nil {
				t.Fatal(err)
			}
			if len(tx.pending) != 0 {
				t.Fatalf("boundary update allocated %d unnecessary tree pages", len(tx.pending))
			}
			validateTransactionFreeTree(t, tx)
		})
	}
}
func TestFinalizeClaimsNewTreePagesBeforeRetirement(t *testing.T) {
	_, tx, base := fragmentedTransaction(t, (treeNode{kind: treeFree}).capacity())
	// The isolated free splits a full leaf; coalescing another gap can retire
	// those new tree pages again. Their claims must precede their retirement.
	tx.release(base+3*PageSize, PageSize, false)
	tx.release(base+52*PageSize, 3*PageSize, false)
	if err := tx.finalize(); err != nil {
		t.Fatal(err)
	}
	validateTransactionFreeTree(t, tx)
}
