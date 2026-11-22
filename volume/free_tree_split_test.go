// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

func TestFreeTreeFragmentedSmallObjectMutations(t *testing.T) {
	v, _ := memoryVolume(t, 16<<20)
	for round := 0; round < 3; round++ {
		for i := 0; i < 600; i++ {
			payload := bytes.Repeat([]byte{byte(i)}, 4096-(i+round)%31)
			if _, err := testPut(v, testKey(uint64(i)), payload, Options{}); err != nil {
				t.Fatalf("round %d, object %d: %v", round, i, err)
			}
		}
	}
}

func TestFreeTreeClaimAfterMergeCanAllocateSplitMetadata(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	tx := v.transaction()
	defer tx.close()
	left, right := treeNode{kind: treeFree}, treeNode{kind: treeFree}
	for i := 0; i < 253; i++ {
		entry := treeEntry{key: spaceKey(2<<20 + uint64(i)*8), value: 1}
		if i < 127 {
			left.entries = append(left.entries, entry)
		} else {
			right.entries = append(right.entries, entry)
		}
	}
	right.entries = append(right.entries, treeEntry{key: spaceKey(3<<20 + 1), value: (1 << 20) - 1})
	leftOff, rightOff := uint64(1<<20), uint64(1<<20+PageSize)
	tx.putNode(leftOff, left)
	tx.putNode(rightOff, right)
	tx.putNode(tx.sb.FreeRoot, treeNode{kind: treeFree, level: 1, entries: []treeEntry{
		tx.summary(leftOff, left), tx.summary(rightOff, right),
	}})
	tx.sb.FreeBytes = 1<<20 - 1 + 253
	// Removing the large extent merges the two leaves into a full root. Its
	// replacement must split that root before either remainder is searchable.
	if _, err := tx.alloc(PageSize, false); err != nil {
		t.Fatal(err)
	}
	if err := tx.finalize(); err != nil {
		t.Fatal(err)
	}
	if len(tx.freeClaims) != 0 || len(tx.freeSiblings) != 0 {
		t.Fatal("transient free extents survived publication")
	}
}

func TestFreeTreeClaimPublishesLargeRemainderBeforeSplitting(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	tx := v.transaction()
	defer tx.close()
	root := treeNode{kind: treeFree}
	for i := 0; i < root.capacity()-1; i++ {
		root.entries = append(root.entries, treeEntry{key: spaceKey(2<<20 + uint64(i)*8), value: 1})
	}
	root.entries = append(root.entries, treeEntry{key: spaceKey(3<<20 + 1), value: (1 << 20) - 1})
	tx.putNode(tx.sb.FreeRoot, root)
	tx.sb.FreeBytes = 1<<20 - 1 + uint64(len(root.entries)-1)
	if _, err := tx.alloc(PageSize, false); err != nil {
		t.Fatal(err)
	}
}

func TestFreeTreeRootSplitAllocatesFromUnpublishedSibling(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	tx := v.transaction()
	defer tx.close()
	root := treeNode{kind: treeFree}
	for i := 0; i < root.capacity()-1; i++ {
		root.entries = append(root.entries, treeEntry{key: spaceKey(2<<20 + uint64(i)*8), value: 1})
	}
	root.entries = append(root.entries, treeEntry{key: spaceKey(3 << 20), value: 1 << 20})
	tx.putNode(tx.sb.FreeRoot, root)
	tx.sb.FreeBytes = 1<<20 + uint64(len(root.entries)-1)
	// The split moves the only page-sized free extent into the right sibling.
	// Allocating the new root must still find that extent before linking it.
	if err := tx.set(&tx.sb.FreeRoot, spaceKey(2<<20+4096), 1); err != nil {
		t.Fatal(err)
	}
	next, err := tx.node(tx.sb.FreeRoot)
	if err != nil || next.level != 1 || len(next.entries) != 2 {
		t.Fatalf("root after split: %+v, %v", next, err)
	}
	if len(tx.freeSiblings) != 0 {
		t.Fatal("published siblings retained as transient allocation roots")
	}
}
