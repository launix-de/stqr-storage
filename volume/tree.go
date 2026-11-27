// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type indexKey [49]byte
type indexEntry struct {
	Key   indexKey
	Value uint64
}

func objectKey(k Key) (x indexKey)     { copy(x[1:17], k.Bucket[:]); copy(x[17:], k.Object[:]); return }
func requestKey(id ID) (x indexKey)    { x[0] = 1; copy(x[1:17], id[:]); return }
func (x indexKey) object() (k Key)     { copy(k.Bucket[:], x[1:17]); copy(k.Object[:], x[17:]); return }
func lessKey(a, b indexKey) bool       { return bytes.Compare(a[:], b[:]) < 0 }
func spaceKey(off uint64) (x indexKey) { binary.BigEndian.PutUint64(x[:8], off); return }
func keyOffset(x indexKey) uint64      { return binary.BigEndian.Uint64(x[:8]) }

const treeObjects = 1
const treeFree = 2

type treeEntry struct {
	key                     indexKey
	value, maximum, aligned uint64
}
type treeNode struct {
	kind, level uint8
	entries     []treeEntry
}

func (n treeNode) capacity() int {
	size := 57
	if n.kind == treeFree {
		size = 16
		if n.level > 0 {
			size = 32
		}
	}
	return (PageSize - 36) / size
}
func encodeNode(n treeNode, id ID, generation uint64) []byte {
	b := make([]byte, PageSize)
	copy(b, "BPT2")
	b[4] = n.kind
	b[5] = n.level
	diskOrder.PutUint16(b[6:], uint16(len(n.entries)))
	diskOrder.PutUint64(b[8:], generation)
	copy(b[16:32], id[:])
	at := 32
	for _, e := range n.entries {
		keylen := 49
		if n.kind == treeFree {
			keylen = 8
		}
		copy(b[at:], e.key[:keylen])
		at += keylen
		diskOrder.PutUint64(b[at:], e.value)
		at += 8
		if n.kind == treeFree && n.level > 0 {
			diskOrder.PutUint64(b[at:], e.maximum)
			diskOrder.PutUint64(b[at+8:], e.aligned)
			at += 16
		}
	}
	diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
	return b
}
func decodeNode(b []byte, id ID) (treeNode, error) {
	var n treeNode
	if len(b) != PageSize || string(b[:4]) != "BPT2" || !bytes.Equal(b[16:32], id[:]) || checksum(b[:PageSize-4]) != diskOrder.Uint32(b[PageSize-4:]) {
		return n, ErrCorrupt
	}
	n.kind = b[4]
	n.level = b[5]
	count := int(diskOrder.Uint16(b[6:]))
	if n.kind < 1 || n.kind > 2 || n.level > 32 || count > n.capacity() || n.level > 0 && count == 0 {
		return n, ErrCorrupt
	}
	at := 32
	for i := 0; i < count; i++ {
		var e treeEntry
		keylen := 49
		if n.kind == treeFree {
			keylen = 8
		}
		copy(e.key[:], b[at:at+keylen])
		at += keylen
		e.value = diskOrder.Uint64(b[at:])
		at += 8
		if n.kind == treeFree && n.level > 0 {
			e.maximum = diskOrder.Uint64(b[at:])
			e.aligned = diskOrder.Uint64(b[at+8:])
			at += 16
		}
		if i > 0 && !lessKey(n.entries[i-1].key, e.key) {
			return n, ErrCorrupt
		}
		n.entries = append(n.entries, e)
	}
	return n, nil
}

// Each descent must lower the level exactly once and retain the tree kind.
// CRCs validate page bytes, not their graph: this also rejects multi-page
// cycles without unbounded recursion or an object-count-sized visited set.
func checkTreeChild(read func(uint64) (treeNode, error), parent treeNode, off uint64) error {
	child, err := read(off)
	if err != nil {
		return err
	}
	return validateTreeChild(parent, child)
}

func validateTreeChild(parent, child treeNode) error {
	if parent.level == 0 || child.level != parent.level-1 || child.kind != parent.kind {
		return ErrCorrupt
	}
	return nil
}

func (v *Volume) node(off uint64) (treeNode, error) {
	if n, ok := v.nodes.get(off); ok {
		return n, nil
	}
	b, e := v.readOnlyPage(off)
	if e != nil {
		return treeNode{}, e
	}
	n, e := decodeNode(b, v.sb.Identity.VolumeID)
	if e == nil {
		v.nodes.put(off, n)
	}
	return n, e
}
func (t *transaction) node(off uint64) (treeNode, error) {
	_, spilled := t.spilled[off]
	if t.acceptedPages != nil {
		flag, err := t.acceptedPages.flag(off)
		if err != nil {
			return treeNode{}, err
		}
		spilled = spilled || flag != 0
	}
	if t.pages[off] != nil || spilled {
		b, e := t.page(off)
		if e != nil {
			return treeNode{}, e
		}
		return decodeNode(b, t.sb.Identity.VolumeID)
	}
	n, e := t.v.node(off)
	if e == nil {
		n.entries = append([]treeEntry(nil), n.entries...)
	}
	return n, e
}
func (t *transaction) putNode(off uint64, n treeNode) {
	t.pages[off] = encodeNode(n, t.sb.Identity.VolumeID, t.sb.Generation+1)
}
func childIndex(n treeNode, key indexKey) int {
	i := sort.Search(len(n.entries), func(i int) bool { return lessKey(key, n.entries[i].key) }) - 1
	if i < 0 {
		i = 0
	}
	return i
}
func (t *transaction) summary(off uint64, n treeNode) treeEntry {
	e := treeEntry{value: off}
	if len(n.entries) == 0 {
		return e
	}
	e.key = n.entries[0].key
	if n.kind == treeFree {
		for _, x := range n.entries {
			if n.level == 0 {
				start := keyOffset(x.key)
				e.maximum = max(e.maximum, x.value)
				if align(start) < start+x.value {
					e.aligned = max(e.aligned, start+x.value-align(start))
				}
			} else {
				e.maximum = max(e.maximum, x.maximum)
				e.aligned = max(e.aligned, x.aligned)
			}
		}
	}
	return e
}
func (t *transaction) insertAt(off uint64, key indexKey, value uint64) (treeEntry, *treeEntry, error) {
	n, e := t.node(off)
	if e != nil {
		return treeEntry{}, nil, e
	}
	if n.level == 0 {
		i := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, key) })
		if i < len(n.entries) && n.entries[i].key == key {
			if n.entries[i].value == value {
				return t.summary(off, n), nil, nil
			}
			n.entries[i].value = value
		} else {
			n.entries = append(n.entries, treeEntry{})
			copy(n.entries[i+1:], n.entries[i:])
			n.entries[i] = treeEntry{key: key, value: value}
		}
	} else {
		i := childIndex(n, key)
		if e := checkTreeChild(t.node, n, n.entries[i].value); e != nil {
			return treeEntry{}, nil, e
		}
		a, b, e := t.insertAt(n.entries[i].value, key, value)
		if e != nil {
			return treeEntry{}, nil, e
		}
		if b == nil && n.entries[i] == a {
			return t.summary(off, n), nil, nil
		}
		n.entries[i] = a
		if b != nil {
			n.entries = append(n.entries, treeEntry{})
			copy(n.entries[i+2:], n.entries[i+1:])
			n.entries[i+1] = *b
		}
	}
	if len(n.entries) <= n.capacity() {
		t.putNode(off, n)
		return t.summary(off, n), nil, nil
	}
	rightOff, e := t.metaPage()
	if e != nil {
		return treeEntry{}, nil, e
	}
	mid := len(n.entries) / 2
	right := treeNode{kind: n.kind, level: n.level, entries: append([]treeEntry(nil), n.entries[mid:]...)}
	n.entries = n.entries[:mid]
	t.putNode(off, n)
	t.putNode(rightOff, right)
	if n.kind == treeFree {
		t.freeSiblings = append(t.freeSiblings, rightOff)
	}
	r := t.summary(rightOff, right)
	return t.summary(off, n), &r, nil
}
func (t *transaction) set(root *uint64, key indexKey, value uint64) error {
	if root == &t.sb.ObjectRoot {
		t.v.addMembershipKey(key)
		if t.changed == nil {
			t.changed = make(map[indexKey]bool)
		}
		t.changed[key] = true
	}
	count := len(t.freeSiblings)
	defer func() { t.freeSiblings = t.freeSiblings[:count] }()
	left, right, e := t.insertAt(*root, key, value)
	if e != nil {
		return e
	}
	if right != nil {
		old, e := t.node(*root)
		if e != nil {
			return e
		}
		off, e := t.metaPage()
		if e != nil {
			return e
		}
		t.putNode(off, treeNode{kind: old.kind, level: old.level + 1, entries: []treeEntry{left, *right}})
		*root = off
	}
	return nil
}

// replaceKeyAt changes one free-range boundary without changing the number of
// entries. The caller has checked its global predecessor/successor bounds.
func (t *transaction) replaceKeyAt(off uint64, key, next indexKey, value uint64) (treeEntry, error) {
	n, e := t.node(off)
	if e != nil {
		return treeEntry{}, e
	}
	if n.kind != treeFree {
		return treeEntry{}, ErrCorrupt
	}
	if n.level == 0 {
		i := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, key) })
		if i == len(n.entries) || n.entries[i].key != key {
			return treeEntry{}, ErrCorrupt
		}
		if i > 0 && !lessKey(n.entries[i-1].key, next) || i+1 < len(n.entries) && !lessKey(next, n.entries[i+1].key) {
			return treeEntry{}, ErrCorrupt
		}
		n.entries[i].key = next
		n.entries[i].value = value
	} else {
		if len(n.entries) == 0 {
			return treeEntry{}, ErrCorrupt
		}
		i := childIndex(n, key)
		if e := checkTreeChild(t.node, n, n.entries[i].value); e != nil {
			return treeEntry{}, e
		}
		entry, e := t.replaceKeyAt(n.entries[i].value, key, next, value)
		if e != nil {
			return treeEntry{}, e
		}
		n.entries[i] = entry
	}
	t.putNode(off, n)
	return t.summary(off, n), nil
}
func (t *transaction) deleteAt(off uint64, key indexKey) (treeEntry, error) {
	n, e := t.node(off)
	if e != nil {
		return treeEntry{}, e
	}
	if n.level == 0 {
		i := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, key) })
		if i == len(n.entries) || n.entries[i].key != key {
			return t.summary(off, n), nil
		}
		n.entries = append(n.entries[:i], n.entries[i+1:]...)
	} else {
		if len(n.entries) == 0 {
			return treeEntry{}, ErrCorrupt
		}
		i := childIndex(n, key)
		if e := checkTreeChild(t.node, n, n.entries[i].value); e != nil {
			return treeEntry{}, e
		}
		entry, e := t.deleteAt(n.entries[i].value, key)
		if e != nil {
			return treeEntry{}, e
		}
		child, e := t.node(entry.value)
		if e != nil {
			return treeEntry{}, e
		}
		if len(child.entries) == 0 {
			t.release(entry.value, PageSize, true)
			n.entries = append(n.entries[:i], n.entries[i+1:]...)
		} else {
			n.entries[i] = entry
			// A claim can replace one free extent with two remainders. Merging
			// here and immediately splitting again creates new pending metadata
			// claims faster than finalize can consume them.
			if len(n.entries) > 1 && !(n.kind == treeFree && t.inClaim) {
				j := i + 1
				if j >= len(n.entries) {
					j = i - 1
				}
				a, b := i, j
				if a > b {
					a, b = b, a
				}
				if e := checkTreeChild(t.node, n, n.entries[j].value); e != nil {
					return treeEntry{}, e
				}
				ln, e := t.node(n.entries[a].value)
				if e != nil {
					return treeEntry{}, e
				}
				rn, e := t.node(n.entries[b].value)
				if e != nil {
					return treeEntry{}, e
				}
				if len(ln.entries)+len(rn.entries) <= ln.capacity() {
					ln.entries = append(ln.entries, rn.entries...)
					t.putNode(n.entries[a].value, ln)
					t.release(n.entries[b].value, PageSize, true)
					n.entries[a] = t.summary(n.entries[a].value, ln)
					n.entries = append(n.entries[:b], n.entries[b+1:]...)
				}
			}
		}
	}
	if len(n.entries) == 0 {
		n.level = 0
	}
	t.putNode(off, n)
	return t.summary(off, n), nil
}
func (t *transaction) remove(root *uint64, key indexKey) error {
	if root == &t.sb.ObjectRoot {
		if t.changed == nil {
			t.changed = make(map[indexKey]bool)
		}
		t.changed[key] = true
	}
	_, e := t.deleteAt(*root, key)
	if e != nil {
		return e
	}
	n, e := t.node(*root)
	if e != nil {
		return e
	}
	if n.level > 0 && len(n.entries) == 1 && !(n.kind == treeFree && t.inClaim) {
		old := *root
		*root = n.entries[0].value
		t.release(old, PageSize, true)
	}
	return nil
}
func treeLookup(read func(uint64) (treeNode, error), root uint64, key indexKey) (uint64, error) {
	for depth := 0; depth < 33; depth++ {
		n, e := read(root)
		if e != nil {
			return 0, e
		}
		if len(n.entries) == 0 {
			return 0, nil
		}
		if n.level == 0 {
			i := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, key) })
			if i < len(n.entries) && n.entries[i].key == key {
				return n.entries[i].value, nil
			}
			return 0, nil
		}
		child := n.entries[childIndex(n, key)].value
		if e := checkTreeChild(read, n, child); e != nil {
			return 0, e
		}
		root = child
	}
	return 0, ErrCorrupt
}
func treeWalk(read func(uint64) (treeNode, error), root uint64, fn func(treeEntry) error) error {
	n, e := read(root)
	if e != nil {
		return e
	}
	for _, x := range n.entries {
		if n.level == 0 {
			e = fn(x)
		} else {
			if e := checkTreeChild(read, n, x.value); e != nil {
				return e
			}
			e = treeWalk(read, x.value, fn)
		}
		if e != nil {
			return e
		}
	}
	return nil
}
func (v *Volume) lookup(key indexKey) (uint64, error) {
	return v.lookupView(key)
}
func (v *Volume) walkIndex(fn func(indexEntry) error) error {
	return v.walkIndexView(nil, nil, fn)
}

// neighbor descends only the search path (and, at a boundary, its neighboring
// subtree). Freelist coalescing and exact claims do not scan the directory.
func treeNeighbor(read func(uint64) (treeNode, error), root uint64, key indexKey, ceil bool) (treeEntry, bool, error) {
	n, e := read(root)
	if e != nil {
		return treeEntry{}, false, e
	}
	if n.level == 0 {
		i := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, key) })
		if ceil {
			if i < len(n.entries) {
				return n.entries[i], true, nil
			}
		} else {
			if i < len(n.entries) && n.entries[i].key == key {
				return n.entries[i], true, nil
			}
			if i > 0 {
				return n.entries[i-1], true, nil
			}
		}
		return treeEntry{}, false, nil
	}
	i := childIndex(n, key)
	if ceil {
		for ; i < len(n.entries); i++ {
			if e := checkTreeChild(read, n, n.entries[i].value); e != nil {
				return treeEntry{}, false, e
			}
			x, ok, e := treeNeighbor(read, n.entries[i].value, key, ceil)
			if e != nil || ok {
				return x, ok, e
			}
		}
	} else {
		for ; i >= 0; i-- {
			if e := checkTreeChild(read, n, n.entries[i].value); e != nil {
				return treeEntry{}, false, e
			}
			x, ok, e := treeNeighbor(read, n.entries[i].value, key, ceil)
			if e != nil || ok {
				return x, ok, e
			}
		}
	}
	return treeEntry{}, false, nil
}

// walkIndexBounds seeks to the lower bound before traversing leaves. Paged
// migration/cleanup must not rescan every preceding object or another bucket
// on every page. Bounds are inclusive lower / exclusive upper.
func (v *Volume) walkIndexBounds(lower, upper indexKey, fn func(indexEntry) error) error {
	return v.walkIndexView(&lower, &upper, fn)
}
func treeWalkBounds(read func(uint64) (treeNode, error), root uint64, lower, upper indexKey, depth int, fn func(treeEntry) error) error {
	n, err := read(root)
	if err != nil {
		return err
	}
	return treeWalkBoundsNode(read, n, lower, upper, depth, fn)
}

// Carry the checked page into recursion: a bounded scan reads each selected
// page once, rather than reading the child again after topology validation.
func treeWalkBoundsNode(read func(uint64) (treeNode, error), n treeNode, lower, upper indexKey, depth int, fn func(treeEntry) error) error {
	if depth > 32 {
		return ErrCorrupt
	}
	if len(n.entries) == 0 {
		return nil
	}
	if n.level == 0 {
		start := sort.Search(len(n.entries), func(i int) bool { return !lessKey(n.entries[i].key, lower) })
		for _, entry := range n.entries[start:] {
			if !lessKey(entry.key, upper) {
				break
			}
			if err := fn(entry); err != nil {
				return err
			}
		}
		return nil
	}
	for _, entry := range n.entries[childIndex(n, lower):] {
		if !lessKey(entry.key, upper) {
			break
		}
		child, err := read(entry.value)
		if err != nil {
			return err
		}
		if err := validateTreeChild(n, child); err != nil {
			return err
		}
		if err := treeWalkBoundsNode(read, child, lower, upper, depth+1, fn); err != nil {
			return err
		}
	}
	return nil
}
