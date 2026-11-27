// SPDX-License-Identifier: GPL-3.0-only
package volume

import "sort"

// transactionTreeView exists only during a read-only allocator search under the
// enclosing volume lock. Committed decoded entries are immutable; dirty encoded
// bytes are borrowed (or an owned spool page), never decoded or retained/cached.
// Mutations must continue to use transaction.node's private entries.
type transactionTreeView struct {
	decoded treeNode
	raw     rawTreePage
}

func (t *transaction) readTreeView(off uint64) (transactionTreeView, error) {
	b, overlay, err := t.nodePage(off)
	if err != nil {
		return transactionTreeView{}, err
	}
	if overlay {
		p, err := parseRawTreePage(b, t.sb.Identity.VolumeID)
		return transactionTreeView{raw: p}, err
	}
	n, err := t.v.node(off)
	return transactionTreeView{decoded: n}, err
}
func (p transactionTreeView) count() int {
	if p.raw.data != nil {
		return p.raw.count
	}
	return len(p.decoded.entries)
}
func (p transactionTreeView) kind() uint8 {
	if p.raw.data != nil {
		return p.raw.kind
	}
	return p.decoded.kind
}
func (p transactionTreeView) level() uint8 {
	if p.raw.data != nil {
		return p.raw.level
	}
	return p.decoded.level
}
func (p transactionTreeView) entry(i int) treeEntry {
	if p.raw.data != nil {
		return p.raw.entry(i)
	}
	return p.decoded.entries[i]
}
func (p transactionTreeView) lower(key indexKey, strict bool) int {
	if p.raw.data != nil {
		// Free-tree encoded keys contain only eight bytes; decoded keys have a zero
		// suffix. Preserve full indexKey comparisons even for noncanonical probes.
		for _, b := range key[p.raw.keyBytes:] {
			if b != 0 {
				strict = true
				break
			}
		}
		return p.raw.lower(key, strict)
	}
	return sort.Search(len(p.decoded.entries), func(i int) bool {
		c := lessKey(p.decoded.entries[i].key, key)
		return !c && (!strict || p.decoded.entries[i].key != key)
	})
}
func treeViewChildValid(parent, child transactionTreeView) bool {
	return parent.level() > 0 && child.level() == parent.level()-1 && parent.kind() == child.kind()
}
func (t *transaction) neighbor(root uint64, key indexKey, ceil bool) (treeEntry, bool, error) {
	p, err := t.readTreeView(root)
	if err != nil {
		return treeEntry{}, false, err
	}
	// Keep the common committed leaf on the existing decoded-node search path;
	// raw view dispatch is needed only for dirty pages or internal traversal.
	if p.raw.data == nil && p.decoded.level == 0 {
		n := p.decoded
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
	return transactionViewNeighbor(t.readTreeView, p, key, ceil, 0)
}
func transactionViewNeighbor(read func(uint64) (transactionTreeView, error), p transactionTreeView, key indexKey, ceil bool, depth int) (treeEntry, bool, error) {
	if depth > 32 {
		return treeEntry{}, false, ErrCorrupt
	}
	if p.level() == 0 {
		i := p.lower(key, false)
		if ceil {
			if i < p.count() {
				return p.entry(i), true, nil
			}
		} else {
			if i < p.count() && p.entry(i).key == key {
				return p.entry(i), true, nil
			}
			if i > 0 {
				return p.entry(i - 1), true, nil
			}
		}
		return treeEntry{}, false, nil
	}
	i := max(0, p.lower(key, true)-1)
	step := -1
	if ceil {
		step = 1
	}
	for ; i >= 0 && i < p.count(); i += step {
		child, err := read(p.entry(i).value)
		if err != nil {
			return treeEntry{}, false, err
		}
		if !treeViewChildValid(p, child) {
			return treeEntry{}, false, ErrCorrupt
		}
		x, ok, err := transactionViewNeighbor(read, child, key, ceil, depth+1)
		if err != nil || ok {
			return x, ok, err
		}
	}
	return treeEntry{}, false, nil
}
func (t *transaction) freeContains(root, off, n uint64) (bool, error) {
	span, ok, err := t.neighbor(root, spaceKey(off), false)
	if err != nil {
		return false, err
	}
	start := keyOffset(span.key)
	return ok && start <= off && off-start <= span.value && n <= span.value-(off-start), nil
}
