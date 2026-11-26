// SPDX-License-Identifier: GPL-3.0-only
package volume

import "bytes"

// rawTreePage borrows the encoded page rather than building a []treeEntry for
// every read. Writers still decode an owned node before editing it. Validation
// checks bytes, key ordering and bounded topology exactly as the decoded path.
type rawTreePage struct {
	data                    []byte
	kind, level             uint8
	count, keyBytes, stride int
}

func (v *Volume) rawTreePage(off uint64) (rawTreePage, error) {
	data, err := v.readOnlyPage(off)
	if err != nil {
		return rawTreePage{}, err
	}
	return parseRawTreePage(data, v.sb.Identity.VolumeID)
}

func parseRawTreePage(data []byte, id ID) (rawTreePage, error) {
	var p rawTreePage
	if len(data) != PageSize || string(data[:4]) != "BPT2" || !bytes.Equal(data[16:32], id[:]) || checksum(data[:PageSize-4]) != diskOrder.Uint32(data[PageSize-4:]) {
		return p, ErrCorrupt
	}
	p = rawTreePage{data: data, kind: data[4], level: data[5], count: int(diskOrder.Uint16(data[6:])), keyBytes: 49, stride: 57}
	if p.kind == treeFree {
		p.keyBytes, p.stride = 8, 16
		if p.level > 0 {
			p.stride = 32
		}
	}
	if p.kind < treeObjects || p.kind > treeFree || p.level > 32 || p.count > (PageSize-36)/p.stride || p.level > 0 && p.count == 0 {
		return rawTreePage{}, ErrCorrupt
	}
	for i := 1; i < p.count; i++ {
		if bytes.Compare(p.key(i-1), p.key(i)) >= 0 {
			return rawTreePage{}, ErrCorrupt
		}
	}
	return p, nil
}

func (p rawTreePage) key(i int) []byte   { at := 32 + i*p.stride; return p.data[at : at+p.keyBytes] }
func (p rawTreePage) value(i int) uint64 { return diskOrder.Uint64(p.data[32+i*p.stride+p.keyBytes:]) }
func (p rawTreePage) entry(i int) treeEntry {
	var e treeEntry
	copy(e.key[:], p.key(i))
	e.value = p.value(i)
	if p.kind == treeFree && p.level > 0 {
		at := 32 + i*p.stride + p.keyBytes + 8
		e.maximum = diskOrder.Uint64(p.data[at:])
		e.aligned = diskOrder.Uint64(p.data[at+8:])
	}
	return e
}
func (p rawTreePage) lower(key indexKey, strict bool) int {
	lo, hi := 0, p.count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		c := bytes.Compare(p.key(mid), key[:p.keyBytes])
		if c < 0 || strict && c == 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
func rawChildValid(parent, child rawTreePage) bool {
	return parent.level > 0 && child.level == parent.level-1 && child.kind == parent.kind
}

func (v *Volume) lookupView(key indexKey) (uint64, error) {
	page, err := v.rawTreePage(v.sb.ObjectRoot)
	if err != nil {
		return 0, err
	}
	if page.kind != treeObjects {
		return 0, ErrCorrupt
	}
	for depth := 0; depth < 33; depth++ {
		if page.count == 0 {
			return 0, nil
		}
		if page.level == 0 {
			i := page.lower(key, false)
			if i < page.count && bytes.Equal(page.key(i), key[:]) {
				return page.value(i), nil
			}
			return 0, nil
		}
		i := max(0, page.lower(key, true)-1)
		child, err := v.rawTreePage(page.value(i))
		if err != nil {
			return 0, err
		}
		if !rawChildValid(page, child) {
			return 0, ErrCorrupt
		}
		page = child
	}
	return 0, ErrCorrupt
}

func (v *Volume) walkIndexView(lower, upper *indexKey, fn func(indexEntry) error) error {
	page, err := v.rawTreePage(v.sb.ObjectRoot)
	if err != nil {
		return err
	}
	if page.kind != treeObjects {
		return ErrCorrupt
	}
	var walk func(rawTreePage, int) error
	walk = func(p rawTreePage, depth int) error {
		if depth > 32 {
			return ErrCorrupt
		}
		start := 0
		if lower != nil {
			start = p.lower(*lower, p.level > 0)
			if p.level > 0 {
				start = max(0, start-1)
			}
		}
		for i := start; i < p.count; i++ {
			if upper != nil && bytes.Compare(p.key(i), upper[:]) >= 0 {
				break
			}
			if p.level == 0 {
				e := p.entry(i)
				if err := fn(indexEntry{e.key, e.value}); err != nil {
					return err
				}
			} else {
				child, err := v.rawTreePage(p.value(i))
				if err != nil {
					return err
				}
				if !rawChildValid(p, child) {
					return ErrCorrupt
				}
				if err = walk(child, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(page, 0)
}
