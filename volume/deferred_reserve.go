// SPDX-License-Identifier: GPL-3.0-only
package volume

import "fmt"

// ErrWritebackPressure is bounded backpressure, never permission to flush. A
// caller may explicitly SYNC or free capacity and retry; a server must not turn
// it into an unsolicited durability fence. No rejected transaction is published.
var ErrWritebackPressure = fmt.Errorf("%w: unsynced writeback needs more RAM or safe redo space; explicit SYNC may release it", ErrBusy)

func (g *deferredGroup) baselineNode(v *Volume, off uint64) (treeNode, error) {
	if n, ok := g.baseNodes.get(off); ok {
		return n, nil
	}
	var page []byte
	if v.fence != nil {
		p, ok, err := v.fence.page(off)
		if err != nil {
			return treeNode{}, err
		}
		if ok {
			page = p
		}
	}
	if page == nil {
		page = make([]byte, PageSize)
		if err := readFullAt(v.dev, page, off); err != nil {
			return treeNode{}, err
		}
	}
	n, err := decodeNode(page, v.sb.Identity.VolumeID)
	if err == nil {
		g.baseNodes.put(off, n)
	}
	return n, err
}
func (g *deferredGroup) baselineFree(v *Volume, off, n uint64) (bool, error) {
	span, found, err := treeNeighbor(func(off uint64) (treeNode, error) { return g.baselineNode(v, off) }, g.baseFreeRoot, spaceKey(off), false)
	if err != nil {
		return false, err
	}
	start := keyOffset(span.key)
	return found && start <= off && off-start <= span.value && n <= span.value-(off-start), nil
}
func freeContains(read func(uint64) (treeNode, error), root, off, n uint64) (bool, error) {
	span, ok, err := treeNeighbor(read, root, spaceKey(off), false)
	if err != nil {
		return false, err
	}
	start := keyOffset(span.key)
	return ok && start <= off && off-start <= span.value && n <= span.value-(off-start), nil
}
func (g *deferredGroup) reserveRedo(t *transaction, protected uint64) error {
	// One header + count + (address,page) records, framed in fixed WAL pages.
	if protected > (^uint64(0)-PageSize-8)/(PageSize+8) {
		return ErrWritebackPressure
	}
	bytes := PageSize + 8 + protected*(PageSize+8)
	needed := (bytes + PageSize - 17) / (PageSize - 16) * PageSize
	if needed <= g.journal.n {
		return nil
	}
	size := max(needed, g.journal.n*2)
	// Extending in place avoids requiring both the old and new arena when free
	// space is ample but fragmented. Neither arena has been written before Sync.
	for _, n := range []uint64{size, needed} {
		candidates := []uint64{g.journal.off}
		if g.journal.off+g.journal.n >= n {
			candidates = append(candidates, (g.journal.off+g.journal.n-n)/PageSize*PageSize)
		}
		for _, off := range candidates {
			current, err := t.freeContains(t.sb.FreeRoot, off, n)
			if err != nil {
				return err
			}
			baseline, err := g.baselineFree(t.v, off, n)
			if err != nil {
				return err
			}
			blocked := false
			if t.v.fence != nil {
				for _, r := range t.v.fence.reserved {
					blocked = blocked || overlap(allocation{off, n, false}, r)
				}
			}
			if current && baseline && !blocked {
				g.journal = allocation{off, n, false}
				return nil
			}
		}
	}

	// Intersect current and baseline free extents. The existing allocator prunes
	// by subtree maxima; failed baseline candidates advance by an extent, not by
	// each byte/page of a large retired-but-not-durable interval.
	for _, n := range []uint64{size, needed} {
		minimum := uint64(0)
		for minimum < t.sb.Identity.Size {
			_, _, off, err := t.findSpace(n, PageSize, false, minimum)
			if err == ErrFull {
				break
			}
			if err != nil {
				return err
			}
			ok, err := g.baselineFree(t.v, off, n)
			if err != nil {
				return err
			}
			if ok {
				g.journal = allocation{off, n, false}
				return nil
			}
			span, found, err := treeNeighbor(func(p uint64) (treeNode, error) { return g.baselineNode(t.v, p) }, g.baseFreeRoot, spaceKey(off), true)
			if err != nil {
				return err
			}
			if !found {
				break
			}
			next := keyOffset(span.key)
			if next <= off {
				next = off + PageSize
			}
			minimum = next
		}
	}
	return ErrWritebackPressure
}

// publishStaging preflights every new protected address and WAL reservation
// before modifying the accepted image. A private spill I/O failure while merging
// fences this instance; reopening retains the previous durable volume image.
func (g *deferredGroup) publishStaging(t *transaction) error {
	staging := t.v.stageStore
	protected := g.protectedPages
	err := staging.each(func(off uint64, _ byte, _ []byte) error {
		prior, err := g.store.flag(off)
		if err != nil {
			return err
		}
		if prior != 0 {
			return nil
		}
		free, err := g.baselineFree(t.v, off, PageSize)
		if err != nil {
			return err
		}
		if !free {
			protected++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if g.store.noSpill {
		count := g.store.count
		if err = staging.each(func(off uint64, _ byte, _ []byte) error {
			flag, err := g.store.flag(off)
			if err == nil && flag == 0 {
				count++
			}
			return err
		}); err != nil {
			return err
		}
		if count > uint64(g.store.limit) {
			return ErrWritebackPressure
		}
	}
	if err = g.reserveRedo(t, protected); err != nil {
		return err
	}
	err = staging.each(func(off uint64, _ byte, page []byte) error {
		flag, err := g.store.flag(off)
		if err != nil {
			return err
		}
		if flag == 0 {
			free, err := g.baselineFree(t.v, off, PageSize)
			if err != nil {
				return err
			}
			flag = 2
			if free {
				flag = 1
			}
		}
		return g.store.put(off, page, flag)
	})
	if err != nil {
		t.v.invalidateKeyMembership()
		t.v.failed = err
		return fmt.Errorf("%w: unsynced spill: %v", ErrRecovery, err)
	}
	g.protectedPages = protected
	return nil
}
