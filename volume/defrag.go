// SPDX-License-Identifier: GPL-3.0-only
package volume

// beginMove reserves only the free prefix of a leftward move. Source/destination
// may overlap: each page and the move cursor commit in one redo transaction.
// A power cut resumes the cursor before any client can access the volume.
func (v *Volume) beginMove(k Key, index int, destination uint64) error {
	o, e := v.loadObject(k)
	if e != nil {
		return e
	}
	s := o.Spans[index]
	if destination >= s.Location || s.Capacity == 0 {
		return ErrRange
	}
	reserve := min(s.Capacity, s.Location-destination)
	t := v.transaction()
	t.busy = append(t.busy, allocation{destination, reserve, false})
	if e = t.claim(destination, reserve); e != nil {
		return e
	}
	t.sb.FreeBytes -= reserve
	t.sb.PayloadBytes += reserve
	t.sb.Moving = true
	t.sb.MoveKey = k
	t.sb.MoveFrom = s.Location
	t.sb.MoveTo = destination
	t.sb.MoveLength = s.Capacity
	t.sb.MoveDone = 0
	t.sb.MoveSpan = uint64(index)
	if e = t.commit(true); e != nil {
		return e
	}
	return v.resumeMoveLocked()
}
func (v *Volume) resumeMoveLocked() error {
	for v.sb.Moving && v.sb.MoveDone < v.sb.MoveLength {
		s := v.sb
		n := min(uint64(PageSize), s.MoveLength-s.MoveDone)
		b := make([]byte, n)
		if e := v.readBytes(b, s.MoveFrom+s.MoveDone); e != nil {
			return e
		}
		t := v.transaction()
		if e := t.patch(s.MoveTo+s.MoveDone, b); e != nil {
			return e
		}
		t.sb.MoveDone += n
		if e := t.commit(true); e != nil {
			return e
		}
	}
	if !v.sb.Moving {
		return nil
	}
	s := v.sb
	o, e := v.loadObject(s.MoveKey)
	if e != nil {
		return e
	}
	if s.MoveSpan >= uint64(len(o.Spans)) || o.Spans[s.MoveSpan].Location != s.MoveFrom {
		return ErrCorrupt
	}
	o = cloneObject(o)
	o.Spans[s.MoveSpan].Location = s.MoveTo
	o.dirtyFrom = int(s.MoveSpan)
	o.dirtyUntil = int(s.MoveSpan) + 1
	t := v.transaction()
	freed := min(s.MoveLength, s.MoveFrom-s.MoveTo)
	off := max(s.MoveFrom, s.MoveTo+s.MoveLength)
	t.reusable = allocation{off, freed, false}
	t.release(off, freed, false)
	t.sb.Moving = false
	t.sb.MoveKey = Key{}
	t.sb.MoveFrom = 0
	t.sb.MoveTo = 0
	t.sb.MoveLength = 0
	t.sb.MoveDone = 0
	t.sb.MoveSpan = 0
	if e = t.saveObject(s.MoveKey, o); e != nil {
		return e
	}
	if e = t.commit(true); e != nil {
		return e
	}
	v.cache.put(s.MoveKey, o)
	return nil
}

// moveTarget prefers the earliest fitting hole, including an adjacent hole too
// small for a full duplicate. Thus defrag needs page-sized redo scratch, not a
// free second copy of the largest object or a permanent second bank.
func (v *Volume) moveTarget(s dataSpan) (uint64, bool, error) {
	var target uint64
	found := false
	e := treeWalk(v.node, v.sb.FreeRoot, func(x treeEntry) error {
		// Compaction must preserve the same payload bounds as allocation.
		start := max(keyOffset(x.key), dataStart)
		if s.Capacity >= PageSize {
			start = align(start)
		}
		end := keyOffset(x.key) + x.value
		if start >= s.Location {
			return stopTree
		}
		if start < end && (s.Capacity <= end-start || end == s.Location) {
			target = start
			found = true
			return stopTree
		}
		return nil
	})
	if e == stopTree {
		e = nil
	}
	return target, found, e
}
