// SPDX-License-Identifier: GPL-3.0-only
package volume

import "bytes"

// compareObject uses the caller's existing volume lock and never retains a
// borrowed mmap/overlay slice. Construct the entire checked view before testing
// equality: corruption in a later chunk must still win over an earlier byte
// mismatch, exactly as the previous readObject-then-Equal comparison did.
func (v *Volume) compareObject(o *object, offset uint64, expected []byte) (bool, error) {
	if offset > o.Info.Length || uint64(len(expected)) > o.Info.Length-offset {
		return false, ErrRange
	}
	view, err := v.objectReadView(o, offset, uint64(len(expected)))
	if err != nil {
		return false, err
	}
	if view.Length != uint64(len(expected)) {
		return false, ErrCorrupt
	}
	at := 0
	for _, part := range view.Segments {
		if part.Length > uint64(len(expected)-at) {
			return false, ErrCorrupt
		}
		n := int(part.Length)
		want := expected[at : at+n]
		if part.Data != nil {
			if len(part.Data) != n {
				return false, ErrCorrupt
			}
			if !bytes.Equal(part.Data, want) {
				return false, nil
			}
		} else {
			for len(want) > 0 {
				n := min(len(want), len(readViewZeros))
				if !bytes.Equal(want[:n], readViewZeros[:n]) {
					return false, nil
				}
				want = want[n:]
			}
		}
		at += n
	}
	if at != len(expected) {
		return false, ErrCorrupt
	}
	return true, nil
}

// compareObjectZeroExtended compares the selected stored prefix with the same
// corruption checks as ordinary CAS, then compares the virtual zero tail.
// It never allocates or reads the gap between EOF and a sparse write offset.
func (v *Volume) compareObjectZeroExtended(o *object, offset uint64, expected []byte) (bool, error) {
	if _, err := add(offset, uint64(len(expected))); err != nil {
		return false, err
	}
	present := uint64(0)
	if offset < o.Info.Length {
		present = min(uint64(len(expected)), o.Info.Length-offset)
	}
	if present > 0 {
		equal, err := v.compareObject(o, offset, expected[:int(present)])
		if err != nil || !equal {
			return equal, err
		}
	}
	for _, b := range expected[int(present):] {
		if b != 0 {
			return false, nil
		}
	}
	return true, nil
}
