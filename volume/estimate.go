// SPDX-License-Identifier: GPL-3.0-only
package volume

// EstimatePutOverheadPPM budgets one packed object descriptor, one retained
// request receipt, two object-tree entries, and two free-tree entries per PUT.
// Leaf occupancy is conservatively one half; 16 bytes cover amortized internal
// nodes. Roots/WAL scratch are already excluded by AdmissionFreePhysical. Raw
// payload has no page rounding charge: every tail byte remains on the free tree.
// Repeated updates, retained requests per object and APPEND slack are separate
// workload costs; compression savings are deliberately not assumed.
func EstimatePutOverheadPPM(size uint64) (uint32, error) {
	if size == 0 || size > MaxBufferedRead {
		return 0, ErrRange
	}
	objectHalf := uint64((PageSize - 36) / 57 / 2)
	freeHalf := uint64((PageSize - 36) / 16 / 2)
	metadata := uint64(objectHeader+spanBytes+receiptBytes) + 2*((PageSize+objectHalf-1)/objectHalf) + 2*((PageSize+freeHalf-1)/freeHalf) + 16
	if size > BlockSize {
		metadata += (size + BlockSize - 1) / BlockSize * 4
	}
	ppm := (metadata*1000000 + size - 1) / size
	return uint32(ppm), nil
}
