// SPDX-License-Identifier: GPL-3.0-only
package volume

// AdmissionFreePhysical returns current unreserved physical capacity without
// collecting identity, object or I/O statistics. Like Stats it is advisory:
// actual mutations recheck admission under the exclusive volume operation lock.
func (v *Volume) AdmissionFreePhysical() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.admissionFreePhysicalLocked()
}
func (v *Volume) admissionFreePhysicalLocked() uint64 {
	free := v.sb.FreeBytes
	if free > v.guaranteed && free-v.guaranteed > v.inFlightReserved {
		return free - v.guaranteed - v.inFlightReserved
	}
	return 0
}
