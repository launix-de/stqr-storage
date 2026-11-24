// SPDX-License-Identifier: GPL-3.0-only
package client

import "testing"

func TestNegotiatedCapabilitiesHaveIndependentBits(t *testing.T) {
	capabilities := []uint64{CapabilityPeerTransport, CapabilityNodeAdmin,
		CapabilityPendingNode, CapabilityClusterControl, CapabilityRangeHandoff,
		CapabilityRetiringNode, CapabilityExclusion, CapabilityControlRecovery,
		CapabilityVolumeRecovery, CapabilityObjectSubscriptions, CapabilityRecoveryFenceClearance, CapabilityPhysicalQuotas, CapabilityReplication, CapabilityDurability}
	var seen uint64
	for _, bit := range capabilities {
		if bit == 0 || bit&(bit-1) != 0 || bit&seen != 0 {
			t.Fatalf("capability %d is not an independent feature bit", bit)
		}
		seen |= bit
	}
}
