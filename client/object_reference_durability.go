// SPDX-License-Identifier: GPL-3.0-only
package client

import "context"

// CapabilityAcceptedReferences permits deferred reference mutations. It is
// meaningful only together with object references, accepted writes and per-
// connection fences. The default reference API keeps its durable ACK contract.
const CapabilityAcceptedReferences uint64 = 1 << 20

type filesystemDeferredKey struct{}

func acceptedReferenceMessage(ctx context.Context, t *objectTransport, m Message) (Message, bool) {
	deferred, _ := ctx.Value(filesystemDeferredKey{}).(bool)
	const required = CapabilityAcceptedReferences | CapabilityObjectReferences | CapabilityDurability | CapabilityConnectionSync
	if !deferred || m.Opcode != ObjectReference || t.capabilities&required != required {
		return m, false
	}
	// Reset/reclaim and their offline safety query are administrative operations,
	// not ordinary filesystem reference lifetime updates.
	if len(m.Body) != 129 || m.Body[112] > 5 {
		return m, false
	}
	m.Body = append(append([]byte(nil), m.Body...), 1)
	return m, true
}
