// SPDX-License-Identifier: GPL-3.0-only
package client

// Targeting changes only the transport path. Volume configuration and operation
// receipts remain on the target node, and inner operation IDs survive retries.
const NodeAdminRequest uint16 = 0x2024
const PeerAdminRequest uint16 = 0x1036
const PeerVerifyAdmin uint16 = 0x1037

type NodeAdmin struct {
	Target ID
	Opcode uint16
	Body   []byte
}

// NodeLocalAdminOpcode is deliberately an allowlist. Users, bucket policies,
// user mutations and recursive proxy calls cannot bypass their own protocols.
// The dashboard may forward native GET/STAT/CAS/inventory requests; these retain
// normal bucket grants, lease exclusion, routing and volume-lock semantics.
func NodeLocalAdminOpcode(op uint16) bool {
	if NodeAdminObjectOpcode(op) {
		return true
	}
	switch op {
	case UserListPage, VolumeAddWithGroups, VolumePlacementRelease, NodeDrain, NodeShutdown, VolumeAdd, VolumeRemove, VolumeRescan, VolumeGrow, VolumeGrowInfo,
		VolumeRecoveryStage, VolumeRecoveryCommit, VolumeRecoveryRelease,
		VolumeCandidates, VolumeCapacityGet, NodeConfigGet, NodeConfigSet,
		AdminOperationStatus:
		return true
	}
	return false
}

func nodeLocalAdminBody(op uint16, body []byte) bool {
	return op != NodeShutdown || len(body) == 17 && body[16] <= 1
}

func (m NodeAdmin) MarshalBinary() ([]byte, error) {
	if m.Target == (ID{}) || !NodeLocalAdminOpcode(m.Opcode) || !nodeLocalAdminBody(m.Opcode, m.Body) || len(m.Body) > MaxMessage-256 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.ID(m.Target)
	e.U16(m.Opcode)
	e.Blob(m.Body)
	return e.B, nil
}

func DecodeNodeAdmin(body []byte) (NodeAdmin, error) {
	d := Decoder{B: body}
	m := NodeAdmin{Target: d.ID(), Opcode: d.U16(), Body: d.Blob()}
	if d.Done() != nil || m.Target == (ID{}) || !NodeLocalAdminOpcode(m.Opcode) || !nodeLocalAdminBody(m.Opcode, m.Body) || len(m.Body) > MaxMessage-256 {
		return NodeAdmin{}, ErrProtocol
	}
	return m, nil
}

type PeerAdmin struct {
	Actor, Operation ID
	Username         string
	Opcode           uint16
	Body             []byte
}

func (m PeerAdmin) MarshalBinary() ([]byte, error) {
	if m.Actor == (ID{}) || m.Operation == (ID{}) || len(m.Username) == 0 || len(m.Username) > 128 || !NodeLocalAdminOpcode(m.Opcode) || !nodeLocalAdminBody(m.Opcode, m.Body) || len(m.Body) > MaxMessage-256 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.ID(m.Actor)
	e.Text(m.Username)
	e.ID(m.Operation)
	e.U16(m.Opcode)
	e.Blob(m.Body)
	return e.B, nil
}

func DecodePeerAdmin(body []byte) (PeerAdmin, error) {
	d := Decoder{B: body}
	m := PeerAdmin{Actor: d.ID(), Username: d.Text(), Operation: d.ID(), Opcode: d.U16(), Body: d.Blob()}
	if d.Done() != nil || m.Actor == (ID{}) || m.Operation == (ID{}) || len(m.Username) == 0 || len(m.Username) > 128 || !NodeLocalAdminOpcode(m.Opcode) || !nodeLocalAdminBody(m.Opcode, m.Body) || len(m.Body) > MaxMessage-256 {
		return PeerAdmin{}, ErrProtocol
	}
	return m, nil
}

// NodeAdminObjectOpcode permits only bounded non-streaming object requests.
// Normal bucket authorization and maintenance exclusion still apply.
func NodeAdminObjectOpcode(op uint16) bool {
	switch op {
	case 0x0100, 0x0107, 0x0108, BucketObjectList:
		return true
	}
	return false
}
