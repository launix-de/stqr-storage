// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"crypto/sha256"
	"math"
)

const (
	ObjectSubscribe               uint16 = 0x0110
	ObjectUnsubscribe             uint16 = 0x0111
	ObjectUpdateMessage           uint16 = 0x0112
	SubscriptionMoved             uint16 = 0x0113
	SubscriptionAck               uint16 = 0x0114
	CapabilityObjectSubscriptions uint64 = 512
)
const (
	UpdateReplace uint8 = iota
	UpdateRangeWrite
	UpdateAppend
	UpdateTruncate
	UpdateDelete
	UpdateResyncRequired
)

// SubscribeOptions selects the same snapshot as Get/GetRange. A resume request
// may return a new complete snapshot when retained replay is unavailable.
type SubscribeOptions struct {
	ID             ID
	Ranged         bool
	Offset, Length uint64
	Resume         *uint64
}

type ObjectUpdate struct {
	Subscription      ID
	Cursor            uint64
	Part, Parts       uint32
	Change            uint8
	Offset, NewLength uint64
	Data              []byte
}

func (u ObjectUpdate) valid() bool {
	return !(u.Subscription == (ID{}) || u.Cursor == 0 || u.Parts == 0 || u.Part >= u.Parts || u.Change > UpdateResyncRequired || uint64(len(u.Data)) > math.MaxUint64-u.Offset)
}
func (u ObjectUpdate) MarshalBinary() ([]byte, error) {
	if !u.valid() {
		return nil, ErrProtocol
	}
	e := Encoder{}
	e.ID(u.Subscription)
	e.U64(u.Cursor)
	e.U32(u.Part)
	e.U32(u.Parts)
	e.U8(u.Change)
	e.U64(u.Offset)
	e.U64(u.NewLength)
	e.U8(0)
	e.U64(uint64(len(u.Data)))
	digest := sha256.Sum256(u.Data)
	e.B = append(e.B, digest[:]...)
	e.Blob(u.Data)
	return e.B, nil
}
func DecodeObjectUpdate(b []byte) (ObjectUpdate, error) {
	d := Decoder{B: b}
	u := ObjectUpdate{Subscription: d.ID(), Cursor: d.U64(), Part: d.U32(), Parts: d.U32(), Change: d.U8(), Offset: d.U64(), NewLength: d.U64()}
	mode, total, digest, data := decodeTransfer(&d)
	if d.Done() != nil || mode != 0 || total != uint64(len(data)) || sha256.Sum256(data) != digest {
		return u, ErrProtocol
	}
	u.Data = data
	if !u.valid() {
		return u, ErrProtocol
	}
	return u, nil
}
