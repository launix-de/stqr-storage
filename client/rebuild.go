// SPDX-License-Identifier: GPL-3.0-only
package client

// These messages define the specified offer/statistics payloads, not a final
// election or binding protocol. Servers without the multinode capability still
// reject them with 501. Conflict/sealing rules follow the measured race design.
const (
	RebuildOfferMessage  uint16 = 0x1004
	StatsFragmentMessage uint16 = 0x1007
)

type RebuildOffer struct {
	Round     RoundID
	Candidate ID
	Reasons   uint64
}
type StatsPart struct {
	Source, Boot ID
	Sequence     uint64
	Index, Count uint32
	Digest       [32]byte
	Data         []byte
}
type StatsFragment struct {
	Round           RoundID
	Part            StatsPart
	ControlSequence uint64
	PendingChanges  []ID
}

func writeRound(e *Encoder, r RoundID) {
	e.U64(r.Membership)
	e.U64(r.Generation)
	e.ID(r.Initiator)
	e.U64(r.Attempt)
}
func readRound(d *Decoder) RoundID {
	return RoundID{Membership: d.U64(), Generation: d.U64(), Initiator: d.ID(), Attempt: d.U64()}
}
func validRound(r RoundID) bool { return r.Membership > 0 && r.Generation > 0 && r.Initiator != (ID{}) }
func (m RebuildOffer) MarshalBinary() ([]byte, error) {
	if !validRound(m.Round) || m.Candidate != (m.Round.Initiator) {
		return nil, ErrProtocol
	}
	e := Encoder{}
	writeRound(&e, m.Round)
	e.ID(m.Candidate)
	e.U64(m.Reasons)
	return e.B, nil
}
func DecodeRebuildOffer(body []byte) (RebuildOffer, error) {
	d := Decoder{B: body}
	m := RebuildOffer{Round: readRound(&d), Candidate: d.ID(), Reasons: d.U64()}
	if d.Done() != nil || !validRound(m.Round) || m.Candidate != m.Round.Initiator {
		return RebuildOffer{}, ErrProtocol
	}
	return m, nil
}
func (m StatsFragment) valid() bool {
	p := m.Part
	if !validRound(m.Round) || p.Source == (ID{}) || p.Boot == (ID{}) || p.Sequence == 0 || p.Count == 0 || p.Count > 64 || p.Index >= p.Count || len(p.Data) == 0 || len(p.Data) > 1<<20 || len(m.PendingChanges) > MaxMessage/16 {
		return false
	}
	for _, id := range m.PendingChanges {
		if id == (ID{}) {
			return false
		}
	}
	return true
}
func (m StatsFragment) MarshalBinary() ([]byte, error) {
	if !m.valid() {
		return nil, ErrProtocol
	}
	// Include all fixed fields and length prefixes before allocating the payload.
	prefix := Encoder{}
	prefix.V(uint64(len(m.Part.Data)))
	prefix.V(uint64(len(m.PendingChanges)))
	if len(m.Part.Data)+16*len(m.PendingChanges)+128+len(prefix.B) > MaxMessage-20 {
		return nil, ErrProtocol
	}
	e := Encoder{}
	writeRound(&e, m.Round)
	p := m.Part
	e.ID(p.Source)
	e.ID(p.Boot)
	e.U64(p.Sequence)
	e.U32(p.Index)
	e.U32(p.Count)
	e.B = append(e.B, p.Digest[:]...)
	e.Blob(p.Data)
	e.U64(m.ControlSequence)
	e.V(uint64(len(m.PendingChanges)))
	for _, id := range m.PendingChanges {
		e.ID(id)
	}
	return e.B, nil
}
func DecodeStatsFragment(body []byte) (StatsFragment, error) {
	if len(body) > MaxMessage-20 {
		return StatsFragment{}, ErrProtocol
	}
	d := Decoder{B: body}
	m := StatsFragment{Round: readRound(&d)}
	p := &m.Part
	p.Source = d.ID()
	p.Boot = d.ID()
	p.Sequence = d.U64()
	p.Index = d.U32()
	p.Count = d.U32()
	copy(p.Digest[:], d.Take(32))
	p.Data = d.Blob()
	m.ControlSequence = d.U64()
	count := d.Count(MaxMessage / 16)
	if d.Err != nil || count > len(d.B)/16 || len(p.Data) > 1<<20 {
		return StatsFragment{}, ErrProtocol
	}
	m.PendingChanges = make([]ID, count)
	for i := range m.PendingChanges {
		m.PendingChanges[i] = d.ID()
	}
	if d.Done() != nil || !m.valid() {
		return StatsFragment{}, ErrProtocol
	}
	p.Data = append([]byte(nil), p.Data...)
	return m, nil
}
