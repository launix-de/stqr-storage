// SPDX-License-Identifier: GPL-3.0-only
// Package client contains the public STQR bucket SDK and shared wire codec.
package client

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"unicode/utf8"
)

// BucketHeader selects the native bucket once during HTTP/WebSocket upgrade.
const BucketHeader = "X-STQR-Bucket"

// FormatHeader declares the opaque client format when opening a bucket.
const FormatHeader = "X-STQR-Format"

const MaxMessage = 16 << 20
const (
	FireAndForget uint8 = iota
	Request
	Response
	Push
)
const (
	Hello                uint16 = 0x0001
	Ping                 uint16 = 0x0002
	Event                uint16 = 0x0004
	StatsGet             uint16 = 0x0008
	StatsSubscribe       uint16 = 0x0009
	StatsUnsubscribe     uint16 = 0x000a
	StatsSnapshotMessage uint16 = 0x000b
	BucketInfoGet        uint16 = 0x010c
	BucketMaintenance    uint16 = 0x0115
	BucketScan           uint16 = 0x0116
	BucketObjectList     uint16 = 0x0117
	BucketFormatOpen     uint16 = 0x0121
	BucketObjectLease    uint16 = 0x011c
	// Deprecated: use BucketObjectLease; the server does not interpret device formats.
	BucketBlockMaintenance         = BucketObjectLease
	BucketMaintenanceEnd    uint16 = 0x011d
	UserListPage            uint16 = 0x2038
	BucketListPage          uint16 = 0x2039
	VolumeAddWithGroups     uint16 = 0x2036
	VolumePlacementRelease  uint16 = 0x2037
	BucketGroupCapacity     uint16 = 0x2035
	BucketCreateInGroup     uint16 = 0x2034
	BucketCreate            uint16 = 0x2003
	BucketSetPolicy         uint16 = 0x2004
	BucketSetQuota          uint16 = 0x2005
	BucketRename            uint16 = 0x2006
	BucketLock              uint16 = 0x2007
	BucketDelete            uint16 = 0x2008
	BucketGrant             uint16 = 0x2009
	BucketRevoke            uint16 = 0x200a
	BucketList              uint16 = 0x2022
	BucketQuotaCapacity     uint16 = 0x2023
	BucketClear             uint16 = 0x2028
	BucketClone             uint16 = 0x2029
	BucketMaintenanceStatus uint16 = 0x202a
	PlacementGroupList      uint16 = 0x2030
	PlacementGroupSet       uint16 = 0x2031
	PlacementGroupDelete    uint16 = 0x2032
	BucketSetPlacementGroup uint16 = 0x2033
	AdminGet                uint16 = 0x2000
	VolumeAdd               uint16 = 0x2001
	VolumeRemove            uint16 = 0x2002
	NodeDrain               uint16 = 0x2010
	NodeShutdown            uint16 = 0x2011
	ClusterShutdown         uint16 = 0x2012
	ForceDisconnect         uint16 = 0x2013
	NodeRecoveryBegin       uint16 = 0x2014
	VolumeRecoveryStage     uint16 = 0x2015
	VolumeRecoveryCommit    uint16 = 0x2016
	VolumeRecoveryRelease   uint16 = 0x2017
	AdminOperationStatus    uint16 = 0x2019
	// Local configuration and candidate discovery extend the administration range.
	NodeConfigGet     uint16 = 0x201a
	NodeConfigSet     uint16 = 0x201b
	VolumeCandidates  uint16 = 0x201c
	VolumeRescan      uint16 = 0x201d
	VolumeCapacityGet uint16 = 0x201e
	VolumeGrowInfo    uint16 = 0x201f
	VolumeGrow        uint16 = 0x2020
	UserS3KeyList     uint16 = 0x2025
	UserS3KeySet      uint16 = 0x2026
	UserS3KeyDelete   uint16 = 0x2027
	UserList          uint16 = 0x2021
	UserCreate        uint16 = 0x200b
	UserUpdate        uint16 = 0x200c
	UserSetPassword   uint16 = 0x200d
	UserDelete        uint16 = 0x200e
)

// Opcodes 0x011e/0x011f and 0x203e/0x203f are retired.

type ID [16]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

type Message struct {
	Kind   uint8
	Opcode uint16
	ID     ID
	Body   []byte
}

func EncodeMessage(m Message) []byte {
	e := Encoder{}
	e.U8(1)
	e.U8(m.Kind)
	e.U16(m.Opcode)
	e.ID(m.ID)
	e.B = append(e.B, m.Body...)
	return e.B
}
func DecodeMessage(b []byte) (Message, error) {
	var m Message
	if len(b) < 20 || len(b) > MaxMessage || b[0] != 1 || b[1] > Push {
		return m, errors.New("invalid STQR envelope")
	}
	m.Kind = b[1]
	m.Opcode = binary.LittleEndian.Uint16(b[2:])
	copy(m.ID[:], b[4:20])
	m.Body = b[20:]
	return m, nil
}

type Encoder struct{ B []byte }

func (e *Encoder) U8(v uint8) { e.B = append(e.B, v) }
func (e *Encoder) Bool(v bool) {
	if v {
		e.U8(1)
	} else {
		e.U8(0)
	}
}
func (e *Encoder) U16(v uint16)  { e.B = binary.LittleEndian.AppendUint16(e.B, v) }
func (e *Encoder) U32(v uint32)  { e.B = binary.LittleEndian.AppendUint32(e.B, v) }
func (e *Encoder) U64(v uint64)  { e.B = binary.LittleEndian.AppendUint64(e.B, v) }
func (e *Encoder) V(v uint64)    { e.B = binary.AppendUvarint(e.B, v) }
func (e *Encoder) ID(v ID)       { e.B = append(e.B, v[:]...) }
func (e *Encoder) Blob(v []byte) { e.V(uint64(len(v))); e.B = append(e.B, v...) }
func (e *Encoder) Text(v string) { e.Blob([]byte(v)) }

type Decoder struct {
	B   []byte
	Err error
}

func (d *Decoder) Take(n int) []byte {
	if d.Err != nil {
		return nil
	}
	if n < 0 || n > len(d.B) {
		d.Err = errors.New("truncated STQR message")
		return nil
	}
	b := d.B[:n]
	d.B = d.B[n:]
	return b
}
func (d *Decoder) U8() uint8 {
	b := d.Take(1)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
func (d *Decoder) Bool() bool {
	b := d.U8()
	if b > 1 {
		d.Err = errors.New("invalid bool")
	}
	return b == 1
}
func (d *Decoder) U16() uint16 {
	b := d.Take(2)
	if len(b) != 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}
func (d *Decoder) U32() uint32 {
	b := d.Take(4)
	if len(b) != 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}
func (d *Decoder) U64() uint64 {
	b := d.Take(8)
	if len(b) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (d *Decoder) V() uint64 {
	if d.Err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.B)
	if n <= 0 {
		d.Err = errors.New("invalid varint")
		return 0
	}
	var buf [10]byte
	if binary.PutUvarint(buf[:], v) != n {
		d.Err = errors.New("noncanonical varint")
		return 0
	}
	d.Take(n)
	return v
}
func (d *Decoder) Count(limit uint64) int {
	n := d.V()
	if n > limit || n > uint64(len(d.B)) {
		d.Err = errors.New("STQR count exceeds message budget")
		return 0
	}
	return int(n)
}
func (d *Decoder) ID() ID       { var id ID; copy(id[:], d.Take(16)); return id }
func (d *Decoder) Blob() []byte { return d.Take(d.Count(MaxMessage)) }
func (d *Decoder) Text() string {
	b := d.Blob()
	if !utf8.Valid(b) {
		d.Err = errors.New("invalid UTF-8")
	}
	return string(b)
}
func (d *Decoder) Done() error {
	if d.Err != nil {
		return d.Err
	}
	if len(d.B) != 0 {
		return errors.New("trailing STQR bytes")
	}
	return nil
}
func ResponseBody(status int, result []byte, detail string) []byte {
	e := Encoder{}
	e.V(uint64(status))
	h := Encoder{}
	if detail != "" {
		h.V(1)
		h.V(3)
		x := Encoder{}
		x.Text(detail)
		h.Blob(x.B)
	} else {
		h.V(0)
	}
	e.Blob(h.B)
	e.B = append(e.B, result...)
	return e.B
}

type StatsFilter struct {
	Scope                   uint8
	Nodes, Volumes, Buckets []ID
	IncludeRanges           bool
}

func DecodeStatsFilter(d *Decoder) StatsFilter {
	f := StatsFilter{Scope: d.U8()}
	for _, p := range []*[]ID{&f.Nodes, &f.Volumes, &f.Buckets} {
		n := d.Count(65536)
		for i := 0; i < n; i++ {
			*p = append(*p, d.ID())
		}
	}
	f.IncludeRanges = d.Bool()
	if f.Scope > 1 {
		d.Err = errors.New("invalid stats scope")
	}
	return f
}
