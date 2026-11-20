// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var diskOrder = binary.LittleEndian
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// Superblock is a validated commit root. Two independently checksummed 4-KiB
// copies alternate; payload/journal sync precedes publication of the new root.
type Superblock struct {
	Identity                              Identity
	Generation, BankEpoch, Sequence       uint64
	Bank                                  uint8
	JournalBytes, DataTop                 uint64
	Objects, LogicalBytes, IndexBytes     uint64
	IndexLocation, IndexCount, ReplayFrom uint64
	LiveData, ArenaUnused                 uint64
	TailSlot                              uint8
	rootSlot                              uint8
}

func (s Superblock) bankSize() uint64 {
	return (s.Identity.Size - superblockBytes) / 2 / PageSize * PageSize
}
func (s Superblock) bankStart() uint64 { return superblockBytes + uint64(s.Bank)*s.bankSize() }
func (s Superblock) metaPages() uint64 {
	if s.JournalBytes == 0 {
		return 0
	}
	return (s.JournalBytes-1)/pagePayload + 1
}

func (s Superblock) MarshalBinary() ([]byte, error) {
	if e := s.validate(); e != nil {
		return nil, e
	}
	b := make([]byte, PageSize)
	copy(b, "STQRVOL1")
	diskOrder.PutUint16(b[8:], FormatVersion)
	b[10] = byte(s.Identity.Class)
	b[11] = s.Bank
	b[12] = s.TailSlot
	copy(b[16:], s.Identity.ClusterID[:])
	copy(b[32:], s.Identity.NodeID[:])
	copy(b[48:], s.Identity.VolumeID[:])
	for i, x := range []uint64{s.Identity.Size, s.Generation, s.BankEpoch, s.Sequence, s.JournalBytes, s.DataTop, s.Objects, s.LogicalBytes, s.IndexBytes, s.IndexLocation, s.IndexCount, s.ReplayFrom, s.LiveData, s.ArenaUnused} {
		diskOrder.PutUint64(b[64+i*8:], x)
	}
	diskOrder.PutUint32(b[PageSize-4:], checksum(b[:PageSize-4]))
	return b, nil
}
func DecodeSuperblock(b []byte) (Superblock, error) {
	var s Superblock
	if len(b) != PageSize {
		return s, ErrUnformatted
	}
	if !bytes.Equal(b[:8], []byte("STQRVOL1")) {
		if bytes.Equal(b[:4], []byte("STQR")) || bytes.Equal(b[4:8], []byte("VOL1")) {
			return s, ErrCorrupt
		}
		return s, ErrUnformatted
	}
	if checksum(b[:PageSize-4]) != diskOrder.Uint32(b[PageSize-4:]) {
		return s, ErrCorrupt
	}
	if diskOrder.Uint16(b[8:]) != FormatVersion {
		return s, ErrVersion
	}
	s.Identity.Class = Class(b[10])
	s.Bank = b[11]
	s.TailSlot = b[12]
	copy(s.Identity.ClusterID[:], b[16:32])
	copy(s.Identity.NodeID[:], b[32:48])
	copy(s.Identity.VolumeID[:], b[48:64])
	p := make([]uint64, 14)
	for i := range p {
		p[i] = diskOrder.Uint64(b[64+i*8:])
	}
	s.Identity.Size = p[0]
	s.Generation = p[1]
	s.BankEpoch = p[2]
	s.Sequence = p[3]
	s.JournalBytes = p[4]
	s.DataTop = p[5]
	s.Objects = p[6]
	s.LogicalBytes = p[7]
	s.IndexBytes = p[8]
	s.IndexLocation = p[9]
	s.IndexCount = p[10]
	s.ReplayFrom = p[11]
	s.LiveData = p[12]
	s.ArenaUnused = p[13]
	return s, s.validate()
}
func (s Superblock) validate() error {
	if !s.Identity.Class.valid() || s.Identity.Size < MinVolumeSize || s.Identity.Size > uint64(^uint64(0)>>1) || s.Bank > 1 || s.TailSlot > 1 || s.Generation == 0 || s.BankEpoch == 0 || s.BankEpoch > s.Generation || s.Identity.VolumeID == (ID{}) {
		return ErrCorrupt
	}
	if s.IndexCount > s.bankSize()/indexEntrySize {
		return ErrCorrupt
	}
	if s.ReplayFrom > s.JournalBytes || s.IndexBytes > s.bankSize()-min(s.DataTop, s.bankSize()) {
		return ErrCorrupt
	}
	if s.IndexCount > 0 && (s.IndexLocation < s.DataTop || s.IndexLocation > s.bankSize() || (s.IndexCount+entriesPerPage-1)/entriesPerPage > (s.bankSize()-s.IndexLocation)/PageSize) {
		return ErrCorrupt
	}
	if s.LiveData > s.bankSize()-min(s.DataTop, s.bankSize())-s.IndexBytes || s.ArenaUnused > s.LiveData {
		return ErrCorrupt
	}
	if s.DataTop > s.bankSize() || s.DataTop%PageSize != 0 || s.JournalBytes > s.bankSize() || s.metaPages()*2*PageSize > s.DataTop {
		return ErrCorrupt
	}
	return nil
}

type cowPage struct {
	VolumeID                 ID
	Epoch, Index, Generation uint64
	Used                     uint32
	Data                     []byte
}

func encodePage(magic string, p cowPage) []byte {
	b := make([]byte, PageSize)
	copy(b, magic)
	diskOrder.PutUint64(b[8:], p.Epoch)
	diskOrder.PutUint64(b[16:], p.Index)
	diskOrder.PutUint64(b[24:], p.Generation)
	diskOrder.PutUint32(b[32:], p.Used)
	copy(b[pageHeaderSize:], p.Data)
	diskOrder.PutUint32(b[36:], checksum(b[pageHeaderSize:]))
	copy(b[44:60], p.VolumeID[:])
	diskOrder.PutUint32(b[60:], checksum(b[:60]))
	return b
}
func decodePage(b []byte, magic string, epoch, index uint64, volumeID ID) (cowPage, error) {
	var p cowPage
	if len(b) != PageSize || string(b[:4]) != magic || diskOrder.Uint64(b[8:]) != epoch || diskOrder.Uint64(b[16:]) != index || !bytes.Equal(b[44:60], volumeID[:]) || checksum(b[:60]) != diskOrder.Uint32(b[60:]) || checksum(b[pageHeaderSize:]) != diskOrder.Uint32(b[36:]) {
		return p, ErrCorrupt
	}
	p.VolumeID = volumeID
	p.Epoch = epoch
	p.Index = index
	p.Generation = diskOrder.Uint64(b[24:])
	p.Used = diskOrder.Uint32(b[32:])
	if p.Used > pagePayload || p.Generation == 0 {
		return p, ErrCorrupt
	}
	p.Data = b[pageHeaderSize:]
	return p, nil
}

const recordHeader = 184
const extentBytes = 56
const arenaUpdateBytes = 16
const (
	opPut uint8 = 1 + iota
	opPutRange
	opAppend
	opTrunc
	opDelete
	opCAS
	opSnapshot
	opDedupe
	opForget
	opIndex
)

type arenaUpdate struct {
	Location uint64
	Slot     uint8
}
type record struct {
	Op                                             uint8
	Codec                                          Compression
	Key                                            Key
	Request                                        ID
	Fingerprint                                    [32]byte
	Sequence, Version, Offset, Length, WriteLength uint64
	Deleted                                        bool
	Arena                                          arena
	Extents                                        []dataExtent
	Updates                                        []arenaUpdate
	Result                                         Result
	Previous                                       uint64
	Replace                                        bool
}

func encodeRecord(r record) ([]byte, error) {
	n := uint64(recordHeader+32+4) + uint64(len(r.Extents))*extentBytes + uint64(len(r.Updates))*arenaUpdateBytes
	if n > uint64(^uint(0)>>1) {
		return nil, ErrRange
	}
	b := make([]byte, int(n))
	copy(b, "TXN1")
	diskOrder.PutUint16(b[4:], 1)
	b[6] = r.Op
	b[7] = byte(r.Codec)
	diskOrder.PutUint64(b[8:], n)
	for i, x := range []uint64{r.Sequence, r.Version, r.Offset, r.Length, r.WriteLength} {
		diskOrder.PutUint64(b[16+i*8:], x)
	}
	copy(b[56:], r.Key.Bucket[:])
	copy(b[72:], r.Key.Object[:])
	copy(b[104:], r.Request[:])
	copy(b[120:], r.Fingerprint[:])
	diskOrder.PutUint32(b[152:], uint32(len(r.Extents)))
	diskOrder.PutUint32(b[156:], uint32(len(r.Updates)))
	if r.Deleted {
		b[160] = 1
	}
	if r.Replace {
		b[161] = 1
	}
	diskOrder.PutUint64(b[176:], r.Previous)
	diskOrder.PutUint64(b[164:], r.Arena.Base)
	diskOrder.PutUint32(b[172:], r.Arena.Pages)
	at := recordHeader
	for _, x := range []uint64{r.Arena.Used, r.Result.Offset, r.Result.Info.Version, r.Result.Info.Length} {
		diskOrder.PutUint64(b[at:], x)
		at += 8
	}
	for _, e := range r.Extents {
		diskOrder.PutUint64(b[at:], e.Offset)
		diskOrder.PutUint64(b[at+8:], e.Length)
		diskOrder.PutUint32(b[at+16:], e.Skip)
		b[at+20] = byte(e.Codec)
		if e.Arena {
			b[at+21] = 1
		}
		diskOrder.PutUint64(b[at+24:], e.Location)
		diskOrder.PutUint64(b[at+32:], e.ArenaOffset)
		diskOrder.PutUint32(b[at+40:], e.Stored)
		diskOrder.PutUint32(b[at+44:], e.Raw)
		diskOrder.PutUint32(b[at+48:], e.StoredCRC)
		diskOrder.PutUint32(b[at+52:], e.RawCRC)
		at += extentBytes
	}
	for _, u := range r.Updates {
		diskOrder.PutUint64(b[at:], u.Location)
		b[at+8] = u.Slot
		at += arenaUpdateBytes
	}
	diskOrder.PutUint32(b[len(b)-4:], checksum(b[:len(b)-4]))
	return b, nil
}
func decodeRecord(b []byte) (record, error) {
	var r record
	if len(b) < recordHeader+36 || string(b[:4]) != "TXN1" || diskOrder.Uint16(b[4:]) != 1 || diskOrder.Uint64(b[8:]) != uint64(len(b)) || checksum(b[:len(b)-4]) != diskOrder.Uint32(b[len(b)-4:]) {
		return r, ErrCorrupt
	}
	r.Op = b[6]
	r.Codec = Compression(b[7])
	if r.Op < opPut || r.Op > opIndex || !r.Codec.valid() || b[160] > 1 || b[161] > 1 {
		return r, ErrCorrupt
	}
	vals := []*uint64{&r.Sequence, &r.Version, &r.Offset, &r.Length, &r.WriteLength}
	for i, p := range vals {
		*p = diskOrder.Uint64(b[16+i*8:])
	}
	copy(r.Key.Bucket[:], b[56:72])
	copy(r.Key.Object[:], b[72:104])
	copy(r.Request[:], b[104:120])
	copy(r.Fingerprint[:], b[120:152])
	ne := uint64(diskOrder.Uint32(b[152:]))
	nu := uint64(diskOrder.Uint32(b[156:]))
	if uint64(len(b)) != recordHeader+36+ne*extentBytes+nu*arenaUpdateBytes {
		return r, ErrCorrupt
	}
	r.Deleted = b[160] != 0
	r.Replace = b[161] != 0
	r.Previous = diskOrder.Uint64(b[176:])
	r.Arena.Base = diskOrder.Uint64(b[164:])
	r.Arena.Pages = diskOrder.Uint32(b[172:])
	at := recordHeader
	for _, p := range []*uint64{&r.Arena.Used, &r.Result.Offset, &r.Result.Info.Version, &r.Result.Info.Length} {
		*p = diskOrder.Uint64(b[at:])
		at += 8
	}
	r.Result.Info.Compression = r.Codec
	r.Result.Info.Deleted = r.Deleted
	for i := uint64(0); i < ne; i++ {
		e := dataExtent{Offset: diskOrder.Uint64(b[at:]), Length: diskOrder.Uint64(b[at+8:]), Skip: diskOrder.Uint32(b[at+16:]), Codec: Compression(b[at+20]), Arena: b[at+21] != 0, Location: diskOrder.Uint64(b[at+24:]), ArenaOffset: diskOrder.Uint64(b[at+32:]), Stored: diskOrder.Uint32(b[at+40:]), Raw: diskOrder.Uint32(b[at+44:]), StoredCRC: diskOrder.Uint32(b[at+48:]), RawCRC: diskOrder.Uint32(b[at+52:])}
		if b[at+21] > 1 || !e.Codec.valid() || e.Length == 0 || e.Raw > BlockSize || e.Stored == 0 || e.Stored > 2*BlockSize || uint64(e.Skip)+e.Length > uint64(e.Raw) || e.Offset > r.Length || e.Length > r.Length-e.Offset {
			return r, ErrCorrupt
		}
		r.Extents = append(r.Extents, e)
		at += extentBytes
	}
	for i := uint64(0); i < nu; i++ {
		u := arenaUpdate{Location: diskOrder.Uint64(b[at:]), Slot: b[at+8]}
		if u.Slot > 1 {
			return r, ErrCorrupt
		}
		r.Updates = append(r.Updates, u)
		at += arenaUpdateBytes
	}
	return r, nil
}

func (v *Volume) readMetaPage(s Superblock, index uint64) (cowPage, uint8, error) {
	var best cowPage
	var slot uint8
	found := false
	for i := uint8(0); i < 2; i++ {
		if index == s.metaPages()-1 && i != s.TailSlot {
			continue
		}
		b := make([]byte, PageSize)
		if e := readFullAt(v.dev, b, s.bankStart()+index*2*PageSize+uint64(i)*PageSize); e != nil {
			return best, 0, e
		}
		p, e := decodePage(b, "JPG1", s.BankEpoch, index, s.Identity.VolumeID)
		if e != nil || p.Generation > s.Generation {
			continue
		}
		if !found || p.Generation > best.Generation || (p.Generation == best.Generation && p.Used > best.Used) {
			best = p
			slot = i
			found = true
		}
	}
	if !found {
		return best, 0, ErrCorrupt
	}
	return best, slot, nil
}
func (v *Volume) appendJournal(s *Superblock, b []byte) error {
	end, e := add(s.JournalBytes, uint64(len(b)))
	if e != nil {
		return e
	}
	pages := (end + pagePayload - 1) / pagePayload
	if pages*2*PageSize > s.DataTop {
		return ErrFull
	}
	for len(b) > 0 {
		index := s.JournalBytes / pagePayload
		at := int(s.JournalBytes % pagePayload)
		data := make([]byte, pagePayload)
		slot := uint8(0)
		if at != 0 {
			p, old, e := v.readMetaPage(*s, index)
			if e != nil {
				return e
			}
			copy(data, p.Data)
			slot = 1 - old
			if p.Generation > v.sb.Generation {
				slot = old
			}
		}
		n := min(len(b), pagePayload-at)
		copy(data[at:], b[:n])
		p := cowPage{VolumeID: s.Identity.VolumeID, Epoch: s.BankEpoch, Index: index, Generation: s.Generation + 1, Used: uint32(at + n), Data: data}
		if e := v.write(encodePage("JPG1", p), s.bankStart()+index*2*PageSize+uint64(slot)*PageSize); e != nil {
			return e
		}
		s.JournalBytes += uint64(n)
		s.TailSlot = slot
		b = b[n:]
	}
	s.Generation++
	return nil
}
func loadSuperblocks(dev VolumeIO) (Superblock, error) {
	var out Superblock
	var errs [2]error
	found := false
	for i := 0; i < 2; i++ {
		b := make([]byte, PageSize)
		e := readFullAt(dev, b, uint64(i*PageSize))
		var s Superblock
		if e == nil {
			s, e = DecodeSuperblock(b)
		}
		errs[i] = e
		if e == nil && (!found || s.Generation > out.Generation) {
			s.rootSlot = uint8(i)
			out = s
			found = true
		}
	}
	for _, e := range errs {
		if e == ErrVersion {
			return out, e
		}
	}
	if found {
		return out, nil
	}
	if errs[0] == ErrUnformatted && errs[1] == ErrUnformatted {
		return out, ErrUnformatted
	}
	return out, fmt.Errorf("%w: no valid superblock (%v, %v)", ErrCorrupt, errs[0], errs[1])
}

// ReadHeader validates the newest superblock without replaying the journal.
func ReadHeader(dev VolumeIO) (Superblock, error) { return loadSuperblocks(dev) }
