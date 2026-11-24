// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"io"
	"os"
)

// Compression and redo staging are bounded in RAM. Transient input spills
// to a 0600 temporary file; committed objects always live in the volume format.
type encodedSpool struct {
	noSpill bool // RAM-class payload must never enter a disk staging file
	memory  []byte
	file    *os.File
	size    int64
}

func (s *encodedSpool) Write(b []byte) (int, error) {
	if !s.noSpill && s.file == nil && s.size+int64(len(b)) > 4<<20 {
		f, e := os.CreateTemp("", "stqr-encode-*")
		if e != nil {
			return 0, e
		}
		s.file = f
		if _, e = f.Write(s.memory); e != nil {
			return 0, e
		}
		s.memory = nil
	}
	var n int
	var e error
	if s.file != nil {
		n, e = s.file.WriteAt(b, s.size)
	} else {
		s.memory = append(s.memory, b...)
		n = len(b)
	}
	s.size += int64(n)
	return n, e
}
func (s *encodedSpool) ReadAt(b []byte, off int64) (int, error) {
	if s.file != nil {
		return s.file.ReadAt(b, off)
	}
	return bytes.NewReader(s.memory).ReadAt(b, off)
}
func (s *encodedSpool) WriteAt(b []byte, off int64) (int, error) {
	if off < 0 || off+int64(len(b)) > s.size {
		return 0, ErrRange
	}
	if s.file != nil {
		return s.file.WriteAt(b, off)
	}
	return copy(s.memory[off:], b), nil
}
func (s *encodedSpool) Close() {
	if s.file != nil {
		name := s.file.Name()
		s.file.Close()
		os.Remove(name)
	}
}
func (t *transaction) storeCompressed(pieces []payloadPiece, codec Compression) ([]dataSpan, error) {
	spool := &encodedSpool{noSpill: t.sb.Identity.Class == RAM}
	defer spool.Close()
	buffer := make([]byte, BlockSize)
	var spans []dataSpan
	for _, p := range pieces {
		if p.length == 0 {
			continue
		}
		chunks := (p.length + BlockSize - 1) / BlockSize
		if chunks > uint64(^uint(0)>>1)/32 {
			return nil, ErrRange
		}
		table := make([]byte, 8+chunks*32)
		diskOrder.PutUint64(table, chunks)
		s := dataSpan{Offset: p.offset, Length: p.length, Location: uint64(spool.size), Codec: codec}
		if _, e := spool.Write(table); e != nil {
			return nil, e
		}
		for i := uint64(0); i < chunks; i++ {
			n := min(uint64(BlockSize), p.length-i*BlockSize)
			raw := buffer[:n]
			if _, e := io.ReadFull(p.src, raw); e != nil {
				return nil, e
			}
			encoded, e := compressBlock(codec, raw)
			if e != nil {
				return nil, e
			}
			actual := codec
			if len(encoded) >= len(raw) {
				actual = Raw
				encoded = raw
			}
			entry := table[8+i*32:]
			diskOrder.PutUint64(entry, uint64(spool.size)-s.Location)
			diskOrder.PutUint32(entry[8:], uint32(len(encoded)))
			diskOrder.PutUint32(entry[12:], uint32(n))
			diskOrder.PutUint32(entry[16:], checksum(raw))
			diskOrder.PutUint32(entry[20:], checksum(encoded))
			entry[24] = byte(actual)
			if _, e = spool.Write(encoded); e != nil {
				return nil, e
			}
		}
		s.Stored = uint64(spool.size) - s.Location
		s.Capacity = s.Stored
		if _, e := spool.WriteAt(table, int64(s.Location)); e != nil {
			return nil, e
		}
		spans = append(spans, s)
	}
	if spool.size == 0 {
		return nil, nil
	}
	base, e := t.alloc(uint64(spool.size), false)
	if e != nil {
		return nil, e
	}
	for off := int64(0); off < spool.size; {
		n := min(int64(len(buffer)), spool.size-off)
		if _, e = spool.ReadAt(buffer[:n], off); e != nil {
			return nil, e
		}
		if e = t.writeBytes(base+uint64(off), buffer[:n]); e != nil {
			return nil, e
		}
		off += n
	}
	for i := range spans {
		spans[i].Location += base
	}
	return spans, nil
}
