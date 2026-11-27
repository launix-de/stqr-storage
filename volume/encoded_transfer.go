// SPDX-License-Identifier: GPL-3.0-only
package volume

// EncodedChunk is a portable compression chunk, without any backend addresses.
// Data is valid only during the source callback. Skip/Length select a logical
// slice of the decoded chunk; holes are represented by omitted intervals.
type EncodedChunk struct {
	Offset, Length, Skip uint64
	DecodedLength        uint32
	Codec                Compression
	RawCRC, EncodedCRC   uint32
	Data                 []byte
}
type EncodedSource func(yield func(EncodedChunk) error) error

// ExportEncoded preserves the stored representation, validating encoded CRCs
// without decompressing it. The callback must not reenter this handle/volume.
func (h *Handle) ExportEncoded(k Key, yield func(EncodedChunk) error) (Info, error) {
	return withHandle(h, false, func(v *Volume) (Info, error) {
		o, err := v.loadObject(k)
		if err != nil {
			return Info{}, err
		}
		for _, s := range o.Spans {
			if s.Codec == Raw && !s.Direct && (s.Skip > s.Stored || s.Length > s.Stored-s.Skip) {
				return Info{}, ErrCorrupt
			}
			emit := func(location, stored, decoded uint64, codec Compression, rawCRC, encodedCRC uint32, chunkStart uint64) error {
				start, stop := max(s.Skip, chunkStart), min(s.Skip+s.Length, chunkStart+decoded)
				if start >= stop {
					return nil
				}
				if stored == 0 || stored > BlockSize || decoded == 0 || decoded > BlockSize || !codec.valid() {
					return ErrCorrupt
				}
				data := make([]byte, int(stored))
				if e := v.readBytes(data, location); e != nil {
					return e
				}
				if checksum(data) != encodedCRC {
					return ErrCorrupt
				}
				return yield(EncodedChunk{Offset: s.Offset + start - s.Skip, Length: stop - start, Skip: start - chunkStart, DecodedLength: uint32(decoded), Codec: codec, RawCRC: rawCRC, EncodedCRC: encodedCRC, Data: data})
			}
			if s.Direct {
				if err = emit(s.Location, s.Stored, s.Checksums>>32, s.Codec, s.CRC, uint32(s.Checksums), 0); err != nil {
					return Info{}, err
				}
				continue
			}
			first, last := s.Skip/BlockSize, (s.Skip+s.Length-1)/BlockSize
			for i := first; i <= last; i++ {
				if s.Codec == Raw {
					n := min(uint64(BlockSize), s.Stored-i*BlockSize)
					crc := s.CRC
					if s.Checksums != 0 {
						var b [4]byte
						if err = v.readBytes(b[:], s.Checksums+i*4); err != nil {
							return Info{}, err
						}
						crc = diskOrder.Uint32(b[:])
					}
					err = emit(s.Location+i*BlockSize, n, n, Raw, crc, crc, i*BlockSize)
				} else {
					var b [32]byte
					if err = v.readBytes(b[:], s.Location+8+i*32); err != nil {
						return Info{}, err
					}
					off, n := diskOrder.Uint64(b[:]), uint64(diskOrder.Uint32(b[8:]))
					if off > s.Stored || n > s.Stored-off {
						return Info{}, ErrCorrupt
					}
					err = emit(s.Location+off, n, uint64(diskOrder.Uint32(b[12:])), Compression(b[24]), diskOrder.Uint32(b[16:]), diskOrder.Uint32(b[20:]), i*BlockSize)
				}
				if err != nil {
					return Info{}, err
				}
			}
		}
		return o.Info, nil
	})
}

// RestoreEncoded imports immutable, replayable encoded chunks. It reads source
// twice, allocates one contiguous payload region and never recompresses data.
// This trusted migration API requires ImportShadow and no request receipt.
func (h *Handle) RestoreEncoded(k Key, info Info, source EncodedSource, opts Options) error {
	return h.doAcceptedOptions(opts, func(v *Volume) error {
		if !opts.ImportShadow || opts.RequestID != (ID{}) || !info.Compression.valid() || info.Deleted && info.Length != 0 || opts.Compression != nil && *opts.Compression != info.Compression || source == nil {
			return ErrRange
		}
		if busy, err := v.pending(k); err != nil {
			return err
		} else if busy {
			return ErrBusy
		}
		old, err := v.loadObject(k)
		if err == ErrNotFound {
			old = nil
		} else if err != nil {
			return err
		}
		if err = v.precondition(old, opts); err != nil {
			return err
		}
		if old != nil && !old.importShadow {
			return ErrPrecondition
		}
		type descriptor struct {
			chunk  EncodedChunk
			stored int
		}
		var descriptors []descriptor
		var total, end uint64
		err = source(func(c EncodedChunk) error {
			if info.Deleted || c.Length == 0 || c.Offset < end || c.Offset > info.Length || c.Length > info.Length-c.Offset || c.DecodedLength == 0 || c.DecodedLength > BlockSize || c.Skip > uint64(c.DecodedLength) || c.Length > uint64(c.DecodedLength)-c.Skip || !c.Codec.valid() || c.Codec != Raw && c.Codec != info.Compression || len(c.Data) == 0 || len(c.Data) > BlockSize || c.Codec == Raw && (len(c.Data) != int(c.DecodedLength) || c.RawCRC != c.EncodedCRC) || checksum(c.Data) != c.EncodedCRC {
				return ErrCorrupt
			}
			total, err = add(total, uint64(len(c.Data)))
			if err != nil {
				return err
			}
			end = c.Offset + c.Length
			n := len(c.Data)
			c.Data = nil
			descriptors = append(descriptors, descriptor{c, n})
			return nil
		})
		if err != nil {
			return err
		}
		t := v.transaction()
		defer t.close()
		var at uint64
		if total > 0 {
			at, err = t.alloc(total, false)
			if err != nil {
				return err
			}
		}
		spans := make([]dataSpan, 0, len(descriptors))
		index := 0
		err = source(func(c EncodedChunk) error {
			if index >= len(descriptors) {
				return ErrCorrupt
			}
			record := descriptors[index]
			want := record.chunk
			index++
			if c.Offset != want.Offset || c.Length != want.Length || c.Skip != want.Skip || c.DecodedLength != want.DecodedLength || c.Codec != want.Codec || c.RawCRC != want.RawCRC || c.EncodedCRC != want.EncodedCRC || len(c.Data) != record.stored || checksum(c.Data) != c.EncodedCRC {
				return ErrCorrupt
			}
			if e := t.writeBytes(at, c.Data); e != nil {
				return e
			}
			n := uint64(len(c.Data))
			spans = append(spans, dataSpan{Offset: c.Offset, Length: c.Length, Skip: c.Skip, Location: at, Stored: n, Capacity: n, Codec: c.Codec, CRC: c.RawCRC, Checksums: uint64(c.DecodedLength)<<32 | uint64(c.EncodedCRC), Direct: true})
			at += n
			return nil
		})
		if err != nil {
			return err
		}
		if index != len(descriptors) {
			return ErrCorrupt
		}
		o := &object{Info: info, Spans: spans, importShadow: true, Segmented: false}
		if old != nil {
			o.Base, o.Size = old.Base, old.Size
			releaseData(t, old)
			if !old.Info.Deleted {
				t.sb.Objects--
				t.sb.LogicalBytes -= old.Info.Length
			}
		}
		if !info.Deleted {
			t.sb.Objects++
			t.sb.LogicalBytes, err = add(t.sb.LogicalBytes, info.Length)
			if err != nil {
				return err
			}
		}
		t.sb.ArenaUnused = arenaAfter(t.sb.ArenaUnused, old, o)
		if err = t.saveObject(k, o); err != nil {
			return err
		}
		if err = t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance); err != nil {
			return err
		}
		v.cache.remove(k)
		return nil
	})
}
