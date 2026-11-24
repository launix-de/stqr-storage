// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"io"
)

const rangeEncodedDescriptorSize = 37

func encodeRangeEncoded(e *client.Encoder, c volume.EncodedChunk) {
	e.U64(c.Offset)
	e.U64(c.Length)
	e.U64(c.Skip)
	e.U32(c.DecodedLength)
	e.U8(uint8(c.Codec))
	e.U32(c.RawCRC)
	e.U32(c.EncodedCRC)
}
func decodeRangeEncoded(d *client.Decoder) volume.EncodedChunk {
	return volume.EncodedChunk{Offset: d.U64(), Length: d.U64(), Skip: d.U64(), DecodedLength: d.U32(), Codec: volume.Compression(d.U8()), RawCRC: d.U32(), EncodedCRC: d.U32()}
}
func (o *rangeIncomingObject) encodedSource() volume.EncodedSource {
	return func(yield func(volume.EncodedChunk) error) error {
		if _, err := o.extents.Seek(0, io.SeekStart); err != nil {
			return err
		}
		var at int64
		for {
			var descriptor [rangeEncodedDescriptorSize + 4]byte
			_, err := io.ReadFull(o.extents, descriptor[:])
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			d := client.Decoder{B: descriptor[:]}
			c := decodeRangeEncoded(&d)
			size := d.U32()
			if size == 0 || size > 1<<20 || d.Done() != nil {
				return client.ErrProtocol
			}
			c.Data = make([]byte, int(size))
			if _, err = o.payload.ReadAt(c.Data, at); err != nil {
				return err
			}
			at += int64(size)
			if err = yield(c); err != nil {
				return err
			}
		}
	}
}
