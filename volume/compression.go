// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"github.com/ulikunitz/xz"
	"io"
)

func compressBlock(c Compression, b []byte) ([]byte, error) {
	if len(b) > BlockSize || !c.valid() {
		return nil, ErrRange
	}
	if c == Raw {
		return append([]byte(nil), b...), nil
	}
	var out bytes.Buffer
	var w io.WriteCloser
	var e error
	switch c {
	case Gzip:
		w, e = gzip.NewWriterLevel(&out, gzip.BestSpeed)
	case LZ4:
		w = lz4.NewWriter(&out)
	case XZ:
		w, e = (xz.WriterConfig{DictCap: 1 << 20}).NewWriter(&out)
	}
	if e != nil {
		return nil, e
	}
	if _, e = w.Write(b); e != nil {
		return nil, e
	}
	if e = w.Close(); e != nil {
		return nil, e
	}
	return out.Bytes(), nil
}
func decompressBlock(c Compression, b []byte, size uint32) ([]byte, error) {
	if size > BlockSize || len(b) > BlockSize*2 || !c.valid() {
		return nil, ErrCorrupt
	}
	if c == Raw {
		if len(b) != int(size) {
			return nil, ErrCorrupt
		}
		return b, nil
	}
	var r io.Reader
	var closeFn func() error
	var e error
	switch c {
	case Gzip:
		var z *gzip.Reader
		z, e = gzip.NewReader(bytes.NewReader(b))
		if e == nil {
			r = z
			closeFn = z.Close
		}
	case LZ4:
		r = lz4.NewReader(bytes.NewReader(b))
	case XZ:
		r, e = (xz.ReaderConfig{DictCap: 1 << 20, SingleStream: true}).NewReader(bytes.NewReader(b))
	}
	if e != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, e)
	}
	if closeFn != nil {
		defer closeFn()
	}
	out, e := io.ReadAll(io.LimitReader(r, int64(size)+1))
	if e != nil || len(out) != int(size) {
		return nil, fmt.Errorf("%w: compressed block length/checksum", ErrCorrupt)
	}
	return out, nil
}
