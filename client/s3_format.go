// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"
)

// STS3 v2: all integers little endian, no implicit padding. The 80-byte header
// is directly addressable by Go/C/Rust. Flags occupy bytes 8..11 (bit 0 deleted).
// Payload:12..43, size:44..51, mtime Unix ns:52..59, MD5 ETag:60..75.
// Metadata length:76..79. Text key and two sorted pair maps follow, then payload.
const S3HeaderSize = 80
const S3FlagsOffset = 8

func encodeS3Object(o S3Object) ([]byte, error) {
	e := Encoder{B: []byte("STS3")}
	version, count := uint16(2), uint16(0)
	etagText := o.ETag
	if text, n, ok := strings.Cut(o.ETag, "-"); ok {
		v, err := strconv.Atoi(n)
		if err != nil || v < 1 || v > 10000 {
			return nil, ErrS3Format
		}
		version = 3
		count = uint16(v)
		etagText = text
	}
	e.U16(version)
	e.U16(count)
	var flags uint32
	if o.Deleted {
		flags = 1
	}
	e.U32(flags)
	e.B = append(e.B, o.Payload[:]...)
	e.U64(o.Size)
	e.U64(uint64(o.Modified.UnixNano()))
	etag, err := hex.DecodeString(etagText)
	if err != nil || len(etag) != 16 {
		return nil, ErrS3Format
	}
	e.B = append(e.B, etag...)
	e.U32(0)
	e.Text(o.Key)
	for _, m := range []map[string]string{o.Headers, o.Metadata} {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		e.V(uint64(len(keys)))
		for _, k := range keys {
			e.Text(k)
			e.Text(m[k])
		}
	}
	if len(e.B) > 64<<10 {
		return nil, ErrRange
	}
	binary.LittleEndian.PutUint32(e.B[76:80], uint32(len(e.B)-S3HeaderSize))
	return e.B, nil
}
func validS3Header(b []byte) bool {
	return len(b) >= S3HeaderSize && string(b[:4]) == "STS3" && (binary.LittleEndian.Uint16(b[4:6]) == 2 && binary.LittleEndian.Uint16(b[6:8]) == 0 || binary.LittleEndian.Uint16(b[4:6]) == 3 && binary.LittleEndian.Uint16(b[6:8]) > 0 && binary.LittleEndian.Uint16(b[6:8]) <= 10000) && binary.LittleEndian.Uint32(b[8:12])&^uint32(1) == 0
}
func decodeS3Header(b []byte) (S3Object, error) {
	var o S3Object
	if !validS3Header(b) {
		return o, ErrS3Format
	}
	o.Schema = int(binary.LittleEndian.Uint16(b[4:6]))
	o.PayloadOffset = uint64(S3HeaderSize) + uint64(binary.LittleEndian.Uint32(b[76:80]))
	if o.PayloadOffset > 64<<10 {
		return o, ErrS3Format
	}
	o.Deleted = binary.LittleEndian.Uint32(b[8:12])&1 != 0
	copy(o.Payload[:], b[12:44])
	o.Size = binary.LittleEndian.Uint64(b[44:52])
	o.Modified = time.Unix(0, int64(binary.LittleEndian.Uint64(b[52:60]))).UTC()
	o.ETag = hex.EncodeToString(b[60:76])
	if o.Schema == 3 {
		o.ETag += "-" + strconv.Itoa(int(binary.LittleEndian.Uint16(b[6:8])))
	}
	if o.Payload == (ObjectID{}) {
		return o, ErrS3Format
	}
	return o, nil
}
func decodeS3Object(b []byte) (S3Object, error) {
	o, err := decodeS3Header(b)
	if err != nil || uint64(len(b)) < o.PayloadOffset {
		return o, ErrS3Format
	}
	d := Decoder{B: b[S3HeaderSize:o.PayloadOffset]}
	o.Key = d.Text()
	maps := []map[string]string{make(map[string]string), make(map[string]string)}
	for _, m := range maps {
		n := d.Count(4096)
		previous := ""
		for i := 0; i < n; i++ {
			k, v := d.Text(), d.Text()
			if k == "" || i > 0 && k <= previous {
				return o, ErrS3Format
			}
			m[k] = v
			previous = k
		}
	}
	if d.Done() != nil || !validS3Key(o.Key) || o.Payload == (ObjectID{}) {
		return o, ErrS3Format
	}
	o.Headers, o.Metadata = maps[0], maps[1]
	return o, nil
}
