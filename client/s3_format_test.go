// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestS3BinaryHeaderABIAndInvalidRecords(t *testing.T) {
	var id ObjectID
	for i := range id {
		id[i] = byte(i + 1)
	}
	o := S3Object{Schema: 1, Key: "123:", Payload: id, Size: 0x1122334455667788, Modified: time.Unix(0, 123456789), ETag: "00112233445566778899aabbccddeeff", Metadata: map[string]string{"owner": "fop"}, Headers: map[string]string{"Content-Type": "text/plain"}}
	raw, err := encodeS3Object(o)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:4]) != "STS3" || S3HeaderSize != 80 || S3FlagsOffset != 8 || binary.LittleEndian.Uint32(raw[8:12]) != 0 || !bytes.Equal(raw[12:44], id[:]) || binary.LittleEndian.Uint64(raw[44:52]) != o.Size || binary.LittleEndian.Uint64(raw[52:60]) != 123456789 || !bytes.Equal(raw[60:76], []byte{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255}) {
		t.Fatal("on-disk header ABI changed")
	}
	decoded, err := decodeS3Object(raw)
	if err != nil || decoded.Key != o.Key || decoded.Metadata["owner"] != "fop" || decoded.Headers["Content-Type"] != "text/plain" {
		t.Fatal(decoded, err)
	}
	for _, off := range []int{0, 4, 6, 8} {
		bad := append([]byte(nil), raw...)
		bad[off] = 255
		if _, err := decodeS3Object(bad); err == nil {
			t.Fatal("accepted corrupt header", off)
		}
	}
	for n := 0; n < len(raw); n++ {
		if _, err := decodeS3Object(raw[:n]); err == nil {
			t.Fatal("accepted truncation", n)
		}
	}
	if _, err := decodeS3Object(append(raw, 0)); err != nil {
		t.Fatal("payload byte treated as metadata", err)
	}
}
