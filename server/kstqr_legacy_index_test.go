// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

// Build the exact historical on-disk shape, independently of current encoders.
// No historical data version is needed for the ordinary local-data case.
func kernelLegacyIndexFixture(t *testing.T, ctx context.Context, c *client.Client, fs *client.Filesystem) {
	t.Helper()
	for _, oldToken := range []uint64{0, 123} {
		name := "legacy-local-index"
		if oldToken != 0 {
			name = "legacy-historical-index"
		}
		file, e := fs.Create(ctx, client.FilesystemRootID(), name, 0600, 0, 0, false)
		if e != nil {
			t.Fatal(e)
		}
		dataID, indexID := sha256.Sum256([]byte("legacy-data-"+name)), sha256.Sum256([]byte("legacy-index-"+name))
		data := append(bytes.Repeat([]byte{'P'}, 9), bytes.Repeat([]byte{'J'}, 16384)...)
		copy(data[9:], []byte("\xda\x1ajour"))
		index := client.Encoder{B: []byte("STIX")}
		index.U16(2)
		index.U32(2)
		index.U64(16384)
		index.B = append(index.B, dataID[:]...)
		index.U64(9)
		index.U64(oldToken)
		index.U64(16384)
		index.B = append(index.B, make([]byte, 32)...)
		index.U64(0)
		index.U64(0)
		node := client.Encoder{B: []byte("STFS")}
		node.U16(5)
		node.U8(1)
		node.U32(0600)
		node.U32(0)
		node.U32(0)
		node.U64(1)
		node.U64(32768)
		node.U8(1)
		node.U8(1)
		node.B = append(node.B, indexID[:]...)
		node.Bool(false)
		node.Text("")
		node.U64(1)
		node.U64(1)
		node.U32(1)
		node.U32(0)
		for id, b := range map[client.ObjectID][]byte{dataID: data, indexID: index.B, file.ID: node.B} {
			if _, e = c.Put(ctx, id, b, client.PutOptions{}); e != nil {
				t.Fatal(e)
			}
		}
	}
}
