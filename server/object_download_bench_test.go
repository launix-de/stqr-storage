// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

// Actual object RPCs on an isolated RAM-backed node. Setup/PUT are excluded;
// counts include framing, transfer checksum and snapshot ownership, not disk I/O.
func BenchmarkNativeObjectGet(b *testing.B) {
	for _, size := range []int{210, 4096, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			c := nativeReadBenchmarkClient(b, "opaque")
			ctx := context.Background()
			id := client.ObjectID{1}
			if _, err := c.Put(ctx, id, bytes.Repeat([]byte("p"), size), client.PutOptions{Accepted: true}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, _, err := c.Get(ctx, id)
				if err != nil || len(got) != size {
					b.Fatal(len(got), err)
				}
			}
		})
	}
}
