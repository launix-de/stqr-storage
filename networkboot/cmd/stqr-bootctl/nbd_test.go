// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/binary"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"net"
	"testing"
	"time"
)

func TestNBDBoundsReadonlyAndFlush(t *testing.T) {
	for _, ro := range []bool{false, true} {
		t.Run(map[bool]string{true: "readonly", false: "writable"}[ro], func(t *testing.T) {
			ctx := context.Background()
			fs, e := client.OpenFilesystem(ctx, client.NewMemoryFilesystemStore(), client.FSOptions{Create: true})
			if e != nil {
				t.Fatal(e)
			}
			disk, e := fs.Create(ctx, client.FilesystemRootID(), "disk", 0600, 0, 0, false)
			if e != nil {
				t.Fatal(e)
			}
			disk, e = fs.Truncate(ctx, disk.ID, 4096)
			if e != nil {
				t.Fatal(e)
			}
			a, b := net.Pipe()
			defer b.Close()
			b.SetDeadline(time.Now().Add(5 * time.Second))
			done := make(chan error, 1)
			go func() { defer a.Close(); done <- nbdSession(ctx, a, fs, disk, ro) }()
			hello := make([]byte, 18)
			if _, e = io.ReadFull(b, hello); e != nil {
				t.Fatal(e)
			}
			if e = send(b, uint32(3), nbdOptionMagic, uint32(1), uint32(0)); e != nil {
				t.Fatal(e)
			}
			export := make([]byte, 10)
			if _, e = io.ReadFull(b, export); e != nil {
				t.Fatal(e)
			}
			transact := func(cmd, flags uint16, off uint64, payload []byte) uint32 {
				t.Helper()
				if e := send(b, uint32(0x25609513), flags, cmd, uint64(7), off, uint32(len(payload))); e != nil {
					t.Fatal(e)
				}
				if cmd == 1 {
					if _, e = b.Write(payload); e != nil {
						t.Fatal(e)
					}
				}
				reply := make([]byte, 16)
				if _, e = io.ReadFull(b, reply); e != nil {
					t.Fatal(e)
				}
				return binary.BigEndian.Uint32(reply[4:])
			}
			want := uint32(0)
			if ro {
				want = 30
			}
			if got := transact(1, 1, 0, make([]byte, 512)); got != want {
				t.Fatal(got, want)
			}
			if got := transact(1, 0, 4096, make([]byte, 512)); got != 22 {
				t.Fatal("out of bounds", got)
			}
			if got := transact(3, 0, 0, nil); got != 0 {
				t.Fatal("flush", got)
			}
			b.Close()
			<-done
		})
	}
}
