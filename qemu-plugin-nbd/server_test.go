// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestNBDBoundsReadonlyAndFlush(t *testing.T) {
	for _, ro := range []bool{false, true} {
		t.Run(map[bool]string{true: "readonly", false: "writable"}[ro], func(t *testing.T) {
			ctx := context.Background()
			catalog := NewCatalog(newTestStore())
			_, e := catalog.Create(ctx, Metadata{Name: "disk", Size: 4096})
			if e != nil {
				t.Fatal(e)
			}
			a, b := net.Pipe()
			defer b.Close()
			b.SetDeadline(time.Now().Add(5 * time.Second))
			done := make(chan error, 1)
			go func() {
				defer a.Close()
				done <- nbdSession(ctx, a, catalog, Options{DefaultExport: "disk", ReadOnly: ro})
			}()
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

func TestNBDExportListAndUnknownName(t *testing.T) {
	ctx := context.Background()
	catalog := NewCatalog(newTestStore())
	for _, name := range []string{"system", "data"} {
		if _, e := catalog.Create(ctx, Metadata{Name: name, Size: 8192}); e != nil {
			t.Fatal(e)
		}
	}
	a, b := net.Pipe()
	defer b.Close()
	b.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() { defer a.Close(); done <- nbdSession(ctx, a, catalog, Options{}) }()
	hello := make([]byte, 18)
	if _, e := io.ReadFull(b, hello); e != nil {
		t.Fatal(e)
	}
	if e := send(b, uint32(3), nbdOptionMagic, uint32(3), uint32(0)); e != nil {
		t.Fatal(e)
	}
	readReply := func() (uint32, []byte) {
		t.Helper()
		header := make([]byte, 20)
		if _, e := io.ReadFull(b, header); e != nil {
			t.Fatal(e)
		}
		if binary.BigEndian.Uint64(header) != nbdReplyMagic {
			t.Fatal("reply magic")
		}
		body := make([]byte, binary.BigEndian.Uint32(header[16:]))
		if _, e := io.ReadFull(b, body); e != nil {
			t.Fatal(e)
		}
		return binary.BigEndian.Uint32(header[12:]), body
	}
	names := map[string]bool{}
	for {
		kind, body := readReply()
		if kind == 1 {
			break
		}
		if kind != 2 || len(body) < 4 {
			t.Fatal("invalid list entry")
		}
		n := binary.BigEndian.Uint32(body)
		names[string(body[4:4+n])] = true
	}
	if len(names) != 2 || !names["system"] || !names["data"] {
		t.Fatal(names)
	}
	body := binary.BigEndian.AppendUint32(nil, 7)
	body = append(body, []byte("missing")...)
	body = binary.BigEndian.AppendUint16(body, 0)
	if e := send(b, nbdOptionMagic, uint32(7), uint32(len(body)), body); e != nil {
		t.Fatal(e)
	}
	if kind, _ := readReply(); kind != 0x80000006 {
		t.Fatalf("unknown export reply: %x", kind)
	}
	if e := send(b, nbdOptionMagic, uint32(1), uint32(4), []byte("data")); e != nil {
		t.Fatal(e)
	}
	info := make([]byte, 10)
	if _, e := io.ReadFull(b, info); e != nil {
		t.Fatal(e)
	}
	if binary.BigEndian.Uint64(info) != 8192 {
		t.Fatal("named geometry")
	}
	b.Close()
	<-done
}
