// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"github.com/launix-de/stqr-storage/volume"
)

type delayedReadClient struct {
	*client.Client
	delay time.Duration
}

func (s delayedReadClient) pause(ctx context.Context) error {
	if s.delay == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(s.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s delayedReadClient) Get(ctx context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	if e := s.pause(ctx); e != nil {
		return nil, client.ObjectInfo{}, e
	}
	return s.Client.Get(ctx, id)
}
func (s delayedReadClient) GetRange(ctx context.Context, id client.ObjectID, off, n uint64) ([]byte, client.ObjectInfo, error) {
	if e := s.pause(ctx); e != nil {
		return nil, client.ObjectInfo{}, e
	}
	return s.Client.GetRange(ctx, id, off, n)
}

func nativeReadBenchmarkClient(b testing.TB, format string) *client.Client {
	b.Helper()
	path := filepath.Join(b.TempDir(), "node.yaml")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	opts := Options{ConfigPath: path, manualRanges: true, values: map[string]string{"listen": ln.Addr().String(), "advertise": "http://" + ln.Addr().String()}}
	s, err := Open(opts)
	if err != nil {
		ln.Close()
		b.Fatal(err)
	}
	h := &http.Server{Handler: s.Handler()}
	go h.Serve(ln)
	b.Cleanup(func() { h.Close(); s.Close() })
	if _, err = s.AddVolume(newID(), AddVolumeRequest{Name: "reads", Class: "ram", Source: 2, SizeBytes: 128 << 20}); err != nil {
		b.Fatal(err)
	}
	bid, _ := volume.NewID()
	draft, _ := NewBucket(bid, "readbench")
	draft.description.Policy.StorageStrategy = "ram"
	info, err := draft.Info(volume.ID{}, true)
	if err != nil {
		b.Fatal(err)
	}
	raw, err := info.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	account, err := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if err != nil {
		b.Fatal(err)
	}
	reply, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: client.BucketCreate, ID: client.NewRequestID(), Body: raw[:len(raw)-4]}, nil, principal{User: account})
	if status != 201 {
		b.Fatalf("bucket create%d %x", status, reply)
	}
	c, err := client.Dial(context.Background(), "http://"+ln.Addr().String(), s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password, client.ID(bid), format)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { c.Close() })
	return c
}

// Real HTTP/WebSocket/placement/volume paths on an isolated RAM-backed node.
// The 1ms profile models independent object latency without a second VM or
// changing any live server; the zero-delay profile reports loopback throughput.
func BenchmarkNativeParallelRead(b *testing.B) {
	const size = 8 << 20
	for _, kind := range []string{"fs", "nbd"} {
		for _, delay := range []time.Duration{0, time.Millisecond} {
			b.Run(fmt.Sprintf("%s/%s", kind, delay), func(b *testing.B) {
				format := "fs"
				if kind == "nbd" {
					format = "bd"
				}
				c := nativeReadBenchmarkClient(b, format)
				ctx := context.Background()
				payload := bytes.Repeat([]byte{0x5a}, size)
				var read func([]byte) error
				if kind == "fs" {
					f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
					if err != nil {
						b.Fatal(err)
					}
					file, err := f.Create(ctx, client.FilesystemRootID(), "read", 0600, 0, 0, false)
					if err != nil {
						b.Fatal(err)
					}
					for off := 0; off < size; off += 256 << 10 {
						if _, err = f.WriteAt(ctx, file.ID, payload[off:off+(256<<10)], int64(off)); err != nil {
							b.Fatal(err)
						}
						if err = c.Sync(ctx); err != nil {
							b.Fatal(err)
						}
					}
					f, err = client.OpenFilesystem(ctx, delayedReadClient{c, delay}, client.FSOptions{ReadOnly: true})
					if err != nil {
						b.Fatal(err)
					}
					read = func(dst []byte) error {
						n, e := f.ReadAt(ctx, file.ID, dst, 0)
						if e == nil && n != len(dst) {
							return fmt.Errorf("short read%d", n)
						}
						return e
					}
				} else {
					catalog := nbd.NewCatalog(delayedReadClient{c, delay})
					disk, err := catalog.Create(ctx, nbd.Metadata{Name: "read", Size: size})
					if err != nil {
						b.Fatal(err)
					}
					for off := 0; off < size; off += 256 << 10 {
						if err = catalog.WriteAt(ctx, disk, payload[off:off+(256<<10)], uint64(off)); err != nil {
							b.Fatal(err)
						}
						if err = c.Sync(ctx); err != nil {
							b.Fatal(err)
						}
					}
					read = func(dst []byte) error { return catalog.ReadAt(ctx, disk, dst, 0) }
				}
				dst := make([]byte, size)
				if err := read(dst); err != nil || !bytes.Equal(dst, payload) {
					b.Fatal("warm read", err)
				}
				b.SetBytes(size)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := read(dst); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// This wrapper controls the first selected payload call at the native client
// boundary. All released calls still use the actual WebSocket/server/volume.
// TestParallelReadOwners separately holds the first response on the owner wire.
type heldNativeReadClient struct {
	*client.Client
	first                         atomic.Bool
	second                        sync.Once
	started, independent, release chan struct{}
	syncs                         atomic.Int32
}

func (s *heldNativeReadClient) Get(ctx context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	if s.first.Load() {
		s.second.Do(func() { close(s.independent) })
	}
	return s.Client.Get(ctx, id)
}
func (s *heldNativeReadClient) GetRange(ctx context.Context, id client.ObjectID, off, n uint64) ([]byte, client.ObjectInfo, error) {
	if s.first.CompareAndSwap(false, true) {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, client.ObjectInfo{}, ctx.Err()
		}
	} else {
		s.second.Do(func() { close(s.independent) })
	}
	return s.Client.GetRange(ctx, id, off, n)
}
func (s *heldNativeReadClient) Sync(ctx context.Context) error {
	s.syncs.Add(1)
	return s.Client.Sync(ctx)
}

func TestNativeParallelReadConsumers(t *testing.T) {
	for _, kind := range []string{"fs", "nbd"} {
		t.Run(kind, func(t *testing.T) {
			format := "fs"
			if kind == "nbd" {
				format = "bd"
			}
			c := nativeReadBenchmarkClient(t, format)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			payload := make([]byte, 3<<20)
			for i := range payload {
				payload[i] = byte(i / 4096)
			}
			held := &heldNativeReadClient{Client: c, started: make(chan struct{}), independent: make(chan struct{}), release: make(chan struct{})}
			var unblocked sync.Once
			unblock := func() { unblocked.Do(func() { close(held.release) }) }
			defer unblock()
			var read func([]byte) (int, error)
			if kind == "fs" {
				f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true})
				if err != nil {
					t.Fatal(err)
				}
				file, err := f.Create(ctx, client.FilesystemRootID(), "read", 0600, 0, 0, false)
				if err != nil {
					t.Fatal(err)
				}
				// Bound setup writeback exactly as the benchmark fixture does;
				// persistence is explicitly requested before the read oracle starts.
				for off := 0; off < len(payload); off += 256 << 10 {
					if _, err = f.WriteAt(ctx, file.ID, payload[off:off+(256<<10)], int64(off)); err != nil {
						t.Fatal(err)
					}
					if err = c.Sync(ctx); err != nil {
						t.Fatal(err)
					}
				}
				f, err = client.OpenFilesystem(ctx, held, client.FSOptions{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				read = func(dst []byte) (int, error) { return f.ReadAt(ctx, file.ID, dst, 7) }
			} else {
				catalog := nbd.NewCatalog(c)
				disk, err := catalog.Create(ctx, nbd.Metadata{Name: "read", Size: uint64(len(payload))})
				if err != nil {
					t.Fatal(err)
				}
				if err = catalog.WriteAt(ctx, disk, payload, 0); err != nil {
					t.Fatal(err)
				}
				if err = c.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				catalog = nbd.NewCatalog(held)
				read = func(dst []byte) (int, error) { err := catalog.ReadAt(ctx, disk, dst, 7); return len(dst), err }
			}
			backing := bytes.Repeat([]byte{0xa5}, (2<<20)+29)
			dst := backing[8 : len(backing)-8]
			done := make(chan error, 1)
			go func() {
				n, err := read(dst)
				if err == nil && n != len(dst) {
					err = fmt.Errorf("short read %d", n)
				}
				done <- err
			}()
			select {
			case <-held.started:
			case <-ctx.Done():
				t.Fatal("first selected native call missing")
			}
			select {
			case <-held.independent:
			case <-ctx.Done():
				t.Fatal("independent native read serialized")
			}
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("native read did not join")
			}
			if !bytes.Equal(dst, payload[7:7+len(dst)]) || !bytes.Equal(backing[:8], bytes.Repeat([]byte{0xa5}, 8)) || !bytes.Equal(backing[len(backing)-8:], bytes.Repeat([]byte{0xa5}, 8)) {
				t.Fatal("native nonzero-offset assembly or destination canary")
			}
			if held.syncs.Load() != 0 {
				t.Fatal("read sent an implicit durability fence")
			}
		})
	}
}
