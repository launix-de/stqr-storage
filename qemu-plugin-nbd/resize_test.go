// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestResizePreservesDataAndSparseExtension(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "grow", Size: StripeSize + 512})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x7b}, 1024)
	if err = c.WriteAt(ctx, m, data, StripeSize-512); err != nil {
		t.Fatal(err)
	}
	objects := len(s.objects)
	grown, err := c.Resize(ctx, m, 3*StripeSize)
	if err != nil {
		t.Fatal(err)
	}
	want := m
	want.Size = 3 * StripeSize
	if grown != want || len(s.objects) != objects || s.syncs != 1 {
		t.Fatal("growth changed identity, allocated stripes or omitted flush", grown, len(s.objects), s.syncs)
	}
	got := make([]byte, 2048)
	if err = c.ReadAt(ctx, grown, got, StripeSize-512); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:1024], data) || !bytes.Equal(got[1024:], make([]byte, 1024)) {
		t.Fatal("old data or sparse tail damaged")
	}
	if err = c.ReadAt(ctx, m, make([]byte, 512), m.Size); !errors.Is(err, client.ErrRange) {
		t.Fatal("old handle gained unnegotiated capacity", err)
	}
	if err = c.WriteAt(ctx, grown, data[:512], grown.Size-512); err != nil {
		t.Fatal(err)
	}
	// A new catalog has no local size cache and sees the persisted descriptor.
	reopened, err := NewCatalog(s).Lookup(ctx, m.Name)
	if err != nil || reopened != grown {
		t.Fatal(reopened, err)
	}
	if removed, err := c.Collect(ctx, nil); err != nil || removed != 0 {
		t.Fatal("GC discarded grown stripes", removed, err)
	}
}

func TestResizeRejectsShrinkInvalidSizesAndStaleGeneration(t *testing.T) {
	ctx := context.Background()
	s := newTestStore()
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "disk", Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []uint64{0, 512, 4097, math.MaxUint64, uint64(math.MaxInt64) + 1} {
		if _, err = c.Resize(ctx, m, size); !errors.Is(err, client.ErrRange) {
			t.Fatal(size, err)
		}
	}
	stale := m
	stale.ID = client.NewRequestID()
	if _, err = c.Resize(ctx, stale, 8192); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal(err)
	}
	actual, err := c.Lookup(ctx, m.Name)
	if err != nil || actual != m || s.syncs != 0 {
		t.Fatal(actual, err, s.syncs)
	}
}

type resizeFailureStore struct {
	*testStore
	conflict bool
	syncErr  error
}

func (s *resizeFailureStore) CAS(ctx context.Context, id client.ObjectID, off uint64, old, data []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	if s.conflict && !opt.Absent {
		return client.ObjectInfo{}, client.ErrPrecondition
	}
	return s.testStore.CAS(ctx, id, off, old, data, opt)
}
func (s *resizeFailureStore) Sync(ctx context.Context) error {
	if s.syncErr != nil {
		return s.syncErr
	}
	return s.testStore.Sync(ctx)
}

func TestResizeConflictAndUnknownFlushOutcome(t *testing.T) {
	ctx := context.Background()
	s := &resizeFailureStore{testStore: newTestStore()}
	c := NewCatalog(s)
	m, err := c.Create(ctx, Metadata{Name: "disk", Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	s.conflict = true
	if _, err = c.Resize(ctx, m, 8192); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal(err)
	}
	actual, _ := c.Lookup(ctx, m.Name)
	if actual.Size != m.Size {
		t.Fatal("CAS conflict changed descriptor")
	}
	s.conflict = false
	s.syncErr = errors.New("lost flush response")
	if _, err = c.Resize(ctx, m, 8192); !errors.Is(err, s.syncErr) {
		t.Fatal(err)
	}
	s.syncErr = nil
	if actual, err = c.Resize(ctx, m, 8192); err != nil || actual.Size != 8192 || s.syncs != 1 {
		t.Fatal("same-size retry did not flush", actual, err, s.syncs)
	}
}

func TestResizeControlPrivateSocketAndReadonly(t *testing.T) {
	ctx := context.Background()
	for _, ro := range []bool{false, true} {
		t.Run(map[bool]string{false: "writable", true: "readonly"}[ro], func(t *testing.T) {
			c := NewCatalog(newTestStore())
			m, err := c.Create(ctx, Metadata{Name: "disk", Size: 4096})
			if err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(t.TempDir(), "nbd")
			stop, err := serveResizeControl(socket+".control", c, ro)
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			info, err := os.Stat(socket + ".control")
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal(info, err)
			}
			grown, err := ResizeExport(ctx, socket, m.Name, 8192)
			if ro {
				if err == nil {
					t.Fatal("readonly exporter grew disk")
				}
				actual, _ := c.Lookup(ctx, m.Name)
				if actual != m {
					t.Fatal(actual)
				}
			} else if err != nil || grown.Size != 8192 || grown.ID != m.ID {
				t.Fatal(grown, err)
			}
		})
	}
}

func TestResizeControlRejectsMalformedRequests(t *testing.T) {
	c := NewCatalog(newTestStore())
	for _, body := range []string{`{`, `{"name":"disk","size":8192,"extra":1}`, `{} {}`, string(bytes.Repeat([]byte("x"), 2049))} {
		w := httptest.NewRecorder()
		resizeHandler(c, false).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/resize", bytes.NewBufferString(body)))
		if w.Code != http.StatusBadRequest {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRunningExporterResizeKeepsNegotiatedBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := NewCatalog(newTestStore())
	m, err := c.Create(ctx, Metadata{Name: "disk", Size: 4096})
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(t.TempDir(), "nbd")
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, socket, c, Options{DefaultExport: m.Name}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("exporter did not stop")
		}
		if _, err := os.Stat(socket + ".control"); !errors.Is(err, os.ErrNotExist) {
			t.Error("control socket was not removed", err)
		}
	})
	connect := func() (net.Conn, uint64) {
		t.Helper()
		var conn net.Conn
		var err error
		deadline := time.Now().Add(5 * time.Second)
		for {
			conn, err = net.Dial("unix", socket)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		hello := make([]byte, 18)
		if _, err = io.ReadFull(conn, hello); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		if err = send(conn, uint32(3), nbdOptionMagic, uint32(1), uint32(0)); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		export := make([]byte, 10)
		if _, err = io.ReadFull(conn, export); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		return conn, binary.BigEndian.Uint64(export)
	}
	old, size := connect()
	defer old.Close()
	if size != m.Size {
		t.Fatal(size)
	}
	if _, err = ResizeExport(ctx, socket, m.Name, 8192); err != nil {
		t.Fatal(err)
	}
	if err = send(old, uint32(0x25609513), uint16(0), uint16(0), uint64(1), uint64(4096), uint32(512)); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 16)
	if _, err = io.ReadFull(old, reply); err != nil {
		t.Fatal(err)
	}
	if code := binary.BigEndian.Uint32(reply[4:]); code != 22 {
		t.Fatal("existing session changed bounds", code)
	}
	old.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		busy := c.writers[m.ID]
		c.mu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disconnected writer was not released")
		}
		time.Sleep(time.Millisecond)
	}
	current, size := connect()
	defer current.Close()
	if size != 8192 {
		t.Fatal("new session did not see growth", size)
	}
}
