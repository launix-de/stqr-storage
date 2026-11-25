// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"github.com/launix-de/stqr-storage/volume"
)

func TestStorageSafetyCASPermissions(t *testing.T) {
	for _, perm := range []BucketPermissions{BucketRead, BucketWrite, BucketRead | BucketWrite} {
		s, b, user := s3TestSetup(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		id := volume.ObjectID{42}
		if _, err := b.Put(ctx, userPrincipal(t, s), id, []byte("secret"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
			t.Fatal(err)
		}
		d := b.Describe()
		d.Grants = []BucketGrant{{UserID: user.ID, Permissions: perm}}
		if _, err := s.buckets.Save(d, d.Revision); err != nil {
			t.Fatal(err)
		}
		host := httptest.NewServer(s.Handler())
		defer host.Close()
		c, err := client.Dial(ctx, host.URL, user.Name, "bucket-test-password", client.ID(b.ID()), "s3")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		for _, guess := range []string{"xxxxxx", "secret"} {
			_, direct := b.CAS(ctx, principal{User: user}, id, 0, []byte(guess), []byte("secret"), volume.Options{RequestID: volume.ID(client.NewRequestID())})
			_, wire := c.CAS(ctx, client.ObjectID(id), 0, []byte(guess), []byte("secret"), client.CASOptions{})
			if perm != BucketRead|BucketWrite {
				if !errors.Is(direct, ErrBucketForbidden) || !errors.Is(wire, client.ErrUnauthorized) {
					t.Fatalf("permissions %d: direct=%v socket=%v", perm, direct, wire)
				}
			} else if guess == "xxxxxx" {
				if !errors.Is(direct, volume.ErrPrecondition) || !errors.Is(wire, client.ErrPrecondition) {
					t.Fatal(direct, wire)
				}
			} else if direct != nil || wire != nil {
				t.Fatal(direct, wire)
			}
		}
	}
}
func TestStorageSafetyRAMWithoutPlacement(t *testing.T) {
	n := startDaemonTestNode(t, filepath.Join(t.TempDir(), "node.yaml"), nil)
	s := n.node
	if _, err := s.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: 4 << 20}); err != nil {
		t.Fatal(err)
	}
	draft := newTestBucket(t, "ram-placement-unavailable")
	draft.description.Policy.StorageStrategy = "ram"
	if _, status, _ := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft)); status != 201 {
		t.Fatal(status)
	}
	admin, err := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if err != nil {
		t.Fatal(err)
	}
	before := s.placement.Load()
	empty, err := buildPlacement(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.placement.Store(empty)
	defer s.placement.Store(before)
	for _, opcode := range []uint16{client.BucketObjectList, 0x100, 0x107} {
		e := client.Encoder{}
		e.ID(client.ID(draft.ID()))
		e.B = append(e.B, make([]byte, 32)...)
		e.U64(0)
		e.U64(0)
		e.ID(client.ID{})
		if opcode == client.BucketObjectList {
			e.Bool(false)
			e.U32(32)
		} else if opcode == 0x100 {
			e.Bool(false)
		}
		_, _, err := s.executeObjects(client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: e.B}, nil, principal{User: admin})
		if err == nil || objectStatus(err) != 503 {
			t.Fatalf("opcode %x: %v", opcode, err)
		}
	}
}
func TestStorageSafetySparseNBDTail(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "sparse", "hdd", 32<<20)
	b, _ := createProtocolBucket(t, s, "sparse")
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	disk, err := nbd.Open(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), false)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	m, err := disk.Create(ctx, nbd.Metadata{Name: "sparse", Size: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	written := bytes.Repeat([]byte{1}, 512)
	if err = disk.WriteAt(ctx, m, written, 0); err != nil {
		t.Fatal(err)
	}
	for _, off := range []uint64{0, 256, 512, 4096, (1 << 20) - 256, 1 << 20, (2 << 20) - 512} {
		got := bytes.Repeat([]byte{99}, 512)
		want := make([]byte, 512)
		if off < 512 {
			copy(want, written[off:])
		}
		if err = disk.ReadAt(ctx, m, got, off); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("offset %d: %v, %x", off, err, got)
		}
	}
	if err = disk.ReadAt(ctx, m, make([]byte, 512), m.Size); !errors.Is(err, client.ErrRange) {
		t.Fatal("outside disk", err)
	}
	// The raw object RPC still rejects the stripe tail; only BD fills holes.
	_, _, err = disk.Client.GetRange(ctx, nbd.StripeID(m.ID, 0), 4096, 512)
	var status *client.StatusError
	if !errors.As(err, &status) || status.Status != 400 {
		t.Fatal("server must reject object tail", err)
	}
}

type safetyBlockedIO struct {
	entered chan struct{}
	release chan struct{}
	data    []byte
}

func (b *safetyBlockedIO) Read(p []byte) (int, error) {
	close(b.entered)
	<-b.release
	return copy(p, b.data), io.EOF
}
func (b *safetyBlockedIO) Write(p []byte) (int, error) {
	close(b.entered)
	<-b.release
	return len(p), nil
}
func TestStorageSafetyClientIOOutsideVolumeLock(t *testing.T) {
	for _, direction := range []string{"upload", "download"} {
		t.Run(direction, func(t *testing.T) {
			s, b, _ := s3TestSetup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			actor := userPrincipal(t, s)
			id := volume.ObjectID{41}
			data := []byte("data")
			if _, err := b.Put(ctx, actor, id, data, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
				t.Fatal(err)
			}
			blocked := &safetyBlockedIO{entered: make(chan struct{}), release: make(chan struct{}), data: data}
			done := make(chan error, 1)
			go func() {
				var err error
				if direction == "upload" {
					_, err = b.PutReader(ctx, actor, id, blocked, 4, volume.Options{RequestID: volume.ID(client.NewRequestID())})
				} else {
					_, _, err = b.CopyTo(ctx, actor, id, 0, 4, blocked)
				}
				done <- err
			}()
			defer func() {
				close(blocked.release)
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-blocked.entered:
			case <-ctx.Done():
				t.Fatal("transfer never reached caller IO")
			}
			progress := make(chan error, 1)
			go func() {
				_, err := b.Put(ctx, actor, volume.ObjectID{42}, []byte("other"), volume.Options{RequestID: volume.ID(client.NewRequestID())})
				progress <- err
			}()
			select {
			case err := <-progress:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("client-held IO blocked unrelated mutation")
			}
		})
	}
}

func TestStorageSafetyDeadlineIsRetryable(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	var dst bytes.Buffer
	_, _, err := b.CopyTo(ctx, userPrincipal(t, s), volume.ObjectID{42}, 0, 4, &dst)
	if !errors.Is(err, context.DeadlineExceeded) || objectStatus(err) != 503 || dst.Len() != 0 {
		t.Fatalf("expired local copy must request retry before output: %v, status %d", err, objectStatus(err))
	}
	if objectStatus(context.Canceled) != 499 {
		t.Fatal("explicit cancellation must remain distinct from retry")
	}
}
