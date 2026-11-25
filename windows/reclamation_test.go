// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type observedFilesystemStore struct {
	*client.MemoryFilesystemStore
	written map[client.ObjectID][]byte
}

type waitingFilesystemStore struct {
	*client.MemoryFilesystemStore
	mu      sync.Mutex
	id      client.ObjectID
	entered chan struct{}
	resume  chan struct{}
}

func (s *waitingFilesystemStore) Get(ctx context.Context, id client.ObjectID) ([]byte, client.ObjectInfo, error) {
	s.mu.Lock()
	entered, resume := s.entered, s.resume
	if id == s.id && entered != nil {
		s.entered = nil
		s.mu.Unlock()
		close(entered)
		select {
		case <-resume:
		case <-ctx.Done():
			return nil, client.ObjectInfo{}, ctx.Err()
		}
	} else {
		s.mu.Unlock()
	}
	return s.MemoryFilesystemStore.Get(ctx, id)
}

func TestCloseWaitsForInFlightUnlinkedRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &waitingFilesystemStore{MemoryFilesystemStore: client.NewMemoryFilesystemStore()}
	volume, err := client.OpenFilesystem(ctx, store, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	filesystem := New(ctx, volume, false)
	h, err := filesystem.Open("/closing", OpenOptions{Read: true, Write: true, Create: true}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := filesystem.Write(h, []byte("retained read"), 0); err != nil {
		t.Fatal(err)
	}
	info, err := filesystem.Stat("", h)
	if err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Remove("/closing", false); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	store.mu.Lock()
	store.id, store.entered, store.resume = info.ID, entered, resume
	store.mu.Unlock()
	read := make(chan error, 1)
	go func() {
		data := make([]byte, len("retained read"))
		n, err := filesystem.Read(h, data, 0)
		if err == nil && (n != len(data) || string(data) != "retained read") {
			err = errors.New("unlinked read lost its contents")
		}
		read <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("read never reached storage")
	}
	closed := make(chan error, 1)
	go func() { closed <- filesystem.Close(h) }()
	// Wait until Close has removed the handle, then prove it still waits for
	// the storage operation before releasing the handle's retained inode.
	for {
		if _, err := filesystem.get(h); errors.Is(err, syscall.EBADF) {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatal("close never removed handle")
		}
	}
	select {
	case err := <-closed:
		t.Fatalf("close finished while storage read was pending: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestLastWindowsCloseReclaimsUnlinkedInode(t *testing.T) {
	filesystem, store := fixture(t)
	first, err := filesystem.Open("/orphan", OpenOptions{Read: true, Write: true, Create: true}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("l"), 8192)
	if _, err := filesystem.Write(first, payload, 0); err != nil {
		t.Fatal(err)
	}
	info, err := filesystem.Stat("", first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := filesystem.Open("/orphan", OpenOptions{Read: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Remove("/orphan", false); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Close(first); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if n, err := filesystem.Read(second, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatalf("first close reclaimed second handle's data: %d %v", n, err)
	}
	if err := filesystem.Close(second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(context.Background(), info.ID); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("last close retained unlinked inode: %v", err)
	}
	if _, err := filesystem.Lookup("/"); err != nil {
		t.Fatal("last close damaged linked root", err)
	}
}

func TestReadonlyWindowsHandlePreservesUnlinkedInodeUntilEligibleCleanup(t *testing.T) {
	filesystem, store := fixture(t)
	w, err := filesystem.Open("/readonly-open", OpenOptions{Read: true, Write: true, Create: true}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := filesystem.Write(w, []byte("read-only lifetime"), 0); err != nil {
		t.Fatal(err)
	}
	info, err := filesystem.Stat("", w)
	if err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Close(w); err != nil {
		t.Fatal(err)
	}
	volume, err := client.OpenFilesystem(context.Background(), store, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	readonly := New(context.Background(), volume, true)
	r, err := readonly.Open("/readonly-open", OpenOptions{Read: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := filesystem.Remove("/readonly-open", false); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("read-only lifetime"))
	if n, err := readonly.Read(r, got, 0); err != nil || n != len(got) || string(got) != "read-only lifetime" {
		t.Fatalf("writer reclaimed readonly reader's inode: %d %v", n, err)
	}
	if err := readonly.Close(r); err != nil {
		t.Fatal(err)
	}
	// The readonly mount unpins, but does not gain the writer's cleanup rights.
	// Its orphan remains discoverable until an eligible writer collects it.
	if err := filesystem.Volume.CollectOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(context.Background(), info.ID); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("released readonly pin prevented later cleanup: %v", err)
	}
}

func (s *observedFilesystemStore) CAS(ctx context.Context, id client.ObjectID, off uint64, before, after []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	info, err := s.MemoryFilesystemStore.CAS(ctx, id, off, before, after, opt)
	if err == nil {
		s.written[id] = bytes.Clone(after)
	}
	return info, err
}

func (s *observedFilesystemStore) PutRange(ctx context.Context, id client.ObjectID, off uint64, data []byte, opt client.PutOptions) (client.ObjectInfo, error) {
	info, err := s.MemoryFilesystemStore.PutRange(ctx, id, off, data, opt)
	if err == nil {
		s.written[id], _, err = s.MemoryFilesystemStore.Get(ctx, id)
	}
	return info, err
}

func (s *observedFilesystemStore) persistentIDs() map[client.ObjectID]bool {
	ids := make(map[client.ObjectID]bool)
	for id, data := range s.written {
		// The volatile test store deliberately retains recovery descriptors.
		// Their reclamation requires exclusive native filesystem collection;
		// this test observes the stable inode/index/data identities separately.
		if len(data) >= 4 && (string(data[:4]) == "STFT" || string(data[:4]) == "STFJ" || string(data[:4]) == "STFD") {
			continue
		}
		ids[id] = true
	}
	return ids
}

func TestRepeatedWindowsOverwriteKeepsGraphAndGoInteroperability(t *testing.T) {
	ctx := context.Background()
	store := &observedFilesystemStore{MemoryFilesystemStore: client.NewMemoryFilesystemStore(), written: map[client.ObjectID][]byte{}}
	volume, err := client.OpenFilesystem(ctx, store, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	filesystem := New(ctx, volume, false)
	handle, err := filesystem.Open("/reused", OpenOptions{Read: true, Write: true, Create: true}, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer filesystem.Close(handle)
	payload := bytes.Repeat([]byte("a"), 8192)
	if _, err := filesystem.Write(handle, payload, 0); err != nil {
		t.Fatal(err)
	}
	info, err := filesystem.Stat("", handle)
	if err != nil {
		t.Fatal(err)
	}
	before := store.persistentIDs()
	for iteration := 0; iteration < 100; iteration++ {
		for i := range payload {
			payload[i] = byte(iteration)
		}
		if n, err := filesystem.Write(handle, payload, 0); err != nil || n != len(payload) {
			t.Fatalf("iteration %d: %d %v", iteration, n, err)
		}
		after := store.persistentIDs()
		if len(after) != len(before) {
			t.Fatalf("iteration %d grew persistent graph: %d -> %d", iteration, len(before), len(after))
		}
		for id := range before {
			if !after[id] {
				t.Fatalf("iteration %d replaced persistent object %x", iteration, id)
			}
		}
	}
	// The ordinary Go client opens and reads the Windows-written graph.
	reopened, err := client.OpenFilesystem(ctx, store, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if n, err := reopened.ReadAt(ctx, info.ID, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatalf("Go reader did not see Windows writes: %d %v", n, err)
	}
	if err := filesystem.Remove("/reused", false); err != nil {
		t.Fatal(err)
	}
	if _, err := filesystem.Write(handle, []byte("still open"), 0); err != nil {
		t.Fatal("unlinked open handle lost write access", err)
	}
	copy(payload, []byte("still open"))
	if n, err := filesystem.Read(handle, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatalf("unlinked open contents reclaimed: %d %v", n, err)
	}
}
