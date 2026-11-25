// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type atomicInodeStore struct {
	*MemoryFilesystemStore
	calls atomic.Int64
	pins  atomic.Int64
}

func (s *atomicInodeStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, data []byte, opt CASOptions) (ObjectInfo, error) {
	s.calls.Add(1)
	return s.MemoryFilesystemStore.CAS(ctx, id, off, expected, data, opt)
}
func (s *atomicInodeStore) PinObject(ctx context.Context, id ObjectID, token ID) error {
	s.pins.Add(1)
	return s.MemoryFilesystemStore.PinObject(ctx, id, token)
}
func TestFilesystemSingleInodeUsesOneAtomicCAS(t *testing.T) {
	ctx := context.Background()
	store := &atomicInodeStore{MemoryFilesystemStore: NewMemoryFilesystemStore()}
	f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Create(ctx, FilesystemRootID(), "small", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.managedReferences(); !ok {
		t.Fatal("fixture must support managed references")
	}
	for _, update := range []func() error{
		func() error {
			mode := uint32(0640)
			_, e := f.SetAttributes(ctx, info.ID, FSAttributes{Mode: &mode})
			return e
		},
		func() error { _, e := f.WriteAt(ctx, info.ID, []byte("tiny"), 0); return e },
	} {
		before, pins := store.calls.Load(), store.pins.Load()
		if err := update(); err != nil {
			t.Fatal(err)
		}
		if n := store.calls.Load() - before; n != 1 {
			t.Fatalf("one inode used %d durable CAS operations; want 1", n)
		}
		if store.pins.Load() != pins {
			t.Fatal("single inode allocated a recovery pin")
		}
	}
}
func TestFilesystemSingleInodeCASConflictDoesNotOverwrite(t *testing.T) {
	f, _ := newTestFS(t)
	ctx := context.Background()
	file := createTestFile(t, f, "conflict")
	before, err := f.snapshot(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := before.node
	next.mode = 0644
	if _, err = f.WriteAt(ctx, file.ID, []byte("new bytes"), 0); err != nil {
		t.Fatal(err)
	}
	committed, err := f.transact(ctx, []fsTxMember{fsMember(before, next)})
	if err != nil || committed {
		t.Fatalf("stale inode committed: %v %v", committed, err)
	}
	if got := string(readTestFile(t, f, file.ID)); got != "new bytes" {
		t.Fatal(got)
	}
}

func TestAtomicInodeLostReplyIsNotReplayedAsConflict(t *testing.T) {
	var commits atomic.Int64
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			m, err := DecodeMessage(raw)
			if err != nil {
				return
			}
			if m.Opcode == objectCAS {
				commits.Add(1)
				return
			} // applied, ACK lost
			result := Encoder{}
			if m.Opcode == Hello {
				result.U16(2)
				result.U64(CapabilityObjectReferences | CapabilityDurability)
				result.ID(ID{})
				result.ID(ID{})
				result.U64(0)
				result.U64(0)
			}
			if err = conn.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(200, result.B, "")})); err != nil {
				return
			}
		}
	}))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, host.URL, "test", "password", BucketID{1}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := NewRequestID()
	_, err = c.CAS(context.WithValue(ctx, fsAtomicCASKey{}, true), ObjectID{1}, 0, []byte("before"), []byte("after"), CASOptions{Scope: CASObject, MutationOptions: MutationOptions{RequestID: id}})
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) || unknown.RequestID != id || commits.Load() != 1 {
		t.Fatalf("CAS replayed or uncertainty lost: commits=%d err=%v", commits.Load(), err)
	}
}
