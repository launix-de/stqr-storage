// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestMutableFilesystemConcurrentSubscribedMounts(t *testing.T) {
	_, a, b, ctx := subscriptionClients(t)
	stores := []*client.SubscriptionStore{client.NewSubscriptionStore(a, 1<<20), client.NewSubscriptionStore(b, 1<<20)}
	for _, s := range stores {
		s.EnableAcceptedWrites()
		defer s.Close()
	}
	first, err := client.OpenFilesystem(ctx, stores[0], client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := first.Create(ctx, client.FilesystemRootID(), "append", 0600, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.WriteAt(ctx, file.ID, make([]byte, 8192), 0); err != nil {
		t.Fatal(err)
	}
	second, err := client.OpenFilesystem(ctx, stores[1], client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for index, fs := range []*client.Filesystem{first, second} {
		wg.Add(1)
		go func(index int, fs *client.Filesystem) {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				if _, e := fs.Append(ctx, file.ID, bytes.Repeat([]byte{byte('a' + index)}, 64)); e != nil {
					t.Error(e)
					return
				}
			}
		}(index, fs)
	}
	wg.Wait()
	for _, fs := range []*client.Filesystem{first, second} {
		got := make([]byte, 8192+24*64)
		n, e := fs.ReadAt(ctx, file.ID, got, 0)
		if e != nil || n != len(got) || bytes.Count(got[8192:], []byte("a")) != 12*64 || bytes.Count(got[8192:], []byte("b")) != 12*64 {
			t.Fatalf("subscribed mounts lost append: bytes=%d error=%v", n, e)
		}
	}
}
