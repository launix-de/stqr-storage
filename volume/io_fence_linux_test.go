// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFileIOFenceAllowsConcurrentPositionedIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent-fences")
	file, err := CreateFile(path, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	start := make(chan struct{})
	pages := make(chan int, 64)
	failures := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		defer close(pages)
		<-start
		for i := 0; i < 64; i++ {
			want := bytes.Repeat([]byte{byte(i)}, PageSize)
			if _, err := file.WriteAt(want, int64((i+8)*PageSize)); err != nil {
				failures <- err
				return
			}
			pages <- i
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := range pages {
			got := make([]byte, PageSize)
			_, err := file.ReadAt(got, int64((i+8)*PageSize))
			if err != nil {
				failures <- err
				return
			}
			if !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, PageSize)) {
				failures <- fmt.Errorf("page %d not coherent after WriteAt", i)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 64; i++ {
			if err := file.Sync(); err != nil {
				failures <- err
				return
			}
		}
	}()
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("positioned I/O and Linux data fences did not complete")
	}
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < 64; i++ {
		got := make([]byte, PageSize)
		_, err := reopened.ReadAt(got, int64((i+8)*PageSize))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i)}, PageSize)) {
			t.Fatalf("page %d after reopen: %v", i, err)
		}
	}
}
