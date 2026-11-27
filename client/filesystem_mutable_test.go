// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestFilesystemMutableWriteAfterSparseGrowth(t *testing.T) {
	for _, size := range []uint64{8192, FSObjectSize + 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			ctx := context.Background()
			f, _ := newTestFS(t)
			file := createTestFile(t, f, "sparse-growth")
			if _, err := f.WriteAt(ctx, file.ID, bytes.Repeat([]byte{'A'}, 4096), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := f.Truncate(ctx, file.ID, size); err != nil {
				t.Fatal(err)
			}
			before, _, err := f.load(ctx, file.ID)
			if err != nil {
				t.Fatal(err)
			}
			parts, err := f.segmentsReadOnly(ctx, before.blob)
			if err != nil || len(parts) == 0 {
				t.Fatal(parts, err)
			}
			id := parts[0].id
			if _, err = f.WriteAt(ctx, file.ID, []byte{'Z'}, 0); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, int(size))
			n, err := f.ReadAt(ctx, file.ID, got, 0)
			if err != nil || n != len(got) {
				t.Fatalf("read after growth: bytes=%d want=%d error=%v", n, len(got), err)
			}
			want := make([]byte, int(size))
			copy(want, bytes.Repeat([]byte{'A'}, 4096))
			want[0] = 'Z'
			if !bytes.Equal(got, want) {
				t.Fatal("write lost data or exposed nonzero hole bytes")
			}
			after, _, err := f.load(ctx, file.ID)
			if err != nil {
				t.Fatal(err)
			}
			parts, err = f.segmentsReadOnly(ctx, after.blob)
			if err != nil || len(parts) == 0 || parts[0].id != id {
				t.Fatal("payload identity changed", err)
			}
		})
	}
}

// Repeated changes must mutate the existing pages, leave unrelated pages alone,
// and discard the transient redo payload once the inode is published.
func TestFilesystemMutablePagesKeepIdentity(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "stable")
	original := bytes.Repeat([]byte("x"), 3*FSObjectSize)
	if _, err := f.WriteAt(ctx, file.ID, original, 0); err != nil {
		t.Fatal(err)
	}
	before, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	parts, err := f.segmentsReadOnly(ctx, before.blob)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatal(len(parts))
	}
	infos := make([][]byte, len(parts))
	for i, p := range parts {
		infos[i], _, err = store.Get(ctx, p.id)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err = f.WriteAt(ctx, file.ID, []byte{byte(i)}, FSObjectSize+123); err != nil {
			t.Fatal(err)
		}
	}
	after, _, err := f.load(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.segmentsReadOnly(ctx, after.blob)
	if err != nil {
		t.Fatal(err)
	}
	if before.blob.root != after.blob.root {
		t.Fatal("index identity changed")
	}
	for i, p := range got {
		if p.id != parts[i].id {
			t.Fatal("payload identity changed")
		}
		data, _, err := store.Get(ctx, p.id)
		if err != nil {
			t.Fatal(err)
		}
		if i != 1 && !bytes.Equal(data, infos[i]) {
			t.Fatal("unchanged payload rewritten")
		}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	for _, o := range store.objects {
		if len(o.data) > 7 && (bytes.HasPrefix(o.data, []byte("STFT")) || bytes.HasPrefix(o.data, []byte("STFJ"))) {
			t.Fatal("completed mutation retained redo payload")
		}
	}
}

func TestFilesystemMutableTruncateDoesNotResurrectTail(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	file := createTestFile(t, f, "tail")
	original := bytes.Repeat([]byte("secret"), FSObjectSize/3)
	if _, err := f.WriteAt(ctx, file.ID, original, 0); err != nil {
		t.Fatal(err)
	}
	n := uint64(FSObjectSize + 17)
	if _, err := f.Truncate(ctx, file.ID, n); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Truncate(ctx, file.ID, uint64(len(original))); err != nil {
		t.Fatal(err)
	}
	got := readTestFile(t, f, file.ID)
	if !bytes.Equal(got[:n], original[:n]) || !bytes.Equal(got[n:], make([]byte, len(got)-int(n))) {
		t.Fatal("truncated data returned after sparse growth")
	}
}

func TestFilesystemMutableCrashRecovery(t *testing.T) {
	for _, operation := range []string{"write", "append", "truncate"} {
		for _, after := range []bool{false, true} {
			for at := 1; at <= 14; at++ {
				t.Run(fmt.Sprintf("%s/after-%t/%d", operation, after, at), func(t *testing.T) {
					ctx := context.Background()
					f, store := newTestFS(t)
					file := createTestFile(t, f, "recover")
					original := bytes.Repeat([]byte("o"), FSObjectSize+257)
					if _, err := f.WriteAt(ctx, file.ID, original, 0); err != nil {
						t.Fatal(err)
					}
					expected := bytes.Clone(original)
					f.store = &fsCrashStore{FilesystemStore: store, at: at, after: after}
					var err error
					switch operation {
					case "write":
						copy(expected[FSObjectSize-19:], bytes.Repeat([]byte("z"), 73))
						_, err = f.WriteAt(ctx, file.ID, bytes.Repeat([]byte("z"), 73), FSObjectSize-19)
					case "append":
						expected = append(expected, []byte("appended")...)
						_, err = f.Append(ctx, file.ID, []byte("appended"))
					case "truncate":
						expected = expected[:FSObjectSize-9]
						_, err = f.Truncate(ctx, file.ID, uint64(len(expected)))
					}
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
					reopened, e := OpenFilesystem(ctx, store, FSOptions{})
					if e != nil {
						t.Fatal(e)
					}
					got := readTestFile(t, reopened, file.ID)
					if !bytes.Equal(got, original) && !bytes.Equal(got, expected) {
						t.Fatal("torn mutation after recovery")
					}
					if err == nil && !bytes.Equal(got, expected) {
						t.Fatal("acknowledged mutation lost")
					}
				})
			}
		}
	}
}

func TestFilesystemMutableConcurrentMounts(t *testing.T) {
	ctx := context.Background()
	f, store := newTestFS(t)
	file := createTestFile(t, f, "parallel-pages")
	original := bytes.Repeat([]byte("x"), 2*FSObjectSize)
	if _, err := f.WriteAt(ctx, file.ID, original, 0); err != nil {
		t.Fatal(err)
	}
	other, err := OpenFilesystem(ctx, store, FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for mount, fs := range []*Filesystem{f, other} {
		wg.Add(1)
		go func(mount int, fs *Filesystem) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				at := mount*FSObjectSize + i*4096
				payload := bytes.Repeat([]byte{byte(i + 1)}, 4096)
				if _, err := fs.WriteAt(ctx, file.ID, payload, int64(at)); err != nil {
					t.Error(err)
					return
				}
			}
		}(mount, fs)
	}
	wg.Wait()
	got := readTestFile(t, f, file.ID)
	for mount := 0; mount < 2; mount++ {
		for i := 0; i < 20; i++ {
			at := mount*FSObjectSize + i*4096
			copy(original[at:], bytes.Repeat([]byte{byte(i + 1)}, 4096))
		}
	}
	if !bytes.Equal(got, original) {
		t.Fatal("parallel mounted writer lost an update")
	}
	// EOF selection must be repeated after a CAS conflict, even beyond inline size.
	for mount, fs := range []*Filesystem{f, other} {
		wg.Add(1)
		go func(mount int, fs *Filesystem) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := fs.Append(ctx, file.ID, bytes.Repeat([]byte{byte('a' + mount)}, 64)); err != nil {
					t.Error(err)
					return
				}
			}
		}(mount, fs)
	}
	wg.Wait()
	got = readTestFile(t, f, file.ID)
	tail := got[len(original):]
	if len(tail) != 40*64 || bytes.Count(tail, []byte("a")) != 20*64 || bytes.Count(tail, []byte("b")) != 20*64 {
		t.Fatal("parallel append lost or duplicated data")
	}
}
