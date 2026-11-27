// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"os"
	"sync"
	"testing"
)

func spillIndexFixture(t testing.TB) *dirtyStore {
	t.Helper()
	s := newDirtyStore(1, false)
	s.createTemp = func() (*os.File, error) { return os.CreateTemp(t.TempDir(), "index-map-*") }
	if err := s.openSpill(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	return s
}

func TestDirtyIndexMappedGrowthAndHoles(t *testing.T) {
	s := spillIndexFixture(t)
	for _, at := range []uint64{3, 5000, 1 << 20} {
		if err := s.addIndex(at*PageSize, 2); err != nil {
			t.Fatal(err)
		}
		for _, prior := range []uint64{3, 5000, 1 << 20} {
			got, err := s.flag(prior * PageSize)
			want := byte(0)
			if prior <= at {
				want = 2
			}
			if err != nil || got != want {
				t.Fatalf("at=%d prior=%d flag=%d want=%d err=%v", at, prior, got, want, err)
			}
		}
		for _, hole := range []uint64{0, 4, 4999, 5001, (1 << 20) + 1, 1 << 40} {
			got, err := s.flag(hole * PageSize)
			if err != nil || got != 0 {
				t.Fatalf("hole=%d flag=%d err=%v", hole, got, err)
			}
		}
	}
	if len(s.pages) != 0 || len(s.flags) != 0 {
		t.Fatal("mapped index populated heap directory")
	}
	if err := s.addIndex(3*PageSize, 1); err != nil {
		t.Fatal(err)
	}
	if got, err := s.flag(3 * PageSize); err != nil || got != 1 {
		t.Fatal("mapping did not observe positioned overwrite", got, err)
	}
	s.close()
	if s.indexMapping != nil {
		t.Fatal("close retained mapping")
	}
}

func TestDirtyIndexMappingConcurrentReadsAndGrowth(t *testing.T) {
	s := spillIndexFixture(t)
	if err := s.addIndex(PageSize, 1); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 4000; i++ {
				got, err := s.flag(PageSize)
				if err != nil || got != 1 {
					t.Errorf("read during remap %d %v", got, err)
					return
				}
			}
		}()
	}
	for i := uint64(2); i < 32; i++ {
		if err := s.addIndex(i*1024*PageSize, 2); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func BenchmarkDirtyIndexFlag(b *testing.B) {
	for _, mode := range []string{"positioned-byte", "mapped-flag"} {
		b.Run(mode, func(b *testing.B) {
			s := spillIndexFixture(b)
			if err := s.addIndex(8191*PageSize, 2); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				at := uint64(i % 8192)
				if mode == "positioned-byte" {
					var p [1]byte
					if _, err := s.index.ReadAt(p[:], int64(at)); err != nil {
						b.Fatal(err)
					}
				} else {
					if _, err := s.flag(at * PageSize); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
