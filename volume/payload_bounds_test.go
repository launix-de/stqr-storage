// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

// A root can move during a B+-tree split/collapse. The old page is reusable
// metadata space, but payload decoders require all data at or above dataStart.
func relocatePayloadTestRoot(t *testing.T, v *Volume) uint64 {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	tx := v.transaction()
	defer tx.close()
	prior := tx.sb.ObjectRoot
	n, err := tx.node(prior)
	if err != nil {
		t.Fatal(err)
	}
	next, err := tx.metaPage()
	if err != nil {
		t.Fatal(err)
	}
	tx.putNode(next, n)
	tx.sb.ObjectRoot = next
	tx.release(prior, PageSize, true)
	if err = tx.commit(false); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestPackedPayloadBoundsAfterIndexPageRetirement(t *testing.T) {
	for _, op := range []string{"put", "cas", "append"} {
		for _, size := range []int{0, 1, 210, PageSize, PageSize + 1} {
			t.Run(fmt.Sprintf("%s/%d", op, size), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "volume")
				v, err := New(path, 2<<20, HDD)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { v.Close() }()
				relocatePayloadTestRoot(t, v)
				if err = v.SetAdmission(func(StorageChange) (uint64, error) { return 0, nil }); err != nil {
					t.Fatal(err)
				}
				if err = v.SetOwnershipFilter(1, func(Key, bool) bool { return true }); err != nil {
					t.Fatal(err)
				}
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				k := testKey(31)
				want := bytes.Repeat([]byte("x"), size)
				opt := Options{RequestID: testID(42)}
				switch op {
				case "put":
					_, err = h.Put(k, want, opt)
				case "cas":
					opt.WholeObject = true
					opt.CreateOnly = true
					_, err = h.CAS(k, 0, nil, want, opt)
				case "append":
					_, err = h.Append(k, want, opt)
				}
				if err != nil {
					h.Close()
					t.Fatal(err)
				}
				got, _, err := h.Get(k)
				h.Close()
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("payload after mutation", err)
				}
				o, err := v.loadObject(k)
				if err != nil {
					t.Fatal(err)
				}
				for _, s := range o.Spans {
					if s.Location < dataStart {
						t.Fatalf("payload overlaps initial index region at %d", s.Location)
					}
				}
				// Early index space remains useful for metadata; it is not discarded.
				if o.Base >= dataStart {
					if off := relocatePayloadTestRoot(t, v); off >= dataStart {
						t.Fatalf("initial root page was not reusable for metadata: %d", off)
					}
				}
				if err = v.Compact(); err != nil {
					t.Fatal("compact", err)
				}
				if err = v.Scrub(); err != nil {
					t.Fatal("scrub", err)
				}
				if err = v.Close(); err != nil {
					t.Fatal(err)
				}
				v, err = New(path, 0, HDD)
				if err != nil {
					t.Fatal("reopen", err)
				}
				h, err = v.Open(false)
				if err != nil {
					t.Fatal(err)
				}
				got, _, err = h.Get(k)
				h.Close()
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("payload after reopen", err)
				}
			})
		}
	}
}
