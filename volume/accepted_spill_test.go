// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

func fragmentedAcceptedFixture(t *testing.T) (*Volume, *MemoryIO, Key, []byte) {
	t.Helper()
	v, d := memoryVolume(t, 64<<20)
	key := testKey(1)
	expected := bytes.Repeat([]byte("x"), 36<<10)
	if _, err := testPut(v, key, expected, Options{}); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	tx := v.transaction()
	tx.putNode(tx.sb.FreeRoot, treeNode{kind: treeFree})
	const tail = 40 << 20
	if err := tx.set(&tx.sb.FreeRoot, spaceKey(tail), tx.sb.Identity.Size-tail); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 600; i++ {
		if err := tx.set(&tx.sb.FreeRoot, spaceKey((1<<20)+i*(64<<10)+2048), 256); err != nil {
			t.Fatal(err)
		}
	}
	var free uint64
	if err := treeWalk(tx.node, tx.sb.FreeRoot, func(e treeEntry) error { free += e.value; return nil }); err != nil {
		t.Fatal(err)
	}
	for _, p := range tx.pending {
		free -= p.n
	}
	tx.sb.FreeBytes = free
	tx.sb.PayloadBytes = tx.sb.Identity.Size - scratchPages*PageSize - tx.sb.MetadataBytes - free
	if err := tx.commit(false); err != nil {
		t.Fatal(err)
	}
	v.mu.Unlock()
	return v, d, key, expected
}
func TestAcceptedSpillFragmentedGrowingCASNeedsOnlyExplicitSync(t *testing.T) {
	v, d, key, expected := fragmentedAcceptedFixture(t)
	before := v.IOStats()
	for i := 1; i <= 500; i++ {
		next := append(append([]byte{}, expected...), bytes.Repeat([]byte{byte(i)}, 96)...)
		if _, err := testCAS(v, key, 0, expected, next, Options{Accepted: true, WholeObject: true, RequestID: testID(uint64(1000 + i))}); err != nil {
			t.Fatal(i, err)
		}
		expected = next
	}
	after := v.IOStats()
	if after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("fragmentation forced unsolicited durability", before, after)
	}
	mustGet(t, v, key, expected)
	if generation, pending := v.PendingCheckpoint(true, uint64(len(expected))); pending || generation != 0 {
		t.Fatal("pressure asks server to Sync", generation, pending)
	}
	if err := v.Sync(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, w, key, expected)
	t.Logf("500 changing-size CAS: dirty=%d, sync before explicit=%d; explicit added %d barriers", v.Stats().AllocatedPhysical, after.SyncOps-before.SyncOps, v.IOStats().SyncOps-after.SyncOps)
}
func TestAcceptedSpillOversizedReadAfterWriteAndCloseWithoutSync(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "discard-on-close", true: "explicit-sync"}[explicit], func(t *testing.T) {
			d := newCrash(48 << 20)
			v, err := Format(d, FormatOptions{Class: SSD})
			if err != nil {
				t.Fatal(err)
			}
			key := testKey(1)
			old := []byte("previously durable")
			if _, err = testPut(v, key, old, Options{}); err != nil {
				t.Fatal(err)
			}
			before := v.IOStats()
			payload := bytes.Repeat([]byte("unsynced payload"), 1<<20)
			if _, err = testPut(v, key, payload, Options{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			if v.deferred.store.data == nil || len(v.deferred.pages) > deferredPageLimit || len(v.deferred.store.flags) > deferredPageLimit {
				t.Fatal("oversized accepted write did not use bounded spill")
			}
			mustGet(t, v, key, payload)
			if got := v.IOStats(); got.SyncOps != before.SyncOps || got.WriteOps != before.WriteOps {
				t.Fatal("oversized write reached durability backend", before, got)
			}
			recovered, err := Open(d.crash(), OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			mustGet(t, recovered, key, old)
			if explicit {
				if err = v.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			beforeClose := v.IOStats()
			if err = v.Close(); err != nil {
				t.Fatal(err)
			}
			if v.IOStats().SyncOps != beforeClose.SyncOps {
				t.Fatal("Close requested Sync")
			}
			recovered, err = Open(d.crash(), OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if explicit {
				mustGet(t, recovered, key, payload)
			} else {
				mustGet(t, recovered, key, old)
			}
		})
	}
}
func TestAcceptedSpillDynamicRedoAndBackpressure(t *testing.T) {
	d := newCrash(48 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	payload := bytes.Repeat([]byte("a"), 12<<20)
	if _, err = testPut(v, key, payload, Options{}); err != nil {
		t.Fatal(err)
	}
	replacement := bytes.Repeat([]byte("b"), len(payload))
	before := v.IOStats()
	if _, err = testPut(v, key, replacement, Options{Accepted: true}); err != nil {
		t.Fatal("ample safe space must grow beyond initial8MiB redo", err)
	}
	if v.deferred.journal.n <= 8<<20 {
		t.Fatal("redo reservation did not grow")
	}
	if got := v.IOStats(); got.SyncOps != before.SyncOps || got.WriteOps != before.WriteOps {
		t.Fatal("redo growth Sync", got)
	}
	if err = v.Sync(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, key, replacement)
}
func TestAcceptedSpillRAMBackpressureDoesNotCreateFilesOrSync(t *testing.T) {
	v, _ := memoryVolume(t, 32<<20)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("keep"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	_, err := testPut(v, key, bytes.Repeat([]byte("z"), 8<<20), Options{Accepted: true})
	if !errors.Is(err, ErrBusy) {
		t.Fatal("RAM oversized write should report bounded backpressure", err)
	}
	mustGet(t, v, key, []byte("keep"))
	if v.deferred.store.data != nil || v.IOStats().SyncOps != before.SyncOps {
		t.Fatal("RAM write spilled/synced")
	}
}
func TestAcceptedSpillMetadataAndInputFailureDoNotSync(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	before := v.IOStats()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	source := func(yield func(uint64, io.Reader, uint64) error) error {
		return yield(0, bytes.NewReader([]byte("bytes")), 5)
	}
	if err = h.RestoreSeekable(key, Info{Length: 5, Compression: Raw}, source, Options{Accepted: true, ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	if err = h.RestoreRequestWithOptions(RequestRecord{ID: testID(9), Key: key, Result: Result{Info: Info{Length: 5, Compression: Raw}}}, Options{Accepted: true, ImportShadow: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.PutReader(key, bytes.NewReader([]byte("short")), 8<<20, Options{Accepted: true}); err == nil {
		t.Fatal("truncated payload accepted")
	}
	h.Close()
	mustGet(t, v, key, []byte("bytes"))
	if got := v.IOStats(); got.SyncOps != before.SyncOps || got.WriteOps != before.WriteOps {
		t.Fatal("metadata/failure implicitly synced", got)
	}
}
func TestAcceptedSpillDirtyIndexIsBoundedAndSorted(t *testing.T) {
	s := newDirtyStore(4, false)
	defer s.close()
	for i := 200; i >= 0; i-- {
		if err := s.put(uint64(i)*PageSize, bytes.Repeat([]byte{byte(i)}, PageSize), 1); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.pages) > 4 || len(s.flags) > 4 || s.count != 201 {
		t.Fatal("unbounded index", len(s.pages), len(s.flags), s.count)
	}
	for i := 0; i < 300; i++ {
		if err := s.put(0, bytes.Repeat([]byte("q"), PageSize), 1); err != nil {
			t.Fatal(err)
		}
	}
	if s.count != 201 {
		t.Fatal("overwriting a hot page grew the dirty index")
	}
	last := uint64(0)
	count := 0
	if err := s.eachSorted(func(off uint64, _ byte, p []byte) error {
		if count > 0 && off <= last {
			t.Fatal("unsorted", off, last)
		}
		last = off
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 201 {
		t.Fatal(count)
	}
	for _, f := range []*os.File{s.data, s.index, s.log} {
		st, err := f.Stat()
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatal("spill permissions", st, err)
		}
	}
}

func TestAcceptedSpillCrashAtEveryExplicitCheckpointStep(t *testing.T) {
	base := newCrash(4 << 20)
	v, err := Format(base, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	old := bytes.Repeat([]byte("o"), 64<<10)
	next := bytes.Repeat([]byte("n"), len(old))
	if _, err = testPut(v, key, old, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, through := range []bool{false, true} {
		for _, persist := range []bool{false, true} {
			failures := 0
			for step := 1; step < 160; step++ {
				d := base.crash()
				w, err := Open(d, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				w.mu.Lock()
				if err = w.beginDeferred(); err != nil {
					t.Fatal(err)
				}
				w.deferred.store.limit = 2
				w.mu.Unlock()
				if _, err = testPut(w, key, next, Options{Accepted: true}); err != nil {
					t.Fatal(err)
				}
				if w.deferred.store.data == nil {
					t.Fatal("fault test did not enter private spill path")
				}
				if d.steps != 0 {
					t.Fatal("accepted preparation wrote/synced backend", d.steps)
				}
				d.fail = step
				d.writeThrough = through
				d.persistFailedSync = persist
				commitErr := w.Sync()
				if commitErr != nil {
					failures++
					if !errors.Is(commitErr, ErrRecovery) {
						t.Fatal(commitErr)
					}
				}
				recovered, err := Open(d.crash(), OpenOptions{})
				if err != nil {
					t.Fatalf("through=%v persist=%v step=%d recovery=%v", through, persist, step, err)
				}
				got, _, err := testGet(recovered, key)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, old) && !bytes.Equal(got, next) || commitErr == nil && !bytes.Equal(got, next) {
					t.Fatalf("step=%d mixed or lost confirmed image", step)
				}
				w.Close()
				recovered.Close()
				if commitErr == nil {
					break
				}
			}
			if failures < 35 {
				t.Fatal("spilled write/anchor/fence fault points not exercised", failures)
			}
		}
	}
}
func TestAcceptedSpillFailureFencesWithoutSync(t *testing.T) {
	d := newCrash(32 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	if _, err = testPut(v, key, []byte("old durable bytes"), Options{}); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("p"), 8<<20)
	if _, err = testPut(v, key, payload, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if v.deferred.store.data == nil {
		t.Fatal("missing private spill")
	}
	// A failed private-cache eviction is an ambiguous volatile publication and
	// fences this instance. It cannot Sync accepted bytes to repair the failure.
	v.deferred.store.data.Close()
	before := v.IOStats()
	_, err = testPut(v, testKey(9), []byte("must not publish"), Options{Accepted: true})
	if !errors.Is(err, ErrRecovery) {
		t.Fatal("private spill write failure not fenced", err)
	}
	if after := v.IOStats(); after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("spill failure forced durable fallback", before, after)
	}
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, key, []byte("old durable bytes"))
	if _, _, err = testGet(recovered, testKey(9)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	v.Close()
}
func TestAcceptedSpillNoSafeRedoBackpressuresBeforePublication(t *testing.T) {
	d := newCrash(4 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	old := bytes.Repeat([]byte("o"), 3<<20)
	if _, err = testPut(v, key, old, Options{}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats()
	generation := v.Header().Generation
	_, err = testPut(v, key, bytes.Repeat([]byte("n"), len(old)), Options{Accepted: true})
	if !errors.Is(err, ErrWritebackPressure) {
		t.Fatal("no safe redo space must backpressure", err)
	}
	if v.Header().Generation != generation {
		t.Fatal("rejected overwrite published metadata")
	}
	mustGet(t, v, key, old)
	if after := v.IOStats(); after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("redo pressure forced Sync", after)
	}
	v.Close()
}
func TestAcceptedSpillReadersAndLaterTailDuringExplicitFence(t *testing.T) {
	d := newCrash(16 << 20)
	gate := &checkpointGateIO{VolumeIO: d}
	v, err := Format(gate, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	old := bytes.Repeat([]byte("o"), 64<<10)
	next := bytes.Repeat([]byte("n"), len(old))
	if _, err = testPut(v, key, old, Options{}); err != nil {
		t.Fatal(err)
	}
	v.mu.Lock()
	if err = v.beginDeferred(); err != nil {
		t.Fatal(err)
	}
	v.deferred.store.limit = 2
	v.mu.Unlock()
	if _, err = testPut(v, key, next, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	entered, release := gate.arm(nil)
	defer release()
	done := make(chan error, 1)
	go func() { done <- v.Sync() }()
	<-entered
	mustGet(t, v, key, next)
	if _, err = testPut(v, testKey(2), []byte("later unsynced tail"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, testKey(2), []byte("later unsynced tail"))
	release()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	beforeClose := v.IOStats()
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != beforeClose.SyncOps {
		t.Fatal("Close synced later tail")
	}
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, key, next)
	if _, _, err = testGet(recovered, testKey(2)); !errors.Is(err, ErrNotFound) {
		t.Fatal("old fence included a later tail", err)
	}
}

func TestAcceptedSpillInitialMigrationFailureNeverReusesPartialIndex(t *testing.T) {
	s := newDirtyStore(2, false)
	defer s.close()
	old := bytes.Repeat([]byte("old"), PageSize/3+1)[:PageSize]
	for _, off := range []uint64{PageSize, 2 * PageSize} {
		if err := s.put(off, old, 2); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	s.createTemp = func() (*os.File, error) {
		calls++
		f, err := os.CreateTemp(t.TempDir(), "initial-spill-*")
		if err != nil {
			return nil, err
		}
		if calls == 3 {
			name := f.Name()
			f.Close()
			return os.Open(name)
		}
		return f, nil
	}
	if err := s.put(3*PageSize, old, 2); err == nil {
		t.Fatal("read-only spill log did not fail")
	}
	data, _ := s.data.Stat()
	index, _ := s.index.Stat()
	log, _ := s.log.Stat()
	if data.Size() == 0 || index.Size() == 0 || log.Size() != 0 {
		t.Fatal("did not reach partial initial data/index migration", data.Size(), index.Size(), log.Size())
	}
	if p, ok, err := s.get(PageSize); err == nil || p != nil || ok {
		t.Fatal("half-indexed spill looked usable", ok, err)
	}
	if err := s.put(4*PageSize, old, 2); err == nil {
		t.Fatal("poisoned spill reused")
	}
}
func TestAcceptedSpillPartialParentMergeIsFenced(t *testing.T) {
	d := newCrash(8 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	if _, err = testPut(v, key, []byte("durable old"), Options{}); err != nil {
		t.Fatal(err)
	}
	accepted := bytes.Repeat([]byte("a"), 8192)
	if _, err = testPut(v, key, accepted, Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	group := v.deferred
	group.store.limit = len(group.store.pages)
	calls := 0
	group.store.createTemp = func() (*os.File, error) {
		calls++
		f, e := os.CreateTemp(t.TempDir(), "parent-merge-*")
		if e != nil {
			return nil, e
		}
		if calls == 3 {
			name := f.Name()
			f.Close()
			return os.Open(name)
		}
		return f, nil
	}
	before := v.IOStats()
	generation := v.Header().Generation
	_, err = testPut(v, testKey(77), []byte("new object"), Options{Accepted: true})
	if !errors.Is(err, ErrRecovery) {
		t.Fatal("partially indexed parent was not fenced", err)
	}
	if v.Header().Generation != generation {
		t.Fatal("failed parent merge published generation")
	}
	if _, _, err = testGet(v, key); !errors.Is(err, ErrRecovery) {
		t.Fatal("previous accepted bytes silently became zero/readable through poisoned spill", err)
	}
	if after := v.IOStats(); after.SyncOps != before.SyncOps || after.WriteOps != before.WriteOps {
		t.Fatal("failure forced Sync", after)
	}
	v.Close()
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, key, []byte("durable old"))
}
func TestAcceptedSpillExternalMergePassesAreBounded(t *testing.T) {
	s := newDirtyStore(2, false)
	s.sortRun = 3
	defer s.close()
	for i := 200; i >= 0; i-- {
		if err := s.put(uint64(i)*PageSize, make([]byte, PageSize), 1); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	if err := s.eachSorted(func(off uint64, _ byte, _ []byte) error {
		if off != uint64(count)*PageSize {
			t.Fatal("multi-pass merge lost ordering", off, count)
		}
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 201 || len(s.pages) > 2 || len(s.flags) > 2 {
		t.Fatal("external merge changed memory/directory bound", count)
	}
}

func TestAcceptedSpillFullVolumeDeleteUsesScratchWithoutSync(t *testing.T) {
	d := newCrash(1 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(1)
	payload := bytes.Repeat([]byte("p"), (1<<20)-int(dataStart)-objectHeader-spanBytes-2*PageSize)
	if _, err = testPut(v, key, payload, Options{}); err != nil {
		t.Fatal(err)
	}
	if v.Header().FreeBytes >= deferredJournalBytes {
		t.Fatal("fixture is not nearly full", v.Header().FreeBytes)
	}
	before := v.IOStats()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	err = h.Delete(key, Options{Accepted: true})
	h.Close()
	if err != nil {
		t.Fatal("full accepted delete should use fixed scratch", err)
	}
	if got := v.IOStats(); got.SyncOps != before.SyncOps || got.WriteOps != before.WriteOps {
		t.Fatal("accepted delete synced/wrote backend", before, got)
	}
	old, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, old, key, payload)
	if err = v.Sync(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(d.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = testGet(recovered, key); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
