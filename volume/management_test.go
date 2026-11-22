// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLifecycleBindingCheckpointAndScan(t *testing.T) {
	v, d := memoryVolume(t, 4<<20)
	cluster, node := testID(1), testID(2)
	if e := v.Bind(cluster, node); e != nil {
		t.Fatal(e)
	}
	if e := v.VerifyIdentity(cluster, node, v.Identity().VolumeID); e != nil {
		t.Fatal(e)
	}
	if e := v.Bind(cluster, testID(3)); !errors.Is(e, ErrIdentity) {
		t.Fatal(e)
	}
	if e := v.VerifyIdentity(cluster, testID(3), ID{}); !errors.Is(e, ErrIdentity) {
		t.Fatal(e)
	}
	for i := uint64(0); i < 8; i++ {
		if _, e := testPut(v, testKey(i), []byte("payload"), Options{RequestID: testID(i + 20)}); e != nil {
			t.Fatal(e)
		}
	}
	if e := v.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	if e := v.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	var keys []Key
	if e := testScan(v, testKey(0).Bucket, testKey(2).Object, testKey(6).Object, func(k Key, i Info) error { keys = append(keys, k); return nil }); e != nil || len(keys) != 4 {
		t.Fatalf("scan %+v %v", keys, e)
	}
	if e := testSyncThrough(v, testKey(2), 1); e != nil {
		t.Fatal(e)
	}
	if e := testSyncThrough(v, testKey(2), 2); !errors.Is(e, ErrPrecondition) {
		t.Fatal(e)
	}
	if e := v.Sync(); e != nil {
		t.Fatal(e)
	}
	if e := testForgetRequest(v, testID(22)); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	if _, ok, e := testOperationStatus(v, testID(22)); e != nil || ok {
		t.Fatalf("forgotten %v %v", ok, e)
	}
	if e := v.Compact(); e != nil {
		t.Fatal(e)
	}
	v = reopen(t, v, d)
	if _, ok, e := testOperationStatus(v, testID(22)); e != nil || ok {
		t.Fatalf("forgotten after GC %v %v", ok, e)
	}
	desc := v.Describe()
	b, e := json.Marshal(desc)
	if e != nil || !bytes.Contains(b, []byte(`"class":"ram"`)) {
		t.Fatalf("%s %v", b, e)
	}
	if v.Size() != 4<<20 || v.State() != Active {
		t.Fatal(desc)
	}
	if e = v.SetState(Pending); e != nil {
		t.Fatal(e)
	}
	if e = v.SetState(Active); e != nil {
		t.Fatal(e)
	}
	if e = v.Detach(); !errors.Is(e, ErrBusy) {
		t.Fatal(e)
	}
	for i := uint64(0); i < 8; i++ {
		if e = testDelete(v, testKey(i), Options{}); e != nil {
			t.Fatal(e)
		}
	}
	if e = v.SetState(Draining); e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(v, testKey(1), nil, Options{}); !errors.Is(e, ErrDraining) {
		t.Fatal(e)
	}
	if e = v.Detach(); e != nil {
		t.Fatal(e)
	}
	if v.State() != Detached {
		t.Fatal(v.State())
	}
	if _, e = v.Stat(testKey(1)); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if e = v.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestConstructorFormatsExistingAndRAMStartsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unformatted")
	if e := os.WriteFile(path, nil, 0600); e != nil {
		t.Fatal(e)
	}
	v, e := New(path, (1<<20)+1, SSD)
	if e != nil {
		t.Fatal(e)
	}
	if v.Size()%PageSize != 0 || v.Name() != path {
		t.Fatal(v.Describe())
	}
	v.Close()
	v, e = New(path, 0, HDD)
	if e != nil {
		t.Fatal(e)
	}
	if v.Identity().Class != SSD {
		t.Fatal("stored class not preserved")
	}
	v.Close()
	m, e := NewRAM((1 << 20) + 1)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = testPut(m, testKey(1), []byte("volatile"), Options{}); e != nil {
		t.Fatal(e)
	}
	m.Close()
	m, e = NewRAM((1 << 20) + 1)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if _, e = m.Stat(testKey(1)); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if _, e = NewRAM(1); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
	if _, e = New("", 1, Class(255)); !errors.Is(e, ErrRange) {
		t.Fatal(e)
	}
}
func TestPhysicalReserveAndFullVolume(t *testing.T) {
	v, d := memoryVolume(t, 1<<20)
	free := v.Stats().AdmissionFreePhysical
	if e := v.SetGuaranteedReserve(free - PageSize); e != nil {
		t.Fatal(e)
	}
	if _, e := testPut(v, testKey(1), bytes.Repeat([]byte{1}, PageSize), Options{}); !errors.Is(e, ErrFull) {
		t.Fatalf("reserve consumed: %v", e)
	}
	if _, e := v.Stat(testKey(1)); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	if _, e := testPut(v, testKey(1), bytes.Repeat([]byte{1}, PageSize), Options{UseGuaranteed: true}); e != nil {
		t.Fatal(e)
	}
	if e := v.SetGuaranteedReserve(0); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 200; i++ {
		_, e := testPut(v, testKey(2), bytes.Repeat([]byte{byte(i)}, 7000), Options{})
		if e != nil {
			t.Fatalf("churn %d: %v", i, e)
		}
	}
	mustGet(t, v, testKey(2), bytes.Repeat([]byte{199}, 7000))
	before := v.Stats()
	// Replaced allocations are immediately returned to the byte-exact free tree.
	if before.ReclaimablePhysical != 0 {
		t.Fatal("replaced data was not freed", before)
	}
	if e := v.Compact(); e != nil {
		t.Fatal(e)
	}
	after := v.Stats()
	if after.AllocatedPhysical > before.AllocatedPhysical+2*PageSize {
		t.Fatalf("defrag grew storage unexpectedly: %+v -> %+v", before, after)
	}
	v = reopen(t, v, d)
	if v.Stats().Objects != 2 {
		t.Fatal(v.Stats())
	}
	for i := uint64(10); ; i++ {
		_, e := testPut(v, testKey(i), bytes.Repeat([]byte{byte(i)}, 8000), Options{})
		if errors.Is(e, ErrFull) {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if i > 200 {
			t.Fatal("unbounded allocation")
		}
	}
	if e := v.Scrub(); e != nil {
		t.Fatal(e)
	}
}
func TestExplicitRequestDedupeOnFullVolume(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	k := testKey(1)
	id := testID(1)
	data := bytes.Repeat([]byte{1}, 4096)
	info, e := testPut(v, k, data, Options{RequestID: id})
	if e != nil {
		t.Fatal(e)
	}
	if e = v.SetGuaranteedReserve(v.Stats().AdmissionFreePhysical); e != nil {
		t.Fatal(e)
	}
	again, e := testPutReader(v, k, bytes.NewReader(data), uint64(len(data)), Options{RequestID: id})
	if e != nil || again != info {
		t.Fatalf("retry needs new storage %+v %v", again, e)
	}
}
func TestStreamVariants(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	k := testKey(1)
	if _, e := testPutRangeReader(v, k, 5, bytes.NewBufferString("abc"), 3, Options{}); e != nil {
		t.Fatal(e)
	}
	if _, e := testAppendReader(v, k, bytes.NewBufferString("de"), 2, Options{}); e != nil {
		t.Fatal(e)
	}
	mustGet(t, v, k, append(make([]byte, 5), []byte("abcde")...))
}

func TestPhysicalIOCounters(t *testing.T) {
	v, _ := memoryVolume(t, 2<<20)
	before := v.IOStats()
	if _, e := testAppend(v, testKey(1), []byte("ten-bytes!"), Options{}); e != nil {
		t.Fatal(e)
	}
	if _, _, e := testGet(v, testKey(1)); e != nil {
		t.Fatal(e)
	}
	after := v.Stats().IO
	if after.SyncOps-before.SyncOps != 4 || after.ReadBytes <= before.ReadBytes || after.WriteBytes-before.WriteBytes <= 10 {
		t.Fatalf("physical counters %+v => %+v", before, after)
	}
}

func TestInFlightAllowanceCannotConsumeOtherImportsOrGuarantees(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	defer v.Close()
	initial := v.Stats().AdmissionFreePhysical
	guarantee := uint64(64 << 10)
	if err := v.SetGuaranteedReserve(guarantee); err != nil {
		t.Fatal(err)
	}
	reserved := initial - guarantee - PageSize
	if err := v.SetInFlightReserve(reserved); err != nil {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(1), bytes.Repeat([]byte{1}, 64<<10), Options{}); !errors.Is(err, ErrFull) {
		t.Fatal("ordinary write consumed imports", err)
	}
	if _, err := testPut(v, testKey(1), bytes.Repeat([]byte{1}, 64<<10), Options{InFlightAllowance: PageSize}); !errors.Is(err, ErrFull) {
		t.Fatal("import consumed another import", err)
	}
	if _, err := testPut(v, testKey(1), bytes.Repeat([]byte{1}, 64<<10), Options{InFlightAllowance: 128 << 10}); err != nil {
		t.Fatal("own import allowance", err)
	}
	stats := v.Stats()
	if stats.GuaranteedRemainingPhysical != guarantee || stats.InFlightReservedPhysical >= reserved {
		t.Fatal("wrong reserve accounting", stats)
	}
	if stats.AdmissionFreePhysical+stats.GuaranteedRemainingPhysical+stats.InFlightReservedPhysical+stats.MetadataPhysical+stats.DataPhysical+stats.OperatingReservePhysical != stats.CapacityPhysical {
		t.Fatal("physical accounting does not sum to capacity")
	}
	if err := v.SetInFlightReserve(initial); !errors.Is(err, ErrFull) {
		t.Fatal("accepted over-reservation", err)
	}
	if err := v.SetInFlightReserve(0); err != nil {
		t.Fatal(err)
	}
}

func TestGuaranteedAllowanceProtectsOtherBucketsAndImports(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	defer v.Close()
	initial := v.Stats().AdmissionFreePhysical
	guarantee := uint64(192 << 10)
	if err := v.SetGuaranteedReserve(guarantee); err != nil {
		t.Fatal(err)
	}
	importReserve := initial - guarantee - PageSize
	if err := v.SetInFlightReserve(importReserve); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x71}, 64<<10)
	for _, allowance := range []uint64{0, PageSize} {
		if _, err := testPut(v, testKey(1), payload, Options{GuaranteedAllowance: allowance}); !errors.Is(err, ErrFull) {
			t.Fatal("write consumed someone else's guarantee", allowance, err)
		}
		if _, err := v.Stat(testKey(1)); !errors.Is(err, ErrNotFound) {
			t.Fatal("rejected write became visible", err)
		}
		if stats := v.Stats(); stats.GuaranteedRemainingPhysical != guarantee || stats.InFlightReservedPhysical != importReserve {
			t.Fatal("rejected write consumed reservations", stats)
		}
	}
	before := v.Stats()
	id := ID{0xf1}
	if _, err := testPut(v, testKey(1), payload, Options{RequestID: id, GuaranteedAllowance: 128 << 10}); err != nil {
		t.Fatal("own guaranteed budget", err)
	}
	after := v.Stats()
	growth := after.AllocatedPhysical - before.AllocatedPhysical
	if growth == 0 || guarantee-after.GuaranteedRemainingPhysical != growth || after.GuaranteedRemainingPhysical < 64<<10 || after.InFlightReservedPhysical != importReserve {
		t.Fatal("incorrect guarantee debit", before, after)
	}
	if after.AdmissionFreePhysical+after.GuaranteedRemainingPhysical+after.InFlightReservedPhysical+after.AllocatedPhysical+after.OperatingReservePhysical != after.CapacityPhysical {
		t.Fatal("physical capacity no longer balances", after)
	}
	// A durable request replay returns its original outcome without consuming
	// the guarantee a second time, even with no remaining caller allowance.
	if _, err := testPut(v, testKey(1), payload, Options{RequestID: id}); err != nil {
		t.Fatal("guaranteed retry", err)
	}
	if v.Stats().GuaranteedRemainingPhysical != after.GuaranteedRemainingPhysical {
		t.Fatal("retry debited the guarantee twice")
	}
}

func TestCombinedImportAndGuaranteeAllowanceChargesOnce(t *testing.T) {
	v, _ := memoryVolume(t, 1<<20)
	defer v.Close()
	initial := v.Stats().AdmissionFreePhysical
	guarantee := uint64(128 << 10)
	if err := v.SetGuaranteedReserve(guarantee); err != nil {
		t.Fatal(err)
	}
	reserved := initial - guarantee - PageSize
	if err := v.SetInFlightReserve(reserved); err != nil {
		t.Fatal(err)
	}
	before := v.Stats()
	if _, err := testPut(v, testKey(1), bytes.Repeat([]byte{2}, 64<<10), Options{InFlightAllowance: 32 << 10, GuaranteedAllowance: 64 << 10}); err != nil {
		t.Fatal(err)
	}
	after := v.Stats()
	growth := after.AllocatedPhysical - before.AllocatedPhysical
	importDebit := before.InFlightReservedPhysical - after.InFlightReservedPhysical
	guaranteeDebit := before.GuaranteedRemainingPhysical - after.GuaranteedRemainingPhysical
	if importDebit != 32<<10 || guaranteeDebit == 0 || importDebit+guaranteeDebit != growth {
		t.Fatal("combined allowance counted bytes twice", growth, importDebit, guaranteeDebit)
	}
}

func TestRecompressConsumesOnlyItsOwnImportAllowance(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(5)
	request := testID(80)
	payload := bytes.Repeat([]byte{0x11}, 1<<20)
	first, err := h.Put(key, payload, Options{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Recompress(key, Gzip); err != nil {
		t.Fatal(err)
	}
	h.Close()
	free := v.Stats().AdmissionFreePhysical
	reserve := free - (64 << 10)
	if err := v.SetInFlightReserve(reserve); err != nil {
		t.Fatal(err)
	}
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.RecompressWithOptions(key, Raw, Options{}); !errors.Is(err, ErrFull) {
		t.Fatal("recompression stole pending import space", err)
	}
	if got, info, err := h.Get(key); err != nil || !bytes.Equal(got, payload) || info.Version != first.Version || info.Compression != Gzip {
		t.Fatal("failed rewrite damaged source", info, err)
	}
	if err := h.RecompressWithOptions(key, Raw, Options{InFlightAllowance: 2 << 20}); err != nil {
		t.Fatal("own reserve did not fund rewrite", err)
	}
	if got, info, err := h.Get(key); err != nil || !bytes.Equal(got, payload) || info.Version != first.Version || info.Compression != Raw {
		t.Fatal("rewrite changed logical state", info, err)
	}
	original, found, err := h.OperationStatus(request)
	if err != nil || !found || original.Info != first {
		t.Fatal("physical rewrite changed retry result", original, err)
	}
	version := first.Version
	if err := h.RecompressWithOptions(key, Gzip, Options{ExpectedVersion: &version}); !errors.Is(err, ErrRange) {
		t.Fatal("logical precondition silently ignored", err)
	}
	h.Close()
	after := v.Stats().InFlightReservedPhysical
	if after >= reserve || reserve-after > 2<<20 {
		t.Fatal("rewrite consumed another import's allowance", reserve, after)
	}
}
