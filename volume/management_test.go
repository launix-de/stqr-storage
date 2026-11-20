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
	if before.ReclaimablePhysical == 0 {
		t.Fatalf("dead bytes not accounted %+v", before)
	}
	if e := v.Compact(); e != nil {
		t.Fatal(e)
	}
	after := v.Stats()
	if after.AllocatedPhysical >= before.AllocatedPhysical {
		t.Fatalf("GC failed %+v %+v", before, after)
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
	if after.SyncOps-before.SyncOps != 2 || after.ReadBytes <= before.ReadBytes || after.WriteBytes-before.WriteBytes <= 10 {
		t.Fatalf("physical counters %+v => %+v", before, after)
	}
}
