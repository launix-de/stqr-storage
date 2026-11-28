// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRestartRetentionPublishedStateAndReceipt(t *testing.T) {
	d := newCrash(32 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(701)
	deleted := testKey(702)
	if _, err = testPut(v, key, []byte("old"), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(v, deleted, []byte("delete"), Options{}); err != nil {
		t.Fatal(err)
	}
	before := v.IOStats().SyncOps
	if _, err = testCAS(v, key, 0, []byte("old"), []byte("new"), Options{Accepted: true, WholeObject: true, RequestID: testID(701)}); err != nil {
		t.Fatal(err)
	}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	appendID := testID(703)
	if _, err = h.Append(key, []byte("-append"), Options{Accepted: true, RequestID: appendID}); err != nil {
		t.Fatal(err)
	}
	if err = h.Delete(deleted, Options{Accepted: true, RequestID: testID(704)}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	path := filepath.Join(t.TempDir(), "sealed.wb")
	receipt, err := v.RetainAccepted(path, testID(705))
	if err != nil {
		t.Fatal(err)
	}
	if v.IOStats().SyncOps != before {
		t.Fatal("retention secretly synced")
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	mustGet(t, w, key, []byte("new-append"))
	h, err = w.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = h.Get(deleted); !errors.Is(err, ErrNotFound) {
		t.Fatal("accepted deletion lost", err)
	}
	if _, err = h.Append(key, []byte("-append"), Options{Accepted: true, RequestID: appendID}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	mustGet(t, w, key, []byte("new-append"))
	if w.IOStats().SyncOps != 0 {
		t.Fatal("restore/append replay secretly synced")
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, w, key, []byte("new-append"))
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	x, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal("exact retained canonical checkpoint rejected", err)
	}
	defer x.Close()
	mustGet(t, x, key, []byte("new-append"))
}
func TestRestartRetentionSpillIsLazyAndMutable(t *testing.T) {
	d := newCrash(64 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(710)
	payload := bytes.Repeat([]byte("retained-page"), 700000)
	before := v.IOStats().SyncOps
	if _, err = testPut(v, key, payload, Options{Accepted: true, RequestID: testID(710)}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "spill.wb")
	receipt, err := v.RetainAccepted(path, testID(711))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.deferred == nil || w.deferred.store.retained == nil || len(w.deferred.store.pages) != 0 || w.deferred.store.count != 0 {
		t.Fatal("retention eagerly populated dirty cache")
	}
	h, err := w.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := h.GetRange(key, 12345, 5000)
	h.Close()
	if err != nil || !bytes.Equal(got, payload[12345:17345]) {
		t.Fatal("lazy retained range", err)
	}
	if _, err = testCAS(w, key, 0, payload[:5], []byte("after"), Options{Accepted: true, RequestID: testID(712)}); err != nil {
		t.Fatal(err)
	}
	copy(payload, "after")
	mustGet(t, w, key, payload)
	if v.IOStats().SyncOps != before || w.IOStats().SyncOps != 0 {
		t.Fatal("retention forced a fence")
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, w, key, payload)
}
func TestRestartRetentionRejectsMissingTornWrongAndUnrelated(t *testing.T) {
	d := newCrash(8 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(720)
	if _, err = testPut(v, key, []byte("pending"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image.wb")
	receipt, err := v.RetainAccepted(path, testID(720))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	bad := receipt
	bad.Handoff[0]++
	for _, src := range []RestartSource{{path + "missing", receipt}, {path, bad}} {
		if _, err = Open(d, OpenOptions{Restart: &src}); !errors.Is(err, ErrRecovery) {
			t.Fatal("invalid receipt accepted", err)
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-10] ^= 1
	torn := path + "torn"
	if err = os.WriteFile(torn, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(d, OpenOptions{Restart: &RestartSource{torn, receipt}}); !errors.Is(err, ErrRecovery) {
		t.Fatal("torn required image accepted", err)
	}
	// Ordinary native publication beyond the retained cut is not accepted as
	// proof of ancestry merely because its generation happens to be higher.
	canonical, err := Open(d, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(canonical, testKey(721), []byte("unrelated"), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err = testPut(canonical, testKey(722), []byte("more"), Options{}); err != nil {
		t.Fatal(err)
	}
	canonical.Close()
	if _, err = Open(d, OpenOptions{Restart: &RestartSource{path, receipt}}); !errors.Is(err, ErrRecovery) {
		t.Fatal("unrelated higher checkpoint accepted", err)
	}
}

// A retained image is a restart baseline, never authority to roll a newer
// canonical descendant back. Header ancestry alone still cannot confirm a
// physical fence: explicit confirmation seals only the canonical cut, while a
// newer accepted overlay must stay present and unconfirmed.
func TestRestartRetentionCanonicalDescendantNeverRollsBack(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(740)
	if _, err = testPut(v, key, []byte("old"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "baseline.image")
	receipt, err := v.RetainAccepted(path, testID(740))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err = testCAS(w, key, 0, []byte("old"), []byte("confirmed-new"), Options{WholeObject: true}); err != nil {
		t.Fatal(err)
	}
	confirmed := w.sb.Generation
	if _, err = testPut(w, key, []byte("discarded-tail"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	// Simulated process death: discard private memory, preserve native OS pages.
	if err = w.Abort(); err != nil {
		t.Fatal(err)
	}
	// Positive canonical ancestry makes the now-obsolete sidecar unnecessary.
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	x, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	mustGet(t, x, key, []byte("confirmed-new"))
	if x.IOStats().SyncOps != 0 || x.IsDurableThrough(confirmed) {
		t.Fatal("header visibility falsely confirmed a fence")
	}
	if _, err = testPut(x, key, []byte("newer-overlay"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	later := x.sb.Generation
	if err = x.CheckpointThrough(confirmed); err != nil {
		t.Fatal(err)
	}
	mustGet(t, x, key, []byte("newer-overlay"))
	if !x.IsDurableThrough(confirmed) || x.IsDurableThrough(later) || x.deferred == nil {
		t.Fatal("canonical fence lost or confirmed newer overlay")
	}
	if x.IOStats().SyncOps != 1 {
		t.Fatal("canonical baseline should require exactly one explicit fence", x.IOStats())
	}
	if err = x.Sync(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, x, key, []byte("newer-overlay"))
	reboot, err := Open(d.crash(), OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer reboot.Close()
	mustGet(t, reboot, key, []byte("newer-overlay"))
}

func TestRestartRetentionAccountingUsesBoundAggregatesWithoutScan(t *testing.T) {
	d := newCrash(16 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	cfg := AccountingOptions{CachePath: filepath.Join(t.TempDir(), "ordinary.cache"), Stamp: cacheOwnershipStamp(1), Include: cacheInclude}
	if ready, err := v.ConfigureAccounting(cfg); err != nil || !ready {
		t.Fatal(ready, err)
	}
	for i := uint64(750); i < 800; i++ {
		if _, err = testPut(v, testKey(i), []byte("aggregate"), Options{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	want := accountingUsage(t, v)
	path := filepath.Join(t.TempDir(), "accounted.image")
	receipt, err := v.RetainAccepted(path, testID(750))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	before := w.IOStats()
	if ready, err := w.ConfigureAccounting(cfg); err != nil || !ready {
		t.Fatal("exact retained aggregate cut required census", ready, err)
	}
	if !reflect.DeepEqual(accountingUsage(t, w), want) {
		t.Fatal("retained counters disagree")
	}
	if w.IOStats() != before {
		t.Fatal("aggregate restore accessed object storage", before, w.IOStats())
	}
	if w.IsDurableThrough(receipt.Generation) {
		t.Fatal("aggregate cache asserted physical durability")
	}
	// Same data but changed ownership cannot reuse another predicate's totals.
	cfg.Stamp = cacheOwnershipStamp(2)
	if ready, err := w.ConfigureAccounting(cfg); err != nil || ready {
		t.Fatal("changed ownership reused stale totals", ready, err)
	}
}

func TestRestartRetentionNativeLineageCodecAndVersionExclusion(t *testing.T) {
	d := newCrash(2 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	sb := v.Header()
	sb.RestartLineage = testID(860)
	encoded, err := sb.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if diskOrder.Uint16(encoded[8:]) != 6 {
		t.Fatal("lineage must exclude old native readers")
	}
	decoded, err := DecodeSuperblock(encoded)
	if err != nil || decoded != sb {
		t.Fatal("native lineage not round-tripped", err)
	}
	// A true old v5 native header had no lineage; the new decoder can read it.
	old := bytes.Clone(encoded)
	diskOrder.PutUint16(old[8:], 5)
	clear(old[384:400])
	diskOrder.PutUint32(old[PageSize-4:], checksum(old[:PageSize-4]))
	decoded, err = DecodeSuperblock(old)
	if err != nil || decoded.RestartLineage != (ID{}) {
		t.Fatal("v5 compatibility lost", err)
	}
	// Any unsupported copy prevents fallback to an older otherwise valid header.
	newer := bytes.Clone(encoded)
	diskOrder.PutUint16(newer[8:], 7)
	diskOrder.PutUint32(newer[PageSize-4:], checksum(newer[:PageSize-4]))
	copy(d.live[:PageSize], old)
	copy(d.live[PageSize:2*PageSize], newer)
	if _, err = loadSuperblocks(d); !errors.Is(err, ErrVersion) {
		t.Fatal("unsupported newer copy silently ignored", err)
	}
}

func TestRestartRetentionCanonicalFenceFailureCannotConfirm(t *testing.T) {
	d := newCrash(8 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(870)
	if _, err = testPut(v, key, []byte("visible"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image")
	receipt, err := v.RetainAccepted(path, testID(870))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	x, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer x.Abort()
	generation := x.Header().Generation
	if x.IsDurableThrough(generation) {
		t.Fatal("unverified header confirmed old cut")
	}
	d.fail = d.steps + 1
	if err = x.CheckpointThrough(generation); !errors.Is(err, ErrRecovery) {
		t.Fatal("failed physical fence acknowledged", err)
	}
	if x.IsDurableThrough(generation) {
		t.Fatal("failed canonical fence advanced confirmed cut")
	}
}

func TestRestartRetentionCorruptPayloadCannotConfirm(t *testing.T) {
	d := newCrash(8 << 20)
	v, err := Format(d, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	key := testKey(880)
	if _, err = testPut(v, key, []byte("retained payload"), Options{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image")
	receipt, err := v.RetainAccepted(path, testID(880))
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := Open(d, OpenOptions{Restart: &RestartSource{path, receipt}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	obj, err := w.loadObject(key)
	if err != nil {
		t.Fatal(err)
	}
	r := w.deferred.store.retained
	if r.root.level != 0 {
		t.Fatal("small fixture unexpectedly multilevel")
	}
	page, err := r.node(r.root)
	if err != nil {
		t.Fatal(err)
	}
	off := obj.Spans[0].Location / PageSize * PageSize
	var imageOffset uint64
	for i := 0; i < int(diskOrder.Uint16(page[10:])); i++ {
		raw := page[32+i*restartIndexRecord:]
		if diskOrder.Uint64(raw) == off {
			imageOffset = diskOrder.Uint64(raw[8:])
			break
		}
	}
	if imageOffset == 0 {
		t.Fatal("retained payload missing")
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte{0xff}, int64(imageOffset)); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	if _, _, err = testGet(w, key); err == nil {
		t.Fatal("corrupt payload returned to reader")
	}
	if err = w.Sync(); err == nil {
		t.Fatal("corrupt retained payload physically confirmed")
	}
	if w.IsDurableThrough(receipt.Generation) {
		t.Fatal("corruption advanced confirmed cut")
	}
}
