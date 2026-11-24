// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReplicaPrepareRecoveryVisibilityAndExactlyOnce(t *testing.T) {
	v, disk := memoryVolume(t, 8<<20)
	k, id := testKey(1), testID(88)
	if _, err := testPut(v, k, []byte("old"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.SetAdmission(func(c StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	binding := ReplicaBinding{Epoch: 1, Sequence: 7, SetHash: [32]byte{1}}
	h, _ := v.Open(true)
	result, err := h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id}, binding)
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if result.Offset != 3 || result.Info.Version != 2 {
		t.Fatal(result)
	}
	mustGet(t, v, k, []byte("old"))
	if _, ok, err := testOperationStatus(v, id); err != nil || ok {
		t.Fatalf("prepare became success: %v %v", ok, err)
	}
	if _, err := testAppend(v, k, []byte("other"), Options{}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if _, err := testPut(v, testKey(2), []byte("independent"), Options{}); err != nil {
		t.Fatal(err)
	}
	v = reopen(t, v, disk)
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, k, []byte("old"))
	h, _ = v.Open(true)
	if _, err = h.PublishReplica(id, binding); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	if err = h.DecideReplica(id, binding); err != nil {
		t.Fatal(err)
	}
	if err = h.AbortReplica(id, binding); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	h.Close()
	v = reopen(t, v, disk)
	mustGet(t, v, k, []byte("old"))
	h, _ = v.Open(true)
	published, err := h.PublishReplica(id, binding)
	h.Close()
	if err != nil || published != result {
		t.Fatalf("%+v %v", published, err)
	}
	v = reopen(t, v, disk)
	mustGet(t, v, k, []byte("old!"))
	h, _ = v.Open(true)
	retry, err := h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id}, binding)
	if err != nil || retry != result {
		t.Fatalf("%+v %v", retry, err)
	}
	if _, err = h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("?"), nil, Options{RequestID: id}, binding); !errors.Is(err, ErrRequestID) {
		t.Fatal(err)
	}
	h.Close()
	if err := v.SetAdmission(func(c StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaReadyIsDurableEvenWithAcceptedOption(t *testing.T) {
	disk := newCrash(16 << 20)
	v, err := Format(disk, FormatOptions{Class: SSD})
	if err != nil {
		t.Fatal(err)
	}
	k, id := testKey(1), testID(71)
	if _, err = testPut(v, k, []byte("old"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, _ := v.Open(true)
	_, err = h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id, Accepted: true}, ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(disk.crash(), OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	mustGet(t, recovered, k, []byte("old"))
	h, _ = recovered.Open(false)
	record, found, err := h.ReplicaStatus(id)
	h.Close()
	if err != nil || !found || record.State != ReplicaPrepared {
		t.Fatalf("READY was volatile: %+v %v %v", record, found, err)
	}
}

func TestReplicaAbortReleasesStorageAndFencesBinding(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k, id := testKey(1), testID(7)
	if _, err := testPut(v, k, []byte("original"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.SetAdmission(func(c StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	binding := ReplicaBinding{Epoch: 2, Sequence: 1, SetHash: [32]byte{2}}
	h, _ := v.Open(true)
	if _, err := h.PrepareMutation(opPutRange, k, 2, 3, bytes.NewBufferString("NEW"), nil, Options{RequestID: id}, binding); err != nil {
		t.Fatal(err)
	}
	bad := binding
	bad.Epoch++
	if err := h.DecideReplica(id, bad); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	if err := h.AbortReplica(id, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := h.PrepareMutation(opPutRange, k, 2, 3, bytes.NewBufferString("NEW"), nil, Options{RequestID: id}, binding); !errors.Is(err, ErrPrecondition) {
		t.Fatal("late prepare resurrected an aborted operation", err)
	}
	h.Close()
	mustGet(t, v, k, []byte("original"))
	if _, err := testAppend(v, k, []byte("!"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
}

type replicaGateReader struct {
	begin, release chan struct{}
	once           sync.Once
	data           *bytes.Reader
}

func (r *replicaGateReader) Read(b []byte) (int, error) {
	r.once.Do(func() { close(r.begin) })
	<-r.release
	return r.data.Read(b)
}

func TestReplicaReadersContinueWhilePrepareStreamsPayload(t *testing.T) {
	v, _ := memoryVolume(t, 4<<20)
	k := testKey(1)
	if _, err := testPut(v, k, []byte("committed"), Options{}); err != nil {
		t.Fatal(err)
	}
	reader := &replicaGateReader{begin: make(chan struct{}), release: make(chan struct{}), data: bytes.NewReader(bytes.Repeat([]byte{1}, 128<<10))}
	var release sync.Once
	defer release.Do(func() { close(reader.release) })
	done := make(chan error, 1)
	go func() {
		h, err := v.Open(true)
		if err != nil {
			done <- err
			return
		}
		defer h.Close()
		_, err = h.PrepareMutation(opPut, k, 0, 128<<10, reader, nil, Options{RequestID: testID(90)}, ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}})
		done <- err
	}()
	select {
	case <-reader.begin:
	case <-time.After(time.Second):
		t.Fatal("prepare did not start streaming")
	}
	type answer struct {
		data []byte
		err  error
	}
	read := make(chan answer, 1)
	go func() { b, _, err := testGet(v, k); read <- answer{b, err} }()
	select {
	case a := <-read:
		if a.err != nil || string(a.data) != "committed" {
			t.Fatalf("read %q %v", a.data, a.err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader waited for unfinished PREPARE payload")
	}
	release.Do(func() { close(reader.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare did not finish")
	}
	mustGet(t, v, k, []byte("committed"))
}

func TestReplicaTinyAppendsReuseInvisibleArenaTail(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip} {
		t.Run(codec.String(), func(t *testing.T) {
			v, _ := memoryVolume(t, 8<<20)
			k := testKey(1)
			for i := 0; i < 256; i++ {
				h, err := v.Open(true)
				if err != nil {
					t.Fatal(err)
				}
				binding := ReplicaBinding{Epoch: 1, Sequence: uint64(i + 1), SetHash: [32]byte{1}}
				id := testID(uint64(i + 1000))
				result, err := h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("x"), nil, Options{RequestID: id, Compression: &codec}, binding)
				if err != nil {
					t.Fatal(err)
				}
				if result.Offset != uint64(i) {
					t.Fatal(result)
				}
				if err = h.DecideReplica(id, binding); err != nil {
					t.Fatal(err)
				}
				if _, err = h.PublishReplica(id, binding); err != nil {
					t.Fatal(err)
				}
				h.Close()
			}
			mustGet(t, v, k, bytes.Repeat([]byte("x"), 256))
			h, _ := v.Open(false)
			allocated, err := h.AllocatedPhysical(k)
			h.Close()
			if err != nil || allocated > 2*PageSize {
				t.Fatalf("tiny appends grew per-operation allocation: %d %v", allocated, err)
			}
			if err := v.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicaCopyOnWriteTouchesOnlyDeltaAndRetainsAliasedExtents(t *testing.T) {
	for _, codec := range []Compression{Raw, Gzip, LZ4, XZ} {
		t.Run(codec.String(), func(t *testing.T) { checkReplicaCopyOnWrite(t, codec) })
	}
}
func checkReplicaCopyOnWrite(t *testing.T, codec Compression) {
	v, disk := memoryVolume(t, 16<<20)
	k := testKey(4)
	original := bytes.Repeat([]byte("a"), 2<<20)
	if _, err := testPut(v, k, original, Options{Compression: &codec}); err != nil {
		t.Fatal(err)
	}
	if err := v.SetAdmission(func(StorageChange) (uint64, error) { return 0, nil }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		binding := ReplicaBinding{Epoch: 1, Sequence: uint64(i + 1), SetHash: [32]byte{1}}
		id := testID(uint64(100 + i))
		before := v.Stats().IO.WriteBytes
		h, _ := v.Open(true)
		if _, err := h.PrepareMutation(opPutRange, k, uint64(10+i*3), 1, bytes.NewBufferString("X"), nil, Options{RequestID: id}, binding); err != nil {
			t.Fatal(err)
		}
		if err := h.DecideReplica(id, binding); err != nil {
			t.Fatal(err)
		}
		if _, err := h.PublishReplica(id, binding); err != nil {
			t.Fatal(err)
		}
		h.Close()
		if written := v.Stats().IO.WriteBytes - before; written > 256<<10 {
			t.Fatalf("one-byte change rewrote %d bytes", written)
		}
		original[10+i*3] = 'X'
		v = reopen(t, v, disk)
		mustGet(t, v, k, original)
		if err := v.Scrub(); err != nil {
			t.Fatal(err)
		}
	}
	// Updating two slices of one underlying checksum block must see earlier
	// writes in the same transaction, and freeing them must happen only once.
	if _, err := testPutRange(v, k, 9, []byte("ABCDEFGHIJK"), Options{}); err != nil {
		t.Fatal(err)
	}
	copy(original[9:], "ABCDEFGHIJK")
	mustGet(t, v, k, original)
	if err := v.Compact(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, v, k, original)
	if err := testDelete(v, k, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := v.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaPowerCutAtEveryPrepareDecisionAndPublicationStep(t *testing.T) {
	for phase := 0; phase < 3; phase++ {
		for _, through := range []bool{false, true} {
			base := newCrash(1 << 20)
			v, err := Format(base, FormatOptions{Class: SSD})
			if err != nil {
				t.Fatal(err)
			}
			k, id := testKey(1), testID(200)
			binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
			if _, err = testAppend(v, k, []byte("confirmed"), Options{}); err != nil {
				t.Fatal(err)
			}
			if phase > 0 {
				h, _ := v.Open(true)
				_, err = h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id}, binding)
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase > 1 {
				h, _ := v.Open(true)
				err = h.DecideReplica(id, binding)
				h.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			for fail := 1; fail <= 36; fail++ {
				d := base.crash()
				w, err := Open(d, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				d.steps = 0
				d.fail = fail
				d.writeThrough = through
				d.persistFailedSync = true
				h, _ := w.Open(true)
				switch phase {
				case 0:
					_, err = h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id}, binding)
				case 1:
					err = h.DecideReplica(id, binding)
				case 2:
					_, err = h.PublishReplica(id, binding)
				}
				h.Close()
				if err != nil && !errors.Is(err, ErrRecovery) {
					t.Fatalf("phase=%d fail=%d: %v", phase, fail, err)
				}
				recovered, err := Open(d.crash(), OpenOptions{})
				if err != nil {
					t.Fatalf("phase=%d fail=%d reopen: %v", phase, fail, err)
				}
				got, _, err := testGet(recovered, k)
				if err != nil || string(got) != "confirmed" && string(got) != "confirmed!" {
					t.Fatalf("phase=%d fail=%d read %q %v", phase, fail, got, err)
				}
				if phase < 2 && string(got) != "confirmed" {
					t.Fatal("unpublished data became visible")
				}
				h, _ = recovered.Open(true)
				if _, err = h.PrepareMutation(opAppend, k, 0, 1, bytes.NewBufferString("!"), nil, Options{RequestID: id}, binding); err != nil {
					t.Fatal(err)
				}
				if err = h.DecideReplica(id, binding); err != nil {
					t.Fatal(err)
				}
				if _, err = h.PublishReplica(id, binding); err != nil {
					t.Fatal(err)
				}
				h.Close()
				mustGet(t, recovered, k, []byte("confirmed!"))
				if err = recovered.Scrub(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
