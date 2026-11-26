// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestBufferedCleanupDurabilityAndTinyVolume(t *testing.T) {
	for _, size := range []int{4 << 20, 32 << 20} {
		t.Run(fmt.Sprintf("%dMiB", size>>20), func(t *testing.T) {
			dev := newCrash(size)
			v, err := Format(dev, FormatOptions{Class: HDD})
			if err != nil {
				t.Fatal(err)
			}
			key, keep := testKey(1), testKey(2)
			if _, err = testPut(v, key, []byte("doomed"), Options{RequestID: testID(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err = testPut(v, keep, []byte("keep"), Options{}); err != nil {
				t.Fatal(err)
			}
			before := v.IOStats().SyncOps
			h, _ := v.Open(true)
			generation, err := h.PurgeBatchAccepted([]ObjectRecord{{Key: key}})
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			buffered := size >= 32<<20
			if buffered && v.IOStats().SyncOps != before {
				t.Fatal("accepted metadata performed a physical fence")
			}
			if buffered {
				old, err := Open(dev.crash(), OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				got, _, err := testGet(old, key)
				if err != nil || string(got) != "doomed" {
					t.Fatal("unconfirmed cleanup changed durable image", err)
				}
				old.Close()
			}
			if err = v.CheckpointThrough(generation); err != nil {
				t.Fatal(err)
			}
			// A power cut after confirmation must preserve the purge and live payload.
			recoveredDevice := dev.crash()
			restored, err := Open(recoveredDevice, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = testGet(restored, key); !errors.Is(err, ErrNotFound) {
				t.Fatal("confirmed purge reappeared", err)
			}
			got, _, err := testGet(restored, keep)
			if err != nil || !bytes.Equal(got, []byte("keep")) {
				t.Fatal("unrelated payload changed", err)
			}
			rh, _ := restored.Open(true)
			generation, err = rh.ForgetRequestsAccepted([]ID{testID(1)})
			rh.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err = restored.CheckpointThrough(generation); err != nil {
				t.Fatal(err)
			}
			if err = restored.Scrub(); err != nil {
				t.Fatal(err)
			}
			restored.Close()
			confirmed, err := Open(recoveredDevice.crash(), OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ch, _ := confirmed.Open(false)
			_, found, err := ch.OperationStatus(testID(1))
			ch.Close()
			if err != nil || found {
				t.Fatal("confirmed receipt returned after crash", err)
			}
			confirmed.Close()
			v.Close()
		})
	}
}

func TestBufferedCleanupRejectsReadHandleAndDuplicateBatch(t *testing.T) {
	v, _ := newCheckpointGate(t)
	key := testKey(1)
	if _, err := testPut(v, key, []byte("keep"), Options{}); err != nil {
		t.Fatal(err)
	}
	h, _ := v.Open(false)
	_, err := h.PurgeBatchAccepted([]ObjectRecord{{Key: key}})
	h.Close()
	if !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	h, _ = v.Open(true)
	_, err = h.PurgeBatchAccepted([]ObjectRecord{{Key: key}, {Key: key}})
	h.Close()
	if !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	got, _, err := testGet(v, key)
	if err != nil || string(got) != "keep" {
		t.Fatal("rejected batch changed data", err)
	}
}
