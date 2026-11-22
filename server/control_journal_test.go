// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestControlJournalDurableChainGrowthAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.stqr")
	cluster, node := volume.ID{1}, volume.ID{2}
	j, err := openControlJournal(path, cluster, node, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var proofs []client.Proof
	payload := bytes.Repeat([]byte{7}, 32768)
	for i := 0; i < 80; i++ {
		payload[0] = byte(i)
		p, err := j.append(1, payload)
		if err != nil || p.Node != client.ID(node) || p.RecordSequence != uint64(i+1) || p.Digest == ([32]byte{}) {
			t.Fatal("durable append", i, err)
		}
		proofs = append(proofs, p)
	}
	if j.store.Size() <= 1<<20 {
		t.Fatal("journal did not grow beyond its initial capacity")
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	count := 0
	reopened, err := openControlJournal(path, cluster, node, false, func(record controlRecord) error {
		if record.Kind != 1 || record.Proof != proofs[count] || len(record.Data) != len(payload) || record.Data[0] != byte(count) {
			t.Fatal("replay altered an acknowledged record", count)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if count != len(proofs) {
		t.Fatal("replay lost durable records", count)
	}
	p, err := reopened.append(2, []byte("continue after process restart"))
	if err != nil || p.RecordSequence != 81 {
		t.Fatal("sequence did not continue", p, err)
	}
}

func TestControlJournalRejectsCorruptionAndValidPrefixRollback(t *testing.T) {
	for _, how := range []string{"payload", "truncated-header", "valid-prefix"} {
		t.Run(how, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "control.stqr")
			cluster, node := volume.ID{1}, volume.ID{2}
			j, err := openControlJournal(path, cluster, node, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := j.append(1, []byte("first")); err != nil {
				t.Fatal(err)
			}
			firstLength := j.length
			if _, err := j.append(2, []byte("second")); err != nil {
				t.Fatal(err)
			}
			h, err := j.store.Open(true)
			if err != nil {
				t.Fatal(err)
			}
			data, _, err := h.Get(controlLogKey)
			if err != nil {
				t.Fatal(err)
			}
			switch how {
			case "payload":
				data[controlRecordHeader] ^= 1
			case "truncated-header":
				data = data[:firstLength+20]
			case "valid-prefix":
				data = data[:firstLength]
			}
			// This deliberately commits otherwise valid volume bytes. The private
			// control-chain parser must detect corruption beyond volume checksums.
			_, err = h.Put(controlLogKey, data, volume.Options{})
			h.Close()
			if err != nil {
				t.Fatal(err)
			}
			j.close()
			if reopened, err := openControlJournal(path, cluster, node, false, nil); !errors.Is(err, volume.ErrCorrupt) {
				if reopened != nil {
					reopened.close()
				}
				t.Fatal("accepted corrupt/rewound journal", err)
			}
		})
	}
}

func TestControlJournalIdentityAndMissingStateNeverReformat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.stqr")
	cluster, node := volume.ID{1}, volume.ID{2}
	if j, err := openControlJournal(path, cluster, node, false, nil); err == nil || j != nil {
		t.Fatal("expected journal was silently recreated")
	}
	if err := os.WriteFile(path, []byte("unrelated data"), 0600); err != nil {
		t.Fatal(err)
	}
	if j, err := openControlJournal(path, cluster, node, true, nil); err == nil || j != nil {
		t.Fatal("unformatted existing file was reformatted")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "unrelated data" {
		t.Fatal("existing file changed")
	}
	other := filepath.Join(t.TempDir(), "control.stqr")
	j, err := openControlJournal(other, cluster, node, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.append(1, nil); err != nil {
		t.Fatal(err)
	}
	j.close()
	if reopened, err := openControlJournal(other, cluster, volume.ID{3}, false, nil); !errors.Is(err, volume.ErrIdentity) {
		if reopened != nil {
			reopened.close()
		}
		t.Fatal("foreign node claimed journal", err)
	}
}
