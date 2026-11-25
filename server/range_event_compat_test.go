// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestRangeReaderAcceptsOnlyEmptyPGExtension(t *testing.T) {
	legacy, err := json.Marshal(rangeEvent{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacy, []byte("PGGroup")) {
		t.Fatal("compatibility reader changed legacy writer")
	}
	for _, test := range []struct {
		name                 string
		group                any
		field                string
		unsupported, invalid bool
	}{
		{name: "empty", group: client.ID{}, field: "PGGroup"},
		{name: "active", group: client.ID{1}, field: "PGGroup", unsupported: true},
		{name: "other extension", group: client.ID{}, field: "UnknownGroup", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var fields map[string]any
			if err := json.Unmarshal(legacy, &fields); err != nil {
				t.Fatal(err)
			}
			fields[test.field] = test.group
			raw, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			r := &rangeManager{}
			err = r.replay(controlRecord{Kind: rangeGenesis, Proof: client.Proof{RecordSequence: 1}, Data: raw})
			if test.unsupported {
				if !errors.Is(err, volume.ErrUnsupported) || r.initialized {
					t.Fatal("active PG state was ignored", err)
				}
				return
			}
			if test.invalid {
				if err == nil || r.initialized {
					t.Fatal("unknown state was ignored")
				}
				return
			}
			if err != nil || !r.initialized {
				t.Fatal("empty extension could not reopen", err)
			}
		})
	}
}
