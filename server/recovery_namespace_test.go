// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/launix-de/stqr-storage/volume"
)

func TestRecoverOperationsRequiresCanonicalSystemObjectIdentity(t *testing.T) {
	s := testServer(t)
	fake := Operation{ID: "00112233445566778899aabbccddeeff", Kind: "not-an-admin-operation", State: "requested", Digest: strings.Repeat("a", 64)}
	// Valid Operation-shaped JSON stored under another system name is not an
	// operation. Hashing the names means a textual prefix cannot be recovered.
	if err := s.writeBlob("ordinary-object", fake); err != nil {
		t.Fatal(err)
	}
	if err := s.writeBlob("admin-op/another-name", fake); err != nil {
		t.Fatal(err)
	}
	other := systemKey("admin-op/" + fake.ID)
	other.Bucket = volume.ID{0x99}
	data, err := json.Marshal(fake)
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.system.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Put(other, data, volume.Options{})
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	real := Operation{ID: "ffeeddccbbaa99887766554433221100", Kind: "node-short", State: "requested", Digest: strings.Repeat("b", 64)}
	if err = s.saveOperation(real); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverOperations(); err != nil {
		t.Fatal("ordinary JSON interpreted as an operation", err)
	}
	var got Operation
	if err = s.readBlob("admin-op/"+real.ID, &got); err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.Status != 503 {
		t.Fatal("real interrupted operation was not reconciled", got)
	}
	if err = s.readBlob("ordinary-object", &got); err != nil || got != fake {
		t.Fatal("ordinary object changed", got, err)
	}
}
