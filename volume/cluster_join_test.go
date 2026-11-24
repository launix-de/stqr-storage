// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"testing"
)

func TestEmptyClusterJoinRetainsIdentityAndRejectsHistory(t *testing.T) {
	old, next, node, id := ID{1}, ID{2}, ID{3}, ID{4}
	mem, err := NewMemory(2 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := Format(mem, FormatOptions{ClusterID: old, NodeID: node, VolumeID: id, Class: HDD})
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.SetLocalMetadata([]byte("old ownership")); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err = v.CheckEmpty(); err != nil {
		t.Fatal(err)
	}
	if err = v.JoinEmptyCluster(old, next, node, id); err != nil {
		t.Fatal(err)
	}
	if err = v.VerifyIdentity(next, node, id); err != nil {
		t.Fatal(err)
	}
	if err = v.JoinEmptyCluster(old, next, node, id); err != nil {
		t.Fatal("resume", err)
	}
	if err = v.JoinEmptyCluster(old, ID{8}, ID{7}, id); !errors.Is(err, ErrIdentity) {
		t.Fatal("foreign owner accepted", err)
	}
	h, err = v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := h.LocalMetadata()
	if err != nil || len(metadata) != 0 {
		t.Fatal("old provenance retained", err)
	}
	key := Key{Bucket: ID{6}, Object: ObjectID{7}}
	if _, err = h.Put(key, []byte("data"), Options{RequestID: ID{8}}); err != nil {
		t.Fatal(err)
	}
	if err = h.Delete(key, Options{RequestID: ID{9}}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	if err = v.CheckEmpty(); !errors.Is(err, ErrBusy) {
		t.Fatal("tombstone/receipt treated as empty", err)
	}
	if err = v.JoinEmptyCluster(next, old, node, id); !errors.Is(err, ErrBusy) {
		t.Fatal("used volume rebound", err)
	}
}
