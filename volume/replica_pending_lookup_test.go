// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"errors"
	"testing"
)

func TestHasPendingReplicaExactKeyLifecycleAndHandle(t *testing.T) {
	v, _ := memoryVolume(t, 8<<20)
	key, other := testKey(1), testKey(2)
	id := testID(77)
	binding := ReplicaBinding{Epoch: 1, Sequence: 1, SetHash: [32]byte{1}}
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	check := func(want bool) {
		t.Helper()
		got, e := h.HasPendingReplica(key)
		if e != nil || got != want {
			t.Fatal(got, want, e)
		}
		got, e = h.HasPendingReplica(other)
		if e != nil || got {
			t.Fatal("unrelated key", got, e)
		}
	}
	check(false)
	if _, err = h.PrepareMutation(opPut, key, 0, 3, bytes.NewReader([]byte("new")), nil, Options{RequestID: id}, binding); err != nil {
		t.Fatal(err)
	}
	check(true)
	if err = h.DecideReplica(id, binding); err != nil {
		t.Fatal(err)
	}
	check(true)
	if _, err = h.PublishReplica(id, binding); err != nil {
		t.Fatal(err)
	}
	check(false)
	h.Close()
	if _, err = h.HasPendingReplica(key); !errors.Is(err, ErrClosed) {
		t.Fatal("closed handle", err)
	}
}
