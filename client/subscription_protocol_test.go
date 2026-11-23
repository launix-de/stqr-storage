// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"errors"
	"testing"
)

func TestObjectUpdateCodecAndDigest(t *testing.T) {
	want := ObjectUpdate{Subscription: NewRequestID(), Cursor: 7, PreviousVersion: 3, NewVersion: 4, Parts: 1, Change: UpdateReplace, NewLength: 3, Data: []byte("abc")}
	raw, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeObjectUpdate(raw)
	if err != nil || got.Cursor != 7 || !bytes.Equal(got.Data, want.Data) {
		t.Fatal(got, err)
	}
	raw[len(raw)-1] ^= 1
	if _, err = DecodeObjectUpdate(raw); !errors.Is(err, ErrProtocol) {
		t.Fatal("accepted damaged update", err)
	}
}
func TestSubscriptionNeverAppliesGapsOrPartialVersions(t *testing.T) {
	for _, change := range []ObjectUpdate{
		{Cursor: 3, PreviousVersion: 1, NewVersion: 2, Parts: 1, Change: UpdateReplace, NewLength: 3, Data: []byte("new")},
		{Cursor: 2, PreviousVersion: 2, NewVersion: 3, Parts: 1, Change: UpdateReplace, NewLength: 3, Data: []byte("new")},
		{Cursor: 2, PreviousVersion: 1, NewVersion: 2, Parts: 2, Change: UpdateReplace, NewLength: 3, Data: []byte("new")},
	} {
		sub := &Subscription{ready: true, valid: true, cursor: 1, info: ObjectInfo{Version: 1, Length: 3}, data: []byte("old")}
		sub.applyLocked(change)
		if _, _, _, err := sub.Snapshot(); !errors.Is(err, ErrSubscriptionResync) {
			t.Fatal("served a gapped snapshot", err)
		}
	}
}
