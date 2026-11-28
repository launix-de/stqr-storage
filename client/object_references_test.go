// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestObjectReferenceCapabilityNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		advertised uint64
		managed    bool
	}{
		// Independent wire allocations: format-only servers do not promise
		// durable pins, even when they understand opaque bucket formats.
		{"format-only", 1 << 17, false},
		{"references", 1 << 18, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, host := newWirePeer(t)
			peer.helloCaps.Store(tc.advertised)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := Dial(ctx, host.URL, "user", "credential", peer.bucket, "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			f := &Filesystem{store: c}
			_, managed := f.managedReferences()
			if c.ObjectReferencesSupported() != tc.managed || managed != tc.managed {
				t.Fatal("HELLO enabled the wrong reference lifetime policy", managed)
			}
			if !tc.managed {
				token, err := f.Retain(ctx, ObjectID{42})
				if err != nil || token != (ID{}) {
					t.Fatal("unsupported server attempted to pin", token, err)
				}
				if err := f.CollectOrphans(ctx); err != nil {
					t.Fatal("conservative collection fallback", err)
				}
				if peer.commits.Load() != 0 {
					t.Fatal("format-only server received reference-aware mutations")
				}
			}
		})
	}
}

func TestReadObjectDependenciesBeyondHistoricalReferenceLimit(t *testing.T) {
	peer, host := newWirePeer(t)
	const count = 8193
	e := Encoder{B: []byte("ORF1")}
	e.ID(peer.bucket)
	e.U32(count)
	expected := make([]ObjectID, count)
	for i := range expected {
		binary.LittleEndian.PutUint64(expected[i][:8], uint64(i+1))
		e.B = append(e.B, expected[i][:]...)
		e.B = append(e.B, make([]byte, 32)...)
		e.ID(ID{3})
		e.U8(2)
	}
	peer.mu.Lock()
	peer.data[ObjectReferenceRootID()] = e.B
	peer.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Dial(ctx, host.URL, "user", "credential", peer.bucket, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got, err := c.ReadObjectDependencies(ctx)
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatal("valid dynamic dependency image rejected", len(got), err)
	}
}
func TestReadObjectDependenciesRejectsMalformedCountBeforeAllocation(t *testing.T) {
	for _, kind := range []string{"forged-count", "truncated-entry", "bad-kind"} {
		t.Run(kind, func(t *testing.T) {
			peer, host := newWirePeer(t)
			e := Encoder{B: []byte("ORF1")}
			e.ID(peer.bucket)
			e.U32(1)
			e.B = append(e.B, make([]byte, 81)...)
			switch kind {
			case "forged-count":
				binary.LittleEndian.PutUint32(e.B[20:24], math.MaxUint32)
			case "truncated-entry":
				e.B = e.B[:len(e.B)-1]
			case "bad-kind":
				e.B[len(e.B)-1] = 3
			}
			peer.mu.Lock()
			peer.data[ObjectReferenceRootID()] = e.B
			peer.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := Dial(ctx, host.URL, "user", "credential", peer.bucket, "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err = c.ReadObjectDependencies(ctx); !errors.Is(err, ErrProtocol) {
				t.Fatal("malformed reference image accepted", err)
			}
		})
	}
}
