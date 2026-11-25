// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
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
			c, err := Dial(ctx, host.URL, "user", "credential", peer.bucket)
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
