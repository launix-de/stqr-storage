// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
)

func TestDialBucketIDSupportsLegacyUpgrade(t *testing.T) {
	peer, host := newWirePeer(t)
	c, err := DialBucket(context.Background(), host.URL, "user", "credential", peer.bucket.String(), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.BucketID() != peer.bucket {
		t.Fatal("legacy ID changed")
	}
}

func TestDialBucketRejectsInvalidCanonicalResponse(t *testing.T) {
	for _, tc := range []struct {
		name, selector string
		headers        []string
	}{
		{"missing", "photos", nil},
		{"malformed", "photos", []string{"bad"}},
		{"zero", "photos", []string{(ID{}).String()}},
		{"duplicate", "photos", []string{(ID{7}).String(), (ID{7}).String()}},
		{"retarget", "07000000000000000000000000000000", []string{(ID{8}).String()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}
				c, err := u.Upgrade(w, r, http.Header{BucketHeader: tc.headers})
				if err == nil {
					c.Close()
				}
			}))
			defer host.Close()
			c, err := DialBucket(context.Background(), host.URL, "user", "pw", tc.selector, "fs")
			if c != nil {
				c.Close()
			}
			if !errors.Is(err, ErrProtocol) {
				t.Fatal("accepted unbound or retargeted upgrade", err)
			}
		})
	}
}
