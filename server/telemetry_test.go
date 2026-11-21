// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func TestTelemetryCoalescesPerSourceAndPreservesFairness(t *testing.T) {
	q := newStatsSendQueue()
	x, y := client.ID{1}, client.ID{2}
	if !q.put(x, [][]byte{[]byte("old-1"), []byte("old-2")}) || !q.put(y, [][]byte{[]byte("peer")}) || !q.put(x, [][]byte{[]byte("new-1"), []byte("new-2")}) {
		t.Fatal("queue refused bounded snapshots")
	}
	if got := bytes.Join(q.take(), nil); string(got) != "new-1new-2" {
		t.Fatal("did not replace complete source batch", string(got))
	}
	q.put(x, [][]byte{[]byte("newer")})
	if got := bytes.Join(q.take(), nil); string(got) != "peer" {
		t.Fatal("frequent source starved another node", string(got))
	}
	if got := bytes.Join(q.take(), nil); string(got) != "newer" {
		t.Fatal("lost next source snapshot", string(got))
	}
	if q.take() != nil || q.bytes != 0 {
		t.Fatal("queue did not release batches")
	}
	q.put(x, [][]byte{[]byte("pending")})
	q.clear()
	if q.take() != nil || q.bytes != 0 || len(q.pending) != 0 {
		t.Fatal("unsubscribe retained telemetry")
	}
}
