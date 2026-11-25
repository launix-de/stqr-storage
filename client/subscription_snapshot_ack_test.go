// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSubscriptionInitialSnapshotDoesNotQueueRedundantACK(t *testing.T) {
	testSubscriptionSnapshotACK(t, false)
}
func TestSubscriptionBufferedUpdateIsAcknowledgedAfterInitialSnapshot(t *testing.T) {
	testSubscriptionSnapshotACK(t, true)
}
func testSubscriptionSnapshotACK(t *testing.T, bufferedUpdate bool) {
	var acknowledgments atomic.Int64
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			m, err := DecodeMessage(raw)
			if err != nil {
				t.Error(err)
				return
			}
			result := Encoder{}
			switch m.Opcode {
			case Hello:
				result.U16(2)
				result.U64(CapabilityObjectSubscriptions)
				result.ID(ID{1})
				result.ID(ID{2})
				result.U64(1)
				result.U64(1)
			case ObjectSubscribe:
				d := Decoder{B: m.Body}
				d.Take(80)
				id := d.ID()
				result.ID(id)
				result.U64(1)
				fixtureInfo(&result, 1, 4)
				payload := []byte("data")
				sum := sha256.Sum256(payload)
				result.U8(0)
				result.U64(uint64(len(payload)))
				result.B = append(result.B, sum[:]...)
				result.Blob(payload)
				if bufferedUpdate {
					update := ObjectUpdate{Subscription: id, Cursor: 2, Parts: 1, Change: UpdateReplace, NewLength: 4, Data: []byte("next")}
					body, e := update.MarshalBinary()
					if e != nil {
						t.Error(e)
						return
					}
					if e = conn.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Push, Opcode: ObjectUpdateMessage, Body: body})); e != nil {
						return
					}
				}
			case SubscriptionAck:
				acknowledgments.Add(1)
				continue
			case objectStat:
				// This response is a barrier after earlier messages on the same socket.
				fixtureInfo(&result, 1, 4)
			case ObjectUnsubscribe:
				if m.Kind == FireAndForget {
					continue
				}
			default:
				t.Errorf("unexpected opcode %x", m.Opcode)
				return
			}
			if err = conn.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: ResponseBody(200, result.B, "")})); err != nil {
				return
			}
		}
	}))
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, host.URL, "user", "password", BucketID{7}, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err := c.Subscribe(ctx, ObjectID{1}, SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)
	if _, err = c.Stat(ctx, ObjectID{1}); err != nil {
		t.Fatal(err)
	}
	want := int64(0)
	wantData := "data"
	wantCursor := uint64(1)
	if bufferedUpdate {
		want = 1
		wantData = "next"
		wantCursor = 2
	}
	if n := acknowledgments.Load(); n != want {
		t.Fatalf("snapshot ACKs=%d want=%d", n, want)
	}
	data, info, _, err := sub.Snapshot()
	if err != nil || string(data) != wantData || info.Proof.Cursor != wantCursor {
		t.Fatal("buffered snapshot update lost", string(data), info, err)
	}
}
