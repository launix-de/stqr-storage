// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWebSocketCloseReplyRetiresConnectionAuthorization(t *testing.T) {
	ws := &wsSession{authorization: &connectionAuthorization{}, conn: &websocket.Conn{}}
	want := errors.New("close reply failed")
	replied := false
	ws.conn.SetCloseHandler(func(code int, text string) error {
		replied = true
		if !ws.authorization.invalid.Load() {
			t.Fatal("close acknowledged while connection still blocks exclusive leases")
		}
		if code != websocket.CloseNormalClosure || text != "done" {
			t.Fatalf("close arguments changed: %d %q", code, text)
		}
		return want
	})
	ws.invalidateBeforeCloseReply()
	if err := ws.conn.CloseHandler()(websocket.CloseNormalClosure, "done"); err != want || !replied {
		t.Fatalf("close reply result lost: %v, replied=%t", err, replied)
	}
}
