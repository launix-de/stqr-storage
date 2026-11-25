// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"github.com/launix-de/stqr-storage/volume"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

// Runs only against disposable fixtures, never a user's daemon or bucket.
func TestCClient(t *testing.T) {
	if os.Getenv("STQR_TEST_C") != "1" {
		t.Skip("set STQR_TEST_C=1; requires cc, make and OpenSSL development files")
	}
	root, err := filepath.Abs("../client/c")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "c-integration")
	args := []string{"-std=c11", "-Wall", "-Wextra", "-Werror", "-g", "-O1", "-I" + filepath.Join(root, "include"), "-I" + filepath.Join(root, "src"), filepath.Join(root, "src/core.c"), filepath.Join(root, "src/posix.c"), filepath.Join(root, "tests/integration.c"), "-lcrypto", "-o", binary}
	if os.Getenv("STQR_C_SANITIZE") == "1" {
		args = append(args, "-fsanitize=address,undefined", "-fno-omit-frame-pointer")
	}
	if out, err := exec.Command("cc", args...).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	run := func(t *testing.T, url, user, password, bucket, mode string) {
		t.Helper()
		cmd := exec.Command(binary)
		cmd.Env = append(os.Environ(), "STQR_TEST_URL="+url, "STQR_TEST_USER="+user, "STQR_TEST_PASSWORD="+password, "STQR_TEST_BUCKET="+bucket, "STQR_TEST_MODE="+mode)
		if mode == "" {
			cmd.Env = cmd.Env[:len(cmd.Env)-1]
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("C client: %v\n%s", err, out)
		}
	}
	t.Run("objects", func(t *testing.T) {
		s := testServer(t)
		addTestVolume(t, s, "c-client", "hdd", 32<<20)
		b, _ := createProtocolBucket(t, s, "c-client")
		host := httptest.NewServer(s.Handler())
		t.Cleanup(host.Close)
		admin := s.cfg.Auth.BootstrapAdmin
		run(t, host.URL, admin.Username, admin.Password, client.ID(b.ID()).String(), "")
	})
	for _, mode := range []string{"transient", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			upgrade := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}
			var requests int
			done := make(chan struct{})
			var first client.Message
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				c, err := upgrade.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				_, raw, err := c.ReadMessage()
				if err != nil {
					return
				}
				m, err := client.DecodeMessage(raw)
				if err != nil {
					return
				}
				hello := client.Encoder{}
				hello.U16(2)
				hello.U64(0)
				hello.ID(client.ID{})
				hello.ID(client.ID{})
				hello.U64(0)
				hello.U64(0)
				c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(200, hello.B, "")}))
				for {
					_, raw, err = c.ReadMessage()
					if err != nil {
						return
					}
					m, err = client.DecodeMessage(raw)
					if err != nil {
						return
					}
					if requests == 0 {
						first = m
					} else if m.ID != first.ID || m.Opcode != first.Opcode || !bytes.Equal(m.Body, first.Body) {
						t.Error("retry changed operation or identity")
						return
					}
					requests++
					status := 503
					var result []byte
					if mode == "transient" && requests == 4 {
						status = 200
						e := client.Encoder{}
						e.U64(5)
						e.Bool(false)
						e.U32(0)
						e.ID(client.ID{})
						e.U8(0)
						e.U16(5)
						result = e.B
					}
					if err = c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(status, result, "waiting for bucket growth credits")})); err != nil {
						return
					}
				}
			}))
			defer host.Close()
			start := time.Now()
			run(t, host.URL, "user", "password", client.ID{1}.String(), mode)
			<-done
			if mode == "transient" && requests != 4 {
				t.Fatalf("requests=%d", requests)
			}
			if mode == "unavailable" && (requests < 2 || time.Since(start) < 3*time.Second || time.Since(start) > 6*time.Second) {
				t.Fatalf("unbounded/busy retry: requests=%d elapsed=%s", requests, time.Since(start))
			}
		})
	}
	t.Run("cluster-routing", func(t *testing.T) {
		source, target := startAutomaticDaemonPair(t, false)
		addTestVolume(t, source.node, "c-routing", "hdd", 16<<20)
		remote := addTestVolume(t, target.node, "c-routing", "hdd", 16<<20)
		bucket, _ := createProtocolBucket(t, source.node, "c-routing")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin := source.node.cfg.Auth.BootstrapAdmin
		c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID()))
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []client.ObjectID{{1}, {0x81}} {
			if _, err = c.Put(ctx, id, []byte("old"), client.PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		c.Close()
		before, _ := source.node.routingPatch(bucket.ID(), volume.ObjectID{0x81})
		_, moving, err := client.SplitRange(before.Entries[0])
		if err != nil {
			t.Fatal(err)
		}
		targetVolume, _ := parseID(remote.VolumeID)
		plan, err := planRangeMove(before, moving, target.node.control.ledger.node, targetVolume, uint8(volume.HDD), volume.Raw, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if err = source.node.scopes.move(ctx, plan); err != nil {
			t.Fatal(err)
		}
		run(t, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID()).String(), "routing")
	})
	for _, mode := range []string{"disconnect", "digest", "malformed", "wrong-id", "varint", "text", "subscription-digest"} {
		t.Run(mode, func(t *testing.T) {
			upgrade := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := upgrade.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				_, raw, err := c.ReadMessage()
				if err != nil {
					return
				}
				m, err := client.DecodeMessage(raw)
				if err != nil {
					return
				}
				hello := client.Encoder{}
				hello.U16(2)
				hello.U64(0)
				hello.ID(client.ID{})
				hello.ID(client.ID{})
				hello.U64(0)
				hello.U64(0)
				c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(200, hello.B, "")}))
				_, raw, err = c.ReadMessage()
				if err != nil {
					return
				}
				m, err = client.DecodeMessage(raw)
				if err != nil || mode == "disconnect" {
					return
				}
				if mode == "subscription-digest" {
					update := client.ObjectUpdate{Subscription: client.NewRequestID(), Cursor: 1, Parts: 1, Data: []byte("x"), NewLength: 1}
					body, _ := update.MarshalBinary()
					body[len(body)-1] ^= 1
					c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Push, Opcode: client.ObjectUpdateMessage, Body: body}))
					return
				}
				if mode == "wrong-id" {
					m.ID[0] ^= 1
					c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(404, nil, "")}))
					return
				}
				if mode == "varint" {
					c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: []byte{0xc8, 0x81, 0x00, 0x01, 0x00}}))
					return
				}
				if mode == "text" {
					c.WriteMessage(websocket.TextMessage, []byte("invalid"))
					return
				}
				if mode == "malformed" {
					c.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3})
					return
				}
				result := client.Encoder{}
				result.U64(1)
				result.Bool(false)
				result.U32(0)
				result.ID(client.ID{})
				result.U8(0)
				result.U16(2)
				result.U8(0)
				result.U64(1)
				result.B = append(result.B, make([]byte, 32)...)
				result.Blob([]byte("x"))
				c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(200, result.B, "")}))
			}))
			defer host.Close()
			clientMode := mode
			if mode == "wrong-id" || mode == "varint" || mode == "text" {
				clientMode = "malformed"
			}
			run(t, host.URL, "test", "test", "0123456789abcdef0123456789abcdef", clientMode)
		})
	}
}
