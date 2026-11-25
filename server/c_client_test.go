// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/launix-de/stqr-storage/volume"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	args := []string{"-std=c11", "-Wall", "-Wextra", "-Werror", "-g", "-O1", "-I" + filepath.Join(root, "include"), "-I" + filepath.Join(root, "src"), filepath.Join(root, "src/core.c"), filepath.Join(root, "src/posix.c"), filepath.Join(root, "tests/integration.c"), "-lcrypto", "-pthread", "-o", binary}
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
	t.Run("reuse", func(t *testing.T) {
		s := testServer(t)
		addTestVolume(t, s, "c-reuse", "hdd", 4<<20)
		b, _ := createProtocolBucket(t, s, "c-reuse")
		host := httptest.NewServer(s.Handler())
		defer host.Close()
		v := b.allVolumes()[0]
		baseline := v.Stats()
		admin := s.cfg.Auth.BootstrapAdmin
		run(t, host.URL, admin.Username, admin.Password, client.ID(b.ID()).String(), "reuse")
		stats := v.Stats()
		if stats.Objects != baseline.Objects+1 || stats.DataPhysical != baseline.DataPhysical+8192 {
			t.Fatalf("C overwrite/CAS/delete loops retained payload: before=%+v after=%+v", baseline, stats)
		}
		rawID, err := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
		var oid client.ObjectID
		copy(oid[:], rawID)
		if err != nil {
			t.Fatal(err)
		}
		c, err := client.Dial(context.Background(), host.URL, admin.Username, admin.Password, client.ID(b.ID()))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		got, _, err := c.Get(context.Background(), oid)
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x9c}, 8192)) {
			t.Fatalf("Go could not read C client's final object: %d bytes, %v", len(got), err)
		}
		if err := v.Scrub(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("references", func(t *testing.T) {
		s := testServer(t)
		addTestVolume(t, s, "c-references", "hdd", 8<<20)
		b, _ := createProtocolBucket(t, s, "c-references")
		host := httptest.NewServer(s.Handler())
		defer host.Close()
		admin := s.cfg.Auth.BootstrapAdmin
		run(t, host.URL, admin.Username, admin.Password, client.ID(b.ID()).String(), "references")
		if err := b.allVolumes()[0].Scrub(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("independent-fence", func(t *testing.T) {
		upgrade := websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}
		syncStarted, statSeen := make(chan struct{}), make(chan struct{})
		var syncOnce, statOnce sync.Once
		var sequence atomic.Uint64
		var connections atomic.Int32
		boot := client.ID{8}
		host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := upgrade.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer c.Close()
			connections.Add(1)
			for {
				_, raw, err := c.ReadMessage()
				if err != nil {
					return
				}
				m, err := client.DecodeMessage(raw)
				if err != nil {
					t.Error(err)
					return
				}
				result := client.Encoder{}
				var mark *client.CommitMark
				switch m.Opcode {
				case client.Hello:
					result.U16(2)
					result.U64(client.CapabilityDurability)
					result.ID(client.ID{})
					result.ID(client.ID{})
					result.U64(0)
					result.U64(0)
				case client.ObjectAccepted:
					result.U64(5)
					result.Bool(false)
					result.U32(0)
					result.ID(client.ID{})
					result.U8(0)
					result.U16(5)
					value := client.CommitMark{Boot: boot, Sequence: sequence.Add(1)}
					mark = &value
				case 0x0107:
					select {
					case <-syncStarted:
					case <-time.After(3 * time.Second):
						t.Error("no independent fence connection")
						return
					}
					statOnce.Do(func() { close(statSeen) })
					result.U64(5)
					result.Bool(false)
					result.U32(0)
					result.ID(client.ID{})
					result.U8(0)
					result.U16(5)
				case client.BucketSync:
					d := client.Decoder{B: m.Body}
					d.ID()
					requested := client.CommitMark{Boot: d.ID(), Sequence: d.U64()}
					if d.Done() != nil || requested.Boot != boot || requested.Sequence == 0 {
						t.Error("invalid fence target")
						return
					}
					syncOnce.Do(func() { close(syncStarted) })
					select {
					case <-statSeen:
					case <-time.After(3 * time.Second):
						t.Error("fence blocked ordinary object traffic")
						return
					}
					// Returning exactly the requested prefix tests that applying an older
					// fence preserves newer accepted writes on the original session.
					result.B = requested.MarshalBinary()
				default:
					t.Errorf("unexpected opcode %x", m.Opcode)
					return
				}
				body := client.ResponseBody(200, result.B, "")
				if mark != nil {
					e := client.Encoder{}
					e.V(200)
					h := client.Encoder{}
					h.V(1)
					h.V(client.DurabilityHeader)
					h.Blob(mark.MarshalBinary())
					e.Blob(h.B)
					e.B = append(e.B, result.B...)
					body = e.B
				}
				if err = c.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})); err != nil {
					return
				}
			}
		}))
		defer host.Close()
		run(t, host.URL, "fixture", "fixture-password", client.ID{1}.String(), "fence")
		if got := connections.Load(); got != 5 {
			t.Fatalf("fences reauthenticated instead of reusing the control session: %d connections", got)
		}
	})
	for _, mode := range []string{"transient", "unavailable", "transient-stream"} {
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
					// Consume and verify the complete streamed body before
					// rejecting it. A retry must replay the exact payload and ID.
					if mode == "transient-stream" {
						hash := sha256.New()
						var offset, part uint64
						for {
							_, raw, err = c.ReadMessage()
							if err != nil {
								t.Error(err)
								return
							}
							frame, err := client.DecodeMessage(raw)
							if err != nil {
								t.Error(err)
								return
							}
							d := client.Decoder{B: frame.Body}
							if d.ID() != m.ID {
								t.Error("upload retry changed parent ID")
								return
							}
							if frame.Opcode == 0x0005 {
								sequence, at, data := d.U64(), d.U64(), d.Blob()
								if d.Done() != nil || sequence != part || at != offset || len(data) == 0 {
									t.Error("invalid replayed chunk")
									return
								}
								hash.Write(data)
								offset += uint64(len(data))
								part++
							} else if frame.Opcode == 0x0006 {
								size, digest := d.U64(), d.Take(32)
								if d.Done() != nil || size != offset || size != (2<<20)+17 || !bytes.Equal(digest, hash.Sum(nil)) {
									t.Error("upload retry lost or changed payload")
									return
								}
								break
							} else {
								t.Error("unexpected upload frame", frame.Opcode)
								return
							}
						}
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
					if strings.HasPrefix(mode, "transient") && requests == 4 {
						status = 200
						e := client.Encoder{}
						length := uint64(5)
						if mode == "transient-stream" {
							length = (2 << 20) + 17
						}
						e.U64(length)
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
			if strings.HasPrefix(mode, "transient") && requests != 4 {
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
		c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID()), "fs")
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
		run(t, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.ID(bucket.ID()).String(), "routing-lease")
	})
	for _, mode := range []string{"disconnect", "disconnect-stream", "digest", "malformed", "wrong-id", "varint", "text", "subscription-digest"} {
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
