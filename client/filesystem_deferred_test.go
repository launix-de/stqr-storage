// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// This peer implements the native framing independently of the server. The
// memory backend gives visibility immediately; only ConnectionSync records a
// persistence boundary. Every mutation is classified at its real wire envelope.
type deferredFSPeer struct {
	memory                                               *MemoryFilesystemStore
	caps                                                 uint64
	accepted, durable, refsAccepted, refsDurable, fences atomic.Int64
	recoverReceipts                                      atomic.Bool
	confirmedReceipts                                    atomic.Bool
	receiptFences                                        atomic.Int64
	boot                                                 ID
}

func newDeferredFSPeer(t *testing.T, caps uint64) (*deferredFSPeer, *httptest.Server) {
	p := &deferredFSPeer{memory: NewMemoryFilesystemStore(), caps: caps, boot: ID{33}}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		var sequence uint64
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			m, err := DecodeMessage(raw)
			if err != nil {
				t.Error(err)
				return
			}
			op := m.Opcode
			body := m.Body
			accepted := false
			if op == ObjectAccepted {
				if len(body) < 82 {
					t.Error("short accepted envelope")
					return
				}
				accepted = true
				d := Decoder{B: body[80:]}
				op = d.U16()
				body = append(append([]byte(nil), body[:80]...), d.B...)
			}
			d := Decoder{B: body}
			e := Encoder{}
			status := 200
			mutation := false
			var backendErr error
			if op == Hello {
				d.U16()
				d.U16()
				advertised := d.U64()
				d.Text()
				if advertised&CapabilityAcceptedReferences == 0 {
					t.Error("client did not advertise accepted references")
				}
				e.U16(2)
				e.U64(caps)
				e.ID(ID{1})
				e.ID(ID{2})
				e.U64(1)
				e.U64(1)
			} else if op == ConnectionSync {
				p.fences.Add(1)
				e.ID(p.boot)
				e.U64(sequence)
			} else if op == BucketSync {
				d.ID()
				mark, decodeErr := DecodeCommitMark(d.Take(24))
				if decodeErr != nil {
					t.Error(decodeErr)
					return
				}
				p.receiptFences.Add(1)
				if !p.recoverReceipts.Load() || mark.Boot != p.boot {
					status = 410
				} else {
					e.B = mark.MarshalBinary()
				}
			} else if op == 7 {
				continue
			} else {
				d.ID()
				var id ObjectID
				copy(id[:], d.Take(32))
				d.Take(32)
				switch op {
				case ObjectReference:
					d.Take(32)
					action := d.U8()
					token := d.ID()
					var target ObjectID
					copy(target[:], body[80:112])
					if len(d.B) > 0 {
						accepted = d.Bool()
						if caps&CapabilityAcceptedReferences == 0 {
							t.Error("trailer sent to legacy reference peer")
							return
						}
					}
					mutation = true
					switch action {
					case 0:
						backendErr = p.memory.PinObject(r.Context(), target, token)
					case 1:
						backendErr = p.memory.UnpinObject(r.Context(), target, token)
					case 2:
						backendErr = p.memory.ClaimObjectRetirement(r.Context(), target, token)
					case 3:
						backendErr = p.memory.ReleaseObjectRetirement(r.Context(), target, token)
					case 4:
						backendErr = p.memory.PinObjectDependency(r.Context(), target, token)
					case 5:
						backendErr = p.memory.UnpinObjectDependency(r.Context(), target, token)
					default:
						t.Errorf("unexpected reference action %d", action)
						return
					}
					if accepted {
						p.refsAccepted.Add(1)
					} else {
						p.refsDurable.Add(1)
					}
					e.Bool(backendErr == nil)
				case objectGet, objectGetRange, objectStat:
					var payload []byte
					var info ObjectInfo
					off, n := uint64(0), uint64(0)
					if op == objectGetRange {
						off = d.U64()
						n = d.U64()
					}
					if op != objectStat && d.Bool() {
						if d.Bool() {
							d.U64()
						}
						d.U64()
						d.Blob()
					}
					if op == objectGetRange {
						payload, info, backendErr = p.memory.GetRange(r.Context(), id, off, n)
					} else {
						payload, info, backendErr = p.memory.Get(r.Context(), id)
					}
					if backendErr == nil {
						fixtureInfo(&e, 0, info.Length)
						if op == objectGetRange {
							e.U64(off)
							e.U64(uint64(len(payload)))
							e.U64(info.Length)
						}
						if op != objectStat {
							sum := sha256.Sum256(payload)
							e.U8(0)
							e.U64(uint64(len(payload)))
							e.B = append(e.B, sum[:]...)
							e.Blob(payload)
						}
					}
				case objectCAS:
					scope, off := CASScope(d.U8()), d.U64()
					opt := fixturePrecondition(&d)
					expected, replacement := d.Blob(), d.Blob()
					var info ObjectInfo
					info, backendErr = p.memory.CAS(r.Context(), id, off, expected, replacement, CASOptions{Scope: scope, MutationOptions: opt})
					if backendErr == nil {
						fixtureInfo(&e, 0, info.Length)
					}
					mutation = true
				case objectPut, objectPutRange:
					off := uint64(0)
					if op == objectPutRange {
						off = d.U64()
					}
					opt := fixturePrecondition(&d)
					mode, n, sum, payload := decodeTransfer(&d)
					if mode != 0 || n != uint64(len(payload)) || sha256.Sum256(payload) != sum {
						t.Error("invalid inline payload")
						return
					}
					var info ObjectInfo
					if op == objectPutRange {
						info, backendErr = p.memory.PutRange(r.Context(), id, off, payload, opt)
					} else {
						before, _, err := p.memory.Get(r.Context(), id)
						absent := errors.Is(err, ErrNotFound)
						if err != nil && !absent {
							backendErr = err
						} else {
							opt.Absent = absent
							info, backendErr = p.memory.CAS(r.Context(), id, 0, before, payload, CASOptions{Scope: CASObject, MutationOptions: opt})
						}
					}
					if backendErr == nil {
						fixtureInfo(&e, 0, info.Length)
						if op == objectPutRange {
							e.U64(off)
							e.U64(n)
							e.U64(info.Length)
						}
					}
					mutation = true
				case objectDelete:
					opt := fixturePrecondition(&d)
					backendErr = p.memory.Delete(r.Context(), id, opt)
					mutation = true
				default:
					t.Errorf("unexpected deferred FS opcode %x", op)
					return
				}
			}
			if d.Done() != nil {
				t.Errorf("bad %x framing: %v", op, d.Done())
				return
			}
			if errors.Is(backendErr, ErrNotFound) {
				status = 404
			} else if errors.Is(backendErr, ErrPrecondition) {
				status = 412
			} else if errors.Is(backendErr, ErrWouldBlock) {
				status = 409
			} else if backendErr != nil {
				t.Error(backendErr)
				status = 500
			}
			if mutation {
				if accepted {
					p.accepted.Add(1)
				} else {
					p.durable.Add(1)
				}
			}
			response := ResponseBody(status, e.B, "")
			if mutation && accepted && status == 200 {
				sequence++
				header := Encoder{}
				header.V(1)
				if p.confirmedReceipts.Load() {
					header.V(49) // actual server confirmation-only reply
				} else {
					header.V(DurabilityHeader)
				}
				header.Blob((CommitMark{p.boot, sequence}).MarshalBinary())
				envelope := Encoder{}
				envelope.V(uint64(status))
				envelope.Blob(header.B)
				envelope.B = append(envelope.B, e.B...)
				response = envelope.B
			}
			if err = ws.WriteMessage(websocket.BinaryMessage, EncodeMessage(Message{Kind: Response, Opcode: m.Opcode, ID: m.ID, Body: response})); err != nil {
				return
			}
		}
	}))
	t.Cleanup(host.Close)
	return p, host
}
func fixturePrecondition(d *Decoder) MutationOptions {
	var opt MutationOptions
	if d.Bool() {
		n := d.U64()
		opt.ExpectedLength = &n
	}
	opt.Absent = d.Bool()
	if d.Bool() {
		opt.CompareOffset = d.U64()
		opt.Compare = bytes.Clone(d.Blob())
	}
	return opt
}

func TestFilesystemNativeMutationsDeferUntilExplicitSync(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	for _, cached := range []bool{false, true} {
		name := "direct"
		if cached {
			name = "subscription"
		}
		t.Run(name, func(t *testing.T) {
			p, host := newDeferredFSPeer(t, caps)
			c := dialConnectionTest(t, host)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var store FilesystemStore = c
			if cached {
				cache := NewSubscriptionStore(c, 0)
				defer cache.Close()
				store = cache
			}
			f, err := OpenFilesystem(ctx, store, FSOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			node, err := f.Create(ctx, FilesystemRootID(), "file", 0644, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			data := bytes.Repeat([]byte("payload"), 1600)
			if _, err = f.WriteAt(ctx, node.ID, data, 0); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(data))
			if _, err = f.ReadAt(ctx, node.ID, got, 0); err != nil || !bytes.Equal(got, data) {
				t.Fatal("accepted write lacks visibility", err)
			}
			token := NewRequestID()
			refs, _ := f.managedReferences()
			if err = refs.PinObject(ctx, node.ID, token); err != nil {
				t.Fatal(err)
			}
			if err = refs.UnpinObject(ctx, node.ID, token); err != nil {
				t.Fatal(err)
			}
			if err = f.Unlink(ctx, FilesystemRootID(), "file"); err != nil {
				t.Fatal(err)
			}
			if p.fences.Load() != 0 || p.durable.Load() != 0 || p.refsDurable.Load() != 0 {
				t.Fatal("normal filesystem operation requested persistence", p.fences.Load(), p.durable.Load(), p.refsDurable.Load())
			}
			if p.accepted.Load() < 10 || p.refsAccepted.Load() == 0 {
				t.Fatal("metadata/reference acceptance not exercised", p.accepted.Load(), p.refsAccepted.Load())
			}
			if err = f.Sync(ctx, FilesystemRootID()); err != nil {
				t.Fatal(err)
			}
			if p.fences.Load() != 1 {
				t.Fatal("explicit Sync did not cover owner", p.fences.Load())
			}
			// Same native client remains durable outside the filesystem wrapper.
			before := p.durable.Load()
			if _, err = c.Put(ctx, ObjectID{91}, []byte("default durable"), PutOptions{}); err != nil {
				t.Fatal(err)
			}
			if p.durable.Load() != before+1 {
				t.Fatal("filesystem weakened raw client")
			}
			if err = c.PinObject(ctx, ObjectID{92}, NewRequestID()); err != nil {
				t.Fatal(err)
			}
			if p.refsDurable.Load() != 1 {
				t.Fatal("filesystem reference mode leaked into raw client")
			}
		})
	}
}
func TestAcceptedReferencesRejectDurableLegacyFallback(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences
	p, host := newDeferredFSPeer(t, caps)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	fs := filesystemDeferredStore(c).(*filesystemNativeStore)
	var unsupported *StatusError
	if err := fs.PinObject(ctx, ObjectID{3}, NewRequestID()); !errors.As(err, &unsupported) || unsupported.Status != 426 {
		t.Fatal("legacy accepted reference did not fail explicitly", err)
	}
	if p.refsDurable.Load() != 0 || p.refsAccepted.Load() != 0 || p.fences.Load() != 0 {
		t.Fatal("unsupported reference performed storage/fence")
	}

}

func TestS3PublicationNeverInheritsAcceptedFilesystemMode(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	p, host := newDeferredFSPeer(t, caps)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	fs, err := OpenFilesystem(ctx, c, FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	s := NewS3(fs.store.(S3Store))
	before := p.accepted.Load()
	if _, err = s.Put(ctx, "default", bytes.NewReader([]byte("data")), 4, S3PutOptions{}); err != nil {
		t.Fatal(err)
	}
	publication := PutOptions{Accepted: true, RequestID: NewRequestID(), Absent: true}
	if _, err = s.Put(ctx, "publication", bytes.NewReader([]byte("data")), 4, S3PutOptions{Publication: &publication, Modified: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if p.accepted.Load() != before+2 || p.durable.Load() != 0 || p.fences.Load() != 2 || !publication.Accepted {
		t.Fatal("S3 publication lacked explicit fence", p.accepted.Load()-before, p.durable.Load(), p.fences.Load(), publication.Accepted)
	}
}

func TestAcceptedReferencesLostConnectionRequiresExactReceiptProof(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	_, host := newDeferredFSPeer(t, caps)
	c := dialConnectionTest(t, host)
	ctx := context.Background()
	fs := filesystemDeferredStore(c).(*filesystemNativeStore)
	if err := fs.PinObject(ctx, ObjectID{3}, NewRequestID()); err != nil {
		t.Fatal(err)
	}
	c.transport.fail(errors.New("test connection lost before explicit Sync"))
	var status *StatusError
	if err := c.Sync(ctx); !errors.As(err, &status) || status.Status != 410 {
		t.Fatal("missing old receipt proof silently cleared", err)
	}
}

func TestDeferredReferenceKeepsOfflineMaintenanceDurable(t *testing.T) {
	ctx := filesystemDeferredContext(context.Background())
	transport := &objectTransport{capabilities: CapabilityAcceptedReferences | CapabilityObjectReferences | CapabilityDurability | CapabilityConnectionSync}
	for action := byte(0); action <= 8; action++ {
		body := make([]byte, 129)
		body[112] = action
		out, accepted := acceptedReferenceMessage(ctx, transport, Message{Opcode: ObjectReference, Body: body})
		if action <= 5 {
			if !accepted || len(out.Body) != 130 || out.Body[129] != 1 {
				t.Fatalf("ordinary action%d not deferred", action)
			}
		} else if accepted || len(out.Body) != 129 {
			t.Fatalf("offline action%d weakened", action)
		}
	}
}

// Overlay construction must retain both the native deferred wrapper and the
// readonly upper flag when those independent features are integrated.
func TestOverlayNativeDeferredReadOnlyAndSync(t *testing.T) {
	const caps = CapabilityDurability | CapabilityConnectionSync | CapabilityObjectReferences | CapabilityAcceptedReferences
	for _, cached := range []bool{false, true} {
		name := "direct"
		if cached {
			name = "subscription"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			lowerStore := NewMemoryFilesystemStore()
			lower, err := OpenFilesystem(ctx, lowerStore, FSOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			node, err := lower.Create(ctx, FilesystemRootID(), "allowed", 0600, 42, 43, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = lower.WriteAt(ctx, node.ID, []byte("lower"), 0); err != nil {
				t.Fatal(err)
			}
			p, host := newDeferredFSPeer(t, caps)
			c := dialConnectionTest(t, host)
			var upperStore FilesystemStore = c
			if cached {
				cache := NewSubscriptionStore(c, 0)
				defer cache.Close()
				upperStore = cache
			}
			overlay, err := OpenOverlayFilesystem(ctx, lowerStore, upperStore, OverlayOptions{Create: true, LowerBucketID: BucketID{1}, Whitelist: []string{"allowed"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = overlay.WriteAt(ctx, node.ID, []byte("upper"), 0); err != nil {
				t.Fatal(err)
			}
			data := make([]byte, 5)
			if _, err = overlay.ReadAt(ctx, node.ID, data, 0); err != nil || string(data) != "upper" {
				t.Fatal("copy-up visibility", string(data), err)
			}
			if _, err = lower.ReadAt(ctx, node.ID, data, 0); err != nil || string(data) != "lower" {
				t.Fatal("copy-up mutated lower", string(data), err)
			}
			if p.accepted.Load() == 0 || p.durable.Load() != 0 || p.fences.Load() != 0 {
				t.Fatal("overlay did not defer native writes", p.accepted.Load(), p.durable.Load(), p.fences.Load())
			}
			if err = overlay.Sync(ctx, FilesystemRootID()); err != nil {
				t.Fatal(err)
			}
			if p.fences.Load() != 1 {
				t.Fatal("overlay Sync did not fence upper", p.fences.Load())
			}
			before := p.accepted.Load()
			readonly, err := OpenOverlayFilesystem(ctx, lowerStore, upperStore, OverlayOptions{ReadOnly: true, LowerBucketID: BucketID{1}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = readonly.WriteAt(ctx, node.ID, []byte("wrong"), 0); !errors.Is(err, syscall.EROFS) {
				t.Fatal("readonly copy-up/write permitted", err)
			}
			if err = readonly.CollectOrphans(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = readonly.ReadAt(ctx, node.ID, data, 0); err != nil || string(data) != "upper" {
				t.Fatal("readonly upper visibility", string(data), err)
			}
			if p.accepted.Load() != before || p.durable.Load() != 0 || p.fences.Load() != 1 {
				t.Fatal("readonly open/write/cleanup issued mutation or fence")
			}
		})
	}
}
