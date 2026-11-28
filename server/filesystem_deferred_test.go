// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type referenceFailureIO struct {
	volume.VolumeIO
	fail atomic.Bool
}

func (d *referenceFailureIO) Sync() error {
	if d.fail.Load() {
		return errors.New("injected persistent checkpoint failure")
	}
	return d.VolumeIO.Sync()
}

func replaceTestVolumeIO(t *testing.T, s *Server, name string, wrap func(volume.VolumeIO) volume.VolumeIO) *volume.Volume {
	t.Helper()
	var managed *managedVolume
	s.mu.RLock()
	for _, m := range s.volumes {
		if m.config.Name == name {
			managed = m
		}
	}
	s.mu.RUnlock()
	if managed == nil {
		t.Fatal("test volume absent")
	}
	old := managed.store
	identity := old.Identity()
	size := identity.Size
	// This fixture deliberately replaces all contents with a freshly formatted
	// backend, so abandon private accepted pages explicitly.
	if err := old.Abort(); err != nil {
		t.Fatal(err)
	}
	mem, err := volume.NewMemory(int64(size))
	if err != nil {
		t.Fatal(err)
	}
	v, err := volume.Format(wrap(mem), volume.FormatOptions{Class: identity.Class, ClusterID: identity.ClusterID, NodeID: identity.NodeID, VolumeID: identity.VolumeID})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	managed.store = v
	s.mu.Unlock()
	return v
}
func referenceRequest(b *Bucket, target client.ObjectID, token client.ID, trailer *bool) client.Message {
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	root := client.ObjectReferenceRootID()
	e.B = append(e.B, root[:]...)
	e.U64(0)
	e.U64(0)
	e.ID(client.ID{})
	e.B = append(e.B, target[:]...)
	e.U8(0)
	e.ID(token)
	if trailer != nil {
		e.Bool(*trailer)
	}
	return client.Message{Kind: client.Request, Opcode: client.ObjectReference, ID: client.NewRequestID(), Body: e.B}
}
func TestFilesystemReferenceAcceptedAndExplicitFence(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-durable", true: "accepted"}[accepted], func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "refs", "hdd", 16<<20)
			var gate *serverFlushGate
			v := replaceTestVolumeIO(t, s, "refs", func(mem volume.VolumeIO) volume.VolumeIO { gate = &serverFlushGate{VolumeIO: mem}; return gate })
			b, _ := createProtocolBucket(t, s, "refs")
			ws := connectionTestSession(s)
			ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
			ws.authorization.objectReferences.Store(true)
			ws.durabilityCap.Store(true)
			ws.acceptedReferencesCap.Store(true)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			gate.mu.Lock()
			gate.entered, gate.release = entered, release
			gate.mu.Unlock()
			var trailer *bool
			if accepted {
				trailer = &accepted
			}
			m := referenceRequest(b, client.ObjectID{9}, client.NewRequestID(), trailer)
			ws.admitConnectionMessage(m)
			type reply struct {
				body   []byte
				status int
			}
			done := make(chan reply, 1)
			p := quotaPrincipal(t, s)
			go func() {
				body, status := s.dispatchAs(m, ws, p)
				ws.completeConnectionMessage(m, status, body)
				done <- reply{body, status}
			}()
			if accepted {
				select {
				case r := <-done:
					if r.status != 200 {
						t.Fatal(r.status)
					}
					d := client.Decoder{B: r.body}
					d.V()
					if len(d.Blob()) == 0 {
						t.Fatal("accepted reference has no receipt")
					}
				case <-time.After(time.Second):
					t.Fatal("reference request waited for physical checkpoint")
				}
				ws.fenceMu.Lock()
				_, tracked := ws.connectionTouches[v]
				ws.fenceMu.Unlock()
				if !tracked {
					t.Fatal("reference mutation absent from connection touch map")
				}
				fence := connectionTestMessage(client.ConnectionSync, client.Request)
				ws.admitConnectionMessage(fence)
				go func() { body, status := s.dispatchAs(fence, ws, p); done <- reply{body, status} }()
			}
			select {
			case <-entered:
			case r := <-done:
				t.Fatalf("durability ACK before physical sync: %d", r.status)
			case <-time.After(time.Second):
				t.Fatal("explicit durability never reached backend")
			}
			select {
			case r := <-done:
				t.Fatalf("ACK passed blocked backend: %d", r.status)
			default:
			}
			unblock()
			select {
			case r := <-done:
				if r.status != 200 {
					t.Fatal(r.status)
				}
			case <-time.After(time.Second):
				t.Fatal("durability failed after release")
			}
			generation := v.Header().Generation
			if !v.IsDurableThrough(generation) {
				t.Fatal("success without durability")
			}
		})
	}
}
func TestFilesystemAcceptedReferenceRequiresNegotiation(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "refs-caps", "hdd", 16<<20)
	b, _ := createProtocolBucket(t, s, "refs-caps")
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID()}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	accepted := true
	m := referenceRequest(b, client.ObjectID{8}, client.NewRequestID(), &accepted)
	ws.admitConnectionMessage(m)
	if _, status := s.dispatchAs(m, ws, quotaPrincipal(t, s)); status != 501 {
		t.Fatalf("unnegotiated reference: %d", status)
	}
}
func TestS3DurableACKRejectsPhysicalSyncFailure(t *testing.T) {
	s, b, _ := s3TestSetup(t)
	var backend *referenceFailureIO
	replaceTestVolumeIO(t, s, "s3-test", func(mem volume.VolumeIO) volume.VolumeIO {
		backend = &referenceFailureIO{VolumeIO: mem}
		return backend
	})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	a := s.cfg.Auth.BootstrapAdmin
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), "s3")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	api := client.NewS3(c)
	if _, err = api.Put(ctx, "durable-control", strings.NewReader("good"), 4, client.S3PutOptions{}); err != nil {
		t.Fatal(err)
	}
	backend.fail.Store(true)
	if _, err = api.Put(ctx, "must-not-ack", strings.NewReader("lost"), 4, client.S3PutOptions{}); err == nil {
		t.Fatal("S3 acknowledged failed persistence")
	}
}

type filesystemProtocolAudit struct {
	sync.Mutex
	mutations, references, fences, durable int
}

// Transparent WebSocket bridge observes actual client frames, not mocked FS
// helper calls. Both endpoints remain disposable authenticated test servers.
func auditedFilesystemEndpoint(t *testing.T, s *Server, audit *filesystemProtocolAudit) string {
	t.Helper()
	upstream := httptest.NewServer(s.Handler())
	t.Cleanup(upstream.Close)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Clone()
		for _, key := range []string{"Connection", "Upgrade", "Sec-WebSocket-Version", "Sec-WebSocket-Key", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
			header.Del(key)
		}
		peer, _, err := (&websocket.Dialer{Subprotocols: []string{"stqr.v2"}}).Dial("ws"+strings.TrimPrefix(upstream.URL, "http")+r.URL.RequestURI(), header)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer peer.Close()
		downstream, err := (&websocket.Upgrader{Subprotocols: []string{"stqr.v2"}}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer downstream.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer downstream.Close()
			for {
				kind, raw, err := peer.ReadMessage()
				if err != nil {
					return
				}
				if downstream.WriteMessage(kind, raw) != nil {
					return
				}
			}
		}()
		for {
			kind, raw, err := downstream.ReadMessage()
			if err != nil {
				peer.Close()
				<-done
				return
			}
			if kind == websocket.BinaryMessage {
				m, e := client.DecodeMessage(raw)
				if e == nil {
					audit.Lock()
					switch {
					case m.Opcode == client.ConnectionSync || m.Opcode == client.BucketSync || m.Opcode == 0x010a:
						audit.fences++
					case m.Opcode == client.ObjectAccepted:
						audit.mutations++
					case m.Opcode >= 0x0102 && m.Opcode <= 0x0108 && m.Opcode != 0x0107:
						audit.mutations++
						audit.durable++
					case m.Opcode == client.ObjectReference:
						audit.references++
						if len(m.Body) != 130 || m.Body[len(m.Body)-1] != 1 {
							audit.durable++
						}
					}
					audit.Unlock()
				}
			}
			if peer.WriteMessage(kind, raw) != nil {
				downstream.Close()
				<-done
				return
			}
		}
	}))
	t.Cleanup(proxy.Close)
	return proxy.URL
}
func TestFilesystemOrdinaryOperationsDoNotRequestDurability(t *testing.T) {
	for _, subscriptions := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "subscription"}[subscriptions], func(t *testing.T) {
			s := testServer(t)
			addTestVolume(t, s, "normal-fs", "hdd", 32<<20)
			b, _ := createProtocolBucket(t, s, "normal-fs")
			var audit filesystemProtocolAudit
			endpoint := auditedFilesystemEndpoint(t, s, &audit)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			a := s.cfg.Auth.BootstrapAdmin
			c, err := client.Dial(ctx, endpoint, a.Username, a.Password, client.ID(b.ID()), "fs")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			var store client.FilesystemStore = c
			if subscriptions {
				cache := client.NewSubscriptionStore(c, 16<<20)
				defer cache.Close()
				store = cache
			}
			fs, err := client.OpenFilesystem(ctx, store, client.FSOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			file, err := fs.Create(ctx, client.FilesystemRootID(), "ordinary", 0644, 0, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			token, err := fs.Retain(ctx, file.ID)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte(strings.Repeat("payload", 1024))
			if _, err = fs.WriteAt(ctx, file.ID, payload, 0); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err = fs.ReadAt(ctx, file.ID, got, 0); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("read after write: %v", err)
			}
			if err = fs.Rename(ctx, client.FilesystemRootID(), "ordinary", client.FilesystemRootID(), "renamed"); err != nil {
				t.Fatal(err)
			}
			if err = fs.Release(ctx, file.ID, token); err != nil {
				t.Fatal(err)
			}
			audit.Lock()
			mutations, refs, fences, durable := audit.mutations, audit.references, audit.fences, audit.durable
			audit.Unlock()
			if mutations == 0 || refs == 0 {
				t.Fatalf("audit missed data/reference traffic: %d/%d", mutations, refs)
			}
			t.Logf("ordinary FS burst: accepted mutations=%d references=%d fences=%d durable requests=%d", mutations, refs, fences, durable)
			if fences != 0 || durable != 0 {
				t.Fatalf("normal FS requested physical durability: fences=%d durable=%d", fences, durable)
			}
			if err = fs.Sync(ctx, file.ID); err != nil {
				t.Fatal(err)
			}
			audit.Lock()
			after := audit.fences
			audit.Unlock()
			if after == 0 {
				t.Fatal("explicit fsync emitted no owner fence")
			}
			for _, v := range b.allVolumes() {
				if !v.IsDurableThrough(v.Header().Generation) {
					t.Fatal("explicit fsync omitted touched data or metadata")
				}
			}
			c.Close()
			audit.Lock()
			afterClose := audit.fences
			audit.Unlock()
			if afterClose != after {
				t.Fatal("close emitted implicit fence")
			}
		})
	}
}
