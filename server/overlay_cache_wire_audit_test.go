// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
)

type overlayWireKey struct {
	opcode uint16
	id     client.ObjectID
}

// Count decoded requests actually crossing the reader's WebSocket, rather than
// calls to a caching wrapper. Writers connect directly and cannot pollute counts.
type overlayWireCounter struct {
	mu     sync.Mutex
	counts map[overlayWireKey]int
}

func (c *overlayWireCounter) count(op uint16, id client.ObjectID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[overlayWireKey{op, id}]
}

func (c *overlayWireCounter) total(op uint16) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for key, count := range c.counts {
		if key.opcode == op {
			n += count
		}
	}
	return n
}

func overlayCountingProxy(t *testing.T, backend string) (*httptest.Server, *overlayWireCounter) {
	t.Helper()
	target, err := url.Parse(backend)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	counter := &overlayWireCounter{counts: map[overlayWireKey]int{}}
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			forward.ServeHTTP(w, r)
			return
		}
		header := r.Header.Clone()
		for name := range header {
			if strings.HasPrefix(strings.ToLower(name), "sec-websocket-") || strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Upgrade") {
				header.Del(name)
			}
		}
		dialer := websocket.Dialer{Subprotocols: websocket.Subprotocols(r)}
		upstream, response, e := dialer.Dial("ws"+strings.TrimPrefix(backend, "http")+r.URL.RequestURI(), header)
		if e != nil {
			if response != nil {
				response.Body.Close()
			}
			http.Error(w, e.Error(), 502)
			return
		}
		defer upstream.Close()
		upgrade := websocket.Upgrader{Subprotocols: []string{upstream.Subprotocol()}, CheckOrigin: func(*http.Request) bool { return true }}
		downstream, e := upgrade.Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer downstream.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer downstream.Close()
			for {
				kind, data, err := upstream.ReadMessage()
				if err != nil || downstream.WriteMessage(kind, data) != nil {
					return
				}
			}
		}()
		for {
			kind, data, err := downstream.ReadMessage()
			if err != nil {
				break
			}
			m, err := client.DecodeMessage(data)
			if err == nil && m.Kind == client.Request && len(m.Body) >= 48 {
				var id client.ObjectID
				copy(id[:], m.Body[16:48])
				counter.mu.Lock()
				counter.counts[overlayWireKey{m.Opcode, id}]++
				counter.mu.Unlock()
			}
			if upstream.WriteMessage(kind, data) != nil {
				break
			}
		}
		upstream.Close()
		<-done
	}))
	t.Cleanup(host.Close)
	return host, counter
}

func TestOverlayCacheWireChurn(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "data", "hdd", 32<<20)
	lowerBucket, _ := createProtocolBucket(t, s, "wire-lower")
	upperBucket, _ := createProtocolBucket(t, s, "wire-upper")
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dial := func(endpoint string, bucket *Bucket) *client.Client {
		t.Helper()
		admin := s.cfg.Auth.BootstrapAdmin
		conn, err := client.Dial(ctx, endpoint, admin.Username, admin.Password, client.BucketID(bucket.ID()), "fs")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	lowerWriter, upperWriter := dial(host.URL, lowerBucket), dial(host.URL, upperBucket)
	lower, err := client.OpenFilesystem(ctx, lowerWriter, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var files []client.FSInfo
	payload := bytes.Repeat([]byte("lower-content"), 1024)
	for i := 0; i < 3; i++ {
		file, err := lower.Create(ctx, client.FilesystemRootID(), fmt.Sprintf("base-%d", i), 0644, 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lower.WriteAt(ctx, file.ID, payload, 0); err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	copyDir, err := lower.Create(ctx, client.FilesystemRootID(), "copyups", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	var copyFiles []client.FSInfo
	for i := 0; i < 6; i++ {
		file, err := lower.Create(ctx, copyDir.ID, fmt.Sprintf("copy-%d", i), 0644, 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lower.WriteAt(ctx, file.ID, payload, 0); err != nil {
			t.Fatal(err)
		}
		copyFiles = append(copyFiles, file)
	}
	writer, err := client.OpenOverlayFilesystem(ctx, lowerWriter, upperWriter, client.OverlayOptions{Create: true, LowerBucketID: client.BucketID(lowerBucket.ID()), Whitelist: []string{"scratch/", "copyups/"}})
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := writer.Create(ctx, client.FilesystemRootID(), "scratch", 0755, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	upperProxy, upperCount := overlayCountingProxy(t, host.URL)
	lowerProxy, lowerCount := overlayCountingProxy(t, host.URL)
	upperReader := dial(upperProxy.URL, upperBucket)
	upperCache := client.NewSubscriptionStore(upperReader, 8<<20)
	lowerCache := client.NewSubscriptionStore(dial(lowerProxy.URL, lowerBucket), 8<<20)
	defer upperCache.Close()
	defer lowerCache.Close()
	reader, err := client.OpenOverlayFilesystem(ctx, lowerCache, upperCache, client.OverlayOptions{LowerBucketID: client.BucketID(lowerBucket.ID())})
	if err != nil {
		t.Fatal(err)
	}
	observe := func(read bool) {
		t.Helper()
		for _, file := range files {
			if read {
				buf := make([]byte, 64)
				if n, e := reader.ReadAt(ctx, file.ID, buf, 0); e != nil || n != len(buf) || !bytes.Equal(buf, payload[:len(buf)]) {
					t.Fatalf("read: n=%d err=%v data=%q", n, e, buf)
				}
			} else if info, e := reader.Stat(ctx, file.ID); e != nil || info.Size != uint64(len(payload)) {
				t.Fatalf("stat: %+v %v", info, e)
			}
		}
	}
	probes := func() int {
		total := 0
		for _, file := range files {
			total += upperCount.count(client.ObjectSubscribe, file.ID)
		}
		return total
	}
	// Independent positive control proves this observer sees actual missing
	// traffic even if a future optimization eliminates all overlay probes.
	missing := client.ObjectID{0xfe}
	if _, err := upperReader.Subscribe(ctx, missing, client.SubscribeOptions{}); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("missing control: %v", err)
	}
	if got := upperCount.count(client.ObjectSubscribe, missing); got != 1 {
		t.Fatalf("missing control frames=%d want=1", got)
	}
	before := probes()
	observe(false)
	if got := probes() - before; got > len(files) {
		t.Fatalf("cold absence probes=%d exceeds=%d", got, len(files))
	}
	t.Logf("cold lower_files=%d upper_absence_subscribe=%d", len(files), probes()-before)
	for _, file := range files {
		if upperCount.count(0x0100, file.ID) != 0 {
			t.Fatal("unexpected native upper GET fallback")
		}
	}
	observe(true)
	for _, read := range []bool{false, true} {
		before = probes()
		beforeRanges := lowerCount.total(0x0101)
		for i := 0; i < 6; i++ {
			observe(read)
		}
		if got := probes() - before; got != 0 {
			t.Fatalf("quiet read=%v upper absence probes=%d", read, got)
		}
		t.Logf("quiet read=%v rounds=6 lower_files=%d upper_absence_subscribe=0 lower_payload_ranges=%d", read, len(files), lowerCount.total(0x0101)-beforeRanges)
	}
	// Wait for the exact namespace image before each measured observation. This
	// removes notification latency as a cause of missed or duplicated churn.
	waitNamespace := func(name string, present bool) {
		t.Helper()
		for {
			entries, err := reader.ReadDir(ctx, scratch.ID)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range entries {
				found = found || entry.Name == name
			}
			if found == present {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	for _, read := range []bool{false, true} {
		before = probes()
		beforeRanges := lowerCount.total(0x0101)
		for i := 0; i < 3; i++ {
			name := fmt.Sprintf("unrelated-%v-%d", read, i)
			if _, err := writer.Create(ctx, scratch.ID, name, 0600, 0, 0, false); err != nil {
				t.Fatal(err)
			}
			waitNamespace(name, true)
			observe(read)
			if err := writer.Unlink(ctx, scratch.ID, name); err != nil {
				t.Fatal(err)
			}
			waitNamespace(name, false)
			observe(read)
		}
		if got := probes() - before; got > 6*len(files) {
			t.Fatalf("churn read=%v absence probes=%d exceeds=%d", read, got, 6*len(files))
		}
		t.Logf("churn read=%v namespace_publications=6 lower_files=%d upper_absence_subscribe=%d lower_payload_ranges=%d", read, len(files), probes()-before, lowerCount.total(0x0101)-beforeRanges)
	}
	for mode, read := range []bool{false, true} {
		before = probes()
		for i := 0; i < 3; i++ {
			file := copyFiles[mode*3+i]
			if _, err := writer.WriteAt(ctx, file.ID, []byte("UPPER"), 0); err != nil {
				t.Fatal(err)
			}
			// Wait on the actual copied inode's subscription, outside target
			// measurements, until the new content proves copy-up publication.
			for {
				buf := make([]byte, 5)
				if _, err := reader.ReadAt(ctx, file.ID, buf, 0); err != nil {
					t.Fatal(err)
				}
				if string(buf) == "UPPER" {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			observe(read)
		}
		if got := probes() - before; got > 3*len(files) {
			t.Fatalf("copyup read=%v absence probes=%d exceeds=%d", read, got, 3*len(files))
		}
		t.Logf("copyup read=%v namespace_publications=3 lower_files=%d upper_absence_subscribe=%d", read, len(files), probes()-before)
	}
	before = probes()
	for i := 0; i < 6; i++ {
		if _, err := writer.WriteAt(ctx, copyFiles[0].ID, []byte("again"), 0); err != nil {
			t.Fatal(err)
		}
		observe(false)
		observe(true)
	}
	if got := probes() - before; got != 0 {
		t.Fatalf("published-upper data writes caused %d missing probes", got)
	}
	t.Log("published-upper writes=6 lower_stats=18 lower_reads=18 upper_absence_subscribe=0")
	for _, file := range files {
		t.Logf("lower inode=%s subscribe=%d get=%d upper_missing_subscribe=%d", file.ID, lowerCount.count(client.ObjectSubscribe, file.ID), lowerCount.count(0x0100, file.ID), upperCount.count(client.ObjectSubscribe, file.ID))
	}
	t.Log("overlay-cache-wire measurement passed")
}
