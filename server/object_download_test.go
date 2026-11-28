// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func objectDownloadTempDir(t *testing.T, path string) {
	t.Helper()
	for _, key := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(key, path)
	}
}

func TestObjectDownloadMemoryAndSpill(t *testing.T) {
	objectDownloadTempDir(t, t.TempDir())
	snapshot := objectDownload{streaming: true}
	defer snapshot.Close()
	payload := bytes.Repeat([]byte("snapshot"), inlineObjectDownload/8)
	for at := 0; at < len(payload); at += 4096 {
		if n, err := snapshot.Write(payload[at : at+4096]); err != nil || n != 4096 {
			t.Fatal(n, err)
		}
	}
	if snapshot.file != nil || cap(snapshot.memory) > inlineObjectDownload+objectDownloadHeader {
		t.Fatal("inline boundary spilled or exceeded its memory bound")
	}
	header := []byte("response header")
	response := snapshot.inline(header)
	if !bytes.Equal(response[:len(header)], header) || !bytes.Equal(response[len(header):], payload) ||
		&response[len(header)] != &snapshot.memory[objectDownloadHeader] {
		t.Fatal("inline response did not reuse its owned payload")
	}
	if n, err := snapshot.Write([]byte{'!'}); n != 1 || err != nil {
		t.Fatal(n, err)
	}
	if snapshot.file == nil || snapshot.memory != nil {
		t.Fatal("crossing the inline boundary must spill and release memory")
	}
	if _, err := snapshot.file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(snapshot.file)
	if err != nil || !bytes.Equal(got, append(payload, '!')) {
		t.Fatal("spill changed snapshot bytes", err)
	}
	name := snapshot.file.Name()
	snapshot.Close()
	if _, err = os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("snapshot file remained after close", err)
	}
}

func TestObjectDownloadOwnsMemoryAndCleansFailedSpill(t *testing.T) {
	objectDownloadTempDir(t, filepath.Join(t.TempDir(), "unavailable"))
	snapshot := objectDownload{streaming: true}
	defer snapshot.Close()
	payload := []byte("borrowed volume bytes")
	want := bytes.Clone(payload)
	if _, err := snapshot.Write(payload); err != nil {
		t.Fatal(err)
	}
	clear(payload)
	if got := snapshot.inline(nil); !bytes.Equal(got, want) {
		t.Fatal("snapshot retained the volume's borrowed slice")
	}
	if n, err := snapshot.Write(make([]byte, inlineObjectDownload)); n != 0 || err == nil {
		t.Fatal("failed spill was reported as successful", n, err)
	}
	if got := snapshot.inline(nil); !bytes.Equal(got, want) {
		t.Fatal("failed spill changed buffered data")
	}
}

func TestObjectDownloadSmallProtocolReadsNeedNoTempDirectory(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	payloads := [][]byte{nil, bytes.Repeat([]byte("m"), 210), bytes.Repeat([]byte("p"), 4096),
		bytes.Repeat([]byte("b"), inlineObjectDownload), bytes.Repeat([]byte("L"), inlineObjectDownload+1)}
	for i, payload := range payloads {
		if _, err := writer.Put(ctx, client.ObjectID{byte(i + 1)}, payload, client.PutOptions{Accepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	source, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	target, _ := createProtocolBucket(t, s, "download-copy")
	if err = referenceAdminClone(t, s, source, target); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	admin := s.cfg.Auth.BootstrapAdmin
	clone, err := client.Dial(ctx, host.URL, admin.Username, admin.Password, client.ID(target.ID()), "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	objectDownloadTempDir(t, filepath.Join(t.TempDir(), "unavailable"))
	for _, c := range []*client.Client{reader, clone} {
		if _, _, err := c.Get(ctx, client.ObjectID{255}); !errors.Is(err, client.ErrNotFound) {
			t.Fatal("missing object became a temp-file error", err)
		}
		for i, want := range payloads[:4] {
			got, info, err := c.Get(ctx, client.ObjectID{byte(i + 1)})
			if err != nil || info.Length != uint64(len(want)) || !bytes.Equal(got, want) {
				t.Fatal("small GET depended on a temp file", len(want), err)
			}
		}
		got, info, err := c.GetRange(ctx, client.ObjectID{5}, 1, inlineObjectDownload)
		if err != nil || info.Length != inlineObjectDownload+1 || !bytes.Equal(got, payloads[4][1:]) {
			t.Fatal("exact inline ranged boundary", len(got), err)
		}
		got, _, err = c.GetRange(ctx, client.ObjectID{5}, inlineObjectDownload+1, 10)
		if err != nil || len(got) != 0 {
			t.Fatal("empty EOF range", got, err)
		}
		length := uint64(4096)
		var out bytes.Buffer
		_, err = c.GetRangeToMatching(ctx, client.ObjectID{3}, 2, 7, client.ReadCondition{Length: &length, Bytes: []byte("ppp")}, &out)
		if err != nil || out.String() != "ppppppp" {
			t.Fatal("conditional ranged snapshot", out.String(), err)
		}
		out.Reset()
		_, err = c.GetRangeToMatching(ctx, client.ObjectID{3}, 0, 7, client.ReadCondition{Bytes: []byte("wrong")}, &out)
		if !errors.Is(err, client.ErrPrecondition) || out.Len() != 0 {
			t.Fatal("rejected condition emitted data", out.Len(), err)
		}
	}
	if _, _, err = reader.Get(ctx, client.ObjectID{5}); err == nil {
		t.Fatal("large GET unexpectedly succeeded without spill storage")
	}
	if got, _, err := reader.Get(ctx, client.ObjectID{2}); err != nil || !bytes.Equal(got, payloads[1]) {
		t.Fatal("spill error damaged subsequent request", err)
	}
	// HTTP rejects a truly large reply without wasting a disk snapshot, but
	// may still return an inline range from that same large object.
	for _, ranged := range []bool{false, true} {
		e := client.Encoder{}
		e.ID(reader.BucketID())
		object := client.ObjectID{5}
		e.B = append(e.B, object[:]...)
		e.U64(0)
		e.U64(0)
		e.ID(client.ID{})
		opcode := uint16(0x100)
		if ranged {
			opcode = 0x101
			e.U64(1)
			e.U64(inlineObjectDownload)
		}
		e.Bool(false)
		body, status, err := s.executeObjects(client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: e.B}, nil, userPrincipal(t, s))
		if ranged && (err != nil || status != 206) || !ranged && objectStatus(err) != 400 {
			t.Fatal("HTTP inline/stream boundary", ranged, status, err)
		}
		if ranged {
			// Byte-for-byte agreement with the established transfer encoder,
			// including metadata, range fields, digest and Blob framing.
			expected := client.Encoder{}
			encodeObjectInfo(&expected, volume.Info{Length: inlineObjectDownload + 1}, client.ID{})
			expected.U64(1)
			expected.U64(inlineObjectDownload)
			expected.U64(inlineObjectDownload + 1)
			encodeObjectTransfer(&expected, 0, inlineObjectDownload, sha256.Sum256(payloads[4][1:]), payloads[4][1:])
			if !bytes.Equal(body, expected.B) {
				t.Fatal("inline snapshot changed wire encoding")
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			ws := &wsSession{ctx: cancelled, authorization: &connectionAuthorization{bucket: source, bucketID: source.ID()}}
			body, _, err = s.executeObjects(client.Message{Kind: client.Request, Opcode: opcode, ID: client.NewRequestID(), Body: e.B}, ws, userPrincipal(t, s))
			if !errors.Is(err, context.Canceled) || len(body) != 0 {
				t.Fatal("cancelled server snapshot emitted a response", err)
			}
		}
	}
}

func TestObjectDownloadProtocolSnapshotAndStream(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{99}
	want := bytes.Repeat([]byte("large stream"), (inlineObjectDownload*2)/12+1)
	if _, err := writer.Put(ctx, id, want, client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if got, _, err := reader.Get(ctx, id); err != nil || !bytes.Equal(got, want) {
		t.Fatal("stream readback", len(got), err)
	}
	id = client.ObjectID{100}
	versions := [][]byte{bytes.Repeat([]byte("a"), 8192), bytes.Repeat([]byte("b"), 8192)}
	if _, err := writer.Put(ctx, id, versions[0], client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	writeErrors := make(chan error, 1)
	go func() {
		defer wg.Done()
		for i := 0; i < 32; i++ {
			if _, err := writer.Put(ctx, id, versions[i%2], client.PutOptions{Accepted: true}); err != nil {
				writeErrors <- err
				return
			}
		}
	}()
	defer wg.Wait()
	for i := 0; i < 32; i++ {
		if i%2 != 0 {
			var out bytes.Buffer
			_, err := reader.GetRangeToMatching(ctx, id, 0, 8192, client.ReadCondition{Bytes: []byte("a")}, &out)
			if errors.Is(err, client.ErrPrecondition) && out.Len() == 0 {
				continue
			}
			if err != nil || !bytes.Equal(out.Bytes(), versions[0]) {
				t.Fatal("condition and copy observed different snapshots", err)
			}
			continue
		}
		got, _, err := reader.Get(ctx, id)
		if err != nil || !bytes.Equal(got, versions[0]) && !bytes.Equal(got, versions[1]) {
			t.Fatal("read mixed object snapshots", err)
		}
	}
	wg.Wait()
	select {
	case err := <-writeErrors:
		t.Fatal(err)
	default:
	}
}
