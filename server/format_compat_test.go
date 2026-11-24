// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"github.com/launix-de/stqr-storage/volume"
)

// This producer is part of every checkpoint tag. The compatibility runner
// compiles seed from the tag and verify from HEAD: no current writer or old
// filesystem/block-device decoder may stand in for the version under test.
type formatManifest struct {
	Schema int
	Cases  []formatCase
}
type formatCase struct {
	Kind, Bucket, Name, Codec, SHA256 string
	Size                              uint64
	Quota                             uint64
	Object                            client.ObjectID
	File                              client.FSInfo
	Device                            nbd.Metadata
	S3                                client.S3Object
	Request                           client.RequestID
	Append                            client.AppendResult
}

func formatHash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func formatPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + i/251) % 256)
	}
	return b
}

func TestFormatCompatibilityFixture(t *testing.T) {
	dir := os.Getenv("STQR_FORMAT_FIXTURE_DIR")
	if dir == "" {
		t.Skip("used by tools/compat/check.py with a persistent private fixture directory")
	}
	mode := os.Getenv("STQR_FORMAT_FIXTURE_MODE")
	if mode != "seed" && mode != "verify" {
		t.Fatal("STQR_FORMAT_FIXTURE_MODE must be seed or verify")
	}
	path := filepath.Join(dir, "node.yaml")
	if mode == "seed" {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("refusing to seed over existing state", err)
		}
	}
	if mode == "verify" {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("missing old server configuration", err)
		}
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	address := cfg.Node.Listen
	if mode == "seed" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Node.Listen = listener.Addr().String()
	cfg.Node.Advertise = "http://" + cfg.Node.Listen
	if err := saveConfig(path, cfg); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	s, err := Open(Options{ConfigPath: path})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	host := &http.Server{Handler: s.Handler()}
	go host.Serve(listener)
	t.Cleanup(func() {
		host.Close()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	dial := func(bucket string) *client.Client {
		id, err := bucketID(bucket)
		if err != nil {
			t.Fatal(err)
		}
		c, err := client.Dial(ctx, cfg.Node.Advertise, cfg.Auth.BootstrapAdmin.Username, cfg.Auth.BootstrapAdmin.Password, client.BucketID(id))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if mode == "seed" {
		addTestVolume(t, s, "compat-data", "hdd", 128<<20)
		m := seedFormatFixture(t, ctx, s, dial)
		body, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifestPath, body, 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("generated %d persisted cases", len(m.Cases))
		return
	}
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m formatManifest
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if m.Schema != 1 || len(m.Cases) == 0 {
		t.Fatal("unsupported or empty checkpoint manifest")
	}
	for _, c := range m.Cases {
		t.Run(c.Kind+"/"+c.Codec+"/"+c.Name, func(t *testing.T) { verifyFormatCase(t, ctx, c, dial) })
	}
	if t.Failed() {
		return
	}
	// In addition to application bytes, validate the persistent bucket catalog,
	// central PG projections and checksummed native volume/index structures.
	seen := map[string]bool{}
	for _, c := range m.Cases {
		if c.Kind == "deleted" {
			b, err := s.Bucket(newVolumeID(c.Bucket))
			if err != nil {
				t.Fatal(err)
			}
			_, info, found, err := b.selectVolume(b.Describe(), volume.Key{Bucket: b.ID(), Object: volume.ObjectID(c.Object)}, volume.ID{}, false)
			if err != nil || !found || !info.Deleted || info.Version != 2 {
				t.Fatal("retained tombstone/version changed", info, err)
			}
		}
		if seen[c.Bucket] {
			continue
		}
		seen[c.Bucket] = true
		b, err := s.Bucket(newVolumeID(c.Bucket))
		if err != nil {
			t.Fatal(err)
		}
		d := b.Describe()
		if d.Policy.Compression != c.Codec || d.Policy.StorageStrategy != "hdd" {
			t.Fatalf("bucket policy changed: %+v", d.Policy)
		}
		if c.Quota > 0 && (d.Policy.Limits.Unlimited || d.Policy.Limits.Absolute != c.Quota) {
			t.Fatal("persisted bucket quota changed", d.Policy.Limits)
		}
		if _, err := s.policyForGroup(d.Policy, d.Policy.PlacementGroup, false); err != nil {
			t.Fatal("PG catalog", err)
		}
	}
	for _, store := range s.quotaVolumes() {
		if err := store.Scrub(); err != nil {
			t.Fatal("volume scrub", err)
		}
	}
	t.Logf("verified every case (%d), bucket/PG catalogs and native volume checksums", len(m.Cases))
}

func seedFormatFixture(t *testing.T, ctx context.Context, s *Server, dial func(string) *client.Client) formatManifest {
	t.Helper()
	m := formatManifest{Schema: 1}
	create := func(name, codec string) string {
		draft := newTestBucket(t, name)
		draft.description.Policy.Compression = codec
		if name == "compat-objects-raw" {
			draft.description.Policy.Limits.Unlimited = false
			draft.description.Policy.Limits.Absolute = 16 << 20
		}
		_, status, reply := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft))
		if status != 201 {
			t.Fatal(status, reply)
		}
		return draft.ID().String()
	}
	for _, codec := range []string{"raw", "gzip", "lz4", "xz"} {
		bucket := create("compat-objects-"+codec, codec)
		c := dial(bucket)
		payload := formatPattern((1 << 20) + 733)
		oid := client.ObjectID{1}
		if _, err := c.Put(ctx, oid, payload, client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "object", Bucket: bucket, Codec: codec, Name: "large", Object: oid, Size: uint64(len(payload)), SHA256: formatHash(payload)})
		oid = client.ObjectID{2}
		sparse := make([]byte, (2<<20)+9)
		copy(sparse[len(sparse)-4:], []byte("tail"))
		if _, err := c.PutRange(ctx, oid, uint64(len(sparse)-4), []byte("tail"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "object", Bucket: bucket, Codec: codec, Name: "sparse", Object: oid, Size: uint64(len(sparse)), SHA256: formatHash(sparse)})
		oid = client.ObjectID{3}
		request := client.NewRequestID()
		result, err := c.Append(ctx, oid, []byte("journal-record"), client.AppendOptions{RequestID: request})
		if err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "append", Bucket: bucket, Codec: codec, Name: "retry", Object: oid, Size: 14, SHA256: formatHash([]byte("journal-record")), Request: request, Append: result})
		oid = client.ObjectID{4}
		if _, err := c.Put(ctx, oid, []byte("deleted"), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, oid, client.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "deleted", Bucket: bucket, Codec: codec, Name: "tombstone", Object: oid})
		if codec == "raw" {
			for i := range m.Cases {
				if m.Cases[i].Bucket == bucket {
					m.Cases[i].Quota = 16 << 20
				}
			}
		}
		if err := c.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	bucket := create("compat-filesystem", "gzip")
	c := dial(bucket)
	f, err := client.OpenFilesystem(ctx, c, client.FSOptions{Create: true, Mode: 0750, UID: 42, GID: 43})
	if err != nil {
		t.Fatal(err)
	}
	for i, payload := range [][]byte{[]byte("hello filesystem\n"), formatPattern((2 << 20) + 31), make([]byte, (2<<20)+7)} {
		name := []string{"small", "segmented", "sparse"}[i]
		info, err := f.Create(ctx, client.FilesystemRootID(), name, 0640, 42, 43, false)
		if err != nil {
			t.Fatal(err)
		}
		if name == "sparse" {
			copy(payload[len(payload)-4:], []byte("tail"))
			_, err = f.WriteAt(ctx, info.ID, []byte("tail"), int64(len(payload)-4))
		} else {
			_, err = f.WriteAt(ctx, info.ID, payload, 0)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := f.SetXattr(ctx, info.ID, "user.compat", []byte("attribute"), 0); err != nil {
			t.Fatal(err)
		}
		if name == "small" {
			if _, err := f.Link(ctx, client.FilesystemRootID(), "hardlink", info.ID); err != nil {
				t.Fatal(err)
			}
		}
		info, err = f.Stat(ctx, info.ID)
		if err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "file", Bucket: bucket, Codec: "gzip", Name: name, File: info, Size: uint64(len(payload)), SHA256: formatHash(payload)})
	}
	link, err := f.Symlink(ctx, client.FilesystemRootID(), "symlink", "small", 42, 43)
	if err != nil {
		t.Fatal(err)
	}
	m.Cases = append(m.Cases, formatCase{Kind: "symlink", Bucket: bucket, Codec: "gzip", Name: "symlink", File: link, SHA256: formatHash([]byte("small")), Size: 5})
	if err := c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()
	bucket = create("compat-block", "raw")
	c = dial(bucket)
	if err := c.BeginBlockMaintenance(ctx, false); err != nil {
		t.Fatal(err)
	}
	catalog := nbd.NewCatalog(c)
	device, err := catalog.Create(ctx, nbd.Metadata{Name: "disk", Size: 4 << 20, Serial: "compat-serial"})
	if err != nil {
		t.Fatal(err)
	}
	block := make([]byte, device.Size)
	part := formatPattern(8192)
	copy(block[(1<<20)-512:], part)
	if err := catalog.WriteAt(ctx, device, part, (1<<20)-512); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()
	m.Cases = append(m.Cases, formatCase{Kind: "block", Bucket: bucket, Codec: "raw", Name: "disk", Device: device, Size: uint64(len(block)), SHA256: formatHash(block)})
	bucket = create("compat-s3", "lz4")
	c = dial(bucket)
	s3 := client.NewS3(c)
	for _, key := range []string{"folder/text.txt", "large.bin"} {
		payload := []byte("S3 compatibility\n")
		if key == "large.bin" {
			payload = formatPattern((2 << 20) + 99)
		}
		object, err := s3.Put(ctx, key, bytes.NewReader(payload), uint64(len(payload)), client.S3PutOptions{Headers: map[string]string{"content-type": "application/octet-stream"}, Metadata: map[string]string{"compat": "checkpoint"}, Modified: time.Unix(1700000000, 0)})
		if err != nil {
			t.Fatal(err)
		}
		m.Cases = append(m.Cases, formatCase{Kind: "s3", Bucket: bucket, Codec: "lz4", Name: key, S3: object, Size: uint64(len(payload)), SHA256: formatHash(payload)})
	}
	if err := c.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	c.Close()
	return m
}

func verifyFormatCase(t *testing.T, ctx context.Context, want formatCase, dial func(string) *client.Client) {
	t.Helper()
	c := dial(want.Bucket)
	defer c.Close()
	var data []byte
	var err error
	switch want.Kind {
	case "object", "append":
		if want.Kind == "append" {
			result, e := c.Append(ctx, want.Object, []byte("journal-record"), client.AppendOptions{RequestID: want.Request})
			if e != nil || result != want.Append {
				t.Fatal("retained APPEND result", result, e)
			}
		}
		data, _, err = c.Get(ctx, want.Object)
		if want.Quota > 0 && want.Name == "large" {
			if _, e := c.Trunc(ctx, want.Object, want.Quota+1, client.TruncOptions{}); !errors.Is(e, client.ErrFull) {
				t.Fatal("upgraded server no longer enforces the old quota", e)
			}
		}
	case "deleted":
		_, _, err = c.Get(ctx, want.Object)
		if !errors.Is(err, client.ErrNotFound) {
			t.Fatal("deleted object resurrected", err)
		}
		return
	case "file", "symlink":
		f, e := client.OpenFilesystem(ctx, c, client.FSOptions{ReadOnly: true})
		if e != nil {
			t.Fatal(e)
		}
		info, e := f.Lookup(ctx, client.FilesystemRootID(), want.Name)
		if e != nil {
			t.Fatal(e)
		}
		if info.ID != want.File.ID || info.Kind != want.File.Kind || info.Mode != want.File.Mode || info.UID != want.File.UID || info.GID != want.File.GID || info.Nlink != want.File.Nlink || info.MtimeNS != want.File.MtimeNS {
			t.Fatalf("filesystem metadata changed: %+v != %+v", info, want.File)
		}
		if want.Kind == "symlink" {
			data, err = f.Readlink(ctx, info.ID)
		} else {
			if info.Size != want.Size {
				t.Fatal("file size changed", info.Size, want.Size)
			}
			data = make([]byte, want.Size)
			n, e := f.ReadAt(ctx, info.ID, data, 0)
			if e != nil && !errors.Is(e, io.EOF) {
				t.Fatal(e)
			}
			if uint64(n) != want.Size {
				t.Fatal("short file read", n)
			}
			x, e := f.GetXattr(ctx, info.ID, "user.compat")
			if e != nil || string(x) != "attribute" {
				t.Fatal("xattr changed", string(x), e)
			}
			if want.Name == "small" {
				link, e := f.Lookup(ctx, client.FilesystemRootID(), "hardlink")
				if e != nil || link.ID != info.ID {
					t.Fatal("hardlink identity changed", e)
				}
			}
		}
		entries, e := f.ReadDir(ctx, client.FilesystemRootID())
		if e != nil {
			t.Fatal(e)
		}
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name)
		}
		sort.Strings(names)
		if !reflect.DeepEqual(names, []string{"hardlink", "segmented", "small", "sparse", "symlink"}) {
			t.Fatal("filesystem namespace changed", names)
		}
	case "block":
		if err := c.BeginBlockMaintenance(ctx, true); err != nil {
			t.Fatal(err)
		}
		catalog := nbd.NewCatalog(c)
		device, e := catalog.Lookup(ctx, want.Name)
		if e != nil || device != want.Device {
			t.Fatal("block descriptor changed", device, e)
		}
		data = make([]byte, want.Size)
		err = catalog.ReadAt(ctx, device, data, 0)
		devices, e := catalog.List(ctx)
		if e != nil || len(devices) != 1 {
			t.Fatal("block device inventory", devices, e)
		}
	case "s3":
		s3 := client.NewS3(c)
		object, e := s3.Head(ctx, want.Name)
		if e != nil {
			t.Fatal(e)
		}
		if object.Size != want.Size || object.ETag != want.S3.ETag || !object.Modified.Equal(want.S3.Modified) || !reflect.DeepEqual(object.Headers, want.S3.Headers) || !reflect.DeepEqual(object.Metadata, want.S3.Metadata) {
			t.Fatalf("S3 metadata changed: %+v != %+v", object, want.S3)
		}
		var out bytes.Buffer
		err = s3.ReadTo(ctx, object, 0, object.Size, &out)
		data = out.Bytes()
		keys, e := s3.Keys(ctx, "")
		if e != nil {
			t.Fatal(e)
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, []string{"folder/text.txt", "large.bin"}) {
			t.Fatal("S3 inventory changed", keys)
		}
	default:
		t.Fatal("unknown checkpoint case", want.Kind)
	}
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(data)) != want.Size || formatHash(data) != want.SHA256 {
		t.Fatal(fmt.Sprintf("corrupted %s: size=%d hash=%s, want size=%d hash=%s", want.Name, len(data), formatHash(data), want.Size, want.SHA256))
	}
}
