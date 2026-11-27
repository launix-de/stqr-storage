// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	"github.com/launix-de/stqr-storage/server"
)

type imageWire struct {
	net.Conn
	accepted atomic.Bool
	fences   atomic.Int32
}

func (c *imageWire) Write(p []byte) (int, error) {
	if len(p) >= 2 && p[0] == 0x82 && p[1]&128 == 0 {
		off, n := 2, int(p[1]&127)
		if n == 126 && len(p) >= 4 {
			off, n = 4, int(binary.BigEndian.Uint16(p[2:4]))
		}
		if n < 65536 && len(p) == off+n {
			m, e := client.DecodeMessage(p[off:])
			if e == nil && m.Kind == client.Response {
				if m.Opcode == client.ObjectAccepted {
					c.accepted.Store(true)
				}
				if m.Opcode == client.ConnectionSync {
					c.fences.Add(1)
				}
			}
		}
	}
	return c.Conn.Write(p)
}

type imageListener struct {
	net.Listener
	mu          sync.Mutex
	connections []*imageWire
	userCreates atomic.Int32
}

func (l *imageListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	w := &imageWire{Conn: c}
	l.mu.Lock()
	l.connections = append(l.connections, w)
	l.mu.Unlock()
	return w, nil
}
func (l *imageListener) importWasFenced() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	found := false
	for _, c := range l.connections {
		found = found || c.accepted.Load()
		if c.accepted.Load() && c.fences.Load() == 0 {
			return false
		}
	}
	return found
}
func imageTestNode(t *testing.T) (config, *[]string, *imageListener) {
	t.Helper()
	httpServer := httptest.NewUnstartedServer(nil)
	listener := &imageListener{Listener: httpServer.Listener}
	httpServer.Listener = listener
	path := filepath.Join(t.TempDir(), "node.yaml")
	opts, err := server.ParseOptions([]string{"--config", path, "--listen", httpServer.Listener.Addr().String(), "--advertise", "http://" + httpServer.Listener.Addr().String(), "--system-volume-bytes", "1048576"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	node, err := server.Open(opts)
	if err != nil {
		httpServer.Close()
		t.Fatal(err)
	}
	var mu sync.Mutex
	creates := []string{}
	handler := node.Handler()
	httpServer.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/message" {
			raw, e := io.ReadAll(r.Body)
			if e != nil {
				t.Error(e)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			m, e := client.DecodeMessage(raw)
			if e == nil && m.Opcode == client.UserCreate {
				listener.userCreates.Add(1)
			}
			if e == nil && (m.Opcode == client.BucketCreate || m.Opcode == client.BucketCreateInGroup) {
				d := client.Decoder{B: m.Body}
				if m.Opcode == client.BucketCreateInGroup {
					d.U16()
					d.ID()
				} else {
					d.ID()
					d.U64()
				}
				name := d.Text()
				mu.Lock()
				creates = append(creates, name)
				mu.Unlock()
			}
		}
		handler.ServeHTTP(w, r)
	})
	httpServer.Start()
	t.Cleanup(func() { node.Close(); httpServer.Close() })
	if _, err = node.AddVolume(client.NewRequestID().String(), server.AddVolumeRequest{Name: "images", Class: "hdd", SizeBytes: 128 << 20}); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, &creates, listener
}
func imageTestArchive(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "rootfs.tar"))
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	payload := []byte("native image data")
	for _, h := range []*tar.Header{{Name: "./", Typeflag: tar.TypeDir, Mode: 0755}, {Name: "./hello", Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(payload))}} {
		if err = tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err = tw.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"vmlinuz", "initrd.img", "release.json"} {
		if err = os.WriteFile(filepath.Join(dir, name), []byte("artifact-"+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
func TestPublishedRootfsAndArtifactsUseNamedBucket(t *testing.T) {
	cfg, creates, wire := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := imageTestArchive(t)
	if err := publish(ctx, cfg, "ubuntu", dir); err != nil {
		t.Fatal(err)
	}
	if !wire.importWasFenced() {
		t.Fatal("published root import without explicit connection sync")
	}
	if err := publish(ctx, cfg, "ubuntu", dir); err != nil {
		t.Fatal("idempotent publication retry", err)
	}
	if len(*creates) != 1 || (*creates)[0] != "ubuntu" {
		t.Fatalf("unexpected hidden buckets: %v", *creates)
	}
	conn, err := connectImage(ctx, cfg, "ubuntu", false)
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := conn.Get(ctx, manifestID)
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.RootBucket != conn.BucketID() {
		t.Fatal("manifest points outside image bucket")
	}
	fs, err := client.OpenFilesystem(ctx, conn, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	file, err := fs.Lookup(ctx, client.FilesystemRootID(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, file.Size)
	if _, err = fs.ReadAt(ctx, file.ID, data, 0); err != nil || string(data) != "native image data" {
		t.Fatal(string(data), err)
	}
	lower := conn.BucketID()
	conn.Close()
	unlock := client.Encoder{}
	unlock.ID(lower)
	unlock.Bool(false)
	if _, err = control(ctx, cfg, client.BucketLock, unlock.B); err != nil {
		t.Fatal(err)
	}
	conn, err = connectImage(ctx, cfg, "ubuntu", false)
	if err != nil {
		t.Fatal(err)
	}
	orphan := client.ObjectID{0xee}
	if _, err = conn.Put(ctx, orphan, []byte("unreachable"), client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	cfgPath := filepath.Join(t.TempDir(), "credentials.json")
	cfgRaw, _ := json.Marshal(cfg)
	if err = os.WriteFile(cfgPath, cfgRaw, 0600); err != nil {
		t.Fatal(err)
	}
	previous := os.Args
	os.Args = []string{"stqr-bootctl", "--config", cfgPath, "gc", "ubuntu"}
	err = run()
	os.Args = previous
	if err != nil {
		t.Fatal("filesystem-aware image GC", err)
	}
	conn, err = connectImage(ctx, cfg, "ubuntu", false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Stat(ctx, orphan); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("orphan retained", err)
	}
	if _, _, err = conn.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal("rootfs was swept", err)
	}
	conn.Close()
	lock := client.Encoder{}
	lock.ID(lower)
	lock.Bool(true)
	if _, err = control(ctx, cfg, client.BucketLock, lock.B); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if err = fetch(ctx, cfg, "ubuntu", dest); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dest, "initrd.img"))
	if err != nil || string(got) != "artifact-initrd.img" {
		t.Fatal(string(got), err)
	}
	// A retry after a completed root import reuses that tree, not another bucket.
	id, err := publishNativeRoot(ctx, cfg, dir, lower)
	if err != nil || id != lower {
		t.Fatal(id, err)
	}
	if len(*creates) != 1 {
		t.Fatal("retry created another root bucket", *creates)
	}
	session, err := newBootSession(ctx, cfg, lower.String(), 8<<20, "")
	if err != nil {
		t.Fatal(err)
	}
	if session.Lower != lower || session.Upper == lower {
		t.Fatal("invalid upper/lower scope")
	}
	if err = removeBootSession(ctx, cfg, session.User, session.Upper, session.Lower); err != nil {
		t.Fatal(err)
	}
	// Session deletion must never delete or revoke the actual published image.
	conn, err = connectImage(ctx, cfg, "ubuntu", false)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, _, err = conn.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal(err)
	}
}
func TestMissingPublishedRootCreatesNoTemporaryResources(t *testing.T) {
	cfg, creates, wire := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := newBootSession(ctx, cfg, client.NewRequestID().String(), 8<<20, "")
	if err == nil {
		t.Fatal("missing image accepted")
	}
	if wire.userCreates.Load() != 0 {
		t.Fatal("created temporary account before validating image")
	}
	if len(*creates) != 0 {
		t.Fatal("allocated temporary bucket before checking lower", *creates)
	}
}

func TestUninitializedPublishedRootCreatesNoTemporaryAccount(t *testing.T) {
	cfg, creates, wire := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := create(ctx, cfg, "empty-image")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := connectImage(ctx, cfg, "empty-image", false)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if _, err = newBootSession(ctx, cfg, id.String(), 8<<20, ""); err == nil {
		t.Fatal("uninitialized root accepted")
	}
	if len(*creates) != 1 || wire.userCreates.Load() != 0 {
		t.Fatal("allocated resources before validating filesystem")
	}
}

func TestImageFetchRejectsBDBucketsWithoutChangingTheirFormat(t *testing.T) {
	cfg, creates, _ := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	id, err := create(ctx, cfg, "legacy-ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := connect(ctx, cfg, "legacy-ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	m := manifest{Schema: 2, RootBucket: client.NewRequestID(), Files: map[string]artifact{}}
	for _, name := range []string{"vmlinuz", "initrd.img", "release.json"} {
		data := []byte("existing-" + name)
		sum := sha256.Sum256(data)
		oid := client.ObjectID(sum)
		if _, err = conn.Put(ctx, oid, data, client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
			t.Fatal(err)
		}
		m.Files[name] = artifact{ID: oid, Size: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	raw, _ := json.Marshal(m)
	if _, err = conn.Put(ctx, manifestID, raw, client.PutOptions{RequestID: client.NewRequestID()}); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	dest := t.TempDir()
	if err = fetch(ctx, cfg, "legacy-ubuntu", dest); err == nil {
		t.Fatal("boot fetch accepted a separate BD artifact bucket")
	}
	if _, err = os.Stat(filepath.Join(dest, "initrd.img")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rejected BD image wrote boot artifacts", err)
	}

	conn, err = connect(ctx, cfg, "legacy-ubuntu")
	if err != nil {
		t.Fatal("legacy format changed", err)
	}
	defer conn.Close()
	if conn.BucketID() != id || len(*creates) != 1 {
		t.Fatal("legacy read replaced the bucket")
	}
}

func TestNativeImageEditorAndRecursiveCOW(t *testing.T) {
	cfg, creates, _ := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := publish(ctx, cfg, "ubuntu", imageTestArchive(t)); err != nil {
		t.Fatal(err)
	}
	if err := cloneImage(ctx, cfg, "ubuntu", "ubuntu-next"); err != nil {
		t.Fatal(err)
	}
	if err := cloneImage(ctx, cfg, "ubuntu-next", "ubuntu-next-next"); err != nil {
		t.Fatal(err)
	}
	if err := cloneImage(ctx, cfg, "ubuntu", "ubuntu-next"); err == nil {
		t.Fatal("clobbered an existing clone")
	}
	for _, name := range []string{"ubuntu", "ubuntu-next", "ubuntu-next-next"} {
		conn, err := connectImage(ctx, cfg, name, false)
		if err != nil {
			t.Fatal(err)
		}
		raw, _, err := conn.Get(ctx, manifestID)
		if err != nil {
			t.Fatal(err)
		}
		var m manifest
		if json.Unmarshal(raw, &m) != nil || m.RootBucket != conn.BucketID() {
			t.Fatal("clone root not rebased", name)
		}
		conn.Close()
	}
	if len(*creates) != 3 {
		t.Fatal("hidden installation or root bucket", *creates)
	}
	session, err := newImageSession(ctx, cfg, "ubuntu-next-next", "ubuntu-next-next", 8<<20, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if session.Mode != "edit" || session.Upper != session.Lower {
		t.Fatal("editor did not mount image itself")
	}
	if len(*creates) != 3 {
		t.Fatal("editor allocated an upper bucket", *creates)
	}
	// A misclassified cleanup must be rejected before revoking the editor.
	if err = removeBootSession(ctx, cfg, session.User, session.Upper, session.Lower); err == nil {
		t.Fatal("overlay cleanup accepted image as disposable upper")
	}
	editor, err := client.Dial(ctx, session.URI, session.Username, session.Password, session.Upper, "fs")
	if err != nil {
		t.Fatal(err)
	}
	fs, err := client.OpenFilesystem(ctx, editor, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := fs.Lookup(ctx, client.FilesystemRootID(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	edited := []byte("edited grandchild")
	if _, err = fs.WriteAt(ctx, entry.ID, edited, 0); err != nil {
		t.Fatal(err)
	}
	if err = editor.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err = removeImageSession(ctx, cfg, session.User, session.Upper, session.Lower, true); err == nil {
		t.Fatal("cleanup revoked an active editor")
	}
	if _, _, err = editor.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal("active editor credentials revoked", err)
	}
	editor.Close()
	if err = removeImageSession(ctx, cfg, session.User, session.Upper, session.Lower, true); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ubuntu", "ubuntu-next", "ubuntu-next-next"} {
		conn, err := connectImage(ctx, cfg, name, false)
		if err != nil {
			t.Fatal("image deleted on editor disconnect", name, err)
		}
		fs, err := client.OpenFilesystem(ctx, conn, client.FSOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		entry, err := fs.Lookup(ctx, client.FilesystemRootID(), "hello")
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, entry.Size)
		if _, err = fs.ReadAt(ctx, entry.ID, got, 0); err != nil {
			t.Fatal(err)
		}
		want := "native image data"
		if name == "ubuntu-next-next" {
			want = string(edited)
		}
		if string(got) != want {
			t.Fatalf("COW isolation %s: %q != %q", name, got, want)
		}
		conn.Close()
	}
	boot, err := newBootSession(ctx, cfg, "ubuntu", 8<<20, "", "ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	if len(*creates) != 4 || (*creates)[3] != "ubuntu-"+boot.Upper.String() {
		t.Fatal("session upper not named [image]-UUID", *creates)
	}
	if err = removeBootSession(ctx, cfg, boot.User, boot.Upper, boot.Lower); err != nil {
		t.Fatal(err)
	}
}

// Execute the real CLI entry point in a disposable test child, so os.Exit(75)
// and silent stderr are verified rather than inferred from the typed error.
func TestBootctlCleanupProcess(t *testing.T) {
	if os.Getenv("STQR_BOOTCTL_CLEANUP_TEST_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child arguments")
	}
	os.Args = append([]string{"stqr-bootctl"}, os.Args[separator+1:]...)
	main()
}

func TestBootCleanupRetainsMountedUpperUntilStorageConnectionCloses(t *testing.T) {
	cfg, _, _ := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := publish(ctx, cfg, "ubuntu", imageTestArchive(t)); err != nil {
		t.Fatal(err)
	}
	session, err := newBootSession(ctx, cfg, "ubuntu", 8<<20, "", "ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	mounted, err := client.Dial(ctx, session.URI, session.Username, session.Password, session.Upper, "fs")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = mounted.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal(err)
	}
	other, err := newBootSession(ctx, cfg, "ubuntu", 8<<20, "", "ubuntu")
	if err != nil {
		t.Fatal(err)
	}
	otherMount, err := client.Dial(ctx, other.URI, other.Username, other.Password, other.Upper, "fs")
	if err != nil {
		t.Fatal(err)
	}
	defer otherMount.Close()

	// Repeated failed collectors must release their own sockets and keep the
	// mounted guest usable. Otherwise every later attempt would see a leak.
	for attempt := 0; attempt < 8; attempt++ {
		if err = removeBootSession(ctx, cfg, session.User, session.Upper, session.Lower); !errors.Is(err, errSessionActive) {
			t.Fatalf("expected typed active deferral, got %v", err)
		}
	}
	configPath := filepath.Join(t.TempDir(), "cleanup.json")
	configBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, configBytes, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBootctlCleanupProcess$", "--",
		"--config", configPath, "boot-session-delete", session.User.String(), session.Upper.String(), session.Lower.String())
	child.Env = append(os.Environ(), "STQR_BOOTCTL_CLEANUP_TEST_CHILD=1")
	output, childErr := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(childErr, &exit) || exit.ExitCode() != 75 || len(output) != 0 {
		t.Fatalf("active cleanup CLI: error=%v output=%q", childErr, output)
	}
	// Failed collection must not revoke the currently active VM's identity.
	if _, _, err = mounted.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal("live upper access revoked", err)
	}
	mounted.Close()
	for {
		err = removeBootSession(ctx, cfg, session.User, session.Upper, session.Lower)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A concurrent guest on the same lower has an independent identity/upper.
	// Collecting one must neither block on nor revoke the other.
	if _, _, err = otherMount.Get(ctx, client.FilesystemRootID()); err != nil {
		t.Fatal("independent live guest was disrupted", err)
	}
	if conn, e := client.Dial(ctx, session.URI, session.Username, session.Password, session.Lower, "fs"); e == nil {
		conn.Close()
		t.Fatal("temporary account retained")
	}
	conn, e := connectImage(ctx, cfg, "ubuntu", false)
	if e != nil {
		t.Fatal("published image lost", e)
	}
	conn.Close()
	// Replaying cleanup after a lost result is safe once the namespace is gone.
	if err = removeBootSession(ctx, cfg, session.User, session.Upper, session.Lower); err != nil {
		t.Fatal("cleanup not retryable", err)
	}
}

func TestInstallationCommandsKeepCanonicalBucketAfterNameReuse(t *testing.T) {
	cfg, _, _ := imageTestNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	original, err := create(ctx, cfg, "ubuntu-bd")
	if err != nil {
		t.Fatal(err)
	}
	rename := client.Encoder{}
	rename.ID(original)
	rename.Text("original-installation")
	if _, err = control(ctx, cfg, client.BucketRename, rename.B); err != nil {
		t.Fatal(err)
	}
	replacement, err := create(ctx, cfg, "ubuntu-bd")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "credentials.json")
	raw, _ := json.Marshal(cfg)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	invoke := func(command, selector string) error {
		previous := os.Args
		defer func() { os.Args = previous }()
		os.Args = []string{"stqr-bootctl", "--config", path, "--size", "1048576", command, selector}
		return run()
	}
	if err = invoke("init-disk", original.String()); err != nil {
		t.Fatal("canonical installation identity was re-resolved as a name", err)
	}
	if err = invoke("check-install-disk", original.String()); err != nil {
		t.Fatal("canonical installation could not be resumed", err)
	}
	other, err := connect(ctx, cfg, replacement.String())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	// A real successful initialisation publishes this descriptor. The new
	// bucket which took the original label must remain empty.
	if _, err = other.Stat(ctx, nbd.MetadataID("disk.raw")); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("name-reusing replacement was initialised", err)
	}
	if err = invoke("delete", original.String()); err != nil {
		t.Fatal(err)
	}
	if got, err := resolve(ctx, cfg, "ubuntu-bd"); err != nil || got != replacement {
		t.Fatal("cleanup deleted the replacement", got, err)
	}
}
