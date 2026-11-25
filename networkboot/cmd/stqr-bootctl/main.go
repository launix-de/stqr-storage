// SPDX-License-Identifier: GPL-3.0-only
// stqr-bootctl is the bounded streaming storage bridge used by the boot tools.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

type config struct {
	URI      string `json:"uri"`
	Username string `json:"username"`
	Password string `json:"password"`
}
type artifact struct {
	ID     client.ObjectID `json:"id"`
	Size   uint64          `json:"size"`
	SHA256 string          `json:"sha256"`
}
type manifest struct {
	Schema     int                 `json:"schema"`
	RootBucket client.ID           `json:"root_bucket,omitempty"`
	Files      map[string]artifact `json:"files"`
}

var manifestID = client.ObjectID(sha256.Sum256([]byte("STQR networkboot manifest v1")))
var names = []string{"rootfs.tar", "disk.qcow2", "vmlinuz", "initrd.img", "release.json"}

type apiError struct {
	Status uint64
	Detail string
}

func (e *apiError) Error() string { return fmt.Sprintf("STQR status %d: %s", e.Status, e.Detail) }
func request(ctx context.Context, c config, op uint16, body []byte) ([]byte, error) {
	return requestWithID(ctx, c, op, body, client.NewRequestID())
}
func requestWithID(ctx context.Context, c config, op uint16, body []byte, id client.ID) ([]byte, error) {
	m := client.Message{Kind: client.Request, Opcode: op, ID: id, Body: body}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(c.URI, "/")+"/api/message", bytes.NewReader(client.EncodeMessage(m)))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.Username, c.Password)
	req.Header.Set("Content-Type", "application/octet-stream")
	h := http.Client{Timeout: 24 * time.Hour, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if op == client.BucketInfoGet {
		h.Timeout = 30 * time.Second
	}
	r, err := h.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, client.MaxMessage+1))
	if err != nil {
		return nil, err
	}
	if len(b) > client.MaxMessage {
		return nil, client.ErrProtocol
	}
	reply, err := client.DecodeMessage(b)
	if err != nil {
		return nil, fmt.Errorf("invalid response (HTTP %d): %w", r.StatusCode, err)
	}
	if reply.ID != m.ID || reply.Opcode != op || reply.Kind != client.Response {
		return nil, client.ErrProtocol
	}
	d := client.Decoder{B: reply.Body}
	status := d.V()
	headers := d.Blob()
	if d.Err != nil {
		return nil, d.Err
	}
	if status >= 300 {
		detail := "request rejected"
		hd := client.Decoder{B: headers}
		for n := hd.V(); n > 0 && hd.Err == nil; n-- {
			key := hd.V()
			value := hd.Blob()
			if key == 3 {
				text := client.Decoder{B: value}
				detail = text.Text()
			}
		}
		return nil, &apiError{status, detail}
	}
	return d.B, nil
}
func resolve(ctx context.Context, c config, name string) (client.ID, error) {
	e := client.Encoder{}
	e.Text(name)
	b, err := request(ctx, c, client.BucketInfoGet, e.B)
	if err != nil {
		return client.ID{}, err
	}
	d := client.Decoder{B: b}
	id := d.ID()
	if d.Err != nil || id == (client.ID{}) {
		return id, client.ErrProtocol
	}
	return id, nil
}
func create(ctx context.Context, c config, name string) (client.ID, error) {
	id := client.NewRequestID()
	return createID(ctx, c, name, id)
}
func createID(ctx context.Context, c config, name string, id client.ID) (client.ID, error) {
	return createIDWithQuota(ctx, c, name, id, 0)
}
func createIDWithQuota(ctx context.Context, c config, name string, id client.ID, quota uint64) (client.ID, error) {
	e := client.Encoder{}
	e.ID(id)
	e.U64(1)
	e.Text(name)
	e.U8(0)
	e.U8(0)
	e.U8(0)
	e.U8(0)
	e.U8(0)
	e.U16(1)
	e.U16(0)
	e.U16(0)
	e.Bool(false)
	e.U16(0)
	e.U64(0)
	e.U8(0)
	e.U64(0)
	e.Bool(quota == 0)
	e.U64(quota)
	e.U32(150000)
	_, err := control(ctx, c, client.BucketCreate, e.B)
	return id, err
}
func loadConfig(path string) (config, error) {
	c, err := nbd.LoadConfig(path)
	return config{URI: c.URL, Username: c.Username, Password: c.Password}, err
}
func connect(ctx context.Context, c config, name string) (*client.Client, error) {
	id, err := resolve(ctx, c, name)
	if err != nil {
		return nil, err
	}
	return client.Dial(ctx, c.URI, c.Username, c.Password, id, "bd")
}
func publish(ctx context.Context, c config, bucket, dir string) error {
	release, err := lockPublication(dir)
	if err != nil {
		return err
	}
	defer release()

	conn, err := connect(ctx, c, bucket)
	if err != nil {
		return err
	}
	defer conn.Close()
	old, _, err := conn.Get(ctx, manifestID)
	manifestAbsent := errors.Is(err, client.ErrNotFound)
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return err
	}

	for _, name := range []string{"rootfs.tar", "vmlinuz", "initrd.img", "release.json"} {
		if info, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return err
		} else if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact %s is not a regular file", name)
		}
	}
	m := manifest{Schema: 2, Files: map[string]artifact{}}
	m.RootBucket, err = publishNativeRoot(ctx, c, dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == "disk.qcow2" || (name == "rootfs.tar" && m.Schema == 2) {
			continue
		}
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		h := sha256.New()
		n, err := io.Copy(h, f)
		if err != nil {
			f.Close()
			return err
		}
		if _, err = f.Seek(0, 0); err != nil {
			f.Close()
			return err
		}
		digest := h.Sum(nil)
		var id client.ObjectID
		copy(id[:], digest)
		fmt.Fprintf(os.Stderr, "Publishing %s (%d bytes)\n", name, n)
		_, err = uploadArtifact(ctx, conn, id, f, uint64(n), artifactTransferBytes)
		f.Close()
		if err != nil {
			return err
		}
		m.Files[name] = artifact{id, uint64(n), hex.EncodeToString(digest)}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	opt := client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID()}}
	if manifestAbsent {
		opt.Absent = true
	}
	_, err = conn.CAS(ctx, manifestID, 0, old, raw, opt)
	return err
}
func fetch(ctx context.Context, c config, bucket, dir string, native bool) error {
	conn, err := connect(ctx, c, bucket)
	if err != nil {
		return err
	}
	defer conn.Close()
	raw, _, err := conn.Get(ctx, manifestID)
	if err != nil {
		return err
	}
	var m manifest
	if err = json.Unmarshal(raw, &m); err != nil || (m.Schema != 1 && m.Schema != 2) {
		return client.ErrProtocol
	}
	if native && (m.Schema != 2 || m.RootBucket == (client.ID{})) {
		return errors.New("legacy boot release: run prepare-system --publish-only BUCKET")
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for _, name := range names {
		if name == "disk.qcow2" || (name == "rootfs.tar" && m.Schema == 2) {
			continue
		}
		a, ok := m.Files[name]
		if !ok {
			return fmt.Errorf("missing artifact %s", name)
		}
		f, err := os.CreateTemp(dir, ".download-*")
		if err != nil {
			return err
		}
		h := sha256.New()
		info, err := copyArtifact(ctx, conn, a.ID, a.Size, artifactTransferBytes, io.MultiWriter(f, h))
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil && (info.Length != a.Size || hex.EncodeToString(h.Sum(nil)) != a.SHA256) {
			err = errors.New("artifact checksum mismatch")
		}
		if err == nil {
			err = os.Rename(f.Name(), filepath.Join(dir, name))
		}
		if err != nil {
			os.Remove(f.Name())
			return err
		}
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600)
}
func run() error {
	flags := flag.NewFlagSet("stqr-bootctl", flag.ContinueOnError)
	cfg := flags.String("config", "stqr.yaml", "STQR YAML or private JSON credentials file")
	size := flags.Uint64("size", 32<<30, "virtual disk bytes")
	socket := flags.String("socket", "", "NBD Unix socket")
	device := flags.String("device", "disk.raw", "named block device/default export")
	ro := flags.Bool("readonly", false, "readonly NBD export")
	record := flags.String("session-record", "", "private boot-session recovery record")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: stqr-bootctl --config FILE [--size BYTES|--socket PATH] COMMAND BUCKET [ARG]\nCommands: prepare-logins OUTPUT_JSON, resolve, manifest, create, clear, delete, clone SOURCE DEST, publish BUCKET DIRECTORY, fetch BUCKET DIRECTORY, init-disk, nbd, gc, boot-session-create ROOT_BUCKET, boot-session-delete USER_ID UPPER_ID LOWER_ID")
		flags.PrintDefaults()
	}
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	args := flags.Args()
	if len(args) < 2 {
		flags.Usage()
		return errors.New("command and bucket required")
	}
	c, err := loadConfig(*cfg)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cmd, name := args[0], args[1]
	switch cmd {
	case "prepare-logins":
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		return prepareLogins(ctx, c, name)
	case "boot-session-create", "boot-session-delete":
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		return nativeCommand(ctx, c, cmd, args[1:], *size, *record)
	case "manifest":
		conn, err := connect(ctx, c, name)
		if err != nil {
			return err
		}
		defer conn.Close()
		raw, _, err := conn.Get(ctx, manifestID)
		if err != nil {
			return err
		}
		fmt.Println(string(raw))
		return nil
	case "check-exclusive":
		_, err := request(ctx, c, client.BucketMaintenanceStatus, nil)
		return err
	case "create":
		id, err := create(ctx, c, name)
		if err == nil {
			fmt.Println(id.String())
		}
		return err
	case "resolve":
		id, err := resolve(ctx, c, name)
		if err == nil {
			fmt.Println(id.String())
		}
		return err
	case "delete":
		id, err := parseBootID(name)
		if err != nil {
			id, err = resolve(ctx, c, name)
		}
		if err != nil {
			return err
		}
		e := client.Encoder{}
		e.ID(id)
		_, err = control(ctx, c, client.BucketDelete, e.B)
		return err
	case "clear", "clone":
		src, err := parseBootID(name)
		if err != nil {
			src, err = resolve(ctx, c, name)
		}
		if err != nil {
			return err
		}
		e := client.Encoder{}
		e.ID(src)
		op := client.BucketClear
		if cmd == "clone" {
			if len(args) != 3 {
				return errors.New("clone requires existing empty destination")
			}
			dst, err := resolve(ctx, c, args[2])
			if err != nil {
				return err
			}
			e.ID(dst)
			op = client.BucketClone
		}
		_, err = control(ctx, c, op, e.B)
		return err
	case "publish", "fetch", "fetch-native":
		if len(args) != 3 {
			return errors.New("directory required")
		}
		if cmd == "publish" {
			return publish(ctx, c, name, args[2])
		}
		return fetch(ctx, c, name, args[2], cmd == "fetch-native")
	case "gc":
		conn, err := connect(ctx, c, name)
		if err != nil {
			return err
		}
		defer conn.Close()
		if err = conn.BeginMaintenance(ctx); err != nil {
			return err
		}
		raw, _, err := conn.Get(ctx, manifestID)
		if err != nil {
			return err
		}
		var m manifest
		if err = json.Unmarshal(raw, &m); err != nil || (m.Schema != 1 && m.Schema != 2) {
			return client.ErrProtocol
		}
		roots := []client.ObjectID{manifestID}
		for _, name := range names {
			if name == "disk.qcow2" || (name == "rootfs.tar" && m.Schema == 2) {
				continue
			}
			a, ok := m.Files[name]
			if !ok {
				return client.ErrProtocol
			}
			info, err := conn.Stat(ctx, a.ID)
			if err != nil {
				return err
			}
			if info.Length != a.Size {
				return client.ErrProtocol
			}
			roots = append(roots, a.ID)
		}
		result, err := nbd.NewCatalog(conn).Collect(ctx, roots)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	case "init-disk", "check-install-disk", "nbd":
		id, err := resolve(ctx, c, name)
		if err != nil {
			return err
		}
		b, err := nbd.Open(ctx, c.URI, c.Username, c.Password, id, *ro)
		if err != nil {
			return err
		}
		defer b.Close()
		if cmd == "check-install-disk" {
			if _, err := b.Client.Stat(ctx, manifestID); err == nil {
				return errors.New("image is already published; use edit-image instead")
			} else if !errors.Is(err, client.ErrNotFound) {
				return err
			}
			m, err := b.Lookup(ctx, *device)
			if err != nil {
				return err
			}
			if m.Size != *size {
				return fmt.Errorf("existing disk is %d bytes; requested %d bytes", m.Size, *size)
			}
			return nil
		}
		if cmd == "init-disk" {
			if *ro {
				return errors.New("readonly connection")
			}
			_, err = b.Create(ctx, nbd.Metadata{Name: *device, Size: *size})
			return err
		}
		if *socket == "" {
			return errors.New("--socket required")
		}
		return nbd.Serve(ctx, *socket, b.Catalog, nbd.Options{DefaultExport: *device, ReadOnly: *ro})
	}
	return errors.New("unknown command")
}
func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "stqr-bootctl:", err)
		os.Exit(1)
	}
}
