// SPDX-License-Identifier: GPL-3.0-only
// Read-only NFSv3 frontend for an extracted immutable networkboot release.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	billy "github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
)

type readonly struct{ billy.Filesystem }

func (r readonly) Capabilities() billy.Capability    { return billy.ReadCapability | billy.SeekCapability }
func (r readonly) Create(string) (billy.File, error) { return nil, os.ErrPermission }
func (r readonly) OpenFile(p string, flags int, mode os.FileMode) (billy.File, error) {
	if flags != os.O_RDONLY {
		return nil, os.ErrPermission
	}
	return r.Filesystem.Open(p)
}
func (r readonly) Rename(string, string) error                 { return os.ErrPermission }
func (r readonly) Remove(string) error                         { return os.ErrPermission }
func (r readonly) MkdirAll(string, os.FileMode) error          { return os.ErrPermission }
func (r readonly) TempFile(string, string) (billy.File, error) { return nil, os.ErrPermission }
func (r readonly) Symlink(string, string) error                { return os.ErrPermission }
func (r readonly) Chroot(p string) (billy.Filesystem, error) {
	f, e := r.Filesystem.Chroot(p)
	if e != nil {
		return nil, e
	}
	return readonly{f}, nil
}

type handler struct {
	mu      sync.RWMutex
	root    billy.Filesystem
	handles map[string][]string
	release string
}

func (h *handler) Mount(ctx context.Context, c net.Conn, r nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	p := string(r.Dirpath)
	if p != "/" {
		if len(p) != 65 || p[0] != '/' {
			return nfs.MountStatus(13), nil, nil
		}
		for _, ch := range p[1:] {
			if !strings.ContainsRune("0123456789abcdef", ch) {
				return nfs.MountStatus(13), nil, nil
			}
		}
	}
	f, err := h.root.Chroot(p)
	if err != nil {
		return nfs.MountStatus(2), nil, nil
	}
	if _, err = f.Stat("."); err != nil {
		return nfs.MountStatus(2), nil, nil
	}
	return nfs.MountStatusOk, f, []nfs.AuthFlavor{nfs.AuthFlavorUnix, nfs.AuthFlavorNull}
}
func (h *handler) Change(billy.Filesystem) billy.Change                        { return nil }
func (h *handler) FSStat(context.Context, billy.Filesystem, *nfs.FSStat) error { return nil }
func (h *handler) ToHandle(f billy.Filesystem, p []string) []byte {
	if f == nil {
		return nil
	}
	prefix, err := filepath.Rel(h.root.Root(), f.Root())
	if err != nil || strings.HasPrefix(prefix, "..") {
		return nil
	}
	if prefix != "." {
		p = append([]string{filepath.ToSlash(prefix)}, p...)
	}
	joined := path.Clean(path.Join(p...))
	if joined == "." {
		joined = ""
	}
	if strings.HasPrefix(joined, "../") || strings.HasPrefix(joined, "/") || joined == ".." {
		return nil
	}
	sum := sha256.Sum256([]byte(h.release + "\x00" + joined))
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.handles[string(sum[:])]; !ok {
		probe := joined
		if probe == "" {
			probe = "."
		}
		if _, err := h.root.Lstat(probe); err != nil {
			return nil
		}
		var parts []string
		if joined != "" {
			parts = strings.Split(joined, "/")
		}
		h.handles[string(sum[:])] = parts
	}
	return sum[:]
}
func (h *handler) FromHandle(b []byte) (billy.Filesystem, []string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, ok := h.handles[string(b)]
	if !ok {
		return nil, nil, os.ErrNotExist
	}
	return h.root, p, nil
}
func (h *handler) InvalidateHandle(billy.Filesystem, []byte) error { return os.ErrPermission }
func (h *handler) HandleLimit() int                                { h.mu.RLock(); defer h.mu.RUnlock(); return len(h.handles) + 1 }

type allowedListener struct {
	net.Listener
	subnet *net.IPNet
}

func (l allowedListener) Accept() (net.Conn, error) {
	for {
		c, e := l.Listener.Accept()
		if e != nil {
			return nil, e
		}
		ip, _, e := net.SplitHostPort(c.RemoteAddr().String())
		if e == nil && l.subnet.Contains(net.ParseIP(ip)) {
			return c, nil
		}
		c.Close()
	}
}
func run() error {
	root := flag.String("root", "", "immutable extracted root")
	listen := flag.String("listen", "127.0.0.1:2049", "TCP NFS and mount protocol endpoint")
	cidr := flag.String("allow", "", "permitted client CIDR")
	release := flag.String("release", "", "immutable release digest")
	flag.Parse()
	if *root == "" || *cidr == "" || len(*release) != 64 {
		return errors.New("--root, --allow and --release required")
	}
	_, subnet, err := net.ParseCIDR(*cidr)
	if err != nil {
		return err
	}
	h := &handler{root: readonly{osfs.New(*root, osfs.WithBoundOS(), osfs.WithDeduplicatePath(false))}, handles: map[string][]string{}, release: *release}
	err = filepath.WalkDir(*root, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(*root, p)
		if e != nil {
			return e
		}
		var parts []string
		if rel != "." {
			parts = strings.Split(filepath.ToSlash(rel), "/")
		}
		sum := sha256.Sum256([]byte(h.release + "\x00" + path.Join(parts...)))
		h.handles[string(sum[:])] = parts
		return nil
	})
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer l.Close()
	return nfs.Serve(allowedListener{l, subnet}, h)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
