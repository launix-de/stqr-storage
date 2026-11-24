// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"crypto/sha256"
	"github.com/go-git/go-billy/v5/osfs"
	nfs "github.com/willscott/go-nfs"
	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"io"
	"net"
	"os"
	"strings"
	"testing"
)

func TestNFSReadOnlyExport(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(dir+"/rootfs.squashfs", []byte("image contents"), 0644); e != nil {
		t.Fatal(e)
	}
	h := &handler{root: readonly{osfs.New(dir, osfs.WithBoundOS())}, handles: map[string][]string{}, release: "test-release"}
	for _, p := range []string{"", "rootfs.squashfs"} {
		sum := sha256.Sum256([]byte(h.release + "\x00" + p))
		var parts []string
		if p != "" {
			parts = []string{p}
		}
		h.handles[string(sum[:])] = parts
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	go nfs.Serve(l, h)
	c, e := rpc.DialTCP("tcp", l.Addr().String(), false)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	m := nfsc.Mount{Client: c}
	target, e := m.Mount("/", rpc.AuthNull)
	if e != nil {
		t.Fatal(e)
	}
	f, e := target.Open("rootfs.squashfs")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := io.ReadAll(f)
	if e != nil || string(b) != "image contents" {
		t.Fatal(string(b), e)
	}
	if _, e = f.Write([]byte("changed")); e == nil {
		t.Fatal("NFS write succeeded")
	}
	if _, e = h.root.OpenFile("rootfs.squashfs", os.O_WRONLY, 0); e == nil {
		t.Fatal("write open succeeded")
	}

	oldHandle := h.ToHandle(h.root, []string{"rootfs.squashfs"})
	if e := os.Mkdir(dir+"/"+strings.Repeat("a", 64), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(dir+"/"+strings.Repeat("a", 64)+"/rootfs.squashfs", []byte("new release"), 0644); e != nil {
		t.Fatal(e)
	}
	sub, e := h.root.Chroot(strings.Repeat("a", 64))
	if e != nil {
		t.Fatal(e)
	}
	if h.ToHandle(sub, []string{"rootfs.squashfs"}) == nil {
		t.Fatal("newly cached release could not be registered")
	}
	if _, _, e := h.FromHandle(oldHandle); e != nil {
		t.Fatal("old release handle lost", e)
	}
	if _, _, e = h.FromHandle([]byte("unknown")); e == nil {
		t.Fatal("unknown handle accepted")
	}
}
