// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"github.com/launix-de/stqr-storage/client"
	"syscall"
	"testing"
)

func TestWindowsCreationInheritsSetgidDirectory(t *testing.T) {
	f, _ := fixture(t)
	f.UID, f.GID = 1234, 2345
	mode, group := uint32(02777), uint32(45678)
	if _, err := f.Volume.SetAttributes(f.Context, client.FilesystemRootID(), client.FSAttributes{Mode: &mode, GID: &group}); err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name      string
		create    func() error
		directory bool
	}{
		{"file", func() error {
			h, err := f.Open("/file", OpenOptions{Create: true, Write: true}, 0644)
			if err == nil {
				err = f.Close(h)
			}
			return err
		}, false},
		{"directory", func() error { return f.Mkdir("/directory", 0755) }, true},
		{"symlink", func() error { return f.Symlink("file", "/symlink") }, false},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			if err := op.create(); err != nil {
				t.Fatal(err)
			}
			info, err := f.Lookup("/" + op.name)
			if err != nil {
				t.Fatal(err)
			}
			if info.UID != f.UID || info.GID != group || op.directory && info.Mode&syscall.S_ISGID == 0 {
				t.Fatalf("lost inherited group/mode: %+v", info)
			}
		})
	}
}
