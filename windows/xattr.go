// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"strings"
	"syscall"
)

type xattrView interface {
	GetXattr(context.Context, client.ObjectID, string) ([]byte, error)
	ListXattr(context.Context, client.ObjectID) ([]string, error)
}
type xattrWriter interface {
	SetXattr(context.Context, client.ObjectID, string, []byte, uint32) error
	RemoveXattr(context.Context, client.ObjectID, string) error
}

// A mounting user's configured Unix UID is not proof of Unix root privilege.
// Windows exposes user attributes and preserves privileged attributes unchanged.
func windowsXattr(name string) error {
	if strings.HasPrefix(name, "trusted.") {
		return syscall.EACCES
	}
	if !strings.HasPrefix(name, "user.") {
		return syscall.EOPNOTSUPP
	}
	return nil
}
func (f *Filesystem) GetXattr(p, name string) ([]byte, error) {
	if err := windowsXattr(name); err != nil {
		return nil, err
	}
	info, err := f.Lookup(p)
	if err != nil {
		return nil, err
	}
	x, ok := f.Volume.(xattrView)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	return x.GetXattr(f.Context, info.ID, name)
}
func (f *Filesystem) ListXattr(p string) ([]string, error) {
	info, err := f.Lookup(p)
	if err != nil {
		return nil, err
	}
	x, ok := f.Volume.(xattrView)
	if !ok {
		return nil, syscall.EOPNOTSUPP
	}
	names, err := x.ListXattr(f.Context, info.ID)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		if strings.HasPrefix(name, "user.") {
			out = append(out, name)
		}
	}
	return out, nil
}
func (f *Filesystem) SetXattr(p, name string, value []byte, flags int) error {
	if err := f.writable(); err != nil {
		return err
	}
	if err := windowsXattr(name); err != nil {
		return err
	}
	if flags < 0 || flags > 2 {
		return syscall.EINVAL
	}
	info, err := f.Lookup(p)
	if err != nil {
		return err
	}
	x, ok := f.Volume.(xattrWriter)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return x.SetXattr(f.Context, info.ID, name, value, uint32(flags))
}
func (f *Filesystem) RemoveXattr(p, name string) error {
	if err := f.writable(); err != nil {
		return err
	}
	if err := windowsXattr(name); err != nil {
		return err
	}
	info, err := f.Lookup(p)
	if err != nil {
		return err
	}
	x, ok := f.Volume.(xattrWriter)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return x.RemoveXattr(f.Context, info.ID, name)
}
