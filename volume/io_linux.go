// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

func parentDir(p string) string { return filepath.Dir(p) }
func lockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
func fileSize(f *os.File) (int64, error) {
	st, e := f.Stat()
	if e != nil {
		return 0, e
	}
	if st.Mode().IsRegular() {
		return st.Size(), nil
	}
	if st.Mode()&os.ModeDevice == 0 || st.Mode()&os.ModeCharDevice != 0 {
		return 0, ErrUnsupported
	}
	var size uint64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), 0x80081272, uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		return 0, errno
	}
	if size > uint64(^uint64(0)>>1) {
		return 0, ErrRange
	}
	return int64(size), nil
}

func (f *FileIO) Alignment() uint32 {
	st, e := f.file.Stat()
	if e != nil {
		return 0
	}
	if st.Mode().IsRegular() {
		return PageSize
	}
	var n uint32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.file.Fd(), 0x1268, uintptr(unsafe.Pointer(&n)))
	if errno != 0 {
		return 0
	}
	return n
}

// O_EXCL on Linux block devices takes the kernel's exclusive device claim. This
// complements mountinfo checks and prevents a competing mount after discovery.
func openVolumeFile(path string) (*os.File, error) {
	st, e := os.Stat(path)
	if e != nil {
		return nil, e
	}
	flags := os.O_RDWR
	if st.Mode()&os.ModeDevice != 0 {
		if st.Mode()&os.ModeCharDevice != 0 {
			return nil, ErrUnsupported
		}
		flags |= syscall.O_EXCL
	}
	f, e := os.OpenFile(path, flags, 0)
	if errors.Is(e, syscall.EBUSY) {
		return nil, fmt.Errorf("%w: %v", ErrBusy, e)
	}
	return f, e
}
