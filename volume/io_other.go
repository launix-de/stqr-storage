// SPDX-License-Identifier: GPL-3.0-only
//go:build !linux

package volume

import (
	"os"
	"path/filepath"
)

func parentDir(p string) string { return filepath.Dir(p) }
func lockFile(f *os.File) error { return ErrUnsupported }
func fileSize(f *os.File) (int64, error) {
	s, e := f.Stat()
	if e != nil {
		return 0, e
	}
	if !s.Mode().IsRegular() {
		return 0, ErrUnsupported
	}
	return s.Size(), nil
}

func (f *FileIO) Alignment() uint32 { return PageSize }

func openVolumeFile(path string) (*os.File, error) { return os.OpenFile(path, os.O_RDWR, 0) }

func ensureFileBacking(f *os.File, size int64) error { return ErrUnsupported }
