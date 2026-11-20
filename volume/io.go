// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"io"
	"os"
	"sync"
)

// FileIO owns an exclusively locked regular file or raw block device. Existing
// files are never resized by OpenFile. Creation is exclusive and explicit.
type FileIO struct {
	file     *os.File
	size     int64
	once     sync.Once
	closeErr error
}

func OpenFile(path string) (*FileIO, error) {
	f, e := openVolumeFile(path)
	if e != nil {
		return nil, e
	}
	if e = checkDeviceAvailable(f); e != nil {
		f.Close()
		return nil, e
	}
	if e = lockFile(f); e != nil {
		f.Close()
		return nil, e
	}
	size, e := fileSize(f)
	if e != nil {
		f.Close()
		return nil, e
	}
	return &FileIO{file: f, size: size}, nil
}
func CreateFile(path string, size int64) (*FileIO, error) {
	if size < MinVolumeSize || size%PageSize != 0 {
		return nil, ErrRange
	}
	f, e := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	if e = lockFile(f); e == nil {
		e = f.Truncate(size)
	}
	if e == nil {
		e = f.Sync()
	}
	if e != nil {
		f.Close()
		os.Remove(path)
		return nil, e
	}
	// The directory entry must survive a power loss as well as the file contents.
	dir, e := os.Open(parentDir(path))
	if e == nil {
		e = dir.Sync()
		dir.Close()
	}
	if e != nil {
		f.Close()
		return nil, e
	}
	return &FileIO{file: f, size: size}, nil
}
func (f *FileIO) ReadAt(p []byte, off int64) (int, error)  { return f.file.ReadAt(p, off) }
func (f *FileIO) WriteAt(p []byte, off int64) (int, error) { return f.file.WriteAt(p, off) }
func (f *FileIO) Sync() error                              { return f.file.Sync() }
func (f *FileIO) Size() (int64, error)                     { return f.size, nil }
func (f *FileIO) Close() error                             { f.once.Do(func() { f.closeErr = f.file.Close() }); return f.closeErr }

// MemoryIO is a bounded, sparse RAM device. Closing it discards every page.
// It intentionally has no eviction policy and no disk-persistence claim.
type MemoryIO struct {
	mu     sync.RWMutex
	size   int64
	pages  map[int64][]byte
	closed bool
}

func NewMemory(size int64) (*MemoryIO, error) {
	if size < MinVolumeSize || size%PageSize != 0 {
		return nil, ErrRange
	}
	return &MemoryIO{size: size, pages: make(map[int64][]byte)}, nil
}
func (m *MemoryIO) Size() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0, ErrClosed
	}
	return m.size, nil
}
func (m *MemoryIO) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}
func (m *MemoryIO) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return 0, ErrClosed
	}
	if off < 0 {
		return 0, ErrRange
	}
	n := 0
	for n < len(p) && off < m.size {
		i := off / PageSize
		at := int(off % PageSize)
		k := min(len(p)-n, PageSize-at, int(m.size-off))
		if page := m.pages[i]; page != nil {
			copy(p[n:n+k], page[at:at+k])
		} else {
			clear(p[n : n+k])
		}
		off += int64(k)
		n += k
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (m *MemoryIO) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	if off < 0 || off > m.size || int64(len(p)) > m.size-off {
		return 0, ErrFull
	}
	n := 0
	for n < len(p) {
		i := off / PageSize
		at := int(off % PageSize)
		k := min(len(p)-n, PageSize-at)
		page := m.pages[i]
		if page == nil {
			page = make([]byte, PageSize)
			m.pages[i] = page
		}
		copy(page[at:at+k], p[n:n+k])
		off += int64(k)
		n += k
	}
	return n, nil
}
func (m *MemoryIO) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.pages = nil
	return nil
}
