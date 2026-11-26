// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"errors"
	"io"
	"os"
	"sync"
)

// FileIO owns an exclusively locked regular file or raw block device. Existing
// files are never resized by OpenFile. Creation is exclusive and explicit.
type FileIO struct {
	file     *os.File
	mapMu    sync.RWMutex
	mapping  []byte // demand-paged, read-only; mapping changes require Volume exclusivity
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
	return newFileIO(f, size)
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
	return newFileIO(f, size)
}
func (f *FileIO) ReadAt(p []byte, off int64) (int, error) {
	f.mapMu.RLock()
	defer f.mapMu.RUnlock()
	if f.mapping == nil {
		return f.file.ReadAt(p, off)
	}
	if off < 0 {
		return 0, ErrRange
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= int64(len(f.mapping)) {
		return 0, io.EOF
	}
	n := copy(p, f.mapping[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (f *FileIO) WriteAt(p []byte, off int64) (int, error) { return f.file.WriteAt(p, off) }
func (f *FileIO) Sync() error                              { return syncVolumeFile(f.file) }
func (f *FileIO) Size() (int64, error)                     { return fileSize(f.file) }
func (f *FileIO) Close() error {
	f.once.Do(func() {
		f.mapMu.Lock()
		defer f.mapMu.Unlock()
		f.closeErr = errors.Join(unmapVolumeFile(f.mapping), f.file.Close())
		f.mapping = nil
	})
	return f.closeErr
}

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

// grow changes only the bounded RAM address space. Existing sparse pages remain
// intact; the owning Volume rewrites/publishes its layout while holding its lock.
func (m *MemoryIO) grow(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if size < MinVolumeSize || size%PageSize != 0 {
		return ErrRange
	}
	if size > m.size {
		m.size = size
	}
	return nil
}

// newFileIO maps only virtual address space. It does not populate pages or build
// a RAM index of every object. Passing FileIO to Open/Format transfers exclusive
// lifecycle ownership: callers must not independently Close, resize or truncate
// its file while a Volume (and any of its handles) is alive.
func newFileIO(file *os.File, size int64) (*FileIO, error) {
	f := &FileIO{file: file, size: size}
	if err := f.remap(size); err != nil {
		file.Close()
		return nil, err
	}
	return f, nil
}

// remap is called only with the owning Volume's exclusive lock and no active
// checkpoint, or before ownership transfer. Existing read handles therefore pin
// their mapping and borrowed slices for their complete lifetime. Map the new
// extent first: a failed mapping leaves the previous extent usable for recovery.
func (f *FileIO) remap(size int64) error {
	f.mapMu.Lock()
	defer f.mapMu.Unlock()
	if int64(len(f.mapping)) == size {
		return nil
	}
	mapping, err := mapVolumeFile(f.file, size)
	if err != nil {
		return err
	}
	if err = unmapVolumeFile(f.mapping); err != nil {
		unmapVolumeFile(mapping)
		return err
	}
	f.mapping, f.size = mapping, size
	return nil
}

// borrowAt is an internal read-only capability, not a public unlocked view.
// The owning Volume pins its lifetime with its read/write lock. Callers must
// never modify this slice or retain it beyond the Volume's protected operation.
func (f *FileIO) borrowAt(off uint64, n int) ([]byte, int, error) {
	f.mapMu.RLock()
	defer f.mapMu.RUnlock()
	if f.mapping == nil {
		return nil, 0, ErrUnsupported
	}
	if n < 0 || off > uint64(len(f.mapping)) || uint64(n) > uint64(len(f.mapping))-off {
		return nil, 0, ErrRange
	}
	return f.mapping[off : off+uint64(n) : off+uint64(n)], n, nil
}

// A RAM volume borrows its existing sparse page, without a disk mapping or a
// copy. Missing pages represent zero runs and allocate no resident payload.
func (m *MemoryIO) borrowAt(off uint64, n int) ([]byte, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, 0, ErrClosed
	}
	if n < 0 || off > uint64(m.size) || uint64(n) > uint64(m.size)-off {
		return nil, 0, ErrRange
	}
	at := int(off % PageSize)
	n = min(n, PageSize-at)
	if page := m.pages[int64(off/PageSize)]; page != nil {
		return page[at : at+n : at+n], n, nil
	}
	return nil, n, nil
}

func (f *FileIO) hasMapping() bool {
	f.mapMu.RLock()
	defer f.mapMu.RUnlock()
	return f.mapping != nil
}
