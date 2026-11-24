// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"path"
	"syscall"
)

// Directory snapshots are bounded independently of file contents. They permit
// exact create/remove notifications for folders already visited by Explorer.
func (f *Filesystem) rememberDirectoryLocked(p string, entries []Entry) {
	if len(f.directories) >= 256 {
		if _, exists := f.directories[p]; !exists {
			for old := range f.directories {
				if old != "/" {
					delete(f.directories, old)
					break
				}
			}
		}
	}
	f.directories[p] = append([]Entry(nil), entries...)
}
func (f *Filesystem) directoryEntries(p string, id client.ObjectID) ([]Entry, error) {
	info, err := f.Volume.Stat(f.Context, id)
	if err != nil {
		return nil, err
	}
	entries := []Entry{{".", info}}
	parent, err := f.Lookup(path.Dir(p))
	if err != nil {
		return nil, err
	}
	entries = append(entries, Entry{"..", parent})
	children, err := f.Volume.ReadDir(f.Context, id)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		info, err := f.Volume.Stat(f.Context, child.ID)
		if errors.Is(err, client.ErrNotFound) || errors.Is(err, syscall.ENOENT) {
			continue
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{child.Name, info})
	}
	return entries, nil
}

// RefreshDirectory starts a fresh enumeration at offset zero; subsequent pages
// keep the same snapshot, avoiding duplicate/skipped entries during a refresh.
func (f *Filesystem) RefreshDirectory(h uint64) error {
	current, err := f.get(h)
	if err != nil {
		return err
	}
	if !current.options.Directory {
		return syscall.ENOTDIR
	}
	entries, err := f.directoryEntries(current.path, current.id)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	latest, exists := f.handles[h]
	if !exists {
		return syscall.EBADF
	}
	latest.entries = entries
	f.handles[h] = latest
	f.rememberDirectoryLocked(current.path, entries)
	return nil
}

type Change struct {
	Path                        string
	Created, Removed, Directory bool
}

// Changes compares subscribed directory views. It runs off the notification
// reader thread; native reads cannot deadlock delivery of their own responses.
func (f *Filesystem) Changes(ctx context.Context) []Change {
	worker := &Filesystem{Context: ctx, Volume: f.Volume}
	f.mu.Lock()
	previous := make(map[string][]Entry, len(f.directories))
	for p, v := range f.directories {
		previous[p] = v
	}
	open := map[string]bool{}
	for _, h := range f.handles {
		if !h.options.Directory {
			open[h.path] = true
		}
	}
	f.mu.Unlock()
	var changes []Change
	for p, old := range previous {
		info, err := worker.Lookup(p)
		if err != nil {
			if errors.Is(err, syscall.ENOENT) || errors.Is(err, client.ErrNotFound) {
				changes = append(changes, Change{Path: p, Removed: true, Directory: true})
				f.mu.Lock()
				delete(f.directories, p)
				f.mu.Unlock()
			}
			continue
		}
		entries, err := worker.directoryEntries(p, info.ID)
		if err != nil {
			continue
		}
		before := map[string]client.FSInfo{}
		for _, e := range old {
			if e.Name != "." && e.Name != ".." {
				before[e.Name] = e.Info
			}
		}
		for _, e := range entries {
			if e.Name == "." || e.Name == ".." {
				continue
			}
			name := path.Join(p, e.Name)
			prior, exists := before[e.Name]
			delete(before, e.Name)
			if !exists || prior.ID != e.Info.ID {
				if exists {
					changes = append(changes, Change{Path: name, Removed: true, Directory: prior.Directory})
				}
				changes = append(changes, Change{Path: name, Created: true, Directory: e.Info.Directory})
			} else if prior.Size != e.Info.Size || prior.MtimeNS != e.Info.MtimeNS || prior.CtimeNS != e.Info.CtimeNS || prior.Mode != e.Info.Mode || prior.Nlink != e.Info.Nlink {
				changes = append(changes, Change{Path: name, Directory: e.Info.Directory})
			}
		}
		for name, info := range before {
			changes = append(changes, Change{Path: path.Join(p, name), Removed: true, Directory: info.Directory})
		}
		f.mu.Lock()
		f.rememberDirectoryLocked(p, entries)
		f.mu.Unlock()
	}
	for p := range open {
		changes = append(changes, Change{Path: p})
	}
	return changes
}
