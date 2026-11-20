// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"os"
	"path/filepath"
)

// BlockDevice is a UI-independent discovery result. SizeBytes is always bytes;
// Type is file/disk/partition/lvm/block, Class is hdd/ssd (when detected).
// Exported JSON-tagged fields can be marshalled by the later dashboard API.
type BlockDevice struct {
	Path          string   `json:"path"`
	SizeBytes     uint64   `json:"size_bytes"`
	Type          string   `json:"type"`
	Class         string   `json:"class"`
	ClassDetected bool     `json:"class_detected"`
	Aliases       []string `json:"aliases,omitempty"`
}

// ListBlockdevices returns unmounted, unclaimed block devices (including
// partitions and device-mapper/LVM), plus unclaimed regular files in VOLUMES_DIR.
// The optional directory overrides VOLUMES_DIR for callers/tests. Missing volume
// directories are empty, not errors. Enumeration is advisory: New rechecks the
// actual target and obtains its exclusive lock, closing the discovery/open race.
func ListBlockdevices(directory ...string) ([]BlockDevice, error) {
	if len(directory) > 1 {
		return nil, ErrRange
	}
	dir := os.Getenv("VOLUMES_DIR")
	if dir == "" {
		dir = "volumes"
	}
	if len(directory) == 1 {
		dir = directory[0]
	}
	abs, e := filepath.Abs(dir)
	if e != nil {
		return nil, e
	}
	dir = abs
	devices, e := listSystemDevices()
	if e != nil {
		return nil, e
	}
	files, e := os.ReadDir(dir)
	if os.IsNotExist(e) {
		return devices, nil
	}
	if e != nil {
		return nil, e
	}
	for _, f := range files {
		if !f.Type().IsRegular() {
			continue
		}
		path := filepath.Join(dir, f.Name())
		st, e := f.Info()
		if e != nil {
			return nil, e
		}
		if st.Size() < 0 || !probeUnclaimed(path) {
			continue
		}
		if backingFileMounted(path) {
			continue
		}
		devices = append(devices, BlockDevice{Path: path, SizeBytes: uint64(st.Size()), Type: "file", Class: "hdd", ClassDetected: false})
	}
	sortDevices(devices)
	return devices, nil
}
