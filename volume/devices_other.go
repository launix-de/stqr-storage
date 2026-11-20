// SPDX-License-Identifier: GPL-3.0-only
//go:build !linux

package volume

import (
	"os"
	"sort"
)

func listSystemDevices() ([]BlockDevice, error) { return nil, nil }
func sortDevices(d []BlockDevice) {
	sort.Slice(d, func(i, j int) bool { return d[i].Path < d[j].Path })
}
func probeUnclaimed(path string) bool       { return false }
func backingFileMounted(path string) bool   { return true }
func checkDeviceAvailable(f *os.File) error { return ErrUnsupported }
