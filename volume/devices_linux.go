// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package volume

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

type linuxDevicePaths struct{ sys, dev, mapper, mountinfo, swaps string }

func systemDevicePaths() linuxDevicePaths {
	return linuxDevicePaths{"/sys/class/block", "/dev", "/dev/mapper", "/proc/self/mountinfo", "/proc/swaps"}
}
func sortDevices(d []BlockDevice) {
	sort.Slice(d, func(i, j int) bool { return d[i].Path < d[j].Path })
}
func deviceID(path string) (string, error) {
	st, e := os.Stat(path)
	if e != nil {
		return "", e
	}
	raw, ok := st.Sys().(*syscall.Stat_t)
	if !ok || st.Mode()&os.ModeDevice == 0 || st.Mode()&os.ModeCharDevice != 0 {
		return "", ErrUnsupported
	}
	r := uint64(raw.Rdev)
	major := ((r >> 8) & 0xfff) | ((r >> 32) & 0xfffff000)
	minor := (r & 0xff) | ((r >> 12) & 0xffffff00)
	return fmt.Sprintf("%d:%d", major, minor), nil
}
func mountIDs(path string) (map[string]bool, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	ids := map[string]bool{}
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 1<<20)
	for scan.Scan() {
		x := strings.Fields(scan.Text())
		if len(x) < 6 {
			return nil, ErrCorrupt
		}
		if !strings.Contains(x[2], ":") {
			return nil, ErrCorrupt
		}
		ids[x[2]] = true
	}
	return ids, scan.Err()
}
func readTrim(path string) string {
	b, e := os.ReadFile(path)
	if e != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
func blockedDevice(p linuxDevicePaths, name string, mounted map[string]bool, seen map[string]bool) bool {
	if seen[name] {
		return false
	}
	seen[name] = true
	base := filepath.Join(p.sys, name)
	if id := readTrim(filepath.Join(base, "dev")); id != "" && mounted[id] {
		return true
	}
	holders, e := os.ReadDir(filepath.Join(base, "holders"))
	if e != nil && !os.IsNotExist(e) {
		return true
	}
	if len(holders) > 0 {
		return true
	} // claimed as an LVM/DM/RAID input, even if its consumer is not mounted
	// A whole disk cannot be used while one of its partitions is mounted/claimed.
	children, e := os.ReadDir(base)
	if e != nil {
		return true
	}
	for _, c := range children {
		child := filepath.Join(base, c.Name())
		if readTrim(filepath.Join(child, "partition")) != "" {
			id := readTrim(filepath.Join(child, "dev"))
			if mounted[id] {
				return true
			}
			hs, e := os.ReadDir(filepath.Join(child, "holders"))
			if e != nil && !os.IsNotExist(e) {
				return true
			}
			if len(hs) > 0 {
				return true
			}
		}
	}
	return false
}
func activeSwapIDs(path string) map[string]bool {
	out := map[string]bool{}
	f, e := os.Open(path)
	if e != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	first := true
	for sc.Scan() {
		if first {
			first = false
			continue
		}
		x := strings.Fields(sc.Text())
		if len(x) > 0 {
			if id, e := deviceID(x[0]); e == nil {
				out[id] = true
			}
		}
	}
	return out
}
func availableDevicePaths(p linuxDevicePaths) ([]BlockDevice, error) {
	mounted, e := mountIDs(p.mountinfo)
	if e != nil {
		return nil, e
	}
	for id := range activeSwapIDs(p.swaps) {
		mounted[id] = true
	}
	names, e := os.ReadDir(p.sys)
	if e != nil {
		return nil, e
	}
	aliases := map[string][]string{}
	if ds, e := os.ReadDir(p.mapper); e == nil {
		for _, d := range ds {
			path := filepath.Join(p.mapper, d.Name())
			real, e := filepath.EvalSymlinks(path)
			if e == nil {
				aliases[filepath.Base(real)] = append(aliases[filepath.Base(real)], path)
			}
		}
	}
	var out []BlockDevice
	for _, entry := range names {
		name := entry.Name()
		base := filepath.Join(p.sys, name)
		if readTrim(filepath.Join(base, "ro")) == "1" || blockedDevice(p, name, mounted, map[string]bool{}) {
			continue
		}
		sector, e := strconv.ParseUint(readTrim(filepath.Join(base, "size")), 10, 64)
		if e != nil || sector == 0 || sector > ^uint64(0)/512 {
			continue
		}
		path := filepath.Join(p.dev, name)
		if _, e = os.Stat(path); e != nil {
			continue
		}
		if !probeUnclaimed(path) {
			continue
		}
		kind := "disk"
		if readTrim(filepath.Join(base, "partition")) != "" {
			kind = "partition"
		}
		if strings.HasPrefix(name, "dm-") {
			kind = "device-mapper"
			if strings.HasPrefix(readTrim(filepath.Join(base, "dm/uuid")), "LVM-") {
				kind = "lvm"
			}
		}
		if strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "md") {
			kind = "block"
		}
		class, detected := rotationalClass(p.sys, name, map[string]bool{})
		a := aliases[name]
		sort.Strings(a)
		if len(a) > 0 {
			a = append(a, path)
			path = a[0]
			a = a[1:]
		}
		out = append(out, BlockDevice{Path: path, SizeBytes: sector * 512, Type: kind, Class: class, ClassDetected: detected, Aliases: a})
	}
	sortDevices(out)
	return out, nil
}
func rotationalClass(sys, name string, seen map[string]bool) (string, bool) {
	if seen[name] {
		return "hdd", false
	}
	seen[name] = true
	defer delete(seen, name)
	base := filepath.Join(sys, name)
	switch readTrim(filepath.Join(base, "queue/rotational")) {
	case "0":
		return "ssd", true
	case "1":
		return "hdd", true
	}
	if readTrim(filepath.Join(base, "partition")) != "" {
		real, e := filepath.EvalSymlinks(base)
		if e == nil {
			parent := filepath.Base(filepath.Dir(real))
			if parent != name {
				if c, ok := rotationalClass(sys, parent, seen); ok {
					return c, true
				}
			}
		}
	}
	slaves, e := os.ReadDir(filepath.Join(base, "slaves"))
	if e == nil && len(slaves) > 0 {
		allSSD := true
		for _, s := range slaves {
			c, ok := rotationalClass(sys, s.Name(), seen)
			if !ok {
				return "hdd", false
			}
			if c != "ssd" {
				allSSD = false
			}
		}
		if allSSD {
			return "ssd", true
		}
		return "hdd", true
	}
	return "hdd", false
}
func listSystemDevices() ([]BlockDevice, error) { return availableDevicePaths(systemDevicePaths()) }
func probeUnclaimed(path string) bool {
	f, e := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if e != nil {
		return false
	}
	defer f.Close()
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return false
	}
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) == nil
}
func checkDeviceAvailable(f *os.File) error {
	st, e := f.Stat()
	if e != nil {
		return e
	}
	if st.Mode().IsRegular() {
		if backingFileMounted(f.Name()) {
			return ErrBusy
		}
		return nil
	}
	id, e := deviceID(f.Name())
	if e != nil {
		return e
	}
	p := systemDevicePaths()
	ids, e := mountIDs(p.mountinfo)
	if e != nil {
		return e
	}
	for k := range activeSwapIDs(p.swaps) {
		ids[k] = true
	}
	real, e := filepath.EvalSymlinks(filepath.Join("/sys/dev/block", id))
	if e != nil {
		return e
	}
	if blockedDevice(p, filepath.Base(real), ids, map[string]bool{}) {
		return ErrBusy
	}
	return nil
}

// backingFileMounted also rejects active swap files and files claimed by a
// configured loop device, even when no filesystem is currently mounted on it.
func backingFileMounted(path string) bool {
	p := systemDevicePaths()
	st, e := os.Stat(path)
	if e != nil {
		return true
	}
	if f, e := os.Open(p.swaps); e == nil {
		sc := bufio.NewScanner(f)
		first := true
		for sc.Scan() {
			if first {
				first = false
				continue
			}
			parts := strings.Fields(sc.Text())
			if len(parts) > 0 {
				other, e := os.Stat(parts[0])
				if e == nil && os.SameFile(st, other) {
					f.Close()
					return true
				}
			}
		}
		if sc.Err() != nil {
			f.Close()
			return true
		}
		f.Close()
	} else {
		return true
	}
	names, e := os.ReadDir(p.sys)
	if e != nil {
		return true
	}
	for _, n := range names {
		if !strings.HasPrefix(n.Name(), "loop") {
			continue
		}
		file := readTrim(filepath.Join(p.sys, n.Name(), "loop/backing_file"))
		if file == "" {
			continue
		}
		other, e := os.Stat(file)
		if e == nil && os.SameFile(st, other) {
			return true
		}
	}
	return false
}
