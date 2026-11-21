// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/launix-de/stqr-storage/volume"
)

type volumeCapacity struct {
	FileAvailable uint64
	RAMAvailable  uint64
	RAMLimit      uint64
}

// filesystemAvailable uses the same non-root available block count as df, not
// total capacity or blocks reserved for root. It measures the active volume dir.
func filesystemAvailable(path string) (uint64, error) {
	var st syscall.Statfs_t
	if e := syscall.Statfs(path, &st); e != nil {
		return 0, e
	}
	if st.Bsize <= 0 || st.Bavail > math.MaxUint64/uint64(st.Bsize) {
		return 0, fmt.Errorf("invalid filesystem capacity")
	}
	return st.Bavail * uint64(st.Bsize), nil
}
func memoryAvailable() (uint64, error) {
	f, e := os.Open("/proc/meminfo")
	if e != nil {
		return 0, e
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 3 && fields[0] == "MemAvailable:" && fields[2] == "kB" {
			n, e := strconv.ParseUint(fields[1], 10, 64)
			if e != nil || n > math.MaxUint64/1024 {
				return 0, fmt.Errorf("invalid MemAvailable")
			}
			return n * 1024, nil
		}
	}
	if e = scanner.Err(); e != nil {
		return 0, e
	}
	return 0, fmt.Errorf("MemAvailable is unavailable")
}
func (s *Server) volumeCapacity() (volumeCapacity, error) {
	disk, e := filesystemAvailable(s.volumesDir)
	if e != nil {
		return volumeCapacity{}, e
	}
	ram, e := memoryAvailable()
	if e != nil {
		return volumeCapacity{}, e
	}
	limit := ram
	// Zero means automatic sizing from available system RAM. A positive explicit
	// budget additionally protects capacity already assigned to other RAM volumes.
	s.mu.RLock()
	budget := s.running.RAMBytes
	used := uint64(0)
	for _, v := range s.volumes {
		if v.config.Class == "ram" {
			used += uint64(v.config.SizeBytes)
		}
	}
	s.mu.RUnlock()
	if budget > 0 {
		remaining := uint64(0)
		if uint64(budget) > used {
			remaining = uint64(budget) - used
		}
		limit = min(limit, remaining)
	}
	return volumeCapacity{FileAvailable: disk, RAMAvailable: ram, RAMLimit: limit}, nil
}
func alignedCapacity(n uint64) uint64 { return n / volume.PageSize * volume.PageSize }
