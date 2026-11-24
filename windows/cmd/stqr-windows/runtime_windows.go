//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	"path/filepath"
	"runtime"
	"sync"
)

var runtimeMu sync.Mutex
var runtimeLibrary windows.Handle // One process-lifetime reference, also used by cgofuse's loader.
func ensureWinFsp() error {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()
	if runtimeLibrary != 0 {
		return nil
	}
	name := "winfsp-x64.dll"
	if runtime.GOARCH == "arm64" {
		name = "winfsp-a64.dll"
	} else if runtime.GOARCH == "386" {
		name = "winfsp-x86.dll"
	}
	for _, view := range []uint32{registry.WOW64_32KEY, registry.WOW64_64KEY} {
		key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\WinFsp`, registry.QUERY_VALUE|view)
		if err != nil {
			continue
		}
		dir, _, err := key.GetStringValue("InstallDir")
		key.Close()
		if err != nil || !filepath.IsAbs(dir) {
			continue
		}
		handle, err := windows.LoadLibraryEx(filepath.Join(dir, "bin", name), 0, windows.LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR|windows.LOAD_LIBRARY_SEARCH_SYSTEM32)
		if err != nil {
			continue
		}
		if _, err = windows.GetProcAddress(handle, "fuse_main_real"); err != nil {
			windows.FreeLibrary(handle)
			continue
		}
		runtimeLibrary = handle
		return nil
	}
	return fmt.Errorf("%w: WinFsp wurde nicht gefunden. Bitte die WinFsp-Runtime über https://winfsp.dev/rel/ installieren und STQR danach erneut öffnen", errMountUnavailable)
}
