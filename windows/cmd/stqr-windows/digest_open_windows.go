// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

func openExecutableForDigest(path string) (*os.File, error) {
	// Preserve os.Open's long-path support for this native read handle too.
	absolute, err := windowsExecutablePath(path)
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(absolute)
	if err != nil {
		return nil, err
	}
	// An integrity check must not block another startup's atomic cache repair.
	// The read handle still pins the original bytes if the directory entry is
	// replaced; sharing deletion grants no write access to this handle.
	handle, err := windows.CreateFile(name, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}
