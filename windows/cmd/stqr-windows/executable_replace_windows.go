// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Native FILE_RENAME_INFO layout: the DWORD Flags union precedes a naturally
// aligned HANDLE. FileNameLength counts UTF-16 bytes, excluding the terminator.
// unsafe.Offsetof keeps the variable tail correct on both 32- and 64-bit builds.
type executableRenameInfo struct {
	flags  uint32
	root   windows.Handle
	length uint32
	name   [1]uint16
}

type executableRenameCall func(windows.Handle, uint32, *byte, uint32) error

// Software contract: source is a fully verified, closed temporary file in the
// target's directory. Publication changes its one stable target name atomically;
// existing delete-shared readers keep the old bytes and new opens see the new
// file. FILE_SHARE_DELETE alone does not make MoveFileEx replace an open target.
//
// Windows 10 RS1+ exposes FileRenameInfoEx; the product baseline is Windows 10
// 1809+. File-system support is still required. On an unsupported OS/FS we fail
// explicitly and leave source/target intact. Never fall back to remove+rename
// or claim atomic replacement using the older MoveFileEx API.
// https://learn.microsoft.com/windows/win32/api/winbase/ns-winbase-file_rename_info
// https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information
func replaceExecutable(source, target string) error {
	return replaceExecutableWith(source, target, windows.SetFileInformationByHandle)
}

func replaceExecutableWith(source, target string, setInfo executableRenameCall) error {
	linkError := func(err error) error { return &os.LinkError{Op: "replace", Old: source, New: target, Err: err} }
	sourcePath, err := executableWindowsPath(source)
	if err != nil {
		return linkError(err)
	}
	sourceName, err := windows.UTF16PtrFromString(sourcePath)
	if err != nil {
		return linkError(err)
	}
	targetPath, err := executableWindowsPath(target)
	if err != nil {
		return linkError(err)
	}
	targetName, err := windows.UTF16FromString(targetPath)
	if err != nil {
		return linkError(err)
	}
	if len(targetName) > windows.MAX_LONG_PATH {
		return linkError(windows.ERROR_FILENAME_EXCED_RANGE)
	}
	handle, err := windows.CreateFile(sourceName, windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return linkError(err)
	}
	defer windows.CloseHandle(handle)
	var layout executableRenameInfo
	tail := int(unsafe.Offsetof(layout.name))
	buffer := make([]byte, tail+len(targetName)*2)
	info := (*executableRenameInfo)(unsafe.Pointer(&buffer[0]))
	info.flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.length = uint32((len(targetName) - 1) * 2)
	copy(unsafe.Slice((*uint16)(unsafe.Pointer(&buffer[tail])), len(targetName)), targetName)
	err = setInfo(handle, windows.FileRenameInfoEx, &buffer[0], uint32(len(buffer)))
	runtime.KeepAlive(buffer)
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_INVALID_FUNCTION) || errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED) || errors.Is(err, windows.ERROR_INVALID_LEVEL) {
			err = fmt.Errorf("atomic executable replacement is unsupported by this Windows version or file system: %w", err)
		}
		return linkError(err)
	}
	return nil
}
