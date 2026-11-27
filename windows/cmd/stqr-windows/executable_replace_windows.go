// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The flags union, HANDLE alignment and filename offset match FILE_RENAME_INFO
// on both 32- and 64-bit Windows. FileNameLength excludes a terminating NUL.
type executableRenameInfo struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

func replaceExecutable(source, target string) error {
	fail := func(err error) error { return &os.LinkError{Op: "rename", Old: source, New: target, Err: err} }
	from, err := executableWindowsPath(source)
	if err != nil {
		return fail(err)
	}
	to, err := executableWindowsPath(target)
	if err != nil {
		return fail(err)
	}
	fromName, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fail(err)
	}
	toName, err := windows.UTF16FromString(to)
	if err != nil {
		return fail(err)
	}
	if len(toName) > windows.MAX_LONG_PATH {
		return fail(windows.ERROR_FILENAME_EXCED_RANGE)
	}
	handle, err := windows.CreateFile(fromName, windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return fail(err)
	}
	defer windows.CloseHandle(handle)
	var layout executableRenameInfo
	nameBytes := (len(toName) - 1) * 2
	buffer := make([]byte, int(unsafe.Offsetof(layout.FileName))+nameBytes+2)
	info := (*executableRenameInfo)(unsafe.Pointer(&buffer[0]))
	// MoveFileEx (used by os.Rename) cannot replace a target with an open
	// integrity reader even when that reader shares DELETE. POSIX rename
	// semantics atomically publish the new entry while old handles keep their
	// original bytes. Never remove the target or fall back to in-place writes.
	info.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.FileNameLength = uint32(nameBytes)
	copy(unsafe.Slice(&info.FileName[0], len(toName)), toName)
	if err = windows.SetFileInformationByHandle(handle, windows.FileRenameInfoEx, &buffer[0], uint32(len(buffer))); err != nil {
		return fail(err)
	}
	return nil
}
