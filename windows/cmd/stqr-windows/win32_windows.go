// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"golang.org/x/sys/windows"
	"unsafe"
)

var user32 = windows.NewLazySystemDLL("user32.dll")
var createWindow = user32.NewProc("CreateWindowExW")
var destroyWindow = user32.NewProc("DestroyWindow")
var defWindowProc = user32.NewProc("DefWindowProcW")
var getMessage = user32.NewProc("GetMessageW")

func u16(s string) *uint16 { p, _ := windows.UTF16PtrFromString(s); return p }
func messageBox(owner uintptr, title, text string, flags uint32) {
	user32.NewProc("MessageBoxW").Call(owner, uintptr(unsafe.Pointer(u16(text))), uintptr(unsafe.Pointer(u16(title))), uintptr(flags))
}
func hideConsole() {
	// Never hide a PowerShell/cmd window shared with the calling process.
	var processes [2]uint32
	n, _, _ := windows.NewLazySystemDLL("kernel32.dll").NewProc("GetConsoleProcessList").Call(uintptr(unsafe.Pointer(&processes[0])), 2)
	if n != 1 {
		return
	}
	h, _, _ := windows.NewLazySystemDLL("kernel32.dll").NewProc("GetConsoleWindow").Call()
	if h != 0 {
		user32.NewProc("ShowWindow").Call(h, 0)
	}
}

type windowClass struct {
	Size, Style                        uint32
	Proc                               uintptr
	ClassExtra, WindowExtra            int32
	Instance, Icon, Cursor, Background uintptr
	Menu, Class                        *uint16
	IconSmall                          uintptr
}
type windowMessage struct {
	Window         uintptr
	Message        uint32
	WParam, LParam uintptr
	Time           uint32
	X, Y           int32
	Private        uint32
}
