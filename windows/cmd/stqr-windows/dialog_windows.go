//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"runtime"
	"syscall"
	"unsafe"
)

var user32 = windows.NewLazySystemDLL("user32.dll")
var createWindow = user32.NewProc("CreateWindowExW")
var sendMessage = user32.NewProc("SendMessageW")
var getText = user32.NewProc("GetWindowTextW")
var destroyWindow = user32.NewProc("DestroyWindow")
var defWindowProc = user32.NewProc("DefWindowProcW")
var getMessage = user32.NewProc("GetMessageW")
var getDlgItem = user32.NewProc("GetDlgItem")

const setupClass = "STQRMountSetupV1"
const (
	idURI      = 100
	idUser     = 101
	idPassword = 102
	idBucket   = 103
	idDrive    = 104
	idReadonly = 105
	idPersist  = 106
)

type setupChoice struct {
	config                  mountConfig
	persist, remove, cancel bool
}
type setupState struct{ choice setupChoice }

var dialogState *setupState

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
func field(hwnd uintptr, id int) uintptr { h, _, _ := getDlgItem.Call(hwnd, uintptr(id)); return h }
func textField(hwnd uintptr, id int) string {
	b := make([]uint16, 4098)
	n, _, _ := getText.Call(field(hwnd, id), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
	return windows.UTF16ToString(b[:n])
}
func checked(hwnd uintptr, id int) bool {
	v, _, _ := sendMessage.Call(field(hwnd, id), 0xf0, 0, 0)
	return v == 1
}

var setupCallback = syscall.NewCallback(func(hwnd uintptr, msg uint32, wparam, lparam uintptr) uintptr {
	switch msg {
	case 0x111:
		if dialogState == nil {
			break
		}
		switch uint16(wparam) {
		case 1:
			c := dialogState.choice.config
			c.URI = textField(hwnd, idURI)
			c.Username = textField(hwnd, idUser)
			c.Password = textField(hwnd, idPassword)
			c.Bucket = textField(hwnd, idBucket)
			c.Drive = textField(hwnd, idDrive)
			c.ReadOnly = checked(hwnd, idReadonly)
			if e := c.validate(); e != nil {
				messageBox(hwnd, "STQR", e.Error(), 0x10)
				return 0
			}
			dialogState.choice = setupChoice{config: c, persist: checked(hwnd, idPersist)}
			destroyWindow.Call(hwnd)
			return 0
		case 2:
			dialogState.choice.cancel = true
			destroyWindow.Call(hwnd)
			return 0
		case 3:
			d, e := normalizeDrive(textField(hwnd, idDrive))
			if e != nil {
				messageBox(hwnd, "STQR", e.Error(), 0x10)
				return 0
			}
			dialogState.choice = setupChoice{config: mountConfig{Drive: d}, remove: true}
			destroyWindow.Call(hwnd)
			return 0
		}
	case 0x10:
		if dialogState != nil {
			dialogState.choice.cancel = true
		}
		destroyWindow.Call(hwnd)
		return 0
	case 2:
		user32.NewProc("PostQuitMessage").Call(0)
		return 0
	}
	r, _, _ := defWindowProc.Call(hwnd, uintptr(msg), wparam, lparam)
	return r
})

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

func showSetup(initial mountConfig, persist bool) (setupChoice, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var module windows.Handle
	if e := windows.GetModuleHandleEx(2, nil, &module); e != nil {
		return setupChoice{}, e
	}
	cursor, _, _ := user32.NewProc("LoadCursorW").Call(0, 32512)
	wc := windowClass{Proc: setupCallback, Instance: uintptr(module), Cursor: cursor, Background: 16, Class: u16(setupClass)}
	wc.Size = uint32(unsafe.Sizeof(wc))
	atom, _, e := user32.NewProc("RegisterClassExW").Call(uintptr(unsafe.Pointer(&wc)))
	if atom == 0 && e != windows.ERROR_CLASS_ALREADY_EXISTS {
		return setupChoice{}, fmt.Errorf("setup window: %w", e)
	}
	state := &setupState{choice: setupChoice{config: initial, persist: persist, cancel: true}}
	dialogState = state
	defer func() { dialogState = nil }()
	hwnd, _, e := createWindow.Call(0x10000, uintptr(unsafe.Pointer(u16(setupClass))), uintptr(unsafe.Pointer(u16("STQR – Laufwerk verbinden"))), 0x00ca0000, 0x80000000, 0x80000000, 600, 410, 0, 0, uintptr(module), 0)
	if hwnd == 0 {
		return setupChoice{}, fmt.Errorf("setup window: %w", e)
	}
	font, _, _ := windows.NewLazySystemDLL("gdi32.dll").NewProc("GetStockObject").Call(17)
	makeControl := func(class, text string, style uintptr, x, y, w, h, id int) uintptr {
		control, _, _ := createWindow.Call(0, uintptr(unsafe.Pointer(u16(class))), uintptr(unsafe.Pointer(u16(text))), 0x50000000|style, uintptr(x), uintptr(y), uintptr(w), uintptr(h), hwnd, uintptr(id), uintptr(module), 0)
		sendMessage.Call(control, 0x30, font, 1)
		return control
	}
	fields := []struct {
		label, value string
		id           int
		style        uintptr
	}{{"Server-URI", initial.URI, idURI, 0}, {"Benutzername", initial.Username, idUser, 0}, {"Passwort", initial.Password, idPassword, 0x20}, {"Bucket", initial.Bucket, idBucket, 0}, {"Laufwerk", initial.Drive, idDrive, 0}}
	for n, f := range fields {
		y := 20 + n*38
		makeControl("STATIC", f.label, 0, 20, y+3, 130, 24, 0)
		h := makeControl("EDIT", f.value, 0x00810080|f.style, 155, y, 405, 26, f.id)
		sendMessage.Call(h, 0xc5, 4096, 0)
	}
	ro := makeControl("BUTTON", "Nur lesen", 0x10003, 155, 215, 400, 25, idReadonly)
	remember := makeControl("BUTTON", "Dauerhaft einbinden und Passwort merken", 0x10003, 155, 245, 410, 25, idPersist)
	if initial.ReadOnly {
		sendMessage.Call(ro, 0xf1, 1, 0)
	}
	if persist {
		sendMessage.Call(remember, 0xf1, 1, 0)
	}
	makeControl("STATIC", "Automatisch bei deiner Windows-Anmeldung verbinden.", 0, 155, 273, 410, 22, 0)
	makeControl("BUTTON", "Trennen und Autostart entfernen", 0x10000, 20, 315, 265, 30, 3)
	makeControl("BUTTON", "Verbinden", 0x10001, 315, 315, 115, 30, 1)
	makeControl("BUTTON", "Abbrechen", 0x10000, 445, 315, 115, 30, 2)
	user32.NewProc("ShowWindow").Call(hwnd, 5)
	user32.NewProc("UpdateWindow").Call(hwnd)
	user32.NewProc("SetFocus").Call(field(hwnd, idURI))
	var msg windowMessage
	for {
		r, _, e := getMessage.Call(uintptr(unsafe.Pointer(&msg)), 0, 0, 0)
		if int32(r) == -1 {
			destroyWindow.Call(hwnd)
			return setupChoice{}, e
		}
		if r == 0 {
			break
		}
		if msg.Message == 0x100 && (msg.WParam == 13 || msg.WParam == 27) {
			id := uintptr(1)
			if msg.WParam == 27 {
				id = 2
			}
			sendMessage.Call(hwnd, 0x111, id, 0)
			continue
		}
		handled, _, _ := user32.NewProc("IsDialogMessageW").Call(hwnd, uintptr(unsafe.Pointer(&msg)))
		if handled != 0 {
			continue
		}
		user32.NewProc("TranslateMessage").Call(uintptr(unsafe.Pointer(&msg)))
		user32.NewProc("DispatchMessageW").Call(uintptr(unsafe.Pointer(&msg)))
	}
	return state.choice, nil
}
