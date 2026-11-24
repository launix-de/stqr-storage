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
		case 4:
			if err := windows.ShellExecute(windows.Handle(hwnd), u16("open"), u16("https://winfsp.dev/rel/"), nil, nil, 1); err != nil {
				messageBox(hwnd, "STQR", err.Error(), 0x10)
			}
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
	case 0x138: // WM_CTLCOLORSTATIC
		gdi := windows.NewLazySystemDLL("gdi32.dll")
		gdi.NewProc("SetBkMode").Call(wparam, 1)
		gdi.NewProc("SetTextColor").Call(wparam, 0x29251f)
		brush, _, _ := gdi.NewProc("GetStockObject").Call(0)
		return brush
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
	wc := windowClass{Proc: setupCallback, Instance: uintptr(module), Cursor: cursor, Background: 6, Class: u16(setupClass)}
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

	dpi := uintptr(96)
	if proc := user32.NewProc("GetDpiForWindow"); proc.Find() == nil {
		if value, _, _ := proc.Call(hwnd); value != 0 {
			dpi = value
		}
	}
	scale := func(v int) uintptr { return uintptr(v) * dpi / 96 }
	user32.NewProc("SetWindowPos").Call(hwnd, 0, 0, 0, scale(704), scale(690), 6)
	gdi := windows.NewLazySystemDLL("gdi32.dll")
	fontFor := func(size int, weight uintptr) uintptr {
		negativeHeight := int32(-int(scale(size)))
		font, _, _ := gdi.NewProc("CreateFontW").Call(uintptr(negativeHeight), 0, 0, 0, weight, 0, 0, 0, 1, 0, 0, 5, 0, uintptr(unsafe.Pointer(u16("Segoe UI"))))
		return font
	}
	font, titleFont := fontFor(16, 400), fontFor(28, 600)
	defer gdi.NewProc("DeleteObject").Call(font)
	defer gdi.NewProc("DeleteObject").Call(titleFont)
	makeControl := func(class, text string, style uintptr, x, y, w, h, id int) uintptr {
		ex := uintptr(0)
		if class == "EDIT" {
			ex = 0x200
		}
		control, _, _ := createWindow.Call(ex, uintptr(unsafe.Pointer(u16(class))), uintptr(unsafe.Pointer(u16(text))), 0x50000000|style, scale(x), scale(y), scale(w), scale(h), hwnd, uintptr(id), uintptr(module), 0)
		sendMessage.Call(control, 0x30, font, 1)
		if class == "EDIT" {
			sendMessage.Call(control, 0xc5, 4096, 0)
		}
		if class == "EDIT" || class == "BUTTON" {
			windows.NewLazySystemDLL("uxtheme.dll").NewProc("SetWindowTheme").Call(control, uintptr(unsafe.Pointer(u16("Explorer"))), 0)
		}
		return control
	}
	title := makeControl("STATIC", "Speicher verbinden", 0, 32, 24, 620, 42, 0)
	sendMessage.Call(title, 0x30, titleFont, 1)
	makeControl("STATIC", "Deine Dateien als Laufwerk im Windows-Explorer.", 0, 32, 72, 620, 28, 0)
	fields := []struct {
		label, value string
		id, x, y, w  int
		style        uintptr
	}{
		{"Server-URI", initial.URI, idURI, 32, 116, 624, 0},
		{"Benutzername", initial.Username, idUser, 32, 198, 296, 0},
		{"Passwort", initial.Password, idPassword, 360, 198, 296, 0x20},
		{"Bucket", initial.Bucket, idBucket, 32, 280, 452, 0},
		{"Laufwerk", initial.Drive, idDrive, 516, 280, 140, 0},
	}
	for _, f := range fields {
		makeControl("STATIC", f.label, 0, f.x, f.y, f.w, 24, 0)
		makeControl("EDIT", f.value, 0x10080|f.style, f.x, f.y+28, f.w, 34, f.id)
	}
	ro := makeControl("BUTTON", "Nur lesen", 0x10003, 32, 358, 624, 28, idReadonly)
	remember := makeControl("BUTTON", "Bei der Anmeldung verbinden und Passwort merken", 0x10003, 32, 398, 624, 28, idPersist)
	if initial.ReadOnly {
		sendMessage.Call(ro, 0xf1, 1, 0)
	}
	if persist {
		sendMessage.Call(remember, 0xf1, 1, 0)
	}
	makeControl("STATIC", "Das Passwort wird geschützt in deinem Windows-Konto gespeichert.", 0, 56, 432, 600, 26, 0)
	makeControl("STATIC", "WinFsp wird zum Einbinden als Windows-Laufwerk benötigt.", 0, 32, 482, 624, 28, 0)
	makeControl("BUTTON", "WinFsp-Download", 0x10000, 32, 514, 180, 32, 4)
	makeControl("STATIC", "Nach der Installation STQR erneut öffnen.", 0, 228, 518, 428, 28, 0)
	makeControl("STATIC", "", 0x10, 32, 566, 624, 2, 0)
	makeControl("BUTTON", "Trennen / Autostart entfernen", 0x10000, 32, 590, 276, 40, 3)
	makeControl("BUTTON", "Abbrechen", 0x10000, 332, 590, 140, 40, 2)
	makeControl("BUTTON", "Verbinden", 0x10001, 492, 590, 164, 40, 1)
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
