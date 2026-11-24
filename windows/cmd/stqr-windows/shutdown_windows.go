//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type sessionShutdown struct {
	stop     func()
	finished <-chan struct{}
}

var shutdownMu sync.Mutex
var shutdownWindows = map[uintptr]sessionShutdown{}
var shutdownCallback = syscall.NewCallback(func(hwnd uintptr, msg uint32, wparam, lparam uintptr) uintptr {
	if msg == 0x11 || msg == 0x16 && wparam != 0 { // WM_QUERYENDSESSION / WM_ENDSESSION
		shutdownMu.Lock()
		state, ok := shutdownWindows[hwnd]
		shutdownMu.Unlock()
		if ok {
			state.stop()
			select {
			case <-state.finished:
			case <-time.After(4 * time.Second):
			}
		}
		return 1
	}
	if msg == 0x10 {
		destroyWindow.Call(hwnd)
		return 0
	}
	if msg == 2 {
		user32.NewProc("PostQuitMessage").Call(0)
		return 0
	}
	r, _, _ := defWindowProc.Call(hwnd, uintptr(msg), wparam, lparam)
	return r
})

// GUI applications have no console logoff handler. A hidden top-level window
// receives session broadcasts and gives unmount/Sync a bounded cleanup window.
func watchSessionShutdown(stop func(), finished <-chan struct{}) (uintptr, func(), error) {
	type result struct {
		hwnd uintptr
		err  error
	}
	ready := make(chan result, 1)
	ended := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(ended)
		var module windows.Handle
		if err := windows.GetModuleHandleEx(2, nil, &module); err != nil {
			ready <- result{err: err}
			return
		}
		class := u16("STQRSessionShutdownV1")
		wc := windowClass{Proc: shutdownCallback, Instance: uintptr(module), Class: class}
		wc.Size = uint32(unsafe.Sizeof(wc))
		atom, _, err := user32.NewProc("RegisterClassExW").Call(uintptr(unsafe.Pointer(&wc)))
		if atom == 0 && err != windows.ERROR_CLASS_ALREADY_EXISTS {
			ready <- result{err: err}
			return
		}
		hwnd, _, err := createWindow.Call(0x08000080, uintptr(unsafe.Pointer(class)), uintptr(unsafe.Pointer(u16("STQR"))), 0, 0, 0, 0, 0, 0, 0, uintptr(module), 0)
		if hwnd == 0 {
			ready <- result{err: fmt.Errorf("session window: %w", err)}
			return
		}
		shutdownMu.Lock()
		shutdownWindows[hwnd] = sessionShutdown{stop, finished}
		shutdownMu.Unlock()
		defer func() { shutdownMu.Lock(); delete(shutdownWindows, hwnd); shutdownMu.Unlock() }()
		ready <- result{hwnd: hwnd}
		var msg windowMessage
		for {
			r, _, _ := getMessage.Call(uintptr(unsafe.Pointer(&msg)), 0, 0, 0)
			if int32(r) <= 0 {
				return
			}
			user32.NewProc("TranslateMessage").Call(uintptr(unsafe.Pointer(&msg)))
			user32.NewProc("DispatchMessageW").Call(uintptr(unsafe.Pointer(&msg)))
		}
	}()
	r := <-ready
	if r.err != nil {
		return 0, nil, r.err
	}
	return r.hwnd, func() { user32.NewProc("PostMessageW").Call(r.hwnd, 0x10, 0, 0); <-ended }, nil
}
