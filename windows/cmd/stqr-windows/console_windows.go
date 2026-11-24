//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"golang.org/x/sys/windows"
	"os"
	"strings"
	"syscall"
)

var consoleInterrupt = make(chan struct{}, 1)
var consoleHandler = syscall.NewCallback(func(kind uint32) uintptr {
	if kind != 0 && kind != 1 {
		return 0
	}
	select {
	case consoleInterrupt <- struct{}{}:
	default:
	}
	return 1
})

// A GUI-subsystem executable never allocates a console. Explicit command-line
// use can attach to its existing parent console; setup and autostart never do.
func attachCLIConsole() {
	if len(os.Args) == 1 || hasSetupFlag(os.Args[1:]) {
		return
	}
	for _, a := range os.Args[1:] {
		if strings.TrimLeft(a, "-") == "background" || strings.TrimLeft(a, "-") == "background=true" {
			return
		}
	}
	attached, _, _ := windows.NewLazySystemDLL("kernel32.dll").NewProc("AttachConsole").Call(0xffffffff)
	if attached == 0 {
		return
	} // Redirected handles already work without a console.
	// AttachConsole resets the runtime's handler table; register our cancellation
	// bridge after attaching so Ctrl+C still unmounts and flushes CLI mounts.
	windows.NewLazySystemDLL("kernel32.dll").NewProc("SetConsoleCtrlHandler").Call(consoleHandler, 1)
	bind := func(name string, flag int, dest **os.File) {
		if *dest != nil {
			if info, err := (*dest).Stat(); err == nil && info.Mode()&os.ModeCharDevice == 0 {
				return
			}
		}
		f, err := os.OpenFile(name, flag, 0)
		if err == nil {
			*dest = f
		}
	}
	bind("CONIN$", os.O_RDONLY, &os.Stdin)
	bind("CONOUT$", os.O_WRONLY, &os.Stdout)
	bind("CONOUT$", os.O_WRONLY, &os.Stderr)
}
