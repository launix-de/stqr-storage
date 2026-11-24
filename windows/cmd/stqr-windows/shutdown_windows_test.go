//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"sync"
	"testing"
	"time"
)

func TestHiddenWindowSessionShutdown(t *testing.T) {
	stopped, finished := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hwnd, cleanup, err := watchSessionShutdown(func() { once.Do(func() { close(stopped); close(finished) }) }, finished)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	visible, _, _ := user32.NewProc("IsWindowVisible").Call(hwnd)
	if visible != 0 {
		t.Fatal("shutdown receiver must remain hidden")
	}
	user32.NewProc("SendMessageW").Call(hwnd, 0x11, 0, 0)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("session shutdown did not cancel mount")
	}
}
