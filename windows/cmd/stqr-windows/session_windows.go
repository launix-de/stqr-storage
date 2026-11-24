//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
)

var errAlreadyMounted = errors.New("drive is already mounted by STQR")

func objectName(drive, kind string) (*uint16, error) {
	d, e := normalizeDrive(drive)
	if e != nil {
		return nil, e
	}
	u, e := windows.GetCurrentProcessToken().GetTokenUser()
	if e != nil {
		return nil, e
	}
	return windows.UTF16PtrFromString(`Local\STQR.` + u.User.Sid.String() + "." + d[:1] + "." + kind)
}
func claimDrive(ctx context.Context, drive string) (context.Context, func(), error) {
	name, e := objectName(drive, "owner")
	if e != nil {
		return nil, nil, e
	}
	mutex, e := windows.CreateMutex(nil, false, name)
	if e != nil {
		if mutex != 0 {
			windows.CloseHandle(mutex)
		}
		if errors.Is(e, windows.ERROR_ALREADY_EXISTS) {
			return nil, nil, errAlreadyMounted
		}
		return nil, nil, e
	}
	name, e = objectName(drive, "stop")
	if e != nil {
		windows.CloseHandle(mutex)
		return nil, nil, e
	}
	event, e := windows.CreateEvent(nil, 1, 0, name)
	if e != nil {
		windows.CloseHandle(mutex)
		return nil, nil, e
	}
	child, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-child.Done():
				return
			default:
			}
			state, e := windows.WaitForSingleObject(event, 250)
			if e != nil || state == windows.WAIT_OBJECT_0 {
				cancel()
				return
			}
		}
	}()
	return child, func() { cancel(); <-done; windows.CloseHandle(event); windows.CloseHandle(mutex) }, nil
}
func stopDrive(drive string) error {
	name, e := objectName(drive, "stop")
	if e != nil {
		return e
	}
	event, e := windows.OpenEvent(windows.EVENT_MODIFY_STATE, false, name)
	if errors.Is(e, windows.ERROR_FILE_NOT_FOUND) {
		return nil
	}
	if e != nil {
		return fmt.Errorf("Laufwerk trennen: %w", e)
	}
	defer windows.CloseHandle(event)
	return windows.SetEvent(event)
}
