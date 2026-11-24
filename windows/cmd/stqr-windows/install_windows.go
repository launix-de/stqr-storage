//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

type installation struct{ profiles, autorun, directory string }

func userInstallation() (installation, error) {
	// LocalAppData is owned by the logged-on user, including the inherited ACL.
	dir, e := windows.KnownFolderPath(windows.FOLDERID_LocalAppData, 0)
	if e != nil {
		return installation{}, e
	}
	return installation{`Software\STQR\Mounts`, `Software\Microsoft\Windows\CurrentVersion\Run`, filepath.Join(dir, "STQR")}, nil
}
func protect(raw []byte, decrypt bool) ([]byte, error) {
	if len(raw) == 0 || len(raw) > 65536 {
		return nil, fmt.Errorf("invalid saved profile size")
	}
	in := windows.DataBlob{Size: uint32(len(raw)), Data: &raw[0]}
	var out windows.DataBlob
	var e error
	if decrypt {
		e = windows.CryptUnprotectData(&in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out)
	} else {
		e = windows.CryptProtectData(&in, nil, nil, 0, nil, windows.CRYPTPROTECT_UI_FORBIDDEN, &out)
	}
	runtime.KeepAlive(raw)
	if e != nil {
		return nil, e
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data)))
	result := bytes.Clone(unsafe.Slice(out.Data, int(out.Size)))
	if decrypt {
		clear(unsafe.Slice(out.Data, int(out.Size)))
	}
	return result, nil
}
func (i installation) load(drive string) (mountConfig, error) {
	var c mountConfig
	d, e := normalizeDrive(drive)
	if e != nil {
		return c, e
	}
	k, e := registry.OpenKey(registry.CURRENT_USER, i.profiles, registry.QUERY_VALUE)
	if e != nil {
		return c, e
	}
	defer k.Close()
	raw, _, e := k.GetBinaryValue(d[:1])
	if e != nil {
		return c, e
	}
	raw, e = protect(raw, true)
	if e != nil {
		return c, fmt.Errorf("gespeicherte Zugangsdaten können nicht entschlüsselt werden: %w", e)
	}
	defer clear(raw)
	if e = json.Unmarshal(raw, &c); e != nil {
		return c, e
	}
	if e = c.validate(); e != nil {
		return c, e
	}
	if c.Drive != d {
		return c, fmt.Errorf("saved drive mismatch")
	}
	return c, nil
}
func (i installation) installExecutable(source string) (string, error) {
	f, e := os.Open(source)
	if e != nil {
		return "", e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, 128<<20+1))
	if e != nil {
		return "", e
	}
	if len(raw) > 128<<20 {
		return "", fmt.Errorf("executable too large")
	}
	digest := sha256.Sum256(raw)
	if e = os.MkdirAll(i.directory, 0700); e != nil {
		return "", e
	}
	dest := filepath.Join(i.directory, fmt.Sprintf("stqr-windows-%x.exe", digest[:8]))
	if old, e := os.ReadFile(dest); e == nil && bytes.Equal(old, raw) {
		return dest, nil
	}
	tmp, e := os.CreateTemp(i.directory, ".install-*.exe")
	if e != nil {
		return "", e
	}
	defer os.Remove(tmp.Name())
	if _, e = tmp.Write(raw); e == nil {
		e = tmp.Sync()
	}
	closeErr := tmp.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return "", e
	}
	if e = os.Rename(tmp.Name(), dest); e != nil {
		return "", e
	}
	return dest, nil
}
func (i installation) save(c mountConfig, source string) error {
	if e := c.validate(); e != nil {
		return e
	}
	exe, e := i.installExecutable(source)
	if e != nil {
		return e
	}
	command := windows.ComposeCommandLine([]string{exe, "--background", "--saved-drive", c.Drive})
	if len(windows.StringToUTF16(command)) > 260 {
		return fmt.Errorf("Autostart-Pfad ist zu lang")
	}
	raw, e := json.Marshal(c)
	if e != nil {
		return e
	}
	defer clear(raw)
	sealed, e := protect(raw, false)
	if e != nil {
		return e
	}
	profiles, _, e := registry.CreateKey(registry.CURRENT_USER, i.profiles, registry.QUERY_VALUE|registry.SET_VALUE)
	if e != nil {
		return e
	}
	defer profiles.Close()
	run, _, e := registry.CreateKey(registry.CURRENT_USER, i.autorun, registry.SET_VALUE)
	if e != nil {
		return e
	}
	defer run.Close()
	old, _, oldErr := profiles.GetBinaryValue(c.Drive[:1])
	if oldErr != nil && !errors.Is(oldErr, registry.ErrNotExist) {
		return oldErr
	}
	if e = profiles.SetBinaryValue(c.Drive[:1], sealed); e != nil {
		return e
	}
	if e = run.SetStringValue("STQR-"+c.Drive[:1], command); e != nil {
		var rollback error
		if oldErr == nil {
			rollback = profiles.SetBinaryValue(c.Drive[:1], old)
		} else {
			rollback = profiles.DeleteValue(c.Drive[:1])
		}
		return errors.Join(e, rollback)
	}
	return nil
}
func (i installation) remove(drive string) error {
	d, e := normalizeDrive(drive)
	if e != nil {
		return e
	}
	k, e := registry.OpenKey(registry.CURRENT_USER, i.autorun, registry.SET_VALUE)
	if e == nil {
		defer k.Close()
		e = k.DeleteValue("STQR-" + d[:1])
	}
	if e != nil && !errors.Is(e, registry.ErrNotExist) {
		return e
	}
	k, e = registry.OpenKey(registry.CURRENT_USER, i.profiles, registry.SET_VALUE)
	if e == nil {
		defer k.Close()
		e = k.DeleteValue(d[:1])
	}
	if errors.Is(e, registry.ErrNotExist) {
		return nil
	}
	return e
}
