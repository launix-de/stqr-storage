//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInstallEncryptedProfileAndAutostart(t *testing.T) {
	base := fmt.Sprintf(`Software\STQR\Tests\%d`, time.Now().UnixNano())
	i := installation{base + `\Mounts`, base + `\Run`, filepath.Join(t.TempDir(), "installed client")}
	defer func() {
		registry.DeleteKey(registry.CURRENT_USER, i.profiles)
		registry.DeleteKey(registry.CURRENT_USER, i.autorun)
		registry.DeleteKey(registry.CURRENT_USER, base)
	}()
	source := filepath.Join(t.TempDir(), "source.exe")
	if e := os.WriteFile(source, []byte("test executable"), 0600); e != nil {
		t.Fatal(e)
	}
	c := mountConfig{URI: "http://storage:7419", Username: "alice", Password: "a deliberately unique password", Bucket: "docs", Drive: "s:"}
	if e := i.save(c, source); e != nil {
		t.Fatal(e)
	}
	got, e := i.load("S:")
	if e != nil || got.Password != c.Password || got.Drive != "S:" {
		t.Fatal("profile roundtrip", e)
	}
	k, e := registry.OpenKey(registry.CURRENT_USER, i.profiles, registry.QUERY_VALUE|registry.SET_VALUE)
	if e != nil {
		t.Fatal(e)
	}
	defer k.Close()
	raw, _, e := k.GetBinaryValue("S")
	if e != nil || bytes.Contains(raw, []byte(c.Password)) {
		t.Fatal("password stored as plaintext", e)
	}
	run, e := registry.OpenKey(registry.CURRENT_USER, i.autorun, registry.QUERY_VALUE)
	if e != nil {
		t.Fatal(e)
	}
	command, _, e := run.GetStringValue("STQR-S")
	run.Close()
	if e != nil || !strings.Contains(command, "--saved-drive S:") || strings.Contains(command, c.Password) {
		t.Fatal("unsafe autostart", e)
	}
	args, e := windows.DecomposeCommandLine(command)
	if e != nil || len(args) != 4 || args[1] != "--background" || args[2] != "--saved-drive" || args[3] != "S:" {
		t.Fatal("autostart quoting", e, args)
	}
	if b, e := os.ReadFile(args[0]); e != nil || string(b) != "test executable" {
		t.Fatal("installed executable", e)
	}
	raw[len(raw)-1] ^= 1
	if e = k.SetBinaryValue("S", raw); e != nil {
		t.Fatal(e)
	}
	if _, e = i.load("S:"); e == nil {
		t.Fatal("tampered protected profile accepted")
	}
	if e = i.save(c, source); e != nil {
		t.Fatal(e)
	}
	if e = i.remove("S:"); e != nil {
		t.Fatal(e)
	}
	if _, e = i.load("S:"); !errors.Is(e, registry.ErrNotExist) {
		t.Fatal("saved credentials not removed", e)
	}
	run, e = registry.OpenKey(registry.CURRENT_USER, i.autorun, registry.QUERY_VALUE)
	if e != nil {
		t.Fatal(e)
	}
	defer run.Close()
	if _, _, e = run.GetStringValue("STQR-S"); !errors.Is(e, registry.ErrNotExist) {
		t.Fatal("autostart not removed", e)
	}
}
func TestDriveSessionExclusionAndStop(t *testing.T) {
	ctx, release, e := claimDrive(context.Background(), "Z:")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if _, release2, e := claimDrive(context.Background(), "Z:"); !errors.Is(e, errAlreadyMounted) {
		if e == nil {
			release2()
		}
		t.Fatal("duplicate session", e)
	}
	if e = stopDrive("Z:"); e != nil {
		t.Fatal(e)
	}
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("stop event did not cancel session")
	}
}
func TestInstallWinUIBundle(t *testing.T) {
	source := filepath.Join(t.TempDir(), "stqr-windows.exe")
	i := installation{directory: filepath.Join(t.TempDir(), "installed")}
	for path, content := range map[string]string{source: "backend", uiPath(source): "frontend-v1"} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	first, err := i.installExecutable(source)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(first) != "stqr-windows.exe" || filepath.Base(uiPath(first)) != "stqr-windows.ui.exe" {
		t.Fatal("WinUI executable basename changed", first)
	}
	if raw, err := os.ReadFile(uiPath(first)); err != nil || string(raw) != "frontend-v1" {
		t.Fatal("UI not installed", err)
	}
	if same, err := i.installExecutable(source); err != nil || same != first {
		t.Fatal("unchanged bundle was not reused", err)
	}
	if err := os.WriteFile(uiPath(source), []byte("frontend-v2"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := i.installExecutable(source)
	if err != nil || second == first {
		t.Fatal("UI update did not version the bundle", err)
	}
	if filepath.Base(first) != "stqr-windows.exe" || filepath.Base(uiPath(first)) != "stqr-windows.ui.exe" {
		t.Fatal("WinUI executable basename changed", first)
	}
	if raw, err := os.ReadFile(uiPath(first)); err != nil || string(raw) != "frontend-v1" {
		t.Fatal("running UI version was replaced", err)
	}
	if raw, err := os.ReadFile(uiPath(second)); err != nil || string(raw) != "frontend-v2" {
		t.Fatal("UI update missing", err)
	}
}
