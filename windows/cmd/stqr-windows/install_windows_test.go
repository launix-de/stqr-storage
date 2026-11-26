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
func TestInstallSelfContainedExecutable(t *testing.T) {
	source := filepath.Join(t.TempDir(), "stqr-windows.exe")
	i := installation{directory: filepath.Join(t.TempDir(), "installed")}
	if err := os.WriteFile(source, []byte("client-with-ui-v1"), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := i.installExecutable(source)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(first) != "stqr-windows.exe" {
		t.Fatal("executable basename changed", first)
	}
	entries, err := os.ReadDir(filepath.Dir(first))
	if err != nil || len(entries) != 1 {
		t.Fatal("installation needs additional files", entries, err)
	}
	if same, err := i.installExecutable(source); err != nil || same != first {
		t.Fatal("unchanged executable not reused", err)
	}
	if err := os.WriteFile(source, []byte("client-with-ui-v2"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := i.installExecutable(source)
	if err != nil || second == first {
		t.Fatal("new executable did not version installation", err)
	}
	if raw, err := os.ReadFile(first); err != nil || string(raw) != "client-with-ui-v1" {
		t.Fatal("running version was replaced", err)
	}
}
