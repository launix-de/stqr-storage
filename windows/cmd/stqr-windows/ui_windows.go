// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

const uiProtocolVersion = 1
const uiMaxRequest = 64 << 10

type uiRequest struct {
	Version    int         `json:"version"`
	Action     string      `json:"action"`
	Drive      string      `json:"drive,omitempty"`
	Config     mountConfig `json:"config"`
	Persist    bool        `json:"persist"`
	Initialize bool        `json:"initialize"`
}
type uiProfile struct {
	Drive   string `json:"drive"`
	Bucket  string `json:"bucket"`
	Running bool   `json:"running"`
}
type uiReply struct {
	Version  int          `json:"version"`
	Type     string       `json:"type"`
	Message  string       `json:"message,omitempty"`
	Config   *mountConfig `json:"config,omitempty"`
	Profiles []uiProfile  `json:"profiles,omitempty"`
	Running  bool         `json:"running"`
	Saved    bool         `json:"saved"`
	WinFsp   bool         `json:"winfsp"`
}
type uiStateKey struct{}

func readUIRequest(input io.Reader) (uiRequest, error) {
	var request uiRequest
	raw, err := io.ReadAll(io.LimitReader(input, uiMaxRequest+1))
	if err != nil {
		return request, err
	}
	defer clear(raw)
	if len(raw) > uiMaxRequest {
		return request, fmt.Errorf("Anfrage ist zu groß")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&request); err != nil {
		return request, fmt.Errorf("Ungültige Konfigurationsanfrage")
	}
	var extra any
	if d.Decode(&extra) != io.EOF || request.Version != uiProtocolVersion {
		return request, fmt.Errorf("Nicht unterstütztes Konfigurationsprotokoll")
	}
	return request, nil
}
func uiPath(exe string) string { return strings.TrimSuffix(exe, filepath.Ext(exe)) + ".ui.exe" }
func launchWinUI() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	frontend := uiPath(exe)
	if _, err = os.Stat(frontend); err != nil {
		return fmt.Errorf("Die Konfigurationsoberfläche fehlt. Bitte das vollständige STQR-Windows-Paket verwenden")
	}
	args := append([]string{"--dialog"}, os.Args[1:]...)
	cmd := exec.Command(frontend, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NO_WINDOW}
	return cmd.Run()
}
func driveRunning(drive string) bool {
	name, err := objectName(drive, "owner")
	if err != nil {
		return false
	}
	h, err := windows.OpenMutex(windows.SYNCHRONIZE, false, name)
	if err != nil {
		return false
	}
	windows.CloseHandle(h)
	return true
}
func (i installation) uiProfiles() []uiProfile {
	key, err := registry.OpenKey(registry.CURRENT_USER, i.profiles, registry.QUERY_VALUE)
	if err != nil {
		return nil
	}
	defer key.Close()
	names, err := key.ReadValueNames(0)
	if err != nil {
		return nil
	}
	var out []uiProfile
	for _, name := range names {
		if len(name) != 1 {
			continue
		}
		c, err := i.load(name + ":")
		if err != nil {
			continue
		}
		out = append(out, uiProfile{c.Drive, c.Bucket, driveRunning(c.Drive)})
	}
	return out
}
func runUIRPC() error {
	request, err := readUIRequest(os.Stdin)
	if err != nil {
		return err
	}
	installation, err := userInstallation()
	if err != nil {
		return err
	}
	var mu sync.Mutex
	send := func(reply uiReply) {
		mu.Lock()
		defer mu.Unlock()
		reply.Version = uiProtocolVersion
		_ = json.NewEncoder(os.Stdout).Encode(reply)
	}
	switch request.Action {
	case "status":
		send(uiReply{Type: "status", WinFsp: ensureWinFsp() == nil, Profiles: installation.uiProfiles()})
		return nil
	case "load":
		drive, err := normalizeDrive(request.Drive)
		if err != nil {
			return err
		}
		c, err := installation.load(drive)
		if errors.Is(err, registry.ErrNotExist) {
			send(uiReply{Type: "settings", WinFsp: ensureWinFsp() == nil, Running: driveRunning(drive)})
			return nil
		}
		if err != nil {
			return err
		}
		send(uiReply{Type: "settings", Config: &c, Saved: true, Running: driveRunning(drive), WinFsp: ensureWinFsp() == nil})
		return nil
	case "stop", "forget":
		drive, err := normalizeDrive(request.Drive)
		if err != nil {
			return err
		}
		if request.Action == "forget" {
			if err = installation.remove(drive); err != nil {
				return err
			}
		}
		if err = stopDrive(drive); err != nil {
			return err
		}
		send(uiReply{Type: "stopping"})
		return nil
	case "mount":
		c := request.Config
		if err = c.validate(); err != nil {
			return err
		}
		if _, statErr := os.Stat(c.Drive + `\`); statErr == nil {
			return fmt.Errorf("Laufwerk %s ist bereits belegt", c.Drive)
		}
		if request.Initialize && c.ReadOnly {
			return fmt.Errorf("Ein schreibgeschütztes Laufwerk kann kein Dateisystem initialisieren")
		}
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		go func() {
			select {
			case <-consoleInterrupt:
				cancel()
			case <-ctx.Done():
			}
		}()
		ctx, release, err := claimDrive(ctx, c.Drive)
		if err != nil {
			return err
		}
		defer release()
		finished := make(chan struct{})
		_, closeSession, err := watchSessionShutdown(cancel, finished)
		if err != nil {
			return err
		}
		defer closeSession()
		defer close(finished)
		ctx = context.WithValue(ctx, uiStateKey{}, func() { send(uiReply{Type: "mounted", Running: true}) })
		send(uiReply{Type: "connecting"})
		err = mountResult(ctx, mount(ctx, c, request.Initialize, installation, request.Persist, !request.Persist))
		if err == nil {
			send(uiReply{Type: "stopped"})
			return nil
		}
		return err
	default:
		return fmt.Errorf("Unbekannte Konfigurationsaktion")
	}
}

// The UI only receives readiness after the native drive is actually accessible.
func waitForUIDrive(ctx context.Context, done <-chan struct{}, drive string) {
	notify, ok := ctx.Value(uiStateKey{}).(func())
	if !ok {
		return
	}
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-timer.C:
			if _, err := os.Stat(drive + `\`); err == nil {
				notify()
				return
			}
		}
	}
}
