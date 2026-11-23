//go:build linux

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/server"
	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

// Run the actual daemon entry point in a disposable child, so signals never
// reach the test runner. This tests local persistence, not cluster SHORT/resume.
func TestDaemonSignalHelper(t *testing.T) {
	if os.Getenv("STQR_SIGNAL_TEST_CONFIG") == "" {
		return
	}
	os.Args = []string{"stqr", "--config", os.Getenv("STQR_SIGNAL_TEST_CONFIG"), "--listen", os.Getenv("STQR_SIGNAL_TEST_LISTEN")}
	main()
}

func TestProcessSignalsPreserveVolumes(t *testing.T) {
	for _, signal := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(signal.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.yaml")
			s, err := server.Open(server.Options{ConfigPath: path})
			if err != nil {
				t.Fatal(err)
			}
			id, err := volume.NewID()
			if err != nil {
				s.Close()
				t.Fatal(err)
			}
			_, addErr := s.AddVolume(id.String(), server.AddVolumeRequest{Name: "keep", Class: "hdd", SizeBytes: 2 << 20})
			closeErr := s.Close()
			if addErr != nil || closeErr != nil {
				t.Fatalf("prepare: add=%v close=%v", addErr, closeErr)
			}
			before := readSignalConfig(t, path)
			var filename string
			for _, v := range before.Volumes {
				if v.Name == "keep" {
					filename = v.Path
				}
			}
			if filename == "" {
				t.Fatal("missing test volume")
			}
			if !filepath.IsAbs(filename) {
				dataDir := before.Node.DataDir
				if !filepath.IsAbs(dataDir) {
					dataDir = filepath.Join(filepath.Dir(path), dataDir)
				}
				filename = filepath.Join(dataDir, before.VolumesDir, filename)
			}
			key := volume.Key{Bucket: volume.ID{1}, Object: volume.ObjectID{2}}
			payload := []byte("preserve me across supervisor restarts")
			withSignalVolume(t, filename, func(h *volume.Handle) error {
				_, e := h.Put(key, payload, volume.Options{})
				return e
			})
			// Restart twice: retaining config alone is insufficient if reopening
			// then resumed a mistakenly initiated evacuation.
			for i := 0; i < 2; i++ {
				runAndSignalDaemon(t, path, signal)
				after := readSignalConfig(t, path)
				if after.Node.ID != before.Node.ID || after.ClusterID != before.ClusterID || after.Generation != before.Generation || len(after.Volumes) != len(before.Volumes) {
					t.Fatal("signal changed identity, generation or configured volumes")
				}
				for j, v := range after.Volumes {
					if !reflect.DeepEqual(v, before.Volumes[j]) || v.State != "active" {
						t.Fatal("signal changed volume configuration or initiated drain")
					}
				}
				withSignalVolume(t, filename, func(h *volume.Handle) error {
					var out bytes.Buffer
					_, e := h.CopyTo(key, 0, uint64(len(payload)), &out)
					if e == nil && !bytes.Equal(out.Bytes(), payload) {
						e = fmt.Errorf("payload changed after restart")
					}
					return e
				})
			}
		})
	}
}

func readSignalConfig(t *testing.T, path string) server.Config {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c server.Config
	if err = yaml.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func withSignalVolume(t *testing.T, path string, fn func(*volume.Handle) error) {
	t.Helper()
	v, err := volume.New(path, 0, volume.HDD)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	h, err := v.Open(true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = fn(h); err != nil {
		t.Fatal(err)
	}
}

func runAndSignalDaemon(t *testing.T, config string, signal os.Signal) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonSignalHelper$")
	cmd.Env = append(os.Environ(), "STQR_SIGNAL_TEST_CONFIG="+config, "STQR_SIGNAL_TEST_LISTEN="+address)
	logOutput, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	// No config or credentials are logged even on failure.
	cmd.Stdout = io.Discard
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(logOutput)
		announced := false
		for scanner.Scan() {
			if !announced && strings.HasPrefix(scanner.Text(), "STQR listening on http://") {
				close(ready)
				announced = true
			}
		}
		finished <- cmd.Wait()
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	select {
	case <-ready:
	case err = <-finished:
		t.Fatalf("daemon exited before listening: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not start")
	}
	if err = cmd.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-finished:
		if err != nil {
			t.Fatalf("daemon did not stop cleanly: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not stop")
	}
}
