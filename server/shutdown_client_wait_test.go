// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShortShutdownClosesClientBeforeAdmissionDrain(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(s.ctx)
	ws := &wsSession{server: s, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.wsMu.Lock()
	s.sockets[ws] = struct{}{}
	s.wsMu.Unlock()
	entered := make(chan struct{})
	released := make(chan struct{})
	go func() { s.opMu.RLock(); close(entered); <-ctx.Done(); s.opMu.RUnlock(); close(released) }()
	<-entered
	done := make(chan error, 1)
	go func() { done <- s.prepareShortShutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		s.wsMu.Lock()
		delete(s.sockets, ws)
		s.wsMu.Unlock()
		cancel()
		<-released
		<-done
		t.Fatal("active client prevented short shutdown admission drain")
	}
	<-released
	select {
	case <-ws.done:
	default:
		t.Fatal("serving transport remained open")
	}
	s.mu.RLock()
	paused := s.cfg.Node.Paused
	s.mu.RUnlock()
	if !paused {
		t.Fatal("short state not retained")
	}
}
func TestGoAwayDoesNotWaitForClientWriter(t *testing.T) {
	s := testServer(t)
	ws := &wsSession{server: s, done: make(chan struct{}), terminal: make(chan terminalFrame, 1)}
	// No writer is running. A full notification queue must not delay STOP.
	ws.terminal <- terminalFrame{done: make(chan struct{})}
	s.wsMu.Lock()
	s.sockets[ws] = struct{}{}
	s.wsMu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.sendGoAway(context.Background(), 1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		ws.close()
		<-done
		t.Fatal("GOAWAY waited for stalled terminal socket")
	}
	select {
	case <-ws.done:
	default:
		t.Fatal("stalled terminal writer not disconnected")
	}
}

func TestDaemonStopDoesNotWaitForHTTPBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, input := io.Pipe()
	defer output.Close()
	path := filepath.Join(t.TempDir(), "node.yaml")
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen := probe.Addr().String()
	probe.Close()
	done := make(chan error, 1)
	go func() {
		defer input.Close()
		done <- Run(ctx, Options{ConfigPath: path, transportOnly: true, values: map[string]string{"listen": listen, "system-volume-bytes": "262144"}}, input)
	}()
	reader := bufio.NewReader(output)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("startup output: %v; daemon: %v", err, <-done)
	}
	address := strings.Fields(line)[3]
	address = strings.TrimPrefix(address, "http://")
	if _, err = reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	config, err := loadConfig(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	auth := base64.StdEncoding.EncodeToString([]byte(config.Auth.BootstrapAdmin.Username + ":" + config.Auth.BootstrapAdmin.Password))
	fmt.Fprintf(conn, "POST /api/message HTTP/1.1\r\nHost: %s\r\nAuthorization: Basic %s\r\nContent-Length: 100\r\nExpect: 100-continue\r\n\r\n", address, auth)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "POST"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 100 {
		t.Fatal("handler did not start request body", response.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		conn.Close()
		<-done
		t.Fatal("slow HTTP request body delayed daemon stop")
	}
}
