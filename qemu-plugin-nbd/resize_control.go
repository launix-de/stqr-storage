// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type resizeRequest struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

func resizeHandler(catalog *Catalog, readonly bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/resize" || r.Method != http.MethodPost {
			http.Error(w, "expected POST /resize", http.StatusNotFound)
			return
		}
		if readonly {
			http.Error(w, "readonly exporter", http.StatusForbidden)
			return
		}
		var req resizeRequest
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048))
		d.DisallowUnknownFields()
		if err := d.Decode(&req); err != nil {
			http.Error(w, "invalid resize request", http.StatusBadRequest)
			return
		}
		if err := d.Decode(new(any)); err != io.EOF {
			http.Error(w, "trailing resize request data", http.StatusBadRequest)
			return
		}
		m, err := catalog.Lookup(r.Context(), req.Name)
		if err == nil {
			m, err = catalog.Resize(r.Context(), m, req.Size)
		}
		if err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, client.ErrRange), errors.Is(err, client.ErrProtocol):
				status = http.StatusBadRequest
			case errors.Is(err, client.ErrNotFound):
				status = http.StatusNotFound
			case errors.Is(err, client.ErrPrecondition):
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(m)
	})
}

// serveResizeControl shares the exporter's existing authenticated lease. The
// private socket is never exposed on TCP; its permissions match the NBD socket.
func serveResizeControl(path string, catalog *Catalog, readonly bool) (func(), error) {
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, err
	}
	s := &http.Server{Handler: resizeHandler(catalog, readonly), ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Serve(l)
	}()
	return func() {
		// Wait for active metadata mutations before the owning lease is closed.
		_ = s.Shutdown(context.Background())
		<-done
		_ = os.Remove(path)
	}, nil
}

// ResizeExport grows an export through its running owner's private control
// socket, without opening a second STQR connection or releasing the bucket lease.
func ResizeExport(ctx context.Context, socket, name string, size uint64) (Metadata, error) {
	body, err := json.Marshal(resizeRequest{Name: name, Size: size})
	if err != nil {
		return Metadata{}, err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket+".control")
	}}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/resize", bytes.NewReader(body))
	if err != nil {
		return Metadata{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Transport: transport, Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return Metadata{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return Metadata{}, err
	}
	if response.StatusCode != http.StatusOK {
		return Metadata{}, fmt.Errorf("resize: %s: %s", response.Status, bytes.TrimSpace(raw))
	}
	var m Metadata
	if err = json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	return m, m.validate()
}
