// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestFormatPluginsDiscoveryAndAssets(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	s.cfg.FormatPluginsDir = dir
	// An arbitrary format name is discovered without core registration.
	for name, body := range map[string]string{"custom format.mjs": "export const apiVersion=1; export function open(){}", "custom format.js": "wrong extension priority", "s3.mjs": "export const external=true", "ignore.txt": "ignored"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	handler := s.Handler()
	request := func(path string, auth bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.RemoteAddr = "127.0.0.1:1234"
		if auth {
			r.SetBasicAuth(s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if got := request("/api/format-plugins", false).Code; got != 401 {
		t.Fatal(got)
	}
	w := request("/api/format-plugins", true)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var index struct {
		Plugins []formatPluginFile `json:"plugins"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &index); err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, p := range index.Plugins {
		found[p.Format] = p.URL
		if strings.Contains(w.Body.String(), dir) {
			t.Fatal("local plugin path exposed")
		}
	}
	if found["custom format"] != "/format-plugins/custom%20format.mjs" || found["admin"] != "" || found["ignore"] != "" {
		t.Fatal(found)
	}
	if got := request(found["custom format"], true); got.Code != 200 || !strings.Contains(got.Body.String(), "apiVersion") {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := request(found["s3"], true); got.Code != 200 || got.Body.String() != "export const external=true" {
		t.Fatal(got.Code, got.Body.String())
	}
	for _, path := range []string{"/format-plugins/unknown.mjs", "/format-plugins/custom%20format.js", "/format-plugins/..%2Fconfig.go"} {
		if got := request(path, true).Code; got != 404 {
			t.Fatal(path, got)
		}
	}
	if got := request(found["custom format"], false).Code; got != 401 {
		t.Fatal(got)
	}
	page := request("/", false)
	nonce := regexp.MustCompile(`name="stqr-style-nonce" content="([a-f0-9]+)"`).FindStringSubmatch(page.Body.String())
	if len(nonce) != 2 || !strings.Contains(page.Header().Get("Content-Security-Policy"), "'nonce-"+nonce[1]+"'") {
		t.Fatal("missing matching style nonce")
	}
	again := request("/", false)
	if strings.Contains(again.Body.String(), nonce[1]) {
		t.Fatal("nonce reused")
	}
}
