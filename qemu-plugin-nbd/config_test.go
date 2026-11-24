// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeYAMLConfig(t *testing.T) {
	for _, tt := range []struct{ listen, url string }{{":7419", "http://127.0.0.1:7419"}, {"0.0.0.0:7420", "http://127.0.0.1:7420"}, {"[::]:7419", "http://[::1]:7419"}, {"192.0.2.4:7419", "http://192.0.2.4:7419"}} {
		t.Run(tt.listen, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stqr.yaml")
			raw := "node:\n  listen: '" + tt.listen + "'\nauth:\n  bootstrap_admin:\n    username: admin\n    password: secret-test\nvolumes: []\ncluster_id: test\n"
			if e := os.WriteFile(path, []byte(raw), 0600); e != nil {
				t.Fatal(e)
			}
			c, e := LoadConfig(path)
			if e != nil || c.URL != tt.url || c.Username != "admin" || c.Password != "secret-test" {
				t.Fatal("node config decoding failed", e)
			}
		})
	}
}
func TestConfigRejectsUnsafeAndMissingCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stqr.yaml")
	os.WriteFile(path, []byte("node:\n  listen: ':7419'\n"), 0600)
	if _, e := LoadConfig(path); e == nil {
		t.Fatal("missing credentials accepted")
	}
	os.WriteFile(path, []byte("auth: [secret-test"), 0600)
	if _, e := LoadConfig(path); e == nil || strings.Contains(e.Error(), "secret-test") {
		t.Fatal("invalid YAML must not expose input")
	}
	os.Chmod(path, 0644)
	if _, e := LoadConfig(path); e == nil {
		t.Fatal("public credentials accepted")
	}
}
