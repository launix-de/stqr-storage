// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Client formats are deliberately opaque to the native storage server. S3 is
// the explicitly integrated frontend; physical host-volume discovery is separate.
func TestServerDoesNotInterpretFilesystemOrVirtualDiskFormats(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"FilesystemRootID": true, "OpenFilesystem": true, "OpenOverlayFilesystem": true, "ImportFilesystemTar": true, "ExportFilesystemTar": true, "FSOptions": true, "OverlayOptions": true}
	magic := map[string]bool{"STFS": true, "STDR": true, "STIX": true, "STOR": true, "STNBD001": true}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		aliases := map[string]bool{"client": true}
		for _, imp := range f.Imports {
			name, _ := strconv.Unquote(imp.Path.Value)
			if strings.Contains(name, "qemu-plugin-nbd") || strings.Contains(name, "/windows") {
				t.Errorf("%s imports a client frontend: %s", path, name)
			}
			if name == "github.com/launix-de/stqr-storage/client" && imp.Name != nil {
				aliases[imp.Name.Name] = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if x, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := x.X.(*ast.Ident); ok && aliases[pkg.Name] && forbidden[x.Sel.Name] {
					t.Errorf("%s interprets a client format through %s", path, x.Sel.Name)
				}
			}
			if x, ok := n.(*ast.BasicLit); ok && x.Kind == token.STRING {
				value, _ := strconv.Unquote(x.Value)
				if magic[value] {
					t.Errorf("%s recognizes client payload magic %q", path, value)
				}
			}
			return true
		})
	}
}

func TestObjectLeaseRetainsPersistedCompatibilityKey(t *testing.T) {
	raw := []byte(`{"Block":true,"ReadOnly":true,"AllowHandoff":true}`)
	var lease bucketLease
	if err := json.Unmarshal(raw, &lease); err != nil || !lease.ObjectIO || !lease.ReadOnly || !lease.AllowHandoff {
		t.Fatal("old lease record unreadable", err)
	}
	encoded, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"Block":true`)) || bytes.Contains(encoded, []byte(`"ObjectIO"`)) {
		t.Fatal("lease record encoding changed")
	}
}
