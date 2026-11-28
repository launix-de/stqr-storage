// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

//go:embed format-plugins/*.mjs
var builtinFormatPlugins embed.FS

const defaultFormatPluginsDir = "/usr/share/stqr/formats"

type formatPluginFile struct {
	Format  string `json:"format"`
	URL     string `json:"url"`
	path    string
	builtin bool
}

// Plugins are executable administrator-installed assets. This service knows only
// filenames, never format contents or capabilities. External files override the
// bundled file with the same opaque stem; mjs wins over js within a directory.
func (s *Server) formatPluginFiles() (map[string]formatPluginFile, error) {
	out := map[string]formatPluginFile{}
	add := func(entries []fs.DirEntry, dir string, builtin bool) {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			ext := filepath.Ext(name)
			if ext != ".js" && ext != ".mjs" {
				continue
			}
			format := strings.TrimSuffix(name, ext)
			if format == "" || format == "." || format == ".." {
				continue
			}
			if old, ok := out[format]; ok && old.builtin == builtin && filepath.Ext(old.path) == ".mjs" {
				continue
			}
			out[format] = formatPluginFile{Format: format, URL: "/format-plugins/" + name, path: filepath.Join(dir, name), builtin: builtin}
		}
	}
	builtins, err := builtinFormatPlugins.ReadDir("format-plugins")
	if err != nil {
		return nil, err
	}
	add(builtins, "format-plugins", true)
	s.mu.RLock()
	dir := s.cfg.FormatPluginsDir
	s.mu.RUnlock()
	if dir == "" {
		dir = defaultFormatPluginsDir
	}
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	add(entries, dir, false)
	return out, nil
}
func (s *Server) serveFormatPlugins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	files, err := s.formatPluginFiles()
	if err != nil {
		httpError(w, 500, err)
		return
	}
	if r.URL.Path == "/api/format-plugins" {
		plugins := make([]formatPluginFile, 0, len(files))
		for _, file := range files {
			file.URL = "/format-plugins/" + url.PathEscape(filepath.Base(file.path))
			plugins = append(plugins, file)
		}
		sort.Slice(plugins, func(i, j int) bool { return plugins[i].Format < plugins[j].Format })
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"plugins": plugins})
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/format-plugins/")
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, "/\\") {
		http.NotFound(w, r)
		return
	}
	file, ok := files[strings.TrimSuffix(name, filepath.Ext(name))]
	if !ok || filepath.Base(file.path) != name {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	if !file.builtin {
		http.ServeFile(w, r, file.path)
		return
	}
	data, err := builtinFormatPlugins.ReadFile(file.path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodHead {
		w.Write(data)
	}
}
