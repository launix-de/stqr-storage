// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type mountConfig struct {
	URI         string   `json:"uri"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	Bucket      string   `json:"bucket"`
	LowerBucket string   `json:"lower_bucket,omitempty"`
	Whitelist   []string `json:"whitelist,omitempty"`
	Drive       string   `json:"drive"`
	ReadOnly    bool     `json:"readonly"`
	UID         *uint32  `json:"uid,omitempty"`
	GID         *uint32  `json:"gid,omitempty"`
}

func normalizeDrive(d string) (string, error) {
	d = strings.ToUpper(strings.TrimSpace(d))
	if len(d) != 2 || d[1] != ':' || d[0] < 'A' || d[0] > 'Z' {
		return "", fmt.Errorf("Laufwerk muss ein Buchstabe mit Doppelpunkt sein, z. B. S:")
	}
	return d, nil
}
func (c *mountConfig) validate() error {
	d, e := normalizeDrive(c.Drive)
	if e != nil {
		return e
	}
	c.Drive = d
	u, e := url.Parse(c.URI)
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ws" && u.Scheme != "wss") {
		return fmt.Errorf("Bitte eine Server-URI ohne eingebettete Zugangsdaten angeben")
	}
	if c.Username == "" || c.Password == "" || c.Bucket == "" {
		return fmt.Errorf("Benutzername, Passwort und Bucket sind erforderlich")
	}
	if c.LowerBucket == c.Bucket && c.LowerBucket != "" {
		return fmt.Errorf("Basis und beschreibbares Bucket müssen verschieden sein")
	}
	if c.LowerBucket == "" && len(c.Whitelist) > 0 {
		return fmt.Errorf("Eine Pfadfreigabe benötigt ein Overlay-Basisbucket")
	}
	if len(c.Whitelist) > 1024 {
		return fmt.Errorf("Zu viele Overlay-Pfadfreigaben")
	}
	values := append([]string{c.URI, c.Username, c.Password, c.Bucket, c.LowerBucket}, c.Whitelist...)
	for _, s := range values {
		if len(s) > 4096 || strings.ContainsRune(s, 0) {
			return fmt.Errorf("Eingabe ungültig oder zu lang")
		}
	}
	return nil
}
func (c mountConfig) httpURI() string {
	u := strings.Replace(c.URI, "wss://", "https://", 1)
	return strings.Replace(u, "ws://", "http://", 1)
}

// Follow Go flag parsing: values and positional arguments are never mode
// switches. Keep this list aligned with run's actual command-line options.
func cliBoolFlag(args []string, wanted string) bool {
	selected := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" || !strings.HasPrefix(arg, "-") || arg == "-" {
			break
		}
		name, value, assigned := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-"), "=")
		switch name {
		case "setup", "readonly", "init", "password-stdin", "install", "background":
			enabled := true
			if assigned {
				var err error
				enabled, err = strconv.ParseBool(value)
				if err != nil {
					return false
				}
			}
			if name == wanted {
				selected = enabled
			}
		case "uri", "username", "bucket", "lower-bucket", "writable-paths", "uid", "gid", "uninstall", "saved-drive":
			if !assigned {
				i++
				if i >= len(args) {
					return false
				}
			}
		default:
			// Let run report help/unknown options instead of opening the UI.
			return false
		}
	}
	return selected
}
func hasSetupFlag(args []string) bool { return cliBoolFlag(args, "setup") }
