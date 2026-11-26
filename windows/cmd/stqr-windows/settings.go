// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"fmt"
	"net/url"
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
