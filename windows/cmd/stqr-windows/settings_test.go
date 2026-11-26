// SPDX-License-Identifier: GPL-3.0-only
package main

import "testing"

func TestMountConfiguration(t *testing.T) {
	valid := mountConfig{URI: "https://storage:7419", Username: "alice", Password: "secret", Bucket: "documents", Drive: "s:"}
	if e := valid.validate(); e != nil || valid.Drive != "S:" {
		t.Fatal(e, valid.Drive)
	}
	for _, drive := range []string{"S", `S:\`, "AA:", `S: --background`} {
		if _, e := normalizeDrive(drive); e == nil {
			t.Fatal("invalid drive accepted", drive)
		}
	}
	for _, uri := range []string{"file:///tmp/a", "http://alice:secret@host", "http:///bad", "ssh://host"} {
		c := valid
		c.URI = uri
		if e := c.validate(); e == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
	c := valid
	c.URI = "wss://storage/api/ws"
	if c.httpURI() != "https://storage/api/ws" {
		t.Fatal(c.httpURI())
	}
}

func TestSetupFlagDoesNotInterpretCLIValuesAsModes(t *testing.T) {
	for _, args := range [][]string{
		{"--bucket", "setup", "S:"},
		{"--username", "setup", "--bucket", "documents", "S:"},
		{"--bucket", "--setup", "S:"},
		{"--saved-drive", "setup", "--background"},
		{"--writable-paths", "setup", "S:"},
		{"--", "--setup"},
		{"S:", "--setup"},
		{"--setup", "--setup=false"},
		{"--setup=false"},
		{"-h", "--setup"},
	} {
		if hasSetupFlag(args) {
			t.Fatalf("headless arguments incorrectly selected UI: %q", args)
		}
	}
	for _, args := range [][]string{
		{"--setup"}, {"-setup=true"}, {"--setup=TRUE"},
		{"--setup", "--saved-drive", "T:"},
		{"--saved-drive", "T:", "--setup"},
	} {
		if !hasSetupFlag(args) {
			t.Fatalf("explicit setup mode not selected: %q", args)
		}
	}
}

func TestBackgroundFlagDoesNotInterpretCLIValuesAsModes(t *testing.T) {
	for _, args := range [][]string{{"--username", "background"}, {"--bucket", "--background"}, {"--background=false"}, {"S:", "--background"}} {
		if cliBoolFlag(args, "background") {
			t.Fatalf("CLI value hid its console: %q", args)
		}
	}
	if !cliBoolFlag([]string{"--background", "--saved-drive", "S:"}, "background") {
		t.Fatal("explicit background invocation lost its mode")
	}
}
