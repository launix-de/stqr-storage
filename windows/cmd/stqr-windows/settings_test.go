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
