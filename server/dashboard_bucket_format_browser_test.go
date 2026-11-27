// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func TestDashboardBucketFormatCreateBrowser(t *testing.T) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	s := testServer(t)
	addTestVolume(t, s, "bucket-format-create", "hdd", 64<<20)
	user := bucketProtocolUser(t, s)
	dir := t.TempDir()
	s.mu.Lock()
	s.cfg.FormatPluginsDir = dir
	a := s.cfg.Auth.BootstrapAdmin
	s.mu.Unlock()
	custom := `export const apiVersion=1;export function open(){};export async function initialize({connect,state,initializationID,signal}){state.attempts=(state.attempts??0)+1;const client=await connect();await client.put('a'.repeat(64),new TextEncoder().encode('initialized'),{requestId:initializationID,signal});if(state.attempts===1)throw Error('Bitte Initialisierung wiederholen');}`
	for name, source := range map[string]string{"custom-init.mjs": custom, "broken.mjs": `export const apiVersion=1;export function open(){};export const initialize=true;`} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	input, _ := json.Marshal(map[string]string{"url": host.URL, "username": a.Username, "password": a.Password, "ownerID": user.ID})
	cmd := exec.CommandContext(ctx, "node", "dashboard_bucket_format_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bucket format creation: %v\n%s", err, out)
	}
	t.Log(string(out))
	for _, format := range []string{"", "bd", "s3", "fs", "custom-init"} {
		name := "created-" + format
		if format == "" {
			name = "created-bare"
		}
		bucket, err := s.BucketByName(name)
		if err != nil {
			t.Fatal(name, err)
		}
		if bucket.Describe().Format != format {
			t.Fatal(name, bucket.Describe().Format)
		}
		if format == "" {
			continue
		}
		c, err := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(bucket.ID()), format)
		if err != nil {
			t.Fatal(err)
		}
		switch format {
		case "fs":
			f, err := client.OpenFilesystem(ctx, c, client.FSOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			entries, err := f.ReadDir(ctx, client.FilesystemRootID())
			if err != nil || len(entries) != 0 {
				t.Fatal(entries, err)
			}
			info, err := f.Stat(ctx, client.FilesystemRootID())
			if err != nil || info.Nlink != 2 || info.Mode != 0755 {
				t.Fatal(info, err)
			}
		case "bd":
			devices, err := nbd.NewCatalog(c).List(ctx)
			if err != nil || len(devices) != 0 {
				t.Fatal(devices, err)
			}
			objects, _, err := c.ScanObjects(ctx, nil, 100)
			if err != nil || len(objects) != 0 {
				t.Fatal("BD must remain empty", objects, err)
			}
		case "s3":
			keys, err := client.NewS3(c).Keys(ctx, "")
			if err != nil || len(keys) != 0 {
				t.Fatal(keys, err)
			}
			objects, _, err := c.ScanObjects(ctx, nil, 100)
			if err != nil || len(objects) != 0 {
				t.Fatal("S3 must remain empty", objects, err)
			}
		case "custom-init":
			var id client.ObjectID
			for i := range id {
				id[i] = 0xaa
			}
			data, _, err := c.Get(ctx, id)
			if err != nil || string(data) != "initialized" {
				t.Fatal(string(data), err)
			}
		}
		c.Close()
	}
	if _, err := s.BucketByName("created-broken"); err == nil {
		t.Fatal("invalid plugin created a bucket")
	}
}
