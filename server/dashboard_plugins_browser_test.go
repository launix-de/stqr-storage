// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func TestDashboardPluginsBrowser(t *testing.T) {
	dashboardPluginsBrowser(t, false)
}

func TestDashboardPluginsClusterBrowser(t *testing.T) {
	dashboardPluginsBrowser(t, true)
}

func dashboardPluginsBrowser(t *testing.T, cluster bool) {
	if os.Getenv("STQR_TEST_BROWSER_MODULE") == "" {
		t.Skip("set STQR_TEST_BROWSER_MODULE for Playwright")
	}
	var s, storage *Server
	if cluster {
		source, target := startAutomaticDaemonPair(t)
		s, storage = source.node, target.node
	} else {
		s = testServer(t)
		storage = s
	}
	s.cfg.FormatPluginsDir = t.TempDir()
	custom := `export const apiVersion=1;
export const css='[data-stqr-plugin="custom"] .custom-output { color: rgb(12, 34, 56); }';
export async function open({bucket,windows,ui,signal}) {
 const view=windows.open({title:bucket.name,signal});
 const list=ui.createList({container:view.root,root:view.scrollRoot,signal:view.signal,columns:['Result'],key:x=>x.key,
 fetchPage:async(a,n,q,{signal})=>{await new Promise(r=>setTimeout(r,q?10:1200));return {items:[{key:q||'initial'}],next:''}},
 renderRow:item=>{const row=document.createElement('tr');const cell=document.createElement('td');cell.className='custom-output';cell.textContent=item.key;row.append(cell);return row}});
 await list.load();
}`
	if err := os.WriteFile(filepath.Join(s.cfg.FormatPluginsDir, "custom.mjs"), []byte(custom), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg.FormatPluginsDir, "badcss.mjs"), []byte(`export const apiVersion=1;export const css='body{display:none}';export function open(){throw Error('must not run')}`), 0644); err != nil {
		t.Fatal(err)
	}
	addTestVolume(t, storage, "plugins", "hdd", 96<<20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := s.cfg.Auth.BootstrapAdmin
	ids := map[string]string{}
	dial := func(format string) *client.Client {
		b, _ := createProtocolBucket(t, s, "plugin-"+format)
		ids[format] = b.ID().String()
		c, e := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(b.ID()), format)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	fsClient := dial("fs")
	fs, e := client.OpenFilesystem(ctx, fsClient, client.FSOptions{Create: true})
	if e != nil {
		t.Fatal(e)
	}
	dir, e := fs.Create(ctx, client.FilesystemRootID(), "docs", 0755, 0, 0, true)
	if e != nil {
		t.Fatal(e)
	}
	file, e := fs.Create(ctx, dir.ID, "hello.txt", 0644, 0, 0, false)
	if e != nil {
		t.Fatal(e)
	}
	payload := []byte("plugin filesystem download\n")
	if _, e = fs.WriteAt(ctx, file.ID, payload, 0); e != nil {
		t.Fatal(e)
	}
	large, e := fs.Create(ctx, dir.ID, "segmented.bin", 0644, 0, 0, false)
	if e != nil {
		t.Fatal(e)
	}
	largePayload := bytes.Repeat([]byte("0123456789abcdef"), 70000)
	if _, e = fs.WriteAt(ctx, large.ID, largePayload, 0); e != nil {
		t.Fatal(e)
	}
	if e = fsClient.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	fsClient.Close()
	s3Client := dial("s3")
	s3 := client.NewS3(s3Client)
	for i := 0; i < 65; i++ {
		data := []byte(fmt.Sprintf("object %03d <script>window.pluginInjected=true</script>", i))
		if _, e = s3.Put(ctx, fmt.Sprintf("folder/object-%03d.txt", i), bytes.NewReader(data), uint64(len(data)), client.S3PutOptions{Headers: map[string]string{"content-type": "text/plain"}, Metadata: map[string]string{"owner": "plugin-test"}}); e != nil {
			t.Fatal(e)
		}
	}
	image, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=")
	if _, e = s3.Put(ctx, "pixel.png", bytes.NewReader(image), uint64(len(image)), client.S3PutOptions{Headers: map[string]string{"content-type": "image/png"}}); e != nil {
		t.Fatal(e)
	}
	s3Client.Close()
	bdClient := dial("bd")
	if e = bdClient.BeginObjectLease(ctx, false); e != nil {
		t.Fatal(e)
	}
	catalog := nbd.NewCatalog(bdClient)
	device, e := catalog.Create(ctx, nbd.Metadata{Name: "original", Size: 2 << 20})
	if e != nil {
		t.Fatal(e)
	}
	if e = catalog.WriteAt(ctx, device, []byte("preserved device bytes"), 0); e != nil {
		t.Fatal(e)
	}
	if e = catalog.Sync(ctx); e != nil {
		t.Fatal(e)
	}
	bdClient.Close()
	unknown := dial("unknown")
	unknown.Close()
	customClient := dial("custom")
	customClient.Close()
	badCSSClient := dial("badcss")
	badCSSClient.Close()
	ids["admin"] = systemBucket.String()
	sum := sha256.Sum256(largePayload)
	input, _ := json.Marshal(map[string]any{"url": host.URL, "username": a.Username, "password": a.Password, "ids": ids, "largeSHA256": hex.EncodeToString(sum[:])})
	cmd := exec.CommandContext(ctx, "node", "dashboard_plugins_browser.mjs")
	cmd.Stdin = bytes.NewReader(input)
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("plugins browser: %v\n%s", e, out)
	}
	t.Log(string(out))
	// JS mutations must remain compatible with the actual Go clients.
	bdID, _ := parseID(ids["bd"])
	verify, e := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(bdID), "bd")
	if e != nil {
		t.Fatal(e)
	}
	defer verify.Close()
	listed, e := nbd.NewCatalog(verify).List(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if len(listed) != 1 || listed[0].Name != "renamed" || listed[0].Size != 4<<20 || listed[0].ID != device.ID {
		t.Fatal(listed)
	}
	got := make([]byte, len("preserved device bytes"))
	if e = nbd.NewCatalog(verify).ReadAt(ctx, listed[0], got, 0); e != nil || string(got) != "preserved device bytes" {
		t.Fatalf("payload %q: %v", got, e)
	}
	s3ID, _ := parseID(ids["s3"])
	check, e := client.Dial(ctx, host.URL, a.Username, a.Password, client.ID(s3ID), "s3")
	if e != nil {
		t.Fatal(e)
	}
	defer check.Close()
	object, e := client.NewS3(check).Head(ctx, "folder/object-064.txt")
	if e != nil || !object.PublicRead {
		t.Fatal(object, e)
	}
}
