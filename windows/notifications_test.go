// SPDX-License-Identifier: GPL-3.0-only
package winfs

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"testing"
)

func TestRemoteDirectoryChanges(t *testing.T) {
	f, store := fixture(t)
	volume, err := client.OpenFilesystem(context.Background(), store, client.FSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other := New(context.Background(), volume, false)
	dir, err := f.Open("/", OpenOptions{Read: true, Directory: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(dir)
	assertChange := func(name string, created, removed bool) {
		t.Helper()
		changes := f.Changes(context.Background())
		for _, c := range changes {
			if c.Path == name && c.Created == created && c.Removed == removed {
				return
			}
		}
		t.Fatalf("missing change %s created=%v removed=%v: %+v", name, created, removed, changes)
	}
	h, err := other.Open("/remote.txt", OpenOptions{Write: true, Create: true}, 0644)
	if err != nil {
		t.Fatal(err)
	}
	assertChange("/remote.txt", true, false)
	if err = f.RefreshDirectory(dir); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.get(dir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range snapshot.entries {
		if e.Name == "remote.txt" {
			found = true
		}
	}
	if !found {
		t.Fatal("existing directory handle did not refresh")
	}
	if _, err = other.Write(h, []byte("remote data"), 0); err != nil {
		t.Fatal(err)
	}
	assertChange("/remote.txt", false, false)
	if err = other.Close(h); err != nil {
		t.Fatal(err)
	}
	if err = other.Remove("/remote.txt", false); err != nil {
		t.Fatal(err)
	}
	assertChange("/remote.txt", false, true)
	if changes := f.Changes(context.Background()); len(changes) != 0 {
		t.Fatalf("unchanged directory: %+v", changes)
	}
}

func TestRefreshRenamedDirectory(t *testing.T) {
	f, _ := fixture(t)
	if err := f.Mkdir("/before", 0755); err != nil {
		t.Fatal(err)
	}
	h, err := f.Open("/before", OpenOptions{Read: true, Directory: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close(h)
	if err = f.Rename("/before", "/after", 0); err != nil {
		t.Fatal(err)
	}
	if err = f.RefreshDirectory(h); err != nil {
		t.Fatal(err)
	}
}
