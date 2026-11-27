// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestReferenceRootsNeverTraverseCloneGraph(t *testing.T) {
	ctx := context.Background()
	a, b, c := volume.ID{1}, volume.ID{2}, volume.ID{3}
	g := cowGraph{a: {b}, b: {a, c}, c: {b, a}}
	root := volume.ObjectID(client.ObjectReferenceRootID())
	payload := volume.ObjectID{42}
	m := &cowMemory{objects: map[volume.Key]cowMemoryRecord{
		{Bucket: a, Object: root}:    {data: []byte("source holders")},
		{Bucket: a, Object: payload}: {data: []byte("user payload")},
	}}
	if got := cowRead(t, g, m, c, root); got != "<absent>" {
		t.Fatal("inherited reference holders", got)
	}
	if got := cowRead(t, g, m, c, payload); got != "user payload" {
		t.Fatal("payload inheritance broken", got)
	}
	if err := m.write(ctx, g, a, root, []byte("new source holders"), false); err != nil {
		t.Fatal(err)
	}
	if len(m.objects) != 2 {
		t.Fatal("reference change materialized other buckets", len(m.objects))
	}
	if err := m.write(ctx, g, c, root, []byte("target holders"), false); err != nil {
		t.Fatal(err)
	}
	if got := cowRead(t, g, m, a, root); got != "new source holders" {
		t.Fatal(got)
	}
	if got := cowRead(t, g, m, b, root); got != "<absent>" {
		t.Fatal(got)
	}
	if err := m.write(ctx, g, a, payload, []byte("new user payload"), false); err != nil {
		t.Fatal(err)
	}
	if got := cowRead(t, g, m, c, payload); got != "user payload" {
		t.Fatal("ordinary preimage preservation broken", got)
	}
}

func TestAcceptedReferencesInCloneGraphDoNotMaterializeOrSync(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "reference-cow", "hdd", 16<<20)
	source, _ := createProtocolBucket(t, s, "reference-cow-source")
	target, _ := createProtocolBucket(t, s, "reference-cow-target")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	actor := userPrincipal(t, s)
	http := httptest.NewServer(s.Handler())
	defer http.Close()
	dial := func(b *Bucket) *client.Client {
		c, err := client.Dial(ctx, http.URL, s.cfg.Auth.BootstrapAdmin.Username, s.cfg.Auth.BootstrapAdmin.Password, client.ID(b.ID()), "fs")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	src := dial(source)
	root := client.ObjectReferenceRootID()
	object := client.FilesystemRootID()
	srcFS, err := client.OpenFilesystem(ctx, src, client.FSOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srcFS.Retain(ctx, object); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(target.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, actor); err != nil {
		t.Fatal(err)
	}
	dst := dial(target)
	dstFS, err := client.OpenFilesystem(ctx, dst, client.FSOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	before := map[*volume.Volume]volume.IOStats{}
	for _, v := range source.allVolumes() {
		before[v] = v.IOStats()
	}
	if _, _, err := dst.Get(ctx, root); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("clone inherited holder root", err)
	}
	records, _, _, err := s.cowLogicalPage(ctx, target.ID(), nil, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Key.Object == volume.ObjectID(root) {
			t.Fatal("logical clone inventory inherited source reference root")
		}
	}
	other, err := srcFS.Retain(ctx, object)
	if err != nil {
		t.Fatal(err)
	}
	if err := srcFS.Release(ctx, object, other); err != nil {
		t.Fatal(err)
	}
	expected, _, err := src.Get(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dstFS.Retain(ctx, object); err != nil {
		t.Fatal(err)
	}
	actual, _, err := src.Get(ctx, root)
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatal("target changed source holders", err)
	}
	for v, old := range before {
		if got := v.IOStats(); got.SyncOps != old.SyncOps {
			t.Fatal("accepted reference caused COW Sync", old.SyncOps, got.SyncOps)
		}
	}
}
