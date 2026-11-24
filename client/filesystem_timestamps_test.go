// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"testing"
)

func TestFilesystemExplicitTimestampsAndChmod(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestFS(t)
	file := createTestFile(t, f, "time")
	if _, err := f.WriteAt(ctx, file.ID, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	before, err := f.Stat(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	mode := uint32(0777)
	after, err := f.SetAttributes(ctx, file.ID, FSAttributes{Mode: &mode})
	if err != nil {
		t.Fatal(err)
	}
	if after.MtimeNS != before.MtimeNS || after.CtimeNS < before.CtimeNS {
		t.Fatal("chmod changed content time", before, after)
	}
	atime, mtime := uint64(0), uint64(1700000000000000000)
	if _, err = f.SetAttributes(ctx, file.ID, FSAttributes{AtimeNS: &atime, MtimeNS: &mtime}); err != nil {
		t.Fatal(err)
	}
	after, err = f.Stat(ctx, file.ID)
	if err != nil || after.AtimeNS != 0 || after.MtimeNS != mtime {
		t.Fatal(after, err)
	}
	buf := make([]byte, 4)
	if _, err = f.ReadAt(ctx, file.ID, buf, 0); err != nil {
		t.Fatal(err)
	}
	read, err := f.Stat(ctx, file.ID)
	if err != nil || read != after || read.AtimeNS != 0 {
		t.Fatal("read persisted an atime update", read, err)
	}
}
