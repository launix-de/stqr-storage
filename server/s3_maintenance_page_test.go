// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func TestS3MaintenanceBoundedPagesAndBucketFairness(t *testing.T) {
	s, b, user := s3TestSetup(t)
	ctx := context.Background()
	p := userPrincipal(t, s)
	native := s3Store{b: b, p: p}
	payload := bytes.Repeat([]byte("N"), 65536)
	for i := 1; i <= 300; i++ {
		var id client.ObjectID
		binary.BigEndian.PutUint64(id[24:], uint64(i))
		if _, e := native.PutFrom(ctx, id, bytes.NewReader(payload), uint64(len(payload)), client.PutOptions{RequestID: client.NewRequestID()}); e != nil {
			t.Fatal(e)
		}
	}
	other, _ := createProtocolBucket(t, s, "maintenance-other")
	uploads := client.NewS3(s3Store{b: other, p: p})
	u, e := uploads.CreateMultipart(ctx, "expired", user.ID, "", "", client.S3PutOptions{})
	if e != nil {
		t.Fatal(e)
	}
	cursor := s3MaintenanceCursor{}
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		if e = s.maintainS3Page(ctx, time.Now().Add(25*time.Hour), &cursor); e != nil {
			t.Fatal(e)
		}
		seen[cursor.last]++
		if cursor.last == b.ID().String() && seen[cursor.last] == 1 && cursor.after[cursor.last] == nil {
			t.Fatal("first step scanned the whole native bucket")
		}
		if i == 1 {
			if len(seen) != 2 {
				t.Fatal("large bucket starved another bucket", seen)
			}
			if _, e = uploads.Multipart(ctx, u.ID); !errors.Is(e, client.ErrS3Upload) {
				t.Fatal("expired upload not serviced in round-robin step", e)
			}
		}
	}
	if seen[b.ID().String()] != 3 || cursor.after[b.ID().String()] != nil {
		t.Fatal("native inventory cursor did not finish three bounded pages", seen, cursor.after[b.ID().String()])
	}
	records, _, e := native.ScanObjects(ctx, nil, 1000)
	if e != nil || len(records) != 300 {
		t.Fatal("housekeeping changed unrelated native objects", len(records), e)
	}
}
