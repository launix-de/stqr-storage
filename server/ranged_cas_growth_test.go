// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestObjectProtocolRangedWritesGrowWithZeroGap(t *testing.T) {
	_, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{23}
	if _, err := writer.Put(ctx, id, []byte("base"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if info, err := writer.PutRange(ctx, id, 10, []byte("end"), client.PutOptions{Accepted: true}); err != nil || info.Length != 13 {
		t.Fatal(info, err)
	}
	opt := client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true, RequestID: client.NewRequestID()}}
	info, err := writer.CAS(ctx, id, 11, []byte{'n', 'd', 0, 0}, []byte("ABCD"), opt)
	if err != nil || info.Length != 15 {
		t.Fatal(info, err)
	}
	// Native reference-aware sessions deliberately retain no ordinary CAS
	// history: repeating the stale expected bytes must fail comparison.
	if _, err = writer.CAS(ctx, id, 11, []byte{'n', 'd', 0, 0}, []byte("ABCD"), opt); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("stale comparison succeeded", err)
	}
	want := make([]byte, 15)
	copy(want, "base")
	want[10] = 'e'
	copy(want[11:], "ABCD")
	data, got, err := reader.Get(ctx, id)
	if err != nil || got.Length != 15 || !bytes.Equal(data, want) {
		t.Fatal(data, got, err)
	}
	part, _, err := reader.GetRange(ctx, id, 2, 1<<20)
	if err != nil || len(part) != len(want)-2 || !bytes.Equal(part, want[2:]) {
		t.Fatal("GET_RANGE must clip to readable bytes", len(part), err)
	}
	bad := client.ObjectID{24}
	if _, err = writer.CAS(ctx, bad, 9, []byte{1}, []byte{'A'}, client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}}); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("nonzero missing compare", err)
	}
	if _, _, err = reader.Get(ctx, bad); !errors.Is(err, client.ErrNotFound) {
		t.Fatal("failed CAS created object", err)
	}
	info, err = writer.CAS(ctx, bad, 9, make([]byte, 3), []byte("new"), client.CASOptions{MutationOptions: client.MutationOptions{Accepted: true}})
	if err != nil || info.Length != 12 {
		t.Fatal(info, err)
	}
	want = make([]byte, 12)
	copy(want[9:], "new")
	data, _, err = reader.Get(ctx, bad)
	if err != nil || !bytes.Equal(data, want) {
		t.Fatal(data, err)
	}
	if err = writer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRangeCASGrowthHonorsLogicalQuota(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		t.Run(map[bool]string{false: "parallel", true: "exclusive"}[exclusive], func(t *testing.T) {
			s, b, _, _, _ := parallelAdmissionFixture(t, true, "logical", 128)
			p := quotaPrincipal(t, s)
			ctx := context.Background()
			if exclusive {
				ctx = context.WithValue(ctx, exclusiveMutationKey{}, true)
			}
			id := volume.ObjectID{2}
			if _, err := b.Put(ctx, p, id, []byte("head"), parallelOpts()); err != nil {
				t.Fatal(err)
			}
			before, err := b.logicalUsage()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.CAS(ctx, p, id, 20, []byte{1}, []byte{'x'}, parallelOpts()); !errors.Is(err, volume.ErrPrecondition) {
				t.Fatal("compare failure", err)
			}
			if !exclusive {
				assertParallelReservationsEmpty(t, s, b)
			}
			if used, err := b.logicalUsage(); err != nil || used != before {
				t.Fatal("mismatch debited quota", used, before, err)
			}
			info, err := b.CAS(ctx, p, id, 100, make([]byte, 4), []byte("tail"), parallelOpts())
			if err != nil || info.Length != 104 {
				t.Fatal(info, err)
			}
			if used, err := b.logicalUsage(); err != nil || used != before+100 {
				t.Fatal("growth not accounted", used, before, err)
			}
			if _, err = b.CAS(ctx, p, id, 128, []byte{0}, []byte{'x'}, parallelOpts()); statusOf(err) != 507 {
				t.Fatal("quota bypass", err)
			}
			if !exclusive {
				assertParallelReservationsEmpty(t, s, b)
			}
			want := make([]byte, 104)
			copy(want, "head")
			copy(want[100:], "tail")
			data, got, err := b.Get(ctx, p, id)
			if err != nil || got.Length != 104 || !bytes.Equal(data, want) {
				t.Fatal("quota failure changed data", got, err)
			}
		})
	}
}
func TestRangeCASNextMutationLength(t *testing.T) {
	for _, op := range []uint16{0x0103, 0x0108} {
		got, err := nextMutationLength(op, 100, 3, volume.Info{Length: 4}, volume.Options{})
		if err != nil || got != 103 {
			t.Fatal(op, got, err)
		}
		if _, err = nextMutationLength(op, math.MaxUint64, 1, volume.Info{}, volume.Options{}); !errors.Is(err, volume.ErrRange) {
			t.Fatal("overflow", op, err)
		}
	}
	got, err := nextMutationLength(0x0108, 0, 3, volume.Info{Length: 100}, volume.Options{WholeObject: true})
	if err != nil || got != 3 {
		t.Fatal("whole CAS length", got, err)
	}
}

func TestRangeCASGrowthPreservesCOWSource(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "cas-cow-data", "hdd", 16<<20)
	source, _ := createProtocolBucket(t, s, "cas-cow-source")
	clone, _ := createProtocolBucket(t, s, "cas-cow-copy")
	ctx := context.Background()
	p := userPrincipal(t, s)
	id := volume.ObjectID{31}
	if _, err := source.Put(ctx, p, id, []byte("head"), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	e := client.Encoder{}
	e.ID(client.ID(source.ID()))
	e.ID(client.ID(clone.ID()))
	if _, err := s.executeBucketCopy(ctx, client.Message{Kind: client.Request, Opcode: client.BucketClone, ID: client.NewRequestID(), Body: e.B}, p); err != nil {
		t.Fatal(err)
	}
	if info, err := clone.CAS(ctx, p, id, 10, make([]byte, 4), []byte("tail"), parallelOpts()); err != nil || info.Length != 14 {
		t.Fatal(info, err)
	}
	original, _, err := source.Get(ctx, p, id)
	if err != nil || string(original) != "head" {
		t.Fatal("clone CAS changed source", string(original), err)
	}
	want := make([]byte, 14)
	copy(want, "head")
	copy(want[10:], "tail")
	got, _, err := clone.Get(ctx, p, id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal(got, err)
	}
}
