// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestQuotaSplitHeadroomDoesNotDeadlockOnMutualHints(t *testing.T) {
	cluster, low, high := volume.ID{1}, client.ID{2}, client.ID{3}
	bucket, epoch := client.ID{4}, client.ID{5}
	open := func(node client.ID) *quotaLedger {
		t.Helper()
		l, err := openQuotaLedger(filepath.Join(t.TempDir(), "quota"), cluster, volume.ID(node), true)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.journal.close() })
		return l
	}
	a, b := open(low), open(high)
	// Each owner has one unused unit and needs two for its next operation.
	// Reserving both soft hints would refuse every loan despite enough total
	// headroom. Explicit hints make this case independent of thread scheduling.
	qa := quotaManager{ledger: a, demands: []quotaDemand{{bucket: volume.ID(bucket), growth: 7, activation: true, until: time.Now().Add(time.Hour)}}}
	qb := quotaManager{ledger: b, demands: []quotaDemand{{bucket: volume.ID(bucket), growth: 11, activation: true, until: time.Now().Add(time.Hour)}}}
	if n := qa.lendable(volume.ID(bucket), high, 6, 5); n != 0 {
		t.Fatal("higher-priority donor lost its pending headroom", n)
	}
	free := qb.lendable(volume.ID(bucket), low, 10, 9)
	if free != 1 {
		t.Fatal("mutual hints stranded usable credits", free)
	}
	g, err := b.debit(quotaGrant{ID: client.NewRequestID(), Bucket: bucket, Epoch: epoch, Source: high, Target: low, Bytes: 1}, free)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := a.receive(g, high, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.settle(g.ID, proof, low); err != nil {
		t.Fatal(err)
	}
	left, le := a.balance(bucket, epoch, 1, 6)
	right, re := b.balance(bucket, epoch, 1, 10)
	if le != nil || re != nil || left != 7 || right != 9 || left+right != 16 {
		t.Fatal("loan did not preserve authority and permit the next write", left, right, le, re)
	}
	qa.demands[0].exhausted = true
	if n := qa.lendable(volume.ID(bucket), high, 6, 5); n != 1 {
		t.Fatal("an exhausted request still blocks unused credits", n)
	}
}

func TestQuotaTransferCrashWindowsConserveCredits(t *testing.T) {
	for _, phase := range []string{"debit", "receive", "settle"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			cluster, source, target := volume.ID{1}, volume.ID{2}, volume.ID{3}
			open := func(name string, node volume.ID, create bool) *quotaLedger {
				l, err := openQuotaLedger(filepath.Join(dir, name), cluster, node, create)
				if err != nil {
					t.Fatal(err)
				}
				return l
			}
			a, b := open("a", source, true), open("b", target, true)
			defer func() { a.journal.close(); b.journal.close() }()
			grant := quotaGrant{ID: client.ID{4}, Bucket: client.ID{5}, Epoch: client.ID{6}, Source: client.ID(source), Target: client.ID(target), Bytes: 40}
			g, err := a.debit(grant, 70)
			if err != nil {
				t.Fatal(err)
			}
			assertBalances := func(wantA, wantB uint64) {
				t.Helper()
				aa, ae := a.balance(g.Bucket, g.Epoch, 1, 100)
				bb, be := b.balance(g.Bucket, g.Epoch, 1, 0)
				if ae != nil || be != nil || aa != wantA || bb != wantB || aa+bb > 100 {
					t.Fatalf("credits %d/%d, errors %v/%v; want %d/%d", aa, bb, ae, be, wantA, wantB)
				}
			}
			assertBalances(60, 0)
			var receipt client.Proof
			if phase != "debit" {
				receipt, err = b.receive(g, g.Source, g.Epoch)
				if err != nil {
					t.Fatal(err)
				}
				assertBalances(60, 40)
			}
			if phase == "settle" {
				if err := a.settle(g.ID, receipt, g.Target); err != nil {
					t.Fatal(err)
				}
			}
			// Reopen both physical journals at every externally visible crash
			// boundary. Lost replies cause exact retransmission, not refunds.
			if err := errors.Join(a.journal.close(), b.journal.close()); err != nil {
				t.Fatal(err)
			}
			a, b = open("a", source, false), open("b", target, false)
			again, err := a.debit(grant, 0)
			if err != nil || again != g {
				t.Fatalf("repeated debit: %v", err)
			}
			applied, err := b.receive(g, g.Source, g.Epoch)
			if err != nil || receipt != (client.Proof{}) && applied != receipt {
				t.Fatalf("repeated receive: %v", err)
			}
			if err := a.settle(g.ID, applied, g.Target); err != nil {
				t.Fatal(err)
			}
			if err := a.settle(g.ID, applied, g.Target); err != nil {
				t.Fatal("repeated acknowledgement", err)
			}
			assertBalances(60, 40)
			if len(a.pending()) != 0 {
				t.Fatal("settled transfer still blocks a checkpoint")
			}
			// A checkpoint covers both ends. Neither journal may apply the
			// historical transfer again over its newly committed base.
			av, ae := a.balance(g.Bucket, g.Epoch, a.sequence, 60)
			bv, be := b.balance(g.Bucket, g.Epoch, b.sequence, 40)
			if ae != nil || be != nil || av != 60 || bv != 40 {
				t.Fatal("checkpoint counted a transfer twice", av, bv, ae, be)
			}
		})
	}
}

func TestQuotaTransferRejectsReplayMutationAndWrongAuthority(t *testing.T) {
	cluster, source, target := volume.ID{1}, volume.ID{2}, volume.ID{3}
	a, err := openQuotaLedger(filepath.Join(t.TempDir(), "quota"), cluster, source, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.journal.close()
	b, err := openQuotaLedger(filepath.Join(t.TempDir(), "quota"), cluster, target, true)
	if err != nil {
		t.Fatal(err)
	}
	defer b.journal.close()
	g := quotaGrant{ID: client.ID{4}, Bucket: client.ID{5}, Epoch: client.ID{6}, Source: client.ID(source), Target: client.ID(target), Bytes: 41}
	if _, err := a.debit(g, 40); !errors.Is(err, volume.ErrFull) {
		t.Fatal("overspend", err)
	}
	if len(a.pending()) != 0 {
		t.Fatal("failed debit became durable")
	}
	sent, err := a.debit(g, 41)
	if err != nil {
		t.Fatal(err)
	}
	g.Bytes++
	if _, err := a.debit(g, 100); err == nil {
		t.Fatal("request ID changed amount")
	}
	if _, err := b.receive(sent, client.ID{9}, sent.Epoch); err == nil {
		t.Fatal("wrong authenticated source")
	}
	if _, err := b.receive(sent, sent.Source, client.ID{7}); err == nil {
		t.Fatal("obsolete quota epoch")
	}
	receipt, err := b.receive(sent, sent.Source, sent.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	changed := sent
	changed.Bytes++
	if _, err := b.receive(changed, sent.Source, sent.Epoch); err == nil {
		t.Fatal("receive replay changed grant")
	}
	if err := a.settle(sent.ID, receipt, client.ID{8}); err == nil {
		t.Fatal("wrong ACK actor")
	}
	if len(a.pending()) != 1 {
		t.Fatal("unconfirmed debit stopped blocking a checkpoint")
	}
	if _, err := a.balance(sent.Bucket, sent.Epoch, 1, 40); !errors.Is(err, volume.ErrCorrupt) {
		t.Fatal("underflow", err)
	}
	if _, err := b.balance(sent.Bucket, sent.Epoch, 1, math.MaxUint64); !errors.Is(err, volume.ErrRange) {
		t.Fatal("overflow", err)
	}
	if _, err := a.balance(sent.Bucket, sent.Epoch, a.sequence+1, 100); !errors.Is(err, volume.ErrCorrupt) {
		t.Fatal("future checkpoint", err)
	}
	if v, err := a.balance(sent.Bucket, client.ID{7}, 1, 100); err != nil || v != 100 {
		t.Fatal("old epoch affected a new allocation", v, err)
	}
}
