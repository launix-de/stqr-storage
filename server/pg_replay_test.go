package server

import (
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestPGRebuildHistoryOutOfOrder(t *testing.T) {
	parent := rangeWireFixture(t)
	before := parent.After
	before.Begin = parent.Moving.Start
	before.Entries = []client.RangeEntry{parent.After.Entries[1]}
	child, err := planRangeMove(before, before.Entries[0], client.ID{1}, volume.ID{4}, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		name := "parent-first"
		if reverse {
			name = "child-first"
		}
		t.Run(name, func(t *testing.T) {
			s := &Server{}
			s.currentSystem.Store(&systemView{patches: []client.MapPatch{parent.Before}})
			r := &rangeManager{server: s, node: client.ID{9}, txns: map[[32]byte]*rangeTxn{}}
			for _, p := range []rangePlan{parent, child} {
				c := &rangeCommit{ID: p.id(), Digest: [32]byte{1}, Source: client.Proof{Node: p.Source, RecordSequence: 1, Digest: [32]byte{2}}, Target: client.Proof{Node: p.Target, RecordSequence: 2, Digest: [32]byte{3}}}
				txn := &rangeTxn{plan: p, decision: c}
				if !r.validCommit(txn, *c, true) {
					t.Fatal("invalid observed fixture")
				}
				r.txns[p.id()] = txn
			}
			r.order = [][32]byte{parent.id(), child.id()}
			if reverse {
				r.order = [][32]byte{child.id(), parent.id()}
			}
			for i := 0; i < 2; i++ {
				if err := r.rebuildLocked(); err != nil {
					t.Fatal(err)
				}
			}
			got, ok := s.placement.Load().lookup(parent.Before.Bucket, client.ObjectID(parent.Moving.Start))
			if !ok || got.OwnerEpoch != child.After.Entries[0].OwnerEpoch || got.Locations[0].Node != (client.ID{1}) {
				t.Fatalf("valid child commit lost after replay twice: owner=%s epoch=%d; expected owner=%s epoch=%d", got.Locations[0].Node, got.OwnerEpoch, client.ID{1}, child.After.Entries[0].OwnerEpoch)
			}
		})
	}
}

// Legacy tests explicitly inspect all PG plans. Production dispatches by PG.
func (r *rangeManager) planNext() (rangePlan, bool) {
	return r.planNextGroup("*", r.server.bucketSnapshots())
}
