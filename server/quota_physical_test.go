// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func createQuotaBucket(t *testing.T, s *Server, name, accounting string, g, a uint64) *Bucket {
	t.Helper()
	draft := newTestBucket(t, name)
	draft.description.Policy.Limits.Accounting = accounting
	draft.description.Policy.Limits.Guaranteed = g
	draft.description.Policy.Limits.Unlimited = a == 0
	draft.description.Policy.Limits.Absolute = a
	_, status, reply := adminBucketMessage(t, s, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatalf("quota create %s: %d %v", name, status, reply)
	}
	b, err := s.Bucket(draft.ID())
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func quotaPrincipal(t *testing.T, s *Server) principal {
	t.Helper()
	u, err := s.findAccount(s.cfg.Auth.BootstrapAdmin.Username)
	if err != nil {
		t.Fatal(err)
	}
	return principal{User: u}
}
func TestDaemonGuaranteedQuotaProtectsCapacityAndSurvivesRestart(t *testing.T) {
	n, peer := startAutomaticDaemonPair(t, false)
	_ = peer
	addTestVolume(t, n.node, "quota", "hdd", 4<<20)
	b := createQuotaBucket(t, n.node, "protected", "physical", 2<<20, 0)
	protected := uint64(0)
	for _, v := range n.node.quotaVolumes() {
		protected += v.Stats().GuaranteedRemainingPhysical
	}
	if protected != 2<<20 {
		t.Fatal("unbacked guarantee", protected)
	}
	foreign := createQuotaBucket(t, n.node, "other", "logical", 0, 0)
	ctx := context.Background()
	p := quotaPrincipal(t, n.node)
	successes := 0
	for i := 0; i < 8; i++ {
		_, err := foreign.Put(ctx, p, volume.ObjectID{byte(i)}, bytes.Repeat([]byte{1}, 512<<10), volume.Options{RequestID: volume.ID(client.NewRequestID())})
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, volume.ErrFull) && statusOf(err) != 503 {
			t.Fatal(err)
		}
		break
	}
	if successes > 3 {
		t.Fatal("foreign bucket consumed guarantee", successes)
	}
	if _, err := b.Put(ctx, p, volume.ObjectID{1}, bytes.Repeat([]byte{2}, 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("guaranteed write denied", err)
	}
	before := n.node.currentSystem.Load().manifest.Sequence
	impossible := newTestBucket(t, "impossible")
	impossible.description.Policy.Limits.Guaranteed = 8 << 20
	_, status, _ := adminBucketMessage(t, n.node, client.BucketCreate, bucketPolicyBody(t, impossible))
	if status != 507 {
		t.Fatal("overcommitted guarantee", status)
	}
	if n.node.currentSystem.Load().manifest.Sequence != before {
		t.Fatal("rejected guarantee published")
	}
	n = restartDaemonTestNode(t, n)
	b, _ = n.node.Bucket(b.ID())
	p = quotaPrincipal(t, n.node)
	if _, err := b.Put(ctx, p, volume.ObjectID{2}, bytes.Repeat([]byte{3}, 512<<10), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal("promise lost after restart", err)
	}
}
func TestDaemonPhysicalQuotaIncludesSparseMetadataAndRetries(t *testing.T) {
	n, _ := startAutomaticDaemonPair(t, false)
	addTestVolume(t, n.node, "physical", "hdd", 8<<20)
	b := createQuotaBucket(t, n.node, "physical", "physical", 0, 128<<10)
	p := quotaPrincipal(t, n.node)
	ctx := context.Background()
	id := volume.ObjectID{1}
	req := volume.ID(client.NewRequestID())
	data := bytes.Repeat([]byte{1}, 64<<10)
	var first volume.Info
	err := retryQuotaCall(func() error {
		var e error
		first, e = b.Put(ctx, p, id, data, volume.Options{RequestID: req})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	used, err := b.physicalUsage()
	if err != nil || used <= 64<<10 {
		t.Fatal("missing metadata", used, err)
	}
	if again, err := b.Put(ctx, p, id, data, volume.Options{RequestID: req}); err != nil || again != first {
		t.Fatal("retry", again, err)
	}
	retry, _ := b.physicalUsage()
	if retry != used {
		t.Fatal("duplicate physical debit")
	}
	// Sparse lengths count no unallocated payload against a physical quota.
	truncRequest := volume.ID(client.NewRequestID())
	if err := retryQuotaCall(func() error {
		_, err := b.Trunc(ctx, p, volume.ObjectID{2}, 1<<40, volume.Options{RequestID: truncRequest})
		return err
	}); err != nil {
		t.Fatal("sparse logical length charged physically", err)
	}
	// The total exceeds this node's entire bucket allocation; no peer can lend
	// additional capacity because the global bucket quota itself is exhausted.
	opts := volume.Options{RequestID: volume.ID(client.NewRequestID())}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = b.Put(ctx, p, volume.ObjectID{3}, bytes.Repeat([]byte{2}, 96<<10), opts)
		if statusOf(err) == 503 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		break
	}
	if !errors.Is(err, volume.ErrFull) {
		t.Fatal("physical cap", err)
	}
	if _, err = b.Stat(ctx, p, volume.ObjectID{3}); !errors.Is(err, volume.ErrNotFound) {
		t.Fatal("partial quota failure", err)
	}
	n = restartDaemonTestNode(t, n)
	b, _ = n.node.Bucket(b.ID())
	usedAfter, err := b.physicalUsage()
	if err != nil || usedAfter > 128<<10 || usedAfter <= 64<<10 {
		t.Fatal("physical quota restart", usedAfter, err)
	}
}

func retryQuotaCall(call func() error) error {
	until := time.Now().Add(10 * time.Second)
	for {
		err := call()
		if statusOf(err) != 503 || time.Now().After(until) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDaemonPhysicalQuotaHandoffPreservesCreditsAndRetries(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	targetVolume := addTestVolume(t, target.node, "target", "hdd", 4<<20)
	b := createQuotaBucket(t, source.node, "physical-handoff", "physical", 0, 600<<10)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	request := client.NewRequestID()
	object := client.ObjectID{1}
	payload := bytes.Repeat([]byte{9}, 512<<10)
	first, err := c.Put(ctx, object, payload, client.PutOptions{RequestID: request})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID(object))
	vid, _ := parseID(targetVolume.VolumeID)
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal("handoff", err)
	}
	check := func() {
		t.Helper()
		var credits, used uint64
		for _, n := range []*daemonTestNode{source, target} {
			n.node.opMu.Lock()
			b, _ := n.node.Bucket(b.ID())
			u, e := b.physicalUsage()
			a, ae := n.node.quotaAllowance(client.ID(b.ID()))
			n.node.opMu.Unlock()
			if e != nil || ae != nil || u > a {
				t.Fatal("physical allowance invariant", n.node.cfg.Node.ID, "source", source.node.cfg.Node.ID, "used/credits", u, a, "allocation", findQuota(n.node.currentSystem.Load(), client.ID(b.ID())), "range image", n.node.quotaRanges.Load(), e, ae)
			}
			credits += a
			used += u
		}
		if credits != 600<<10 || used < 512<<10 || used > credits {
			t.Fatal("global physical quota", used, credits)
		}
	}
	check()
	again, err := c.Put(ctx, object, payload, client.PutOptions{RequestID: request})
	if err != nil || again != first {
		t.Fatal("receipt transfer", again, err)
	}
	target = restartDaemonTestNode(t, target)
	source = restartDaemonTestNode(t, source)
	check()
	got, _, err := c.Get(ctx, object)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("migrated payload", err)
	}
}

func TestDaemonGuaranteedImportCanUseOwnReserve(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	targetVolume := addTestVolume(t, target.node, "target", "hdd", 4<<20)
	// Restrict the promise to the future destination by making the source drain
	// before the global quota edit. Data is still readable on its current owner.
	b := createQuotaBucket(t, source.node, "guaranteed-import", "logical", 0, 0)
	ctx := context.Background()
	p := quotaPrincipal(t, source.node)
	if _, err := b.Put(ctx, p, volume.ObjectID{1}, bytes.Repeat([]byte{3}, 1<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	source.node.opMu.Lock()
	source.node.mu.Lock()
	for _, m := range source.node.volumes {
		if !m.config.System {
			m.config.State = "draining"

		}
	}
	source.node.mu.Unlock()
	source.node.opMu.Unlock()
	// Native quota edit: accounting:u8,G:u64,unlimited:bool,A:u64,overhead:u32,version:u64.
	e := client.Encoder{}
	e.ID(client.ID(b.ID()))
	e.U8(0)
	e.U64(3 << 20)
	e.Bool(true)
	e.U64(0)
	e.U32(0)
	e.U64(b.Describe().Policy.Version)
	_, status, reply := adminBucketMessage(t, source.node, client.BucketSetQuota, e.B)
	if status != 200 {
		t.Fatal("promise on target", status, reply)
	}
	vid, _ := parseID(targetVolume.VolumeID)
	store := target.node.volumes[vid.String()].store
	if store.Stats().GuaranteedRemainingPhysical != 3<<20 {
		t.Fatal("wrong target reserve", store.Stats())
	}
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{1})
	plan, err := planRangeMove(before, before.Entries[0], target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal("own guaranteed import blocked", err)
	}
	stats := store.Stats()
	if stats.InFlightReservedPhysical != 0 || stats.GuaranteedRemainingPhysical < 1<<20 || stats.GuaranteedRemainingPhysical > 2<<20 {
		t.Fatal("import reserve/guarantee overlap", stats)
	}
}

func TestDaemonPhysicalQuotaParallelOwners(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	v := addTestVolume(t, target.node, "target", "hdd", 4<<20)
	b := createQuotaBucket(t, source.node, "parallel-physical", "physical", 0, 1<<20)
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{})
	_, right, err := client.SplitRange(before.Entries[0])
	if err != nil {
		t.Fatal(err)
	}
	vid, _ := parseID(v.VolumeID)
	plan, err := planRangeMove(before, right, target.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err = source.node.scopes.move(ctx, plan); err != nil {
		t.Fatal(err)
	}
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	parallel := make(chan struct{}, 4)
	var wg sync.WaitGroup
	result := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parallel <- struct{}{}
			defer func() { <-parallel }()
			_, err := c.Put(ctx, client.ObjectID{byte(i%2*128 + 1), byte(i)}, bytes.Repeat([]byte{7}, 64<<10), client.PutOptions{})
			result <- err
		}(i)
	}
	wg.Wait()
	close(result)
	success := 0
	for err := range result {
		if err == nil {
			success++
		} else if !errors.Is(err, client.ErrFull) {
			t.Fatal("parallel mutation", err)
		}
	}
	if success != 15 {
		t.Fatal("physical metadata not counted exactly", success)
	}
	var total uint64
	for _, n := range []*daemonTestNode{source, target} {
		n.node.opMu.Lock()
		local, _ := n.node.Bucket(b.ID())
		used, err := local.physicalUsage()
		allowance, ae := n.node.quotaAllowance(client.ID(b.ID()))
		n.node.opMu.Unlock()
		if err != nil || ae != nil || used > allowance {
			t.Fatal("per-node cap", used, allowance, err, ae)
		}
		total += used
	}
	if total > 1<<20 || total < 15*64<<10 {
		t.Fatal("global physical cap", total)
	}
}

func TestQuotaFlowOverlappingMasksAndFullUint64(t *testing.T) {
	// Flexible bucket must release its first choice for a restricted bucket.
	g := newQuotaFlow(6)
	g.add(0, 1, 10)
	g.add(0, 2, 10)
	g.add(1, 3, ^uint64(0))
	g.add(1, 4, ^uint64(0))
	g.add(2, 3, ^uint64(0))
	g.add(3, 5, 10)
	g.add(4, 5, 10)
	g.run(0, 5)
	for _, e := range g.edges[0] {
		if e.cap != 0 {
			t.Fatal("feasible masked allocation rejected")
		}
	}
	large := newQuotaFlow(3)
	large.add(0, 1, ^uint64(0))
	large.add(1, 2, ^uint64(0))
	large.run(0, 2)
	if large.edges[0][0].cap != 0 {
		t.Fatal("uint64 capacity truncated")
	}
}

func TestDaemonGuaranteesRespectStorageClasses(t *testing.T) {
	for _, tc := range []struct {
		name, physical, strategy string
		want                     int
	}{{"ssd-on-hdd", "hdd", "ssd", 507}, {"hdd-on-ssd", "ssd", "hdd", 201}, {"ram-on-disk", "hdd", "ram", 507}} {
		t.Run(tc.name, func(t *testing.T) {
			n, _ := startAutomaticDaemonPair(t, false)
			addTestVolume(t, n.node, "capacity", tc.physical, 4<<20)
			b := newTestBucket(t, "class-guarantee")
			b.description.Policy.StorageStrategy = tc.strategy
			b.description.Policy.Limits.Guaranteed = 1 << 20
			_, status, _ := adminBucketMessage(t, n.node, client.BucketCreate, bucketPolicyBody(t, b))
			if status != tc.want {
				t.Fatal(status, tc.want)
			}
		})
	}
}

func TestDaemonQuotaStatsReportUnderestimatedGuaranteeWithoutPayloadIO(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "data", "hdd", 4<<20)
	draft := newTestBucket(t, "tiny-guarantee")
	draft.description.Policy.Limits.Guaranteed = 1 << 20
	draft.description.Policy.Limits.MetadataReservePPM = 0
	_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 201 {
		t.Fatal(status)
	}
	b, _ := source.node.Bucket(draft.ID())
	if _, err := b.Put(context.Background(), quotaPrincipal(t, source.node), volume.ObjectID{1}, nil, volume.Options{RequestID: volume.ID(client.NewRequestID())}); err != nil {
		t.Fatal(err)
	}
	v := source.node.quotaVolumes()[0]
	before := v.Stats().IO
	snap := source.node.CollectStats(client.StatsFilter{})
	after := v.Stats().IO
	if before.ReadBytes != after.ReadBytes || before.WriteBytes != after.WriteBytes {
		t.Fatal("stats performed payload/index IO")
	}
	found := false
	for _, s := range snap.Buckets {
		if s.ID != client.ID(b.ID()) {
			continue
		}
		found = true
		if s.ObjectCount != 1 || s.LogicalBytes != 0 || s.AttributedPhysical == 0 || s.GuaranteedReservedPhysical >= 1<<20 {
			t.Fatal("invalid physical telemetry", s)
		}
	}
	if !found {
		t.Fatal("missing bucket telemetry")
	}
	source.node.publishStats()
	target.node.publishStats()
	deadline := time.Now().Add(6 * time.Second)
	for {
		entry := bucketListEntry{BucketDescription: b.Describe()}
		source.node.addQuotaDiagnostics(&entry)
		if entry.GuaranteeEstimateShortfall != nil {
			if *entry.GuaranteeEstimateShortfall == 0 {
				t.Fatal("underestimated metadata guarantee hidden")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pushed guarantee diagnostic not available")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestDaemonGuaranteeBackingLeavesDrainingLowestNode(t *testing.T) {
	a, b := startAutomaticDaemonPair(t, false)
	addTestVolume(t, a.node, "data", "hdd", 4<<20)
	addTestVolume(t, b.node, "data", "hdd", 4<<20)
	bucket := createQuotaBucket(t, a.node, "drain-guarantee", "physical", 3<<20, 0)
	low, other := a, b
	if low.node.cfg.Node.ID > other.node.cfg.Node.ID {
		low, other = other, low
	}
	for _, r := range a.node.currentSystem.Load().manifest.QuotaReserves {
		if r.Bucket == client.ID(bucket.ID()) && r.Node != low.node.control.ledger.node {
			t.Fatal("fixture promise not on lowest node")
		}
	}
	low.node.opMu.Lock()
	low.node.mu.Lock()
	low.node.cfg.Node.Draining = true
	low.node.mu.Unlock()
	low.node.opMu.Unlock()
	low.node.publishStats()
	deadline := time.Now().Add(9 * time.Second)
	for {
		valid := true
		view := other.node.currentSystem.Load()
		for _, r := range view.manifest.QuotaReserves {
			if r.Node == low.node.control.ledger.node {
				valid = false
			}
		}
		if valid {
			var promised uint64
			for _, r := range view.manifest.QuotaReserves {
				if r.Bucket == client.ID(bucket.ID()) {
					promised += r.Backing
				}
			}
			if promised != 3<<20 {
				t.Fatal("drain dropped guarantee", promised)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("explicit draining node retained guarantee backing")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

type rejectQuotaPeerPrepare struct {
	controlParticipants
	owner    client.ID
	prepared atomic.Bool
}

func (p *rejectQuotaPeerPrepare) Prepare(ctx context.Context, node client.ID, proposal controlProposal) (client.Proof, error) {
	if node != p.owner {
		return client.Proof{}, volume.ErrFull
	}
	proof, err := p.controlParticipants.Prepare(ctx, node, proposal)
	if err == nil {
		p.prepared.Store(true)
	}
	return proof, err
}
func TestDaemonQuotaRejectedAfterLocalReadyAbortsAllReservations(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	addTestVolume(t, target.node, "target", "hdd", 4<<20)
	c := source.node.controller
	c.mu.Lock()
	original := c.coordinator.participants
	gate := &rejectQuotaPeerPrepare{controlParticipants: original, owner: source.node.control.ledger.node}
	c.coordinator.participants = gate
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.coordinator.participants = original; c.mu.Unlock() }()
	draft := newTestBucket(t, "aborted-guarantee")
	draft.description.Policy.Limits.Guaranteed = 6 << 20
	before := source.node.currentSystem.Load().root
	_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
	if status != 507 {
		t.Fatal("participant rejection", status)
	}
	if !gate.prepared.Load() {
		t.Fatal("fixture did not reach local durable READY")
	}
	if source.node.currentSystem.Load().root != before {
		t.Fatal("failed promise published")
	}
	for _, n := range []*daemonTestNode{source, target} {
		for _, v := range n.node.quotaVolumes() {
			if v.Stats().GuaranteedRemainingPhysical != 0 {
				t.Fatal("aborted guarantee remains protected")
			}
		}
		n.node.control.ledger.mu.Lock()
		busy := n.node.control.ledger.slot != nil
		n.node.control.ledger.mu.Unlock()
		if busy || n.node.pendingQuotas.Load() != nil {
			t.Fatal("507 left a prepared quota fence")
		}
	}
}

func TestDaemonPhysicalQuotaLocalVolumeHandoffDoesNotChargeShadows(t *testing.T) {
	source, _ := startAutomaticDaemonPair(t, false)
	first := addTestVolume(t, source.node, "first", "hdd", 4<<20)
	b := createQuotaBucket(t, source.node, "local-physical", "physical", 0, 600<<10)
	ctx := context.Background()
	p := quotaPrincipal(t, source.node)
	request := volume.ID(client.NewRequestID())
	payload := bytes.Repeat([]byte{7}, 512<<10)
	if err := retryQuotaCall(func() error {
		_, err := b.Put(ctx, p, volume.ObjectID{1}, payload, volume.Options{RequestID: request})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	second := addTestVolume(t, source.node, "second", "hdd", 4<<20)
	vid, _ := parseID(second.VolumeID)
	before, _ := source.node.routingPatch(b.ID(), volume.ObjectID{1})
	plan, err := planRangeMove(before, before.Entries[0], source.node.control.ledger.node, vid, uint8(volume.HDD), volume.Raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	r := source.node.scopes
	if err = r.reserve(plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.sendSnapshot(ctx, plan, false); err != nil {
		t.Fatal("local bulk copy", err)
	}
	used, err := b.physicalUsage()
	if err != nil || used < 512<<10 || used > 600<<10 {
		t.Fatal("shadow charged twice", used, err)
	}
	source = restartDaemonTestNode(t, source)
	b, _ = source.node.Bucket(b.ID())
	r = source.node.scopes
	p = quotaPrincipal(t, source.node)
	used, err = b.physicalUsage()
	if err != nil || used > 600<<10 {
		t.Fatal("shadow charged after restart", used, err)
	}
	// A new ranged write must stay on the original volume while its local target
	// is uncommitted; otherwise quota and final-snapshot selection would diverge.
	rangeRequest := volume.ID(client.NewRequestID())
	if err = retryQuotaCall(func() error {
		_, err := b.PutRange(ctx, p, volume.ObjectID{1}, 0, []byte{4}, volume.Options{RequestID: rangeRequest})
		return err
	}); err != nil {
		t.Fatal("source mutation during local copy", err)
	}
	originalID, _ := parseID(first.VolumeID)
	h, _ := source.node.volumes[originalID.String()].store.Open(false)
	got, _, err := h.Get(volume.Key{Bucket: b.ID(), Object: volume.ObjectID{1}})
	h.Close()
	if err != nil || got[0] != 4 {
		t.Fatal("write incorrectly chose import shadow", err)
	}
	if err = r.move(ctx, plan); err != nil {
		t.Fatal("local handoff", err)
	}
	used, err = b.physicalUsage()
	if err != nil || used > 600<<10 {
		t.Fatal("local quota after commit", used, err)
	}
	got, _, err = b.Get(ctx, p, volume.ObjectID{1})
	if err != nil || got[0] != 4 {
		t.Fatal("local final snapshot lost mutation", err)
	}
}

func TestDaemonGuaranteedPhysicalQuotaClusterShutdownAndRestart(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	addTestVolume(t, source.node, "source", "hdd", 4<<20)
	addTestVolume(t, target.node, "target", "hdd", 4<<20)
	b := createQuotaBucket(t, source.node, "quota-shutdown", "physical", 2<<20, 3<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin := source.node.cfg.Auth.BootstrapAdmin
	c, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(b.ID()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	payload := bytes.Repeat([]byte{8}, 512<<10)
	object := client.ObjectID{1}
	if _, err = c.Put(ctx, object, payload, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	reserves := map[string]uint64{}
	for _, n := range []*daemonTestNode{source, target} {
		for _, v := range n.node.quotaVolumes() {
			reserves[v.Identity().VolumeID.String()] = v.Stats().GuaranteedRemainingPhysical
		}
	}
	e := client.Encoder{}
	e.ID(source.node.control.ledger.node)
	e.U8(2)
	_, status, _ := adminBucketMessage(t, source.node, client.NodeShutdown, e.B)
	if status != 202 {
		t.Fatal(status)
	}
	for _, n := range []*daemonTestNode{source, target} {
		select {
		case <-n.node.stopRequest:
		case <-ctx.Done():
			t.Fatal("quota cluster did not stop", ctx.Err())
		}
	}
	source.close()
	target.close()
	target = restartDaemonTestNode(t, target)
	source = restartDaemonTestNode(t, source)
	got, _, err := c.Get(ctx, object)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("quota shutdown payload", err)
	}
	for _, n := range []*daemonTestNode{source, target} {
		for _, v := range n.node.quotaVolumes() {
			if v.Stats().GuaranteedRemainingPhysical != reserves[v.Identity().VolumeID.String()] {
				t.Fatal("shutdown changed guarantee backing")
			}
		}
	}
	if _, err = c.Put(ctx, object, []byte("resumed"), client.PutOptions{}); err != nil {
		t.Fatal("quota writes did not resume", err)
	}
}
func TestPhysicalQuotaCapabilitySurvivesLastBucketRemoval(t *testing.T) {
	view := &systemView{manifest: systemManifest{QuotaProtocol: 1}}
	if supportsQuotaPolicies(view, client.CapabilityObjectSubscriptions|client.CapabilityRecoveryFenceClearance) {
		t.Fatal("old peer admitted into physical-credit history")
	}
	if !supportsQuotaPolicies(view, client.CapabilityPhysicalQuotas) {
		t.Fatal("capable peer refused")
	}
}

func TestDaemonRAMGuaranteeProtectsOnlyRAMPool(t *testing.T) {
	source, target := startAutomaticDaemonPair(t, false)
	op, err := source.node.AddVolume(newID(), AddVolumeRequest{Name: "ram", Source: 2, Class: "ram", SizeBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	addTestVolume(t, target.node, "disk", "hdd", 8<<20)
	makeRAM := func(name string, g, a uint64) *Bucket {
		t.Helper()
		draft := newTestBucket(t, name)
		draft.description.Policy.StorageStrategy = "ram"
		draft.description.Policy.Limits.Accounting = "physical"
		draft.description.Policy.Limits.Guaranteed = g
		draft.description.Policy.Limits.Unlimited = a == 0
		draft.description.Policy.Limits.Absolute = a
		_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
		if status != 201 {
			t.Fatal("RAM quota", status)
		}
		b, err := source.node.Bucket(draft.ID())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	protected, foreign := makeRAM("ram-guarantee", 2<<20, 3<<20), makeRAM("ram-other", 0, 0)
	ctx := context.Background()
	p := quotaPrincipal(t, source.node)
	request := volume.ID(client.NewRequestID())
	if err := retryQuotaCall(func() error {
		_, err := protected.Put(ctx, p, volume.ObjectID{1}, bytes.Repeat([]byte{1}, 512<<10), volume.Options{RequestID: request})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Put(ctx, p, volume.ObjectID{2}, bytes.Repeat([]byte{2}, 2<<20), volume.Options{RequestID: volume.ID(client.NewRequestID())}); err == nil {
		t.Fatal("foreign RAM write consumed guarantee")
	}
	vid, _ := parseID(op.VolumeID)
	stats := source.node.volumes[vid.String()].store.Stats()
	if stats.Identity.Class != volume.RAM || stats.GuaranteedRemainingPhysical < 1<<20 {
		t.Fatal("RAM reserve lost", stats)
	}
	for _, v := range target.node.quotaVolumes() {
		if v.Stats().DataPhysical != 0 || v.Stats().GuaranteedRemainingPhysical != 0 {
			t.Fatal("RAM data/guarantee spilled onto disk")
		}
	}
}
