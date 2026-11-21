// SPDX-License-Identifier: GPL-3.0-only
package server

// This is a measurement baseline, NOT a coordinator election algorithm. It
// models only offers, lowest-known-candidate preference and pushed statistics.
// Completing all fragments records a potential binding, never actual authority.
// Multiple potential bindings are deliberately observable instead of hidden by
// an invented grant, timeout, ABORT or failover protocol.
import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

const (
	probeOffer   uint16 = client.RebuildOfferMessage
	probeStats   uint16 = client.StatsFragmentMessage
	probeTrigger uint16 = 0xffff // local event, not a wire opcode
)

type rebuildProbeCase struct {
	Nodes, Candidates                                 int
	TriggerWindow, OfferDelay, StatsDelay, Duplicates int
	PausedNode, ResumeAt                              int
}
type rebuildProbeResult struct {
	Seed                             int64  `json:"seed"`
	Nodes                            int    `json:"nodes"`
	Candidates                       int    `json:"candidates"`
	PotentialBindings                int    `json:"potential_bindings"`
	LowestCandidateCompletes         bool   `json:"lowest_candidate_completes"`
	FirstCompleteTick                int    `json:"first_complete_tick"`
	LastTick                         int    `json:"last_delivery_tick"`
	PreferenceChanges                int    `json:"preference_changes"`
	PreferenceChangesAfterCompletion int    `json:"preference_changes_after_completion"`
	Messages                         int    `json:"messages"`
	Bytes                            uint64 `json:"wire_bytes"`
	DuplicateFragments               int    `json:"duplicate_fragments"`
}
type rebuildProbeEvent struct {
	at, sequence, from, to int
	opcode                 uint16
	body                   []byte
}
type rebuildProbeQueue []rebuildProbeEvent

func (q rebuildProbeQueue) Len() int { return len(q) }
func (q rebuildProbeQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].sequence < q[j].sequence
}
func (q rebuildProbeQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *rebuildProbeQueue) Push(x any)   { *q = append(*q, x.(rebuildProbeEvent)) }
func (q *rebuildProbeQueue) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}

type rebuildProbeParts struct {
	digest   [32]byte
	pieces   [][]byte
	received int
	complete bool
}
type rebuildProbeNode struct {
	preferred         int
	started, complete bool
	stats             map[int]*rebuildProbeParts
}
type rebuildProbe struct {
	config        rebuildProbeCase
	rng           *rand.Rand
	queue         rebuildProbeQueue
	nodes         []rebuildProbeNode
	snapshots     [][]byte
	result        rebuildProbeResult
	now, sequence int
}

func probeID(node int) client.ID {
	var id client.ID
	id[14] = byte((node + 1) >> 8)
	id[15] = byte(node + 1)
	return id
}
func probeNode(id client.ID) int { return (int(id[14])<<8 | int(id[15])) - 1 }
func probeRound(candidate int) client.RoundID {
	return client.RoundID{Membership: 1, Generation: 2, Initiator: probeID(candidate), Attempt: 1}
}
func newRebuildProbe(c rebuildProbeCase, seed int64) *rebuildProbe {
	p := &rebuildProbe{config: c, rng: rand.New(rand.NewSource(seed)), nodes: make([]rebuildProbeNode, c.Nodes), snapshots: make([][]byte, c.Nodes), result: rebuildProbeResult{Seed: seed, Nodes: c.Nodes, Candidates: c.Candidates, FirstCompleteTick: -1}}
	for node := range p.nodes {
		p.nodes[node] = rebuildProbeNode{preferred: -1, stats: map[int]*rebuildProbeParts{}}
		snapshot := client.StatsSnapshot{Schema: 1, Node: probeID(node), BootID: probeID(node), Sequence: 1, MembershipEpoch: 1, Generation: 1, MapRevision: 1, Volumes: []client.VolumeStats{{ID: probeID(node), CapacityPhysical: 4 << 20, AdmissionFreePhysical: 3 << 20}}}
		p.snapshots[node] = client.EncodeStats(snapshot)
	}
	return p
}
func (p *rebuildProbe) schedule(event rebuildProbeEvent) {
	p.sequence++
	event.sequence = p.sequence
	heap.Push(&p.queue, event)
}
func (p *rebuildProbe) send(from, to int, opcode uint16, body []byte) {
	delay := p.config.StatsDelay
	if opcode == probeOffer {
		delay = p.config.OfferDelay
	}
	if delay > 0 {
		delay = 1 + p.rng.Intn(delay)
	}
	at := p.now + delay
	if (from == p.config.PausedNode || to == p.config.PausedNode) && at < p.config.ResumeAt {
		at = p.config.ResumeAt + delay
	}
	p.sequence++
	id := probeID(p.sequence)
	wire := client.EncodeMessage(client.Message{Kind: client.Push, Opcode: opcode, ID: id, Body: body})
	p.schedule(rebuildProbeEvent{at: at, from: from, to: to, opcode: opcode, body: wire})
	p.result.Messages++
	p.result.Bytes += uint64(len(wire))
	if p.config.Duplicates > 0 && p.rng.Intn(100) < p.config.Duplicates {
		p.schedule(rebuildProbeEvent{at: at + 1 + p.rng.Intn(5), from: from, to: to, opcode: opcode, body: wire})
		p.result.Messages++
		p.result.Bytes += uint64(len(wire))
	}
}
func (p *rebuildProbe) prefer(node, candidate int) {
	n := &p.nodes[node]
	if n.preferred >= 0 && n.preferred <= candidate {
		return
	}
	if n.preferred >= 0 {
		p.result.PreferenceChanges++
		if n.complete {
			p.result.PreferenceChangesAfterCompletion++
		}
	}
	n.preferred = candidate
	data := p.snapshots[node]
	digest := sha256.Sum256(data)
	// Small fragments intentionally exercise reassembly, duplicates and reordering.
	// 128 is below the protocol's 1-MiB ceiling; it is not a proposed new limit.
	const fragmentSize = 128
	count := (len(data) + fragmentSize - 1) / fragmentSize
	for i := 0; i < count; i++ {
		fragment := client.StatsFragment{Round: probeRound(candidate), Part: client.StatsPart{Source: probeID(node), Boot: probeID(node), Sequence: 1, Index: uint32(i), Count: uint32(count), Digest: digest, Data: data[i*fragmentSize : min((i+1)*fragmentSize, len(data))]}}
		body, err := fragment.MarshalBinary()
		if err != nil {
			panic(err)
		}
		p.send(node, candidate, probeStats, body)
	}
}
func (p *rebuildProbe) trigger(node int) {
	p.nodes[node].started = true
	p.prefer(node, node)
	body, err := (client.RebuildOffer{Round: probeRound(node), Candidate: probeID(node), Reasons: 1}).MarshalBinary()
	if err != nil {
		panic(err)
	}
	for to := range p.nodes {
		if to != node {
			p.send(node, to, probeOffer, body)
		}
	}
}
func (p *rebuildProbe) receive(event rebuildProbeEvent) error {
	if event.opcode == probeTrigger {
		p.trigger(event.to)
		return nil
	}
	m, err := client.DecodeMessage(event.body)
	if err != nil {
		return err
	}
	if m.Kind != client.Push || m.Opcode != event.opcode {
		return fmt.Errorf("bad probe envelope")
	}
	var candidate int
	if m.Opcode == probeOffer {
		offer, err := client.DecodeRebuildOffer(m.Body)
		if err != nil {
			return err
		}
		candidate = probeNode(offer.Candidate)
		if candidate != event.from || candidate < 0 || candidate >= len(p.nodes) || offer.Round != probeRound(candidate) {
			return fmt.Errorf("bad offer identity/round")
		}
		p.prefer(event.to, candidate)
		return nil
	}
	fragment, err := client.DecodeStatsFragment(m.Body)
	if err != nil {
		return err
	}
	partInfo := fragment.Part
	candidate = probeNode(fragment.Round.Initiator)
	source, boot := probeNode(partInfo.Source), probeNode(partInfo.Boot)
	part, count := int(partInfo.Index), int(partInfo.Count)
	digest, data := partInfo.Digest, partInfo.Data
	if candidate != event.to || source != event.from || boot != source || partInfo.Sequence != 1 || fragment.Round != probeRound(candidate) {
		return fmt.Errorf("invalid probe fragment identity/round")
	}
	n := &p.nodes[candidate]
	assembly := n.stats[source]
	if assembly == nil {
		assembly = &rebuildProbeParts{digest: digest, pieces: make([][]byte, count)}
		n.stats[source] = assembly
	}
	if assembly.digest != digest || len(assembly.pieces) != count {
		return fmt.Errorf("conflicting probe fragments")
	}
	if assembly.pieces[part] != nil {
		p.result.DuplicateFragments++
		return nil
	}
	assembly.pieces[part] = data
	assembly.received++
	if assembly.received == count {
		joined := bytes.Join(assembly.pieces, nil)
		if sha256.Sum256(joined) != digest || !bytes.Equal(joined, p.snapshots[source]) {
			return fmt.Errorf("corrupt assembled snapshot")
		}
		assembly.complete = true
	}
	if !n.complete && len(n.stats) == len(p.nodes) {
		all := true
		for _, a := range n.stats {
			all = all && a.complete
		}
		if all {
			n.complete = true
			p.result.PotentialBindings++
			if p.result.FirstCompleteTick < 0 {
				p.result.FirstCompleteTick = p.now
			}
		}
	}
	return nil
}
func (p *rebuildProbe) run() (rebuildProbeResult, error) {
	for len(p.queue) > 0 {
		event := heap.Pop(&p.queue).(rebuildProbeEvent)
		p.now = event.at
		if err := p.receive(event); err != nil {
			return p.result, err
		}
	}
	p.result.LastTick = p.now
	lowest := len(p.nodes)
	for i, n := range p.nodes {
		if n.started && i < lowest {
			lowest = i
		}
	}
	p.result.LowestCandidateCompletes = lowest < len(p.nodes) && p.nodes[lowest].complete
	return p.result, nil
}
func runRebuildProbe(c rebuildProbeCase, seed int64) (rebuildProbeResult, error) {
	p := newRebuildProbe(c, seed)
	for _, node := range p.rng.Perm(c.Nodes)[:c.Candidates] {
		at := 0
		if c.TriggerWindow > 0 {
			at = p.rng.Intn(c.TriggerWindow + 1)
		}
		p.schedule(rebuildProbeEvent{at: at, to: node, opcode: probeTrigger})
	}
	return p.run()
}
func TestRebuildOfferStressBaseline(t *testing.T) {
	scenarios := []struct {
		name   string
		config rebuildProbeCase
	}{
		{"single_candidate", rebuildProbeCase{Nodes: 5, Candidates: 1, OfferDelay: 20, StatsDelay: 20, PausedNode: -1}},
		{"three_nodes", rebuildProbeCase{Nodes: 3, Candidates: 3, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 25, PausedNode: -1}},
		{"simultaneous", rebuildProbeCase{Nodes: 5, Candidates: 5, OfferDelay: 20, StatsDelay: 5, PausedNode: -1}},
		{"crossing_offers", rebuildProbeCase{Nodes: 5, Candidates: 5, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, PausedNode: -1}},
		{"duplicates", rebuildProbeCase{Nodes: 9, Candidates: 5, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 50, PausedNode: -1}},
		{"thirty_two_nodes", rebuildProbeCase{Nodes: 32, Candidates: 16, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 25, PausedNode: -1}},
		{"pause_and_resume", rebuildProbeCase{Nodes: 9, Candidates: 5, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 25, PausedNode: 0, ResumeAt: 100}},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			summary := struct {
				Scenario                                                                    string  `json:"scenario"`
				ConflictSeeds                                                               []int64 `json:"conflict_seeds,omitempty"`
				Runs, MultiplePotentialBindings, PreferenceChangesAfterCompletion, Messages int
				Bytes                                                                       uint64
				MaxLastTick                                                                 int
			}{Scenario: scenario.name}
			for seed := int64(1); seed <= 200; seed++ {
				r, err := runRebuildProbe(scenario.config, seed)
				if err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				if !r.LowestCandidateCompletes {
					t.Fatalf("seed %d: lowest known candidate never collected all snapshots: %+v", seed, r)
				}
				if scenario.config.Candidates == 1 && r.PotentialBindings != 1 {
					t.Fatal(r)
				}
				summary.Runs++
				if r.PotentialBindings > 1 {
					summary.MultiplePotentialBindings++
					summary.ConflictSeeds = append(summary.ConflictSeeds, seed)
				}
				summary.PreferenceChangesAfterCompletion += r.PreferenceChangesAfterCompletion
				summary.Messages += r.Messages
				summary.Bytes += r.Bytes
				summary.MaxLastTick = max(summary.MaxLastTick, r.LastTick)
			}
			data, _ := json.Marshal(summary)
			t.Log(string(data))
		})
	}
}
func TestRebuildProbeDetectsCompetingPotentialBindings(t *testing.T) {
	p := newRebuildProbe(rebuildProbeCase{Nodes: 2, Candidates: 2, OfferDelay: 1, StatsDelay: 1, PausedNode: -1}, 7)
	p.schedule(rebuildProbeEvent{at: 0, to: 1, opcode: probeTrigger})
	p.schedule(rebuildProbeEvent{at: 5, to: 0, opcode: probeTrigger})
	r, err := p.run()
	if err != nil {
		t.Fatal(err)
	}
	if r.PotentialBindings != 2 || r.PreferenceChangesAfterCompletion == 0 {
		t.Fatalf("baseline hid its unsafe binding ambiguity: %+v", r)
	}
}
func TestRebuildProbeReproducible(t *testing.T) {
	c := rebuildProbeCase{Nodes: 9, Candidates: 5, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 50, PausedNode: 0, ResumeAt: 100}
	a, e := runRebuildProbe(c, 73)
	if e != nil {
		t.Fatal(e)
	}
	b, e := runRebuildProbe(c, 73)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("seed not reproducible: %+v %+v %v", a, b, e)
	}
}
func BenchmarkRebuildOfferBaseline(b *testing.B) {
	c := rebuildProbeCase{Nodes: 9, Candidates: 5, TriggerWindow: 10, OfferDelay: 20, StatsDelay: 5, Duplicates: 25, PausedNode: -1}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, e := runRebuildProbe(c, int64(i+1)); e != nil {
			b.Fatal(e)
		}
	}
}
