// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"math/big"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const (
	quotaGenesis uint16 = 1 + iota
	quotaDebited
	quotaReceived
	quotaSettled
)

// A credit is permission to grow a bucket, not an observation of free space.
// Epoch identifies one committed quota allocation; changing a limit must not
// resurrect transfers from an earlier allocation, even if the limit repeats.
type quotaGrant struct {
	ID, Bucket, Epoch, Source, Target client.ID
	Bytes                             uint64
	Debit                             client.Proof
}

type quotaRecord struct {
	Grant   quotaGrant
	Receipt client.Proof
}

type quotaTransfer struct {
	grant   quotaGrant
	applied client.Proof
	settled bool
}

// The donor flushes DEBIT before sending a grant. The recipient flushes RECEIVE
// before acknowledging it. A lost acknowledgement only strands credits until
// retransmission; it never makes the same credits spendable on both nodes.
// There is deliberately no timeout, refund, or cancellation after DEBIT.
// Ownership and physical-capacity reservations are handled independently.
type quotaLedger struct {
	mu          sync.Mutex
	node        client.ID
	journal     *controlJournal
	sequence    uint64
	initialized bool
	outgoing    map[client.ID]*quotaTransfer
	incoming    map[client.ID]*quotaTransfer
}

func openQuotaLedger(path string, cluster, node volume.ID, create bool) (*quotaLedger, error) {
	l := &quotaLedger{node: client.ID(node), outgoing: make(map[client.ID]*quotaTransfer), incoming: make(map[client.ID]*quotaTransfer)}
	j, err := openControlJournal(path, cluster, node, create, l.replay)
	if err != nil {
		return nil, err
	}
	l.journal = j
	if !l.initialized {
		if _, err := l.persist(quotaGenesis, quotaRecord{}); err != nil {
			j.close()
			return nil, err
		}
	}
	return l, nil
}

func validQuotaGrant(g quotaGrant) bool {
	return g.ID != (client.ID{}) && g.Bucket != (client.ID{}) && g.Epoch != (client.ID{}) && g.Source != (client.ID{}) && g.Target != (client.ID{}) && g.Source != g.Target && g.Bytes != 0
}

func (l *quotaLedger) persist(kind uint16, event quotaRecord) (client.Proof, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return client.Proof{}, err
	}
	proof, err := l.journal.append(kind, data)
	if err != nil {
		return client.Proof{}, err
	}
	if err := l.replay(controlRecord{Kind: kind, Proof: proof, Data: data}); err != nil {
		return client.Proof{}, err
	}
	return proof, nil
}

func (l *quotaLedger) replay(record controlRecord) error {
	var event quotaRecord
	if err := decodeControlRecord(record.Data, &event); err != nil || record.Proof.Node != l.node || record.Proof.RecordSequence != l.sequence+1 {
		return volume.ErrCorrupt
	}
	if record.Kind == quotaGenesis {
		if l.initialized || record.Proof.RecordSequence != 1 || event != (quotaRecord{}) {
			return volume.ErrCorrupt
		}
		l.initialized = true
	} else {
		if !l.initialized || !validQuotaGrant(event.Grant) {
			return volume.ErrCorrupt
		}
		g := event.Grant
		switch record.Kind {
		case quotaDebited:
			if g.Source != l.node || g.Debit != (client.Proof{}) || event.Receipt != (client.Proof{}) || l.outgoing[g.ID] != nil || l.incoming[g.ID] != nil {
				return volume.ErrCorrupt
			}
			g.Debit = record.Proof
			l.outgoing[g.ID] = &quotaTransfer{grant: g}
		case quotaReceived:
			if g.Target != l.node || g.Debit.Node != g.Source || g.Debit.RecordSequence == 0 || g.Debit.Digest == ([32]byte{}) || event.Receipt != (client.Proof{}) || l.incoming[g.ID] != nil || l.outgoing[g.ID] != nil {
				return volume.ErrCorrupt
			}
			l.incoming[g.ID] = &quotaTransfer{grant: g, applied: record.Proof}
		case quotaSettled:
			t := l.outgoing[g.ID]
			if t == nil || t.grant != g || t.settled || event.Receipt.Node != g.Target || event.Receipt.RecordSequence == 0 || event.Receipt.Digest == ([32]byte{}) {
				return volume.ErrCorrupt
			}
			t.settled, t.applied = true, event.Receipt
		default:
			return volume.ErrCorrupt
		}
	}
	l.sequence = record.Proof.RecordSequence
	return nil
}

// balance applies only events after the node's committed checkpoint. The caller
// adds ownership-handoff deltas separately and serializes admission with opMu.
// Returning an error on underflow is essential: saturating arithmetic would
// disguise a corrupt checkpoint and could authorize excess bucket growth.
func (l *quotaLedger) balance(bucket, epoch client.ID, checkpoint, base uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balanceLocked(bucket, epoch, checkpoint, base)
}

func (l *quotaLedger) balanceLocked(bucket, epoch client.ID, checkpoint, base uint64) (uint64, error) {
	if checkpoint > l.sequence {
		return 0, volume.ErrCorrupt
	}
	delta, err := l.deltaLocked(bucket, epoch, checkpoint)
	if err != nil {
		return 0, err
	}
	value := new(big.Int).Add(new(big.Int).SetUint64(base), delta)
	if value.Sign() < 0 {
		return 0, volume.ErrCorrupt
	}
	if !value.IsUint64() {
		return 0, volume.ErrRange
	}
	return value.Uint64(), nil
}

// debit receives the current locally spendable allowance from admission, not a
// stale heartbeat. opMu must remain held from computing it through this append.
func (l *quotaLedger) debit(g quotaGrant, available uint64) (quotaGrant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !validQuotaGrant(g) || g.Source != l.node || g.Debit != (client.Proof{}) {
		return quotaGrant{}, client.ErrProtocol
	}
	if prior := l.outgoing[g.ID]; prior != nil {
		candidate := prior.grant
		candidate.Debit = client.Proof{}
		if candidate != g {
			return quotaGrant{}, errControlConflict
		}
		return prior.grant, nil
	}
	if l.incoming[g.ID] != nil {
		return quotaGrant{}, errControlConflict
	}
	if g.Bytes > available {
		return quotaGrant{}, volume.ErrFull
	}
	if _, err := l.persist(quotaDebited, quotaRecord{Grant: g}); err != nil {
		return quotaGrant{}, err
	}
	return l.outgoing[g.ID].grant, nil
}

// receive must be called only for an authenticated, admitted source and a
// matching committed quota epoch. A source's proof is a durable journal
// reference; cluster authentication supplies provenance, as with range proofs.
func (l *quotaLedger) receive(g quotaGrant, actor, epoch client.ID) (client.Proof, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !validQuotaGrant(g) || g.Source != actor || g.Target != l.node || g.Epoch != epoch || g.Debit.Node != actor || g.Debit.RecordSequence == 0 || g.Debit.Digest == ([32]byte{}) {
		return client.Proof{}, errControlConflict
	}
	if prior := l.incoming[g.ID]; prior != nil {
		if prior.grant != g {
			return client.Proof{}, errControlConflict
		}
		return prior.applied, nil
	}
	if l.outgoing[g.ID] != nil {
		return client.Proof{}, errControlConflict
	}
	return l.persist(quotaReceived, quotaRecord{Grant: g})
}

func (l *quotaLedger) settle(id client.ID, receipt client.Proof, actor client.ID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.outgoing[id]
	if t == nil || t.grant.Target != actor || receipt.Node != actor || receipt.RecordSequence == 0 || receipt.Digest == ([32]byte{}) {
		return errControlConflict
	}
	if t.settled {
		if t.applied != receipt {
			return errControlConflict
		}
		return nil
	}
	_, err := l.persist(quotaSettled, quotaRecord{Grant: t.grant, Receipt: receipt})
	return err
}

func (l *quotaLedger) pending() []quotaGrant {
	l.mu.Lock()
	defer l.mu.Unlock()
	var grants []quotaGrant
	for _, t := range l.outgoing {
		if !t.settled {
			grants = append(grants, t.grant)
		}
	}
	return grants
}

// Net signed deltas avoid both inter-journal ordering assumptions and overflow
// of cumulative credit traffic when the same capacity is lent repeatedly.
func (l *quotaLedger) deltaLocked(bucket, epoch client.ID, checkpoint uint64) (*big.Int, error) {
	if checkpoint > l.sequence {
		return nil, volume.ErrCorrupt
	}
	value := new(big.Int)
	for _, t := range l.outgoing {
		g := t.grant
		if g.Bucket == bucket && g.Epoch == epoch && g.Debit.RecordSequence > checkpoint {
			value.Sub(value, new(big.Int).SetUint64(g.Bytes))
		}
	}
	for _, t := range l.incoming {
		g := t.grant
		if g.Bucket == bucket && g.Epoch == epoch && t.applied.RecordSequence > checkpoint {
			value.Add(value, new(big.Int).SetUint64(g.Bytes))
		}
	}
	return value, nil
}
