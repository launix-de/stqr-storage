// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"encoding/binary"
)

const (
	ObjectAccepted       uint16 = 0x0118
	BucketSync           uint16 = 0x0119
	DurabilityNotice     uint16 = 0x011a
	CapabilityDurability uint64 = 8192
	// Streaming accepted ACKs retain the ordinary chunk framing and require an explicit Sync.
	CapabilityAcceptedStreams uint64 = 1 << 21
	DurabilityHeader          uint64 = 48
)

type CommitMark struct {
	Boot     ID
	Sequence uint64
}

func (m CommitMark) MarshalBinary() []byte {
	e := Encoder{}
	e.ID(m.Boot)
	e.U64(m.Sequence)
	return e.B
}
func DecodeCommitMark(b []byte) (CommitMark, error) {
	d := Decoder{B: b}
	m := CommitMark{d.ID(), d.U64()}
	if d.Done() != nil || m.Boot == (ID{}) || m.Sequence == 0 {
		return m, ErrProtocol
	}
	return m, nil
}

type pendingDurability struct {
	mark     CommitMark
	endpoint string
	node     ID
}

func (c *Client) observeDurability(t *objectTransport, b []byte) error {
	m, err := DecodeCommitMark(b)
	if err != nil {
		return err
	}
	if t.capabilities&CapabilityConnectionSync != 0 {
		t.noteConnectionDependency(m.Boot)
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.durability == nil {
		c.durability = map[ID]pendingDurability{}
	}
	old := c.durability[m.Boot]
	if m.Sequence <= old.mark.Sequence {
		return nil
	}
	if len(c.durability) >= 1024 && old.mark.Sequence == 0 {
		c.durabilityError = ErrFull
		return ErrFull
	}
	c.durability[m.Boot] = pendingDurability{m, t.endpoint, t.node}
	return nil
}

// Sync makes all observed accepted writes durable. It may cover more writes in
// this bucket, but never confirms a mark from another server incarnation.
func (c *Client) Sync(ctx context.Context) error { return c.syncExcept(ctx, nil) }
func (c *Client) syncExcept(ctx context.Context, except *objectTransport) error {
	if c == nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.durabilityError != nil {
		err := c.durabilityError
		c.mu.Unlock()
		return err
	}
	pending := make([]pendingDurability, 0, len(c.durability))
	for _, p := range c.durability {
		if except == nil || p.node != except.node || p.endpoint != except.endpoint {
			pending = append(pending, p)
		}
	}
	c.mu.Unlock()
	if err := c.syncConnections(ctx, except); err != nil {
		return err
	}
	for _, p := range pending {
		t, err := c.connectionFor(ctx, p.endpoint, p.node)
		if err != nil {
			return err
		}
		e := Encoder{}
		e.ID(c.bucket)
		e.B = append(e.B, p.mark.MarshalBinary()...)
		r, err := t.roundTrip(ctx, Message{Kind: Request, Opcode: BucketSync, ID: NewRequestID(), Body: e.B}, false, nil)
		if err != nil {
			return err
		}
		durable, err := DecodeCommitMark(r.result)
		r.close()
		if err != nil {
			return err
		}
		if durable.Boot != p.mark.Boot || durable.Sequence < p.mark.Sequence {
			return ErrGone
		}
		c.mu.Lock()
		if current := c.durability[p.mark.Boot]; current.mark.Sequence <= durable.Sequence {
			delete(c.durability, p.mark.Boot)
		}
		c.mu.Unlock()
	}
	return nil
}
func acceptedMessage(m Message) Message {
	b := make([]byte, 0, len(m.Body)+2)
	b = append(b, m.Body[:80]...)
	b = binary.LittleEndian.AppendUint16(b, m.Opcode)
	b = append(b, m.Body[80:]...)
	m.Opcode = ObjectAccepted
	m.Body = b
	return m
}

type acceptedContextKey struct{}

func withAccepted(ctx context.Context, enabled bool) context.Context {
	if enabled {
		return context.WithValue(ctx, acceptedContextKey{}, true)
	}
	return ctx
}

// outgoingMutation never silently strengthens an accepted write into a durable
// native operation. Legacy native requests keep their existing ACK contract;
// requested acceptance requires negotiated support, including streaming bodies.
func outgoingMutation(ctx context.Context, t *objectTransport, m Message, mutation bool, upload uploadFunc) (Message, bool, error) {
	if mutation && ctx.Value(acceptedContextKey{}) == true {
		if t.capabilities&CapabilityDurability == 0 || upload != nil && t.capabilities&CapabilityAcceptedStreams == 0 {
			return m, false, &StatusError{Status: 426, Message: "peer cannot accept this write without an implicit durability fence"}
		}
		return acceptedMessage(m), false, nil
	}
	deferred, _ := ctx.Value(filesystemDeferredKey{}).(bool)
	if deferred && m.Opcode == ObjectReference && len(m.Body) == 129 && m.Body[112] <= 5 {
		const required = CapabilityAcceptedReferences | CapabilityObjectReferences | CapabilityDurability | CapabilityConnectionSync
		if t.capabilities&required != required {
			return m, false, &StatusError{Status: 426, Message: "peer cannot defer filesystem reference durability"}
		}
	}
	outgoing, reference := acceptedReferenceMessage(ctx, t, m)
	return outgoing, reference, nil
}
