// SPDX-License-Identifier: GPL-3.0-only
package volume

import "fmt"

// Description is an immutable, JSON-ready management view. Configuration and
// synchronization internals are deliberately not marshalled as public fields.
type Description struct {
	ID         string `json:"id"`
	ClusterID  string `json:"cluster_id"`
	NodeID     string `json:"node_id"`
	Name       string `json:"name"`
	SizeBytes  uint64 `json:"size_bytes"`
	Class      string `json:"class"`
	State      string `json:"state"`
	Generation uint64 `json:"generation"`
}

func (s State) String() string {
	switch s {
	case Pending:
		return "pending"
	case Active:
		return "active"
	case Draining:
		return "draining"
	case Detached:
		return "detached"
	case Failed:
		return "failed"
	}
	return "invalid"
}
func (v *Volume) Describe() Description {
	v.mu.RLock()
	defer v.mu.RUnlock()
	s := v.sb
	state := v.state
	if v.failed != nil && !v.closed {
		state = Failed
	}
	return Description{ID: s.Identity.VolumeID.String(), ClusterID: s.Identity.ClusterID.String(), NodeID: s.Identity.NodeID.String(), Name: v.name, SizeBytes: s.Identity.Size, Class: s.Identity.Class.String(), State: state.String(), Generation: s.Generation}
}
func (v *Volume) Size() uint64 { v.mu.RLock(); defer v.mu.RUnlock(); return v.sb.Identity.Size }

// syncThroughLocked is a local flush barrier for a current object. Replicated
// barriers require the server to invoke it on every required replica.
func (v *Volume) syncThroughLocked(k Key) error {
	if e := v.check(false); e != nil {
		return e
	}
	_, e := v.loadObject(k)
	if e != nil {
		return e
	}
	return v.syncVisible()
}

// Detach only closes an empty draining volume. The caller must first ensure
// cluster migrations/subscription handoffs are complete; this library cannot
// prove cluster durability from local bytes. Tombstones/retained RequestIDs may
// remain locally, so only live payload count is used by this local check.
func (v *Volume) Detach() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.preparing {
		return ErrBusy
	}
	if e := v.check(false); e != nil {
		return e
	}
	if v.state != Draining || v.sb.Objects != 0 {
		return ErrBusy
	}
	if e := v.sync(); e != nil {
		return e
	}
	v.closed = true
	v.state = Detached
	e := v.dev.Close()
	if v.onClose != nil {
		v.onClose()
	}
	return e
}

// VerifyIdentity can be used before activating an existing volume on a node.
func (v *Volume) VerifyIdentity(cluster, node, id ID) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if e := v.check(false); e != nil {
		return e
	}
	s := v.sb.Identity
	if (cluster != (ID{}) && s.ClusterID != cluster) || (node != (ID{}) && s.NodeID != node) || (id != (ID{}) && s.VolumeID != id) {
		return fmt.Errorf("%w: owning node/cluster/volume", ErrIdentity)
	}
	return nil
}

// CheckEmpty includes tombstones, retry receipts and private replica records.
// Zero live payload bytes alone do not make a volume eligible for a new cluster.
func (v *Volume) CheckEmpty() error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if err := v.check(false); err != nil {
		return err
	}
	return v.walkIndex(func(indexEntry) error { return ErrBusy })
}

// JoinEmptyCluster preserves a prepared volume's ID, size and allocation format.
// The caller must exclusively own the volume and persist its join intent first.
// Repeating the same transition after a crash is safe; ordinary Bind remains
// strict about cluster identity.
func (v *Volume) JoinEmptyCluster(previous, next, node, id ID) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(true); err != nil {
		return err
	}
	identity := v.sb.Identity
	if previous == (ID{}) || next == (ID{}) || node == (ID{}) || id == (ID{}) ||
		identity.NodeID != node || identity.VolumeID != id ||
		(identity.ClusterID != previous && identity.ClusterID != next) {
		return ErrIdentity
	}
	if err := v.walkIndex(func(indexEntry) error { return ErrBusy }); err != nil {
		return err
	}
	if identity.ClusterID == next {
		return nil
	}
	// Old local ownership provenance belongs to the abandoned empty cluster.
	if err := v.setLocalMetadataLocked(nil, Options{}, nil); err != nil {
		return err
	}
	s := v.sb
	s.Identity.ClusterID = next
	s.Generation++
	return v.publish(s)
}
