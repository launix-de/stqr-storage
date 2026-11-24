// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// STVA is a management-format revision, not an object-content version. Its
// fixed-width Active/Fence fields keep a fence from shifting subsequent records
// and needing a full JSON-sized redo journal on a nearly full source disk.
func encodeVolumeAuthority(record volumeAuthorityRecord) ([]byte, error) {
	if record.Schema != 1 || len(record.Entries) > 200000 {
		return nil, volume.ErrCorrupt
	}
	e := client.Encoder{B: []byte("STVA")}
	e.U16(1)
	e.ID(record.Cluster)
	e.ID(record.Node)
	e.ID(record.Volume)
	e.U32(uint32(len(record.Entries)))
	for _, claim := range record.Entries {
		r := claim.Entry
		if len(r.Locations) > 256 || len(claim.Cohort) > 65535 {
			return nil, volume.ErrCorrupt
		}
		e.ID(claim.Bucket)
		e.B = append(e.B, r.Start[:]...)
		e.U16(r.PrefixBits)
		e.U64(r.PolicyVersion)
		e.U64(r.OwnerEpoch)
		e.U8(r.State)
		e.ID(r.Runtime)
		e.U16(uint16(len(r.Locations)))
		for _, l := range r.Locations {
			e.ID(l.Node)
			e.ID(l.Volume)
			e.U8(l.Role)
			e.U8(l.Class)
			e.U16(l.Fragment)
			e.U32(l.Flags)
		}
		e.U16(uint16(len(claim.Cohort)))
		for _, id := range claim.Cohort {
			e.ID(id)
		}
		e.Bool(claim.Active)
		e.B = append(e.B, claim.Fence[:]...)
		if len(e.B) > volume.MaxLocalMetadataBytes {
			return nil, volume.ErrRange
		}
	}
	return e.B, nil
}
func decodeVolumeAuthority(raw []byte, record *volumeAuthorityRecord) error {
	if !bytes.HasPrefix(raw, []byte("STVA")) {
		return decodeControlRecord(raw, record)
	}
	if len(raw) > volume.MaxLocalMetadataBytes {
		return volume.ErrCorrupt
	}
	d := client.Decoder{B: raw[4:]}
	if d.U16() != 1 {
		return volume.ErrCorrupt
	}
	out := volumeAuthorityRecord{Schema: 1, Cluster: d.ID(), Node: d.ID(), Volume: d.ID()}
	count := d.U32()
	if count > 200000 || uint64(count) > uint64(len(d.B))/120 {
		return volume.ErrCorrupt
	}
	for i := uint32(0); i < count; i++ {
		claim := volumeAuthorityEntry{Bucket: d.ID()}
		r := &claim.Entry
		copy(r.Start[:], d.Take(32))
		r.PrefixBits = d.U16()
		r.PolicyVersion = d.U64()
		r.OwnerEpoch = d.U64()
		r.State = d.U8()
		r.Runtime = d.ID()
		n := d.U16()
		if n > 256 || int(n) > len(d.B)/40 {
			return volume.ErrCorrupt
		}
		for j := uint16(0); j < n; j++ {
			r.Locations = append(r.Locations, client.Location{Node: d.ID(), Volume: d.ID(), Role: d.U8(), Class: d.U8(), Fragment: d.U16(), Flags: d.U32()})
		}
		n = d.U16()
		if int(n) > len(d.B)/16 {
			return volume.ErrCorrupt
		}
		for j := uint16(0); j < n; j++ {
			claim.Cohort = append(claim.Cohort, d.ID())
		}
		claim.Active = d.Bool()
		copy(claim.Fence[:], d.Take(32))
		if d.Err != nil || r.PrefixBits > 256 || r.State > 5 {
			return volume.ErrCorrupt
		}
		out.Entries = append(out.Entries, claim)
	}
	if d.Done() != nil {
		return volume.ErrCorrupt
	}
	*record = out
	return nil
}

// Do not introduce a new persistent encoding while an admitted peer still lacks
// its reader. Older binaries reject STVA as unsupported metadata before mutation;
// the existing JSON representation remains readable by both implementations.
func (s *Server) compactAuthorityReady() bool {
	if s.control == nil {
		return true
	}
	view := s.currentSystem.Load()
	if view == nil {
		return false
	}
	s.peerStatsMu.RLock()
	defer s.peerStatsMu.RUnlock()
	for _, node := range membershipIDs(view.manifest.Membership) {
		if node == s.control.ledger.node {
			continue
		}
		peer := s.peerStats[node]
		if peer == nil || !peer.online || peer.connection == nil || peer.connection.Capabilities&client.CapabilityCompactAuthority == 0 {
			return false
		}
	}
	return true
}

func marshalVolumeAuthority(record volumeAuthorityRecord, compact bool) ([]byte, error) {
	if compact {
		return encodeVolumeAuthority(record)
	}
	return json.Marshal(record)
}
func authorityEncodingForStore(store *volume.Volume, permitNew bool) (bool, error) {
	if permitNew {
		return true, nil
	}
	h, err := store.Open(false)
	if err != nil {
		return false, err
	}
	defer h.Close()
	raw, err := h.LocalMetadata()
	return bytes.HasPrefix(raw, []byte("STVA")), err
}
