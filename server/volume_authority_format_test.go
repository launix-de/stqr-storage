// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func authorityFormatFixture() volumeAuthorityRecord {
	r := volumeAuthorityRecord{Schema: 1, Cluster: client.ID{1}, Node: client.ID{2}, Volume: client.ID{3}}
	for i := 0; i < 400; i++ {
		start := client.ObjectID{byte(i >> 8), byte(i)}
		r.Entries = append(r.Entries, volumeAuthorityEntry{Bucket: client.ID{4}, Entry: client.RangeEntry{Start: start, PrefixBits: 16, PolicyVersion: 1, OwnerEpoch: 1, Locations: []client.Location{{Node: r.Node, Volume: r.Volume}}}, Cohort: []client.ID{r.Volume}, Active: true})
	}
	return r
}
func TestVolumeAuthorityCompactFormatAndStableFence(t *testing.T) {
	r := authorityFormatFixture()
	legacy, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := encodeVolumeAuthority(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(compact)*2 >= len(legacy) {
		t.Fatal("authority encoding did not materially shrink", len(compact), len(legacy))
	}
	for _, raw := range [][]byte{legacy, compact} {
		var got volumeAuthorityRecord
		if err = decodeVolumeAuthority(raw, &got); err != nil || !reflect.DeepEqual(got, r) {
			t.Fatal("authority round trip", err)
		}
	}
	r.Entries[0].Active = false
	r.Entries[0].Fence = [32]byte{1, 2, 3, 4, 255}
	fenced, err := encodeVolumeAuthority(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(fenced) != len(compact) {
		t.Fatal("fence shifted following entries")
	}
	changed := 0
	for i := range compact {
		if compact[i] != fenced[i] {
			changed++
		}
	}
	if changed > 33 {
		t.Fatal("fence changed unrelated entries", changed)
	}
	for _, raw := range [][]byte{compact[:4], compact[:len(compact)-1], append(bytes.Clone(compact), 0)} {
		var got volumeAuthorityRecord
		if decodeVolumeAuthority(raw, &got) == nil {
			t.Fatal("malformed authority accepted")
		}
	}
	unknown := bytes.Clone(compact)
	unknown[4] = 2
	var got volumeAuthorityRecord
	if decodeVolumeAuthority(unknown, &got) == nil {
		t.Fatal("unknown management format accepted")
	}
}
func TestVolumeAuthorityCompactFencesWithoutTemporarySpace(t *testing.T) {
	s := testServer(t)
	d := addTestVolume(t, s, "authority-format", "hdd", 2<<20)
	v := s.volumes[d.VolumeID].store
	r := authorityFormatFixture()
	r.Cluster = client.ID(v.Identity().ClusterID)
	r.Node = client.ID(v.Identity().NodeID)
	r.Volume = client.ID(v.Identity().VolumeID)
	if err := writeVolumeAuthorityBudget(v, r, 0, true); err != nil {
		t.Fatal(err)
	}
	free := v.Stats().AdmissionFreePhysical
	if err := v.SetGuaranteedReserve(free); err != nil {
		t.Fatal(err)
	}
	r.Entries[0].Active = false
	r.Entries[0].Fence = [32]byte{9, 8, 7, 6, 5, 4, 3, 2, 1}
	// No peer-format permission here: an existing compact record must never be
	// expanded back to JSON just because another node temporarily disconnects.
	if err := writeVolumeAuthority(v, r); err != nil {
		t.Fatal("fence needed free WAL space", err)
	}
	got, err := readVolumeAuthority(v)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("fence readback", err)
	}
	if v.Stats().GuaranteedRemainingPhysical != free {
		t.Fatal("fence consumed protected quota")
	}
}

func TestVolumeAuthorityCompactWaitsForEveryAdmittedPeer(t *testing.T) {
	local, other := client.ID{1}, client.ID{2}
	s := &Server{control: &controlRuntime{ledger: &controlLedger{node: local}}, peerStats: map[client.ID]*peerObservation{}}
	s.currentSystem.Store(&systemView{manifest: systemManifest{Membership: client.Membership{Members: []client.Member{{ID: local}, {ID: other}}}}})
	if s.compactAuthorityReady() {
		t.Fatal("activated with missing peer")
	}
	s.peerStats[other] = &peerObservation{online: true, connection: &peerConnection{Capabilities: client.CapabilityVolumeRecovery}}
	if s.compactAuthorityReady() {
		t.Fatal("activated before legacy peer upgrade")
	}
	s.peerStats[other].connection.Capabilities |= client.CapabilityCompactAuthority
	if !s.compactAuthorityReady() {
		t.Fatal("all upgraded peers did not enable encoding")
	}
	s.peerStats[other].online = false
	if s.compactAuthorityReady() {
		t.Fatal("activated with offline peer")
	}
}

func TestVolumeAuthorityInsertionAndRetirementKeepOtherSlots(t *testing.T) {
	s := testServer(t)
	d := addTestVolume(t, s, "authority-slots", "hdd", 2<<20)
	v := s.volumes[d.VolumeID].store
	r := authorityFormatFixture()
	r.Cluster = client.ID(v.Identity().ClusterID)
	r.Node = client.ID(v.Identity().NodeID)
	r.Volume = client.ID(v.Identity().VolumeID)
	if err := writeVolumeAuthorityBudget(v, r, 0, true); err != nil {
		t.Fatal(err)
	}
	free := v.Stats().AdmissionFreePhysical
	if err := v.SetGuaranteedReserve(free); err != nil {
		t.Fatal(err)
	}
	added := r.Entries[0]
	added.Bucket = client.ID{3}
	r.Entries = append([]volumeAuthorityEntry{added}, r.Entries...)
	if err := writeVolumeAuthority(v, r); err != nil {
		t.Fatal("insertion rewrote following entries", err)
	}
	got, err := readVolumeAuthority(v)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("inserted authority", err)
	}
	h, err := v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	before, err := h.LocalMetadata()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	r.Entries = append(r.Entries[:10:10], r.Entries[11:]...)
	if err = writeVolumeAuthority(v, r); err != nil {
		t.Fatal("retirement rewrote following entries", err)
	}
	got, err = readVolumeAuthority(v)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("retired authority", err)
	}
	replacement := added
	replacement.Bucket = client.ID{7}
	r.Entries = append(r.Entries, replacement)
	canonicalAuthorityEntries(&r)
	if err = writeVolumeAuthority(v, r); err != nil {
		t.Fatal("vacant authority slot was not reused", err)
	}
	h, err = v.Open(false)
	if err != nil {
		t.Fatal(err)
	}
	after, err := h.LocalMetadata()
	h.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatal("slot reuse changed physical size", len(before), len(after))
	}
	got, err = readVolumeAuthority(v)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("replacement authority", err)
	}
	if v.Stats().GuaranteedRemainingPhysical != free {
		t.Fatal("slot changes consumed protected quota")
	}
}
