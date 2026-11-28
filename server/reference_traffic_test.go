// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"testing"
)

func TestReferenceControlTrafficExcludesInternalRoot(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "reference-traffic", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "reference-traffic")
	p := quotaPrincipal(t, s)
	entries := make([]objectReferenceEntry, 3218)
	for i := range entries {
		entries[i].object = volume.ObjectID{byte(i), byte(i >> 8)}
		entries[i].proof[0] = byte(i)
		entries[i].account = volume.ID{9}
	}
	raw := encodeObjectReferences(entries, b.ID())
	if _, e := b.Put(context.Background(), p, volume.ObjectID(client.ObjectReferenceRootID()), raw, volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); e != nil {
		t.Fatal(e)
	}
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketAllPermissions}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	ws.connectionFenceCap.Store(true)
	backend := b.allVolumes()[0]
	if err := backend.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	physicalBefore := backend.IOStats()
	before := b.ioSnapshot()
	ur, uw := s.userTraffic(p.User.ID)
	accepted := true
	m := referenceRequest(b, client.ObjectID{0xfe, 0xff}, client.NewRequestID(), &accepted)
	result, e := s.executeObjectReference(context.Background(), m, ws, p)
	if e != nil {
		t.Fatal(e)
	}
	after := b.ioSnapshot()
	vr, vw := s.userTraffic(p.User.ID)
	reply := client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: client.ResponseBody(200, result, "")}
	t.Logf("root=%d request_encoded=%d response_encoded_without_extra_headers=%d ack_result=%d bucket_read_delta=%d bucket_write_delta=%d user_read_delta=%d user_write_delta=%d", len(raw), len(client.EncodeMessage(m)), len(client.EncodeMessage(reply)), len(result), after.LogicalReadBytes-before.LogicalReadBytes, after.LogicalWriteBytes-before.LogicalWriteBytes, vr-ur, vw-uw)
	if after != before || ur != vr || uw != vw {
		t.Fatal("internal root inflated logical/user throughput", before, after, ur, uw, vr, vw)
	}
	if backend.IOStats().SyncOps != physicalBefore.SyncOps {
		t.Fatal("accepted reference unexpectedly synced")
	}
	// The fixture explicitly fences acceptance to measure the real backend
	// write; metering changes must not hide it or introduce this fence.
	if err := backend.FlushAccepted(); err != nil {
		t.Fatal(err)
	}
	if backend.IOStats().WriteBytes <= physicalBefore.WriteBytes {
		t.Fatal("fixture did no backend write work")
	}
	m.ID = client.NewRequestID()
	m.Body[len(m.Body)-18] = 1
	if _, err := s.executeObjectReference(context.Background(), m, ws, p); err != nil {
		t.Fatal(err)
	}
	if now := b.ioSnapshot(); now != before {
		t.Fatal("UNPIN counted internal payload", now, before)
	}
	for _, acceptedWrite := range []bool{false, true} {
		start := b.ioSnapshot()
		if _, err := b.Put(context.Background(), p, volume.ObjectID{88}, []byte("x"), volume.Options{RequestID: volume.ID(client.NewRequestID()), Accepted: acceptedWrite}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := b.Get(context.Background(), p, volume.ObjectID{88}); err != nil {
			t.Fatal(err)
		}
		if now := b.ioSnapshot(); now.LogicalReadBytes-start.LogicalReadBytes != 1 || now.LogicalWriteBytes-start.LogicalWriteBytes != 1 {
			t.Fatal("normal one-byte payload not metered", start, now)
		}
	}
	start := b.ioSnapshot()
	if _, _, err := b.Get(context.Background(), p, volume.ObjectID(client.ObjectReferenceRootID())); err != nil {
		t.Fatal(err)
	}
	if now := b.ioSnapshot(); now.LogicalReadBytes-start.LogicalReadBytes != uint64(len(raw)) {
		t.Fatal("explicit client ORF GET not counted")
	}

}

func TestReferenceControlMarkerDoesNotGrantWrite(t *testing.T) {
	s := testServer(t)
	addTestVolume(t, s, "readonly-reference", "hdd", 8<<20)
	b, _ := createProtocolBucket(t, s, "readonly-reference")
	user := bucketProtocolUser(t, s)
	d := b.Describe()
	d.Grants = []BucketGrant{{UserID: user.ID, Permissions: BucketRead}}
	if _, err := s.buckets.Save(d, d.Revision); err != nil {
		t.Fatal(err)
	}
	ws := connectionTestSession(s)
	ws.authorization = &connectionAuthorization{bucket: b, bucketID: b.ID(), permissions: BucketRead}
	ws.authorization.objectReferences.Store(true)
	ws.durabilityCap.Store(true)
	ws.acceptedReferencesCap.Store(true)
	ws.connectionFenceCap.Store(true)
	actor := principal{User: user, connection: ws.authorization}
	accepted := true
	m := referenceRequest(b, client.ObjectID{42}, client.NewRequestID(), &accepted)
	if _, err := s.executeObjectReference(context.Background(), m, ws, actor); err != nil {
		t.Fatal("readonly PIN", err)
	}
	before := b.ioSnapshot()
	actor.internalCoordination = true
	if _, err := b.Put(context.Background(), actor, volume.ObjectID{77}, []byte("x"), volume.Options{Accepted: true, RequestID: volume.ID(client.NewRequestID())}); err == nil {
		t.Fatal("metrics marker granted write")
	}
	m.Body[16] ^= 0x80 // no longer the reserved coordination root
	if _, err := s.executeObjectReference(context.Background(), m, ws, actor); err == nil {
		t.Fatal("reference authorized other object")
	}
	if after := b.ioSnapshot(); after != before {
		t.Fatal("rejected writes changed logical counters", before, after)
	}
}
func TestReferenceControlMarkerExcludesFrontendRangeTraffic(t *testing.T) {
	s := &Server{}
	r := &rangeManager{server: s, node: client.ID{1}}
	s.scopes = r
	bucket := client.ID{9}
	object := client.ObjectID{7}
	image, err := buildPlacement([]client.MapPatch{{Membership: 1, Generation: 1, Revision: 1, Bucket: bucket, ToEnd: true, Entries: []client.RangeEntry{{OwnerEpoch: 1, PolicyVersion: 1, Locations: []client.Location{{Node: r.node, Volume: client.ID{2}, Class: uint8(volume.HDD)}}}}, Addresses: []client.NodeAddress{{Node: r.node, Addresses: []string{"http://node:7419"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	s.placement.Store(image)
	b, err := NewBucket(volume.ID(bucket), "traffic")
	if err != nil {
		t.Fatal(err)
	}
	b.server = s
	b.recordRangeTraffic(principal{}, volume.ObjectID(object), 1, 2)
	key, ok := image.metricKey(bucket, object)
	if !ok {
		t.Fatal("missing range")
	}
	v, ok := r.server.statistics.ranges.Load(key)
	if !ok {
		t.Fatal("normal frontend traffic not counted")
	}
	metric := v.(*rangeMetric)
	b.recordRangeTraffic(principal{internalCoordination: true}, volume.ObjectID(object), 1000, 2000)
	if metric.read.Load() != 1 || metric.write.Load() != 2 {
		t.Fatal("control root inflated frontend range work")
	}
}
