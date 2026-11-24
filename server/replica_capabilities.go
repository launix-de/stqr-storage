// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"github.com/launix-de/stqr-storage/client"
)

func replicaPoliciesRequired(descriptions []BucketDescription) bool {
	for _, d := range descriptions {
		if d.ID == systemBucket.String() || d.DeletionComplete {
			continue
		}
		for _, p := range []*BucketPolicy{&d.Policy, d.DesiredPolicy} {
			if p != nil && p.Redundancy.Mode == "replication" && (p.PlacementMode == "everywhere" || p.Redundancy.Replicas > 1) {
				return true
			}
		}
	}
	return false
}
func supportsReplicaPolicies(view *systemView, caps uint64) bool {
	return view == nil || (view.manifest.ReplicationProtocol == 0 && !replicaPoliciesRequired(view.descriptions)) || (caps&client.CapabilityReplication != 0 && (view.manifest.ReplicationProtocol < 2 || caps&client.CapabilityReplicaTopology != 0))
}
func (s *Server) requireReplicaCapabilities(descriptions []BucketDescription, nodes []client.ID) error {
	view := s.currentSystem.Load()
	if !replicaPoliciesRequired(descriptions) && (view == nil || view.manifest.ReplicationProtocol == 0) {
		return nil
	}
	s.peerStatsMu.RLock()
	defer s.peerStatsMu.RUnlock()
	for _, node := range nodes {
		if node == s.control.ledger.node {
			continue
		}
		o := s.peerStats[node]
		if o == nil || !o.online || o.connection.Capabilities&(client.CapabilityReplication|client.CapabilityReplicaTopology) != client.CapabilityReplication|client.CapabilityReplicaTopology {
			return fail(503, "required member lacks replication protocol support")
		}
	}
	return nil
}
func (s *Server) preserveReplicaProtocol(bundle *systemBundle, descriptions []BucketDescription) error {
	view := s.currentSystem.Load()
	if !replicaPoliciesRequired(descriptions) && (view == nil || view.manifest.ReplicationProtocol == 0) {
		return nil
	}
	var m systemManifest
	if err := decodeControlRecord(bundle.Objects[bundle.Root], &m); err != nil {
		return err
	}
	if m.ReplicationProtocol == 2 {
		return nil
	}
	m.ReplicationProtocol = 2
	raw, err := json.Marshal(m)
	if err == nil {
		bundle.Root = bundle.add(raw)
	}
	return err
}
