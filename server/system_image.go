// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sort"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"golang.org/x/crypto/bcrypt"
)

// The head and its content-addressed children are ordinary objects in the
// EVERYWHERE system bucket. Copying children is invisible to live account/
// bucket reads. Only a validated atomic head can be installed as the committed
// view; enabling a candidate's runtime authority is a separate controller step.
// Local volume configuration, local admin-operation receipts and the mergeable
// discovery directory are not copied into another node's configuration.
type systemManifest struct {
	VolumeRecoveries     []volumeRecoveryOrigin     `json:",omitempty"`
	QuotaReserves        []quotaPhysicalReservation `json:",omitempty"`
	QuotaProtocol        uint16                     `json:",omitempty"`
	ReplicationProtocol  uint16                     `json:",omitempty"`
	Quotas               []quotaAllocation          `json:",omitempty"`
	Shutdown             *clusterShutdown           `json:",omitempty"`
	Schema               uint16
	Cluster              client.ID
	Sequence             uint64
	Membership           client.Membership
	Generation, Revision uint64
	Accounts, Groups     volume.ObjectID
	Buckets              []volume.ObjectID
	Maps                 []systemMapRef
}

type systemMapRef struct {
	Bucket client.ID
	Object volume.ObjectID
}

type systemHead struct {
	Schema   uint16
	Decision client.Decision
	Manifest volume.ObjectID
}

type systemBundle struct {
	Root    volume.ObjectID
	Objects map[volume.ObjectID][]byte
}

type systemView struct {
	root             volume.ObjectID
	manifest         systemManifest
	accounts, groups []byte
	descriptions     []BucketDescription
	groupImage       *placementGroupImage
	patches          []client.MapPatch
	placement        *placementImage
}

var systemHeadKey = systemKey("cluster-system-head-v1")

func (s *Server) readSystemHead() (systemHead, error) {
	store, err := s.metadataStore()
	if err != nil {
		return systemHead{}, err
	}
	h, err := store.Open(false)
	if err != nil {
		return systemHead{}, err
	}
	defer h.Close()
	raw, _, err := h.Get(systemHeadKey)
	if err != nil {
		return systemHead{}, err
	}
	var head systemHead
	if decodeControlRecord(raw, &head) != nil || head.Schema != 1 || head.Manifest == (volume.ObjectID{}) {
		return systemHead{}, volume.ErrCorrupt
	}
	if _, err := head.Decision.MarshalBinary(); err != nil {
		return systemHead{}, volume.ErrCorrupt
	}
	return head, nil
}

func (b *systemBundle) add(data []byte) volume.ObjectID {
	id := volume.ObjectID(sha256.Sum256(data))
	b.Objects[id] = bytes.Clone(data)
	return id
}

// captureSystemImage is called under opMu by the bound global coordinator.
// roots contains each prospective member's persistent metadata volume identity;
// ranges are the previously agreed current placement/checkpoint, not a plan
// which can silently move ownership during membership admission.
func (s *Server) captureSystemImage(membership client.Membership, sequence, generation, revision uint64, roots map[client.ID]volume.ID, ranges []client.MapPatch, reserves ...[]quotaPhysicalReservation) (*systemBundle, error) {
	if !validateControlMembership(membership) || membership.Epoch == 0 || sequence == 0 || generation == 0 || revision == 0 {
		return nil, ErrBucketInvalid
	}
	s.mu.RLock()
	cluster := s.cfg.ClusterID
	s.mu.RUnlock()
	if membership.Cluster.String() != cluster {
		return nil, volume.ErrIdentity
	}
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		return nil, err
	}
	if err := validateSystemAccounts(accounts); err != nil {
		return nil, err
	}
	// Decode into a fresh value: unmarshalling over a shallow slice copy would
	// reuse and mutate the immutable image published to telemetry/routing.
	raw, err := json.Marshal(s.placementGroups.Load().object)
	var groupObject placementGroupsObject
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &groupObject); err != nil {
		return nil, err
	}
	found := false
	for i := range groupObject.Groups {
		g := &groupObject.Groups[i]
		if g.ID != systemPlacementGroup.String() {
			continue
		}
		found = true
		nodes := []PlacementNode{}
		for _, member := range membership.Members {
			root, ok := roots[member.ID]
			if !ok || root == (volume.ID{}) {
				return nil, fail(507, "member has no persistent metadata placement")
			}
			nodes = append(nodes, PlacementNode{NodeID: member.ID.String(), Mode: VolumesWhitelist, Volumes: []string{root.String()}})
		}
		mask, err := normalizePlacementMask(PlacementMask{Version: g.Effective.Version, Nodes: nodes, Storage: g.Effective.Storage, FailureDomains: g.Effective.FailureDomains})
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(mask.Nodes, g.Effective.Nodes) {
			if g.Revision == ^uint64(0) || mask.Version == ^uint64(0) {
				return nil, volume.ErrRange
			}
			g.Revision++
			mask.Version++
		}
		g.Effective, g.Desired = mask, nil
	}
	if !found {
		return nil, ErrBucketCatalogCorrupt
	}
	compiled, err := compilePlacementGroups(groupObject)
	if err != nil {
		return nil, err
	}
	bundle := &systemBundle{Objects: make(map[volume.ObjectID][]byte)}
	manifest := systemManifest{Schema: 1, Cluster: membership.Cluster, Sequence: sequence, Membership: membership, Generation: generation, Revision: revision}
	if view := s.currentSystem.Load(); view != nil {
		manifest.QuotaProtocol = view.manifest.QuotaProtocol
		manifest.ReplicationProtocol = view.manifest.ReplicationProtocol
		manifest.Shutdown = view.manifest.Shutdown
		manifest.VolumeRecoveries = cloneRecoveryOrigins(view.manifest.VolumeRecoveries)
	}
	if quotaPoliciesRequired(s.bucketSnapshots()) {
		manifest.QuotaProtocol = 1
	}
	if replicaPoliciesRequired(s.bucketSnapshots()) {
		manifest.ReplicationProtocol = max(manifest.ReplicationProtocol, 2)
	}
	if len(reserves) > 0 {
		manifest.QuotaReserves = reserves[0]
	}
	raw, err = json.Marshal(accounts)
	if err != nil {
		return nil, err
	}
	manifest.Accounts = bundle.add(raw)
	raw, err = json.Marshal(compiled.object)
	if err != nil {
		return nil, err
	}
	manifest.Groups = bundle.add(raw)
	for _, description := range s.bucketSnapshots() {
		// Legacy source-local volume choices cannot become a replicated bucket
		// restriction on another node. Actual ownership lives in the range map.
		description.LocalVolumes = nil
		if description.ID == systemBucket.String() {
			description.Policy.PlacementGroup = systemPlacementGroup.String()
		}
		description, err = normalizeBucketDescription(description)
		if err != nil {
			return nil, err
		}
		raw, err = json.Marshal(description)
		if err != nil {
			return nil, err
		}
		manifest.Buckets = append(manifest.Buckets, bundle.add(raw))
	}
	sort.Slice(manifest.Buckets, func(i, j int) bool { return bytes.Compare(manifest.Buckets[i][:], manifest.Buckets[j][:]) < 0 })
	for _, patch := range ranges {
		if patch.Membership != membership.Epoch || patch.Generation != generation || patch.Revision != revision {
			return nil, ErrBucketInvalid
		}
		raw, err := patch.MarshalBinary()
		if err != nil {
			return nil, err
		}
		manifest.Maps = append(manifest.Maps, systemMapRef{Bucket: patch.Bucket, Object: bundle.add(raw)})
	}
	sort.Slice(manifest.Maps, func(i, j int) bool { return bytes.Compare(manifest.Maps[i].Bucket[:], manifest.Maps[j].Bucket[:]) < 0 })
	raw, err = json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	bundle.Root = bundle.add(raw)
	if _, err := decodeSystemView(bundle.Root, func(id volume.ObjectID) ([]byte, error) {
		data, ok := bundle.Objects[id]
		if !ok {
			return nil, volume.ErrNotFound
		}
		return data, nil
	}); err != nil {
		return nil, err
	}
	return bundle, nil
}

func validateSystemAccounts(accounts accountsImage) error {
	if accounts.Schema != 1 || len(accounts.Users) == 0 {
		return ErrBucketCatalogCorrupt
	}
	ids, names := map[string]bool{}, map[string]bool{}
	admin := false
	accessKeys := map[string]bool{}
	for _, user := range accounts.Users {
		if _, err := bucketID(user.ID); err != nil || !validUsername(user.Name) || ids[user.ID] || names[user.Name] || user.PasswordHash == "" || user.Role != "ADMIN" && user.Role != "USER" {
			return ErrBucketCatalogCorrupt
		}
		if _, err := bcrypt.Cost([]byte(user.PasswordHash)); err != nil {
			return ErrBucketCatalogCorrupt
		}
		if len(user.S3Keys) > 32 {
			return ErrBucketCatalogCorrupt
		}
		for _, key := range user.S3Keys {
			sealed, err := base64.StdEncoding.DecodeString(key.Sealed)
			if !validS3AccessID(key.ID) || accessKeys[key.ID] || err != nil || len(sealed) < 28 {
				return ErrBucketCatalogCorrupt
			}
			accessKeys[key.ID] = true
		}
		ids[user.ID], names[user.Name] = true, true
		admin = admin || user.Role == "ADMIN" && !user.Disabled
	}
	if !admin {
		return ErrBucketCatalogCorrupt
	}
	return nil
}

func decodeSystemView(root volume.ObjectID, load func(volume.ObjectID) ([]byte, error)) (*systemView, error) {
	read := func(id volume.ObjectID) ([]byte, error) {
		if id == (volume.ObjectID{}) {
			return nil, volume.ErrCorrupt
		}
		data, err := load(id)
		if err != nil {
			return nil, err
		}
		if volume.ObjectID(sha256.Sum256(data)) != id {
			return nil, volume.ErrCorrupt
		}
		return data, nil
	}
	raw, err := read(root)
	if err != nil {
		return nil, err
	}
	view := &systemView{root: root}
	if err = decodeControlRecord(raw, &view.manifest); err != nil {
		return nil, err
	}
	m := view.manifest
	if m.Schema != 1 || m.Sequence == 0 || m.Generation == 0 || m.Revision == 0 || m.Cluster != m.Membership.Cluster || !validateControlMembership(m.Membership) || m.Membership.Epoch == 0 {
		return nil, volume.ErrCorrupt
	}
	if stop := m.Shutdown; stop != nil && (stop.ID == (client.ID{}) || stop.Coordinator == (client.ID{}) || stop.Sequence == 0 || stop.Sequence > m.Sequence || stop.Membership == 0 || stop.Membership > m.Membership.Epoch) {
		return nil, volume.ErrCorrupt
	}
	view.accounts, err = read(m.Accounts)
	if err != nil {
		return nil, err
	}
	var accounts accountsImage
	if err = decodeControlRecord(view.accounts, &accounts); err != nil {
		return nil, err
	}
	if err = validateSystemAccounts(accounts); err != nil {
		return nil, err
	}
	view.groups, err = read(m.Groups)
	if err != nil {
		return nil, err
	}
	var groups placementGroupsObject
	if err = decodeControlRecord(view.groups, &groups); err != nil {
		return nil, err
	}
	view.groupImage, err = compilePlacementGroups(groups)
	if err != nil {
		return nil, err
	}
	ids, names := map[string]bool{}, map[string]bool{}
	system := false
	for i, id := range m.Buckets {
		if i > 0 && bytes.Compare(m.Buckets[i-1][:], id[:]) >= 0 {
			return nil, volume.ErrCorrupt
		}
		raw, err := read(id)
		if err != nil {
			return nil, err
		}
		d, err := strictBucketJSON(raw)
		if err != nil {
			return nil, err
		}
		for _, policy := range []*BucketPolicy{&d.Policy, d.DesiredPolicy} {
			if groups.Schema >= 2 && !d.DeletionComplete && policy != nil {
				mask, ok := view.groupImage.mask(policy.PlacementGroup, policy.PlacementMaskVersion)
				if !ok || mask.Storage == nil || *mask.Storage != storagePolicy(*policy) {
					return nil, volume.ErrCorrupt
				}
			}
			if !d.DeletionComplete && policy != nil && policy.PlacementGroup != "" && policy.PlacementMaskVersion != 0 && view.groupImage.versions[policy.PlacementGroup][policy.PlacementMaskVersion] == nil {
				return nil, volume.ErrCorrupt
			}
		}
		if ids[d.ID] || names[d.Name] && !d.DeletionComplete || len(d.LocalVolumes) != 0 {
			return nil, volume.ErrCorrupt
		}
		ids[d.ID] = true
		if !d.DeletionComplete {
			names[d.Name] = true
		}
		if d.ID == systemBucket.String() {
			system = true
			if d.Policy.PlacementGroup != systemPlacementGroup.String() {
				return nil, volume.ErrCorrupt
			}
		}
		if !d.DeletionComplete && d.Policy.PlacementGroup != "" && view.groupImage.filters[d.Policy.PlacementGroup] == nil {
			return nil, volume.ErrCorrupt
		}
		view.descriptions = append(view.descriptions, d)
	}
	if !system {
		return nil, volume.ErrCorrupt
	}
	mapBuckets := map[string]bool{}
	for i, ref := range m.Maps {
		if i > 0 && bytes.Compare(m.Maps[i-1].Bucket[:], ref.Bucket[:]) >= 0 {
			return nil, volume.ErrCorrupt
		}
		raw, err := read(ref.Object)
		if err != nil {
			return nil, err
		}
		patch, err := client.DecodeMapPatch(raw)
		if err != nil {
			return nil, err
		}
		if patch.Bucket != ref.Bucket || patch.Membership != m.Membership.Epoch || patch.Generation != m.Generation || patch.Revision != m.Revision || !ids[ref.Bucket.String()] || ref.Bucket == client.ID(systemBucket) {
			return nil, volume.ErrCorrupt
		}
		for _, entry := range patch.Entries {
			for _, loc := range entry.Locations {
				if !hasControlNode(membershipIDs(m.Membership), loc.Node) {
					return nil, volume.ErrCorrupt
				}
			}
		}
		mapBuckets[ref.Bucket.String()] = true
		view.patches = append(view.patches, patch)
	}
	for _, d := range view.descriptions {
		if d.ID != systemBucket.String() && !d.DeletionComplete && !mapBuckets[d.ID] {
			return nil, volume.ErrCorrupt
		}
	}
	view.placement, err = buildPlacement(view.patches)
	if err != nil {
		return nil, err
	}
	nodes := view.groupImage.filters[systemPlacementGroup.String()]
	if len(nodes) != len(m.Membership.Members) {
		return nil, volume.ErrCorrupt
	}
	for _, member := range m.Membership.Members {
		f, ok := nodes[member.ID.String()]
		if !ok || f.mode != VolumesWhitelist || len(f.volumes) != 1 {
			return nil, volume.ErrCorrupt
		}
	}
	if err := validateRecoveryOrigins(view); err != nil {
		return nil, err
	}
	if m.ReplicationProtocol > 2 || replicaPoliciesRequired(view.descriptions) && m.ReplicationProtocol == 0 {
		return nil, volume.ErrCorrupt
	}
	if err := validatePhysicalReservations(view); err != nil {
		return nil, err
	}
	if err := validateQuotaAllocations(view); err != nil {
		return nil, err
	}
	return view, nil
}

func (s *Server) putSystemContent(id volume.ObjectID, data []byte) error {
	if volume.ObjectID(sha256.Sum256(data)) != id {
		return volume.ErrCorrupt
	}
	return s.metadataWrite(func(h *volume.Handle) error {
		key := volume.Key{Bucket: systemBucket, Object: id}
		existing, _, err := h.Get(key)
		if err == nil {
			if !bytes.Equal(existing, data) {
				return volume.ErrCorrupt
			}
			return nil
		}
		if !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		_, err = h.Put(key, data, volume.Options{CreateOnly: true})
		return err
	})
}

func (s *Server) stageSystemBundle(bundle *systemBundle) error {
	// Validate the entire graph before making even unreachable content visible
	// in the system volume. A receiver can additionally receive children one by
	// one and perform the same graph validation before it gives READY.
	if _, err := decodeSystemView(bundle.Root, func(id volume.ObjectID) ([]byte, error) {
		data, ok := bundle.Objects[id]
		if !ok {
			return nil, volume.ErrNotFound
		}
		return data, nil
	}); err != nil {
		return err
	}
	for id, data := range bundle.Objects {
		if volume.ObjectID(sha256.Sum256(data)) != id {
			return volume.ErrCorrupt
		}
	}
	for id, data := range bundle.Objects {
		if err := s.putSystemContent(id, data); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) loadSystemView(root volume.ObjectID) (*systemView, error) {
	store, err := s.metadataStore()
	if err != nil {
		return nil, err
	}
	h, err := store.Open(false)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	return decodeSystemView(root, func(id volume.ObjectID) ([]byte, error) {
		data, _, err := h.Get(volume.Key{Bucket: systemBucket, Object: id})
		return data, err
	})
}

func (s *Server) validateSystemProposal(view *systemView, proposal controlProposal, node client.ID) error {
	membership := proposal.Claim.Base.Membership
	if proposal.NextMembership != nil {
		membership = *proposal.NextMembership
	}
	m := view.manifest
	if m.Cluster != proposal.Claim.Base.Membership.Cluster || m.Sequence != proposal.Claim.Base.Sequence+1 || !equalControlMembership(m.Membership, membership) || m.Generation != proposal.Claim.Base.Generation || m.Revision != proposal.Claim.Base.MapRevision {
		return volume.ErrCorrupt
	}
	if hasControlNode(membershipIDs(membership), node) {
		store, err := s.metadataStore()
		if err != nil {
			return err
		}
		if !view.groupImage.allows(systemPlacementGroup.String(), node.String(), store.Identity().VolumeID.String()) {
			return volume.ErrIdentity
		}
	}
	return nil
}

// A READY proof is issued only after every referenced child has been received,
// checked, flushed, and validated against the prospective membership and PG.
func (s *Server) prepareSystemImage(ledger *controlLedger, proposal controlProposal) (client.Proof, error) {
	// A repeated PREPARE must not repeat exclusion cleanup after APPLIED/
	// RELEASE, when new full PUTs may already have repaired lost objects.
	decision, err := proposal.decision()
	if err != nil {
		return client.Proof{}, err
	}
	ledger.mu.Lock()
	if done, ok := ledger.completed[decision]; ok && equalControlProposal(*done.Slot.Proposal, proposal) {
		ledger.mu.Unlock()
		return done.Slot.Ready, nil
	}
	if slot := ledger.slot; slot != nil && slot.Proposal != nil && slot.Ready.RecordSequence != 0 && equalControlProposal(*slot.Proposal, proposal) {
		proof := slot.Ready
		ledger.mu.Unlock()
		return proof, nil
	}
	ledger.mu.Unlock()
	if proposal.Kind == controlMembership && s.anyBucketLease() {
		return client.Proof{}, fail(409, "release exclusive buckets before changing membership")
	}
	view, err := s.loadSystemView(volume.ObjectID(proposal.Image))
	if err != nil {
		return client.Proof{}, err
	}
	if err := s.validateSystemProposal(view, proposal, ledger.node); err != nil {
		return client.Proof{}, err
	}
	if err := s.prepareVolumeTakeover(view, proposal); err != nil {
		return client.Proof{}, err
	}
	if proposal.Kind == controlForceDisconnect {
		if err := s.prepareExclusionImage(view, proposal); err != nil {
			return client.Proof{}, err
		}
	}
	if proposal.Kind == controlShutdown {
		stop := view.manifest.Shutdown
		if stop == nil || stop.ID == (client.ID{}) || stop.Coordinator != proposal.Claim.Round.Initiator || stop.Sequence != view.manifest.Sequence || stop.Membership != view.manifest.Membership.Epoch {
			return client.Proof{}, volume.ErrCorrupt
		}
		s.opMu.Lock()
		err := s.syncVolumes()
		s.opMu.Unlock()
		if err != nil {
			return client.Proof{}, err
		}
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.prepareQuotaProtection(view); err != nil {
		return client.Proof{}, err
	}
	return ledger.prepare(proposal)
}

// publishSystemImage is the durable publication step, separate from enabling a
// candidate's network authority. The participant must already hold a valid
// ALL-COMMIT; callers mark APPLIED only after this returns successfully.
func (s *Server) publishSystemImage(ledger *controlLedger, decision client.Decision, root volume.ObjectID) (*systemView, error) {
	ledger.mu.Lock()
	allowed := ledger.slot != nil && ledger.slot.Decision == decision && ledger.slot.Committed.RecordSequence != 0 && ledger.slot.Proposal.Image == [32]byte(root)
	var expectedMembership client.Membership
	var expectedGeneration, expectedRevision uint64
	if allowed {
		expectedMembership = cloneControlBase(ledger.slot.Claim.Base).Membership
		if next := ledger.slot.Proposal.NextMembership; next != nil {
			expectedMembership = *next
		}
		expectedGeneration, expectedRevision = ledger.slot.Claim.Base.Generation, ledger.slot.Claim.Base.MapRevision
	}
	ledger.mu.Unlock()
	if !allowed {
		return nil, errControlConflict
	}
	view, err := s.loadSystemView(root)
	if err != nil {
		return nil, err
	}
	if view.manifest.Cluster != ledger.cluster || view.manifest.Sequence != decision.Sequence || !equalControlMembership(view.manifest.Membership, expectedMembership) || view.manifest.Generation != expectedGeneration || view.manifest.Revision != expectedRevision {
		return nil, volume.ErrCorrupt
	}
	// Every admitted local node must resolve its own persistent store through
	// the common PG. A normal leaver can publish its final, non-member head too.
	if hasControlNode(membershipIDs(view.manifest.Membership), ledger.node) {
		store, err := s.metadataStore()
		if err != nil {
			return nil, err
		}
		if !view.groupImage.allows(systemPlacementGroup.String(), ledger.node.String(), store.Identity().VolumeID.String()) {
			return nil, volume.ErrIdentity
		}
	}
	head := systemHead{Schema: 1, Decision: decision, Manifest: root}
	raw, err := json.Marshal(head)
	if err != nil {
		return nil, err
	}
	err = s.metadataWrite(func(h *volume.Handle) error {
		old, _, err := h.Get(systemHeadKey)
		if err == nil {
			var previous systemHead
			if decodeControlRecord(old, &previous) != nil || previous.Schema != 1 {
				return volume.ErrCorrupt
			}
			if previous.Decision == decision && previous.Manifest == root {
				return nil
			}
			if previous.Decision.Sequence >= decision.Sequence || previous.Decision.Digest != decision.Previous {
				return errControlConflict
			}
			_, err = h.Put(systemHeadKey, raw, volume.Options{})
			return err
		}
		if !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		_, err = h.Put(systemHeadKey, raw, volume.Options{CreateOnly: true})
		return err
	})
	return view, err
}
