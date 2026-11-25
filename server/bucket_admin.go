// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Usage is a live snapshot, never persisted as part of the bucket policy.
// TODO(multinode): combine admitted peers using the same accounting as admission.
type bucketListEntry struct {
	BucketDescription
	UsedBytes                   *uint64 `json:"used_bytes,omitempty"`
	UsageError                  string  `json:"usage_error,omitempty"`
	GuaranteedRemainingPhysical *uint64 `json:"guaranteed_remaining_physical,omitempty"`
	GuaranteeEstimateShortfall  *uint64 `json:"guarantee_estimate_shortfall,omitempty"`
}

func bucketAdminOpcode(op uint16) bool {
	return op == client.BucketListPage || op == client.BucketGroupCapacity || op == client.BucketCreateInGroup || op >= 0x2003 && op <= 0x200a || op == client.BucketList || op == client.BucketQuotaCapacity
}
func encodeBucketAck(operation string, id volume.ID, running bool) []byte {
	e := client.Encoder{}
	op, _ := parseID(operation)
	e.ID(client.ID(op))
	if running {
		e.U8(1)
	} else {
		e.U8(2)
	}
	e.Bool(true)
	e.ID(client.ID(id))
	return e.B
}
func decodeBucketPolicy(d *client.Decoder) (volume.ID, string, BucketState, BucketPolicy) {
	id := volume.ID(d.ID())
	version := d.U64()
	name := d.Text()
	state := d.U8()
	strategy := d.U8()
	codec := d.U8()
	placement := d.U8()
	redundancy := d.U8()
	p := BucketPolicy{Version: version, Redundancy: BucketRedundancy{ACK: "all", Replicas: d.U16(), ECK: d.U16(), ECM: d.U16()}}
	p.ReadAcceleration = BucketReadAcceleration{Enabled: d.Bool(), MaxCopies: d.U16(), BytesBudget: d.U64()}
	accounting := d.U8()
	p.Limits = BucketLimits{Guaranteed: d.U64(), Unlimited: d.Bool(), Absolute: d.U64(), MetadataReservePPM: d.U32()}
	if state > 2 || strategy > 2 || codec > 3 || placement > 1 || redundancy > 1 || accounting > 1 {
		d.Err = ErrBucketInvalid
		return id, name, "", p
	}
	p.StorageStrategy = []string{"hdd", "ssd", "ram"}[strategy]
	p.Compression = []string{"raw", "gzip", "lz4", "xz"}[codec]
	p.PlacementMode = []string{"count", "everywhere"}[placement]
	p.Redundancy.Mode = []string{"single", "replication"}[redundancy]
	p.Limits.Accounting = []string{"logical", "physical"}[accounting]
	return id, name, []BucketState{BucketActive, BucketLocked, BucketDeleting}[state], p
}
func (s *Server) validBucketUsers(users []BucketGrant) error {
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		return err
	}
	known := map[string]bool{}
	for _, u := range image.Users {
		known[u.ID] = true
	}
	for _, u := range users {
		if !known[u.UserID] {
			return fail(404, "bucket user does not exist")
		}
	}
	return nil
}
func (s *Server) bucketReceipt(p principal, m client.Message) (BucketReceipt, *Bucket, error) {
	if m.ID == (client.ID{}) {
		return BucketReceipt{}, nil, fail(400, "operation ID is required")
	}
	if err := s.requireAdmin(p); err != nil {
		return BucketReceipt{}, nil, err
	}
	digest := sha256.New()
	digest.Write([]byte(p.User.ID))
	digest.Write([]byte{byte(m.Opcode), byte(m.Opcode >> 8)})
	digest.Write(m.Body)
	receipt := BucketReceipt{ID: m.ID.String(), Actor: p.User.ID, Digest: hex.EncodeToString(digest.Sum(nil)), Opcode: m.Opcode, Status: 200}
	for _, d := range s.bucketSnapshots() {
		for _, r := range d.Receipts {
			if r.ID == receipt.ID {
				if r.Actor != receipt.Actor || r.Digest != receipt.Digest {
					return receipt, nil, fail(400, "operation ID reused with different input")
				}
				id, _ := bucketID(d.ID)
				s.bucketMu.RLock()
				b := s.bucketsByID[id]
				s.bucketMu.RUnlock()
				return r, b, nil
			}
		}
	}
	return receipt, nil, nil
}
func (s *Server) executeBuckets(m client.Message, p principal) ([]byte, int, error) {
	// Telemetry queries may wait for a required remote owner; they must never
	// hold the node's mutation lock while doing so.
	if m.Opcode != client.BucketList && m.Opcode != client.BucketListPage && m.Opcode != client.BucketQuotaCapacity && m.Opcode != client.BucketGroupCapacity {
		s.opMu.Lock()
		defer s.opMu.Unlock()
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, 0, fail(503, "server is stopping")
	}
	if m.Opcode == client.BucketQuotaCapacity || m.Opcode == client.BucketGroupCapacity {
		return s.executeBucketQuotaCapacity(m, p)
	}
	if m.Opcode == client.BucketListPage {
		return s.executeBucketListPage(m.Body, p)
	}
	if m.Opcode == client.BucketList {
		if err := s.requireAdmin(p); err != nil {
			return nil, 0, err
		}
		usageContext, cancelUsage := context.WithTimeout(s.ctx, 3*time.Second)
		defer cancelUsage()
		d := client.Decoder{B: m.Body}
		after := d.Text()
		limit := d.U32()
		if d.Done() != nil || limit == 0 || limit > 1000 {
			return nil, 0, fail(400, "BUCKET_LIST requires after-name Text and limit 1..1000")
		}
		list := s.bucketSnapshots()
		sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
		page := []bucketListEntry{}
		budget := client.MaxMessage - 1024
		next := ""
		for _, b := range list {
			if b.DeletionComplete || b.Name <= after {
				continue
			}
			if len(page) == int(limit) {
				next = page[len(page)-1].Name
				break
			}
			b.Receipts = nil
			entry := bucketListEntry{BucketDescription: b}
			if !b.Policy.Limits.Unlimited {
				id, e := bucketID(b.ID)
				if e != nil {
					return nil, 0, e
				}
				bucket, e := s.Bucket(id)
				if e != nil {
					return nil, 0, e
				}
				used, e := bucket.administrativeUsage(usageContext, p)
				if e != nil {
					entry.UsageError = e.Error()
				} else {
					entry.UsedBytes = &used
				}
			}
			s.addQuotaDiagnostics(&entry)
			encoded, e := json.Marshal(entry)
			if e != nil {
				return nil, 0, e
			}
			if len(encoded)+1 > budget {
				if len(page) == 0 {
					return nil, 0, fail(413, "bucket description exceeds a response page")
				}
				next = page[len(page)-1].Name
				break
			}
			budget -= len(encoded) + 1
			page = append(page, entry)
		}
		return jsonText(struct {
			Buckets []bucketListEntry `json:"buckets"`
			Next    string            `json:"next"`
		}{page, next}), 200, nil
	}
	receipt, replay, err := s.bucketReceipt(p, m)
	if err != nil {
		return nil, 0, err
	}
	if replay != nil {
		if receipt.Status >= 400 {
			return nil, 0, fail(receipt.Status, "bucket operation was superseded")
		}
		if receipt.Running {
			s.startBucketCleanup(replay.ID())
		}
		return encodeBucketAck(receipt.ID, replay.ID(), receipt.Running), receipt.Status, nil
	}
	d := client.Decoder{B: m.Body}
	var target volume.ID
	var grantedUser string
	var deleteUsers bool
	var description BucketDescription
	var b *Bucket
	if m.Opcode == client.BucketCreate || m.Opcode == client.BucketCreateInGroup {
		var id volume.ID
		var name string
		state := BucketActive
		var policy BucketPolicy
		if m.Opcode == client.BucketCreateInGroup {
			if d.U16() != 1 {
				return nil, 0, client.ErrProtocol
			}
			id, name = volume.ID(d.ID()), d.Text()
			group := d.ID().String()
			policy = DefaultBucketPolicy()
			policy.Limits = decodePGLimits(&d)
			var e error
			policy, e = s.policyForGroup(policy, group, true)
			if e != nil {
				return nil, 0, e
			}
		} else {
			id, name, state, policy = decodeBucketPolicy(&d)
			var e error
			policy, e = s.legacyStorageGroup(policy)
			if e != nil {
				return nil, 0, e
			}
		}
		target = id
		if d.Done() != nil || state != BucketActive {
			return nil, 0, ErrBucketInvalid
		}
		b, err = NewBucket(id, name)
		if err != nil {
			return nil, 0, err
		}
		description = b.Describe()
		description.Policy = policy
		if _, e := s.Bucket(id); !errors.Is(e, ErrBucketNotFound) {
			return nil, 0, ErrBucketConflict
		}
	} else {
		if m.Opcode == client.BucketSetPolicy {
			id, name, state, policy := decodeBucketPolicy(&d)
			target = id
			expected := d.U64()
			b, err = s.Bucket(target)
			if err == nil {
				description = b.Describe()
				// The existing object-client Policy wire schema does not edit PG
				// references. Preserve the committed mask on ordinary policy changes.
				policy.PlacementGroup = description.Policy.PlacementGroup
				policy.PlacementMaskVersion = description.Policy.PlacementMaskVersion
				if storagePolicy(policy) != storagePolicy(description.Policy) {
					var e error
					policy, e = s.legacyStorageGroup(policy)
					if e != nil {
						return nil, 0, e
					}
				}
				if description.DesiredPolicy != nil {
					return nil, 0, ErrBucketConflict
				}
				if description.Policy.Version != expected {
					return nil, 0, ErrBucketConflict
				}
				description.Name = name
				description.State = state
				if policy != description.Policy {
					if policy.Version <= description.Policy.Version {
						return nil, 0, ErrBucketInvalid
					}
					if err = bucketLocalPolicy(policy); err != nil {
						return nil, 0, err
					}
					usage, e := b.logicalUsage()
					if e != nil {
						return nil, 0, e
					}
					if usage != 0 && (description.Policy.StorageStrategy == "ram") != (policy.StorageStrategy == "ram") {
						return nil, 0, fail(501, "nonempty RAM/disk policy conversion is not implemented")
					}
					if usage == 0 && len(description.LocalVolumes) == 0 && !s.metadataPreview {
						description.Policy = policy
						description.DesiredPolicy = nil
					} else {
						description.DesiredPolicy = &policy
						receipt.Running = true
						receipt.Status = 202
					}
				}
			}
		} else {
			target = volume.ID(d.ID())
			b, err = s.Bucket(target)
			if err == nil {
				description = b.Describe()
				switch m.Opcode {
				case client.BucketDelete:
					if len(m.Body) > 16 {
						deleteUsers = d.Bool()
					}
					description.State = BucketDeleting
					receipt.Running = true
					receipt.Status = 202
				case client.BucketRename:
					description.Name = d.Text()
				case client.BucketLock:
					if d.Bool() {
						description.State = BucketLocked
					} else {
						description.State = BucketActive
					}
				case client.BucketGrant:
					user := d.ID()
					grantedUser = user.String()
					permissions := BucketPermissions(d.U32())
					found := false
					for i := range description.Grants {
						if description.Grants[i].UserID == user.String() {
							description.Grants[i].Permissions = permissions
							found = true
						}
					}
					if !found {
						description.Grants = append(description.Grants, BucketGrant{UserID: user.String(), Permissions: permissions})
					}
				case client.BucketRevoke:
					user := d.ID()
					out := description.Grants[:0]
					for _, g := range description.Grants {
						if g.UserID != user.String() {
							out = append(out, g)
						}
					}
					description.Grants = out
				case client.BucketSetQuota:
					accounting := d.U8()
					g := d.U64()
					unlimited := d.Bool()
					absolute := d.U64()
					overhead := d.U32()
					expected := d.U64()
					if accounting > 1 {
						return nil, 0, ErrBucketInvalid
					}
					if expected != description.Policy.Version {
						return nil, 0, ErrBucketConflict
					}
					if description.Policy.Version == math.MaxUint64 {
						return nil, 0, ErrBucketInvalid
					}
					description.Policy.Version++
					description.Policy.Limits = BucketLimits{Accounting: []string{"logical", "physical"}[accounting], Guaranteed: g, Unlimited: unlimited, Absolute: absolute, MetadataReservePPM: overhead}
				}
			}
		}
		if err != nil {
			return nil, 0, err
		}
	}
	if d.Done() != nil {
		return nil, 0, fail(400, "invalid bucket administration message")
	}
	if b != nil && b.underMaintenance() {
		return nil, 0, fail(409, "bucket is under exclusive maintenance")
	}
	if m.Opcode == client.BucketSetQuota && description.DesiredPolicy != nil {
		return nil, 0, ErrBucketConflict
	}
	if target == systemBucket {
		return nil, 0, fail(403, "the system bucket cannot be administratively changed")
	}
	if description.State == BucketDeleting && m.Opcode != client.BucketDelete {
		return nil, 0, ErrBucketConflict
	}
	if m.Opcode == client.BucketGrant {
		if err = s.validBucketUsers([]BucketGrant{{UserID: grantedUser}}); err != nil {
			return nil, 0, err
		}
	}
	if err = bucketLocalPolicy(description.Policy); err != nil {
		return nil, 0, err
	}
	if !description.Policy.Limits.Unlimited && !s.metadataPreview {
		usage := uint64(0)
		if m.Opcode != client.BucketCreate && m.Opcode != client.BucketCreateInGroup {
			usage, err = b.accountedUsageFor(description.Policy)
			if err != nil {
				return nil, 0, err
			}
		}
		if usage > description.Policy.Limits.Absolute {
			return nil, 0, volume.ErrFull
		}
	}
	if m.Opcode == client.BucketCreate || m.Opcode == client.BucketCreateInGroup {
		// An empty bucket may be described before hardware is attached. Its writes
		// will report 507 until an eligible local volume exists; no admin-file fallback.
		receipt.Status = 201
	}
	var orphanAccounts *accountsImage
	var orphanIDs []string
	if m.Opcode == client.BucketDelete && deleteUsers {
		orphanAccounts, orphanIDs, err = s.bucketOrphanAccounts(description)
		if err != nil {
			return nil, 0, err
		}
	}
	description.Receipts = append(description.Receipts, receipt)
	revision := description.Revision
	if m.Opcode == client.BucketCreate || m.Opcode == client.BucketCreateInGroup {
		revision = 0
	}
	saved, err := s.buckets.Save(description, revision)
	if err != nil {
		return nil, 0, err
	}
	if orphanAccounts != nil {
		if err = s.writeBlob("accounts", *orphanAccounts); err != nil {
			return nil, 0, err
		}
		for _, id := range orphanIDs {
			s.invalidateUserConnections(id)
		}
		removed := make(map[string]bool, len(orphanIDs))
		for _, id := range orphanIDs {
			removed[id] = true
		}
		s.authMu.Lock()
		for key, session := range s.sessions {
			if removed[session.UserID] {
				delete(s.sessions, key)
			}
		}
		s.authMu.Unlock()
	}
	s.updateAdminCapacity()
	if m.Opcode == client.BucketDelete || description.DesiredPolicy != nil {
		s.startBucketCleanup(saved.ID())
	}
	return encodeBucketAck(receipt.ID, target, receipt.Running), receipt.Status, nil
}
func (s *Server) updateAdminCapacity() {
	size := int64(s.system.Size())
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.cfg.Volumes {
		if s.cfg.Volumes[i].System {
			s.cfg.Volumes[i].SizeBytes = size
			if m := s.volumes[s.cfg.Volumes[i].ID]; m != nil {
				m.config.SizeBytes = size
			}
		}
	}
}
