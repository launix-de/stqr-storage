// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Metadata persistence does not allocate placement, enforce quotas, or implement
// distributed CONTROL_CHANGE/ALL commits. Those remain integration work.

var (
	ErrBucketNotFound       = errors.New("bucket: not found")
	ErrBucketConflict       = errors.New("bucket: metadata conflict")
	ErrBucketInvalid        = errors.New("bucket: invalid description")
	ErrBucketForbidden      = errors.New("bucket: access denied")
	ErrBucketCatalogMissing = errors.New("bucket: catalog missing; explicit initialization required")
	ErrBucketCatalogCorrupt = errors.New("bucket: corrupt or unsupported catalog")
)

type BucketPermissions uint32

const (
	BucketRead BucketPermissions = 1 << iota
	BucketWrite
	BucketAppend
	BucketDelete
	BucketAllPermissions = BucketRead | BucketWrite | BucketAppend | BucketDelete
)

type BucketState string

const (
	BucketActive   BucketState = "active"
	BucketLocked   BucketState = "locked"
	BucketDeleting BucketState = "deleting"
)

type BucketRedundancy struct {
	Mode     string `json:"mode"` // single | replication | erasure (reserved)
	Replicas uint16 `json:"replicas"`
	ACK      string `json:"ack"` // v1: all
	ECK      uint16 `json:"ec_k,omitempty"`
	ECM      uint16 `json:"ec_m,omitempty"`
}

// BucketReadAcceleration describes optional disposable RAM read copies.
type BucketReadAcceleration struct {
	Enabled     bool   `json:"enabled"`
	MaxCopies   uint16 `json:"max_copies_per_hot_range"`
	BytesBudget uint64 `json:"cache_bytes_budget"`
}
type BucketLimits struct {
	Accounting         string `json:"quota_accounting"` // logical | physical
	MetadataReservePPM uint32 `json:"metadata_reserve_ppm"`
	Guaranteed         uint64 `json:"guaranteed_quota_bytes"`
	Absolute           uint64 `json:"absolute_quota_bytes"`
	Unlimited          bool   `json:"absolute_unlimited"`
}
type BucketPolicy struct {
	Version              uint64                 `json:"version"`
	StorageStrategy      string                 `json:"storage_strategy"` // hdd | ssd | ram; cache reserved
	Compression          string                 `json:"compression"`      // raw | gzip | lz4 | xz
	PlacementMode        string                 `json:"placement_mode"`   // count | everywhere
	Redundancy           BucketRedundancy       `json:"redundancy"`
	ReadAcceleration     BucketReadAcceleration `json:"read_acceleration"`
	Limits               BucketLimits           `json:"limits"`
	PlacementGroup       string                 `json:"placement_group,omitempty"`
	PlacementMaskVersion uint64                 `json:"placement_mask_version,omitempty"`
}
type BucketGrant struct {
	UserID      string            `json:"user_id"` // canonical 128-bit hex ID, not a username
	Permissions BucketPermissions `json:"permissions"`
}

// BucketDescription is the complete versioned self-description persisted as an
// ordinary object in the system bucket. Revision tracks catalog edits; Policy's
// Version tracks policy revisions. DesiredPolicy never silently replaces Policy.
// No credential, local volume pointer, or mutable runtime counter belongs here.
type BucketDescription struct {
	Schema        uint16        `json:"schema"`
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	State         BucketState   `json:"state"`
	Revision      uint64        `json:"revision"`
	Policy        BucketPolicy  `json:"policy"`
	DesiredPolicy *BucketPolicy `json:"desired_policy,omitempty"`
	// Grants is the bucket-local user sub-array. It stores stable account IDs
	// and per-user permissions; account/password records stay in the user list.
	Grants           []BucketGrant   `json:"grants"`
	Receipts         []BucketReceipt `json:"receipts,omitempty"`
	DeletionComplete bool            `json:"deletion_complete,omitempty"`
	// LocalVolumes is the committed single-node placement until range maps own it.
	LocalVolumes []string `json:"local_volumes,omitempty"`
}

// Bucket is the server's stable RAM instance for one namespace. Catalog writes
// atomically publish a new description into this instance; Describe returns a
// detached snapshot. Object methods enforce this bucket's current permissions,
// policy and placement. Standalone catalog snapshots have no attached server.
type Bucket struct {
	io          bucketIO
	lease       *bucketLease
	mu          sync.RWMutex
	usage       bucketUsage
	description BucketDescription
	server      *Server
	maintenance *connectionAuthorization // exclusive, socket-scoped fsck owner
}

func (b *Bucket) ID() volume.ID {
	b.mu.RLock()
	defer b.mu.RUnlock()
	id, _ := bucketID(b.description.ID)
	return id
}
func (b *Bucket) Describe() BucketDescription {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return cloneBucketDescription(b.description)
}

// Internal snapshots reference immutable slices: publication replaces the
// complete description. Data operations never copy/scan account grant arrays.
func (b *Bucket) storageDescription() BucketDescription {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.description
}
func cloneBucketDescription(d BucketDescription) BucketDescription {
	d.Grants = append([]BucketGrant(nil), d.Grants...)
	d.Receipts = append([]BucketReceipt(nil), d.Receipts...)
	d.LocalVolumes = append([]string(nil), d.LocalVolumes...)
	if d.DesiredPolicy != nil {
		p := *d.DesiredPolicy
		d.DesiredPolicy = &p
	}
	return d
}
func DefaultBucketPolicy() BucketPolicy {
	overhead, _ := volume.EstimatePutOverheadPPM(1 << 20)
	return BucketPolicy{Version: 1, PlacementGroup: defaultPlacementGroup.String(), PlacementMaskVersion: 1, StorageStrategy: "hdd", Compression: "raw", PlacementMode: "count", Redundancy: BucketRedundancy{Mode: "single", Replicas: 1, ACK: "all"}, Limits: BucketLimits{Accounting: "logical", MetadataReservePPM: overhead, Unlimited: true}}
}
func NewBucket(id volume.ID, name string) (*Bucket, error) {
	d := BucketDescription{Schema: 1, ID: id.String(), Name: name, State: BucketActive, Revision: 1, Policy: DefaultBucketPolicy()}
	if id == systemBucket {
		d.Policy.PlacementMode = "everywhere"
		d.Policy.Redundancy = BucketRedundancy{Mode: "replication", ACK: "all"}
		d.Policy.PlacementGroup = systemPlacementGroup.String()
		d.Policy.PlacementMaskVersion = 0
	}
	d, err := normalizeBucketDescription(d)
	if err != nil {
		return nil, err
	}
	return &Bucket{description: d}, nil
}
func bucketID(s string) (volume.ID, error) {
	var id volume.ID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 16 || s != strings.ToLower(s) {
		return id, ErrBucketInvalid
	}
	copy(id[:], b)
	if id == (volume.ID{}) {
		return id, ErrBucketInvalid
	}
	return id, nil
}
func bucketEnum(value string, values ...string) (uint8, bool) {
	for i, s := range values {
		if s == value {
			return uint8(i), true
		}
	}
	return 0, false
}
func validateBucketPolicy(p BucketPolicy) error {
	if p.PlacementGroup == "" && p.PlacementMaskVersion != 0 {
		return ErrBucketInvalid
	}
	if p.Version == 0 {
		return ErrBucketInvalid
	}
	if p.PlacementGroup != "" {
		if _, err := bucketID(p.PlacementGroup); err != nil {
			return ErrBucketInvalid
		}
	}
	if _, ok := bucketEnum(p.StorageStrategy, "hdd", "ssd", "ram"); !ok {
		return ErrBucketInvalid
	}
	if _, ok := bucketEnum(p.Compression, "raw", "gzip", "lz4", "xz"); !ok {
		return ErrBucketInvalid
	}
	if _, ok := bucketEnum(p.PlacementMode, "count", "everywhere"); !ok {
		return ErrBucketInvalid
	}
	if p.Redundancy.ACK != "all" || p.Redundancy.ECK != 0 || p.Redundancy.ECM != 0 {
		return ErrBucketInvalid
	}
	switch p.Redundancy.Mode {
	case "single":
		if p.Redundancy.Replicas != 1 || p.PlacementMode != "count" {
			return ErrBucketInvalid
		}
	case "replication":
		if p.PlacementMode == "count" && p.Redundancy.Replicas < 1 {
			return ErrBucketInvalid
		}
		if p.PlacementMode == "everywhere" && p.Redundancy.Replicas != 0 {
			return ErrBucketInvalid
		}
	default:
		return ErrBucketInvalid
	}
	if _, ok := bucketEnum(p.Limits.Accounting, "logical", "physical"); !ok {
		return ErrBucketInvalid
	}
	if !p.Limits.Unlimited && p.Limits.Guaranteed > p.Limits.Absolute {
		return ErrBucketInvalid
	}
	if p.ReadAcceleration.Enabled && (p.ReadAcceleration.MaxCopies == 0 || p.ReadAcceleration.BytesBudget == 0) {
		return ErrBucketInvalid
	}
	return nil
}
func normalizeBucketDescription(d BucketDescription) (BucketDescription, error) {
	d = cloneBucketDescription(d)
	id, err := bucketID(d.ID)
	if err != nil || d.Schema != 1 || d.Revision == 0 || len(d.Name) == 0 || len(d.Name) > 255 || !utf8.ValidString(d.Name) || strings.ContainsRune(d.Name, 0) {
		return d, ErrBucketInvalid
	}
	if _, ok := bucketEnum(string(d.State), "active", "locked", "deleting"); !ok {
		return d, ErrBucketInvalid
	}
	if err = validateBucketPolicy(d.Policy); err != nil {
		return d, err
	}
	if d.Policy.PlacementGroup != "" {
		group, _ := bucketID(d.Policy.PlacementGroup)
		d.Policy.PlacementGroup = group.String()
	}
	if d.DeletionComplete && (d.State != BucketDeleting || id == systemBucket) {
		return d, ErrBucketInvalid
	}
	for _, v := range d.LocalVolumes {
		if _, e := bucketID(v); e != nil {
			return d, ErrBucketInvalid
		}
	}
	for _, r := range d.Receipts {
		if _, e := bucketID(r.ID); e != nil {
			return d, ErrBucketInvalid
		}
		if len(r.Digest) != 64 || r.Actor == "" {
			return d, ErrBucketInvalid
		}
	}
	if d.DesiredPolicy != nil {
		if err = validateBucketPolicy(*d.DesiredPolicy); err != nil || d.DesiredPolicy.Version <= d.Policy.Version {
			return d, ErrBucketInvalid
		}
		if d.DesiredPolicy.PlacementGroup != "" {
			group, _ := bucketID(d.DesiredPolicy.PlacementGroup)
			d.DesiredPolicy.PlacementGroup = group.String()
		}
	}
	sort.Slice(d.Grants, func(i, j int) bool { return d.Grants[i].UserID < d.Grants[j].UserID })
	for i, g := range d.Grants {
		if _, err = bucketID(g.UserID); err != nil || g.Permissions == 0 || g.Permissions & ^BucketAllPermissions != 0 || (i > 0 && d.Grants[i-1].UserID == g.UserID) {
			return d, ErrBucketInvalid
		}
	}
	if id == systemBucket {
		validSystem := func(p BucketPolicy) bool {
			return p.StorageStrategy == "hdd" && p.Compression == "raw" && p.PlacementMode == "everywhere" && p.Redundancy.Mode == "replication" && !p.ReadAcceleration.Enabled && p.Limits.Unlimited && p.Limits.Guaranteed == 0 && (p.PlacementGroup == "" || p.PlacementGroup == systemPlacementGroup.String())
		}
		if d.Name != "system" || d.State != BucketActive || len(d.Grants) != 0 || !validSystem(d.Policy) || (d.DesiredPolicy != nil && !validSystem(*d.DesiredPolicy)) {
			return d, ErrBucketInvalid
		}
	} else if d.Name == "system" {
		return d, ErrBucketInvalid
	}
	return d, nil
}

// BucketInfo is the BUCKET_INFO result: a public policy and the caller's rights,
// never the ACL list. MarshalBinary encodes Policy + permissions:u32 from §13.5,
// without response envelope/status. The dispatcher looks up the name, obtains
// Info for its authenticated principal, and wraps these result bytes.
type BucketInfo struct {
	description BucketDescription
	Permissions BucketPermissions
}

func (b *Bucket) Info(user volume.ID, admin bool) (BucketInfo, error) {
	description := b.Describe()
	if description.DeletionComplete {
		return BucketInfo{}, ErrBucketNotFound
	}
	var permissions BucketPermissions
	if admin {
		permissions = BucketAllPermissions
	} else {
		if b.ID() == systemBucket {
			return BucketInfo{}, ErrBucketForbidden
		}
		for _, g := range description.Grants {
			if g.UserID == user.String() {
				permissions = g.Permissions
				break
			}
		}
		if permissions == 0 {
			return BucketInfo{}, ErrBucketForbidden
		}
	}
	if description.State != BucketActive {
		permissions &= BucketRead
	}
	return BucketInfo{description: description, Permissions: permissions}, nil
}
func (i BucketInfo) Describe() BucketDescription {
	d := cloneBucketDescription(i.description)
	d.Grants = nil
	d.DesiredPolicy = nil
	d.Receipts = nil
	return d
}
func (i BucketInfo) MarshalBinary() ([]byte, error) {
	d, err := normalizeBucketDescription(i.description)
	if err != nil || i.Permissions & ^BucketAllPermissions != 0 {
		return nil, ErrBucketInvalid
	}
	id, _ := bucketID(d.ID)
	p := d.Policy
	e := client.Encoder{}
	e.ID(client.ID(id))
	e.U64(p.Version)
	e.Text(d.Name)
	state, _ := bucketEnum(string(d.State), "active", "locked", "deleting")
	e.U8(state)
	strategy, _ := bucketEnum(p.StorageStrategy, "hdd", "ssd", "ram")
	e.U8(strategy)
	codec, _ := bucketEnum(p.Compression, "raw", "gzip", "lz4", "xz")
	e.U8(codec)
	placement, _ := bucketEnum(p.PlacementMode, "count", "everywhere")
	e.U8(placement)
	redundancy, _ := bucketEnum(p.Redundancy.Mode, "single", "replication")
	e.U8(redundancy)
	e.U16(p.Redundancy.Replicas)
	e.U16(p.Redundancy.ECK)
	e.U16(p.Redundancy.ECM)
	e.Bool(p.ReadAcceleration.Enabled)
	e.U16(p.ReadAcceleration.MaxCopies)
	e.U64(p.ReadAcceleration.BytesBudget)
	accounting, _ := bucketEnum(p.Limits.Accounting, "logical", "physical")
	e.U8(accounting)
	e.U64(p.Limits.Guaranteed)
	e.Bool(p.Limits.Unlimited)
	e.U64(p.Limits.Absolute)
	e.U32(p.Limits.MetadataReservePPM)
	e.U32(uint32(i.Permissions))
	return e.B, nil
}

// BucketCatalog stores immutable, content-addressed configuration objects in the
// admin volume's system bucket. The fixed list object contains ONLY sorted raw
// 32-byte SHA-256 object hashes; its versioned key defines this list's format.
// An update first syncs the complete config, then CAS-publishes a replacement
// list. A crash leaves either the old catalog or the complete new one; orphaned
// unpublished configs are retained. Multiple catalog handles use the list CAS,
// never an in-memory lock as a substitute for a durable commit.
// Metadata objects currently use the volume's bounded convenience reads and
// must fit MaxBufferedRead. Payload object sizes are unrelated to this budget.
type BucketCatalog struct {
	admin     *volume.Volume
	onSave    func(BucketDescription)
	upgrading bool // startup-only enrichment of unchanged legacy storage contracts
}

func BucketCatalogKey() volume.Key {
	return volume.Key{Bucket: systemBucket, Object: volume.ObjectID(sha256.Sum256([]byte("STQR bucket catalog v1")))}
}
func NewBucketCatalog(admin *volume.Volume) (*BucketCatalog, error) {
	if admin == nil {
		return nil, ErrBucketInvalid
	}
	class := admin.Identity().Class
	if class != volume.HDD && class != volume.SSD {
		return nil, ErrBucketInvalid
	}
	return &BucketCatalog{admin: admin}, nil
}

// Initialize is an explicit bootstrap, never called implicitly by List/Lookup.
// The caller must authorize creation of a new catalog (including upgrade from a
// pre-catalog admin volume). A missing established catalog is a recovery error,
// not permission to call Initialize and discard the former bucket list.
func (c *BucketCatalog) Initialize() error {
	b, err := NewBucket(systemBucket, "system")
	if err != nil {
		return err
	}
	return c.initializeDescription(b.description)
}
func (c *BucketCatalog) initializeDescription(d BucketDescription) error {
	d, err := normalizeBucketDescription(d)
	if err != nil {
		return err
	}
	hash, err := c.persistDescription(d)
	if err != nil {
		return err
	}
	return c.write(func(h *volume.Handle) error {
		_, err := h.CAS(BucketCatalogKey(), 0, nil, hash[:], volume.Options{WholeObject: true, CreateOnly: true})
		if errors.Is(err, volume.ErrPrecondition) {
			return ErrBucketConflict
		}
		return err
	})
}
func strictBucketJSON(data []byte) (BucketDescription, error) {
	var d BucketDescription
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&d); err != nil {
		return d, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return d, ErrBucketCatalogCorrupt
	}
	normalized, err := normalizeBucketDescription(d)
	if err != nil {
		return d, err
	}
	canonical, err := json.Marshal(normalized)
	if err != nil || !bytes.Equal(canonical, data) {
		return d, ErrBucketCatalogCorrupt
	}
	return normalized, nil
}
func (c *BucketCatalog) load() ([]volume.ObjectID, []BucketDescription, uint64, error) {
	h, err := c.admin.Open(false)
	if err != nil {
		return nil, nil, 0, err
	}
	defer h.Close()
	data, info, err := h.Get(BucketCatalogKey())
	if errors.Is(err, volume.ErrNotFound) {
		return nil, nil, 0, ErrBucketCatalogMissing
	}
	if err != nil {
		return nil, nil, 0, err
	}
	if len(data) == 0 || len(data)%32 != 0 {
		return nil, nil, 0, ErrBucketCatalogCorrupt
	}
	hashes := make([]volume.ObjectID, len(data)/32)
	descriptions := make([]BucketDescription, 0, len(hashes))
	ids := map[string]bool{}
	names := map[string]bool{}
	hasSystem := false
	for i := range hashes {
		copy(hashes[i][:], data[i*32:(i+1)*32])
		if i > 0 && bytes.Compare(hashes[i-1][:], hashes[i][:]) >= 0 {
			return nil, nil, 0, ErrBucketCatalogCorrupt
		}
		payload, _, err := h.Get(volume.Key{Bucket: systemBucket, Object: hashes[i]})
		if err != nil {
			return nil, nil, 0, fmt.Errorf("%w: description %x: %w", ErrBucketCatalogCorrupt, hashes[i], err)
		}
		if sha256.Sum256(payload) != [32]byte(hashes[i]) {
			return nil, nil, 0, ErrBucketCatalogCorrupt
		}
		d, err := strictBucketJSON(payload)
		if err != nil || ids[d.ID] || (!d.DeletionComplete && names[d.Name]) {
			return nil, nil, 0, ErrBucketCatalogCorrupt
		}
		ids[d.ID] = true
		if !d.DeletionComplete {
			names[d.Name] = true
		}
		descriptions = append(descriptions, d)
		hasSystem = hasSystem || d.ID == systemBucket.String()
	}
	if !hasSystem {
		return nil, nil, 0, ErrBucketCatalogCorrupt
	}
	return hashes, descriptions, info.Version, nil
}
func (c *BucketCatalog) List() ([]*Bucket, error) {
	_, descriptions, _, err := c.load()
	if err != nil {
		return nil, err
	}
	out := make([]*Bucket, 0, len(descriptions))
	for _, d := range descriptions {
		out = append(out, &Bucket{description: d})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].description.Name < out[j].description.Name })
	return out, nil
}
func (c *BucketCatalog) Lookup(id volume.ID) (*Bucket, error) {
	list, err := c.List()
	if err != nil {
		return nil, err
	}
	for _, b := range list {
		if b.ID() == id {
			return b, nil
		}
	}
	return nil, ErrBucketNotFound
}
func (c *BucketCatalog) LookupName(name string) (*Bucket, error) {
	list, err := c.List()
	if err != nil {
		return nil, err
	}
	for _, b := range list {
		if b.description.Name == name {
			return b, nil
		}
	}
	return nil, ErrBucketNotFound
}

// Save creates (expectedRevision=0) or replaces a description. It never removes
// a catalog entry: deletion is first described by State=deleting. Payload cleanup
// and physical reservations are intentionally outside this metadata library.
func (c *BucketCatalog) Save(d BucketDescription, expectedRevision uint64) (*Bucket, error) {
	if expectedRevision == math.MaxUint64 {
		return nil, ErrBucketInvalid
	}
	d.Schema = 1
	d.Revision = expectedRevision + 1
	d, err := normalizeBucketDescription(d)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 64; attempt++ {
		hashes, descriptions, version, err := c.load()
		if err != nil {
			return nil, err
		}
		index := -1
		for i, current := range descriptions {
			if current.ID == d.ID {
				index = i
				if current.Revision != expectedRevision {
					return nil, ErrBucketConflict
				}
				if !c.upgrading && (d.Policy.Version < current.Policy.Version || (d.Policy != current.Policy && d.Policy.Version == current.Policy.Version)) {
					return nil, ErrBucketInvalid
				}
			} else if current.Name == d.Name && !current.DeletionComplete && !d.DeletionComplete {
				return nil, ErrBucketConflict
			}
		}
		if (index < 0 && expectedRevision != 0) || (index >= 0 && expectedRevision == 0) {
			return nil, ErrBucketConflict
		}
		hash, err := c.persistDescription(d)
		if err != nil {
			return nil, err
		}
		if index < 0 {
			hashes = append(hashes, hash)
		} else {
			hashes[index] = hash
		}
		sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
		var list []byte
		for _, id := range hashes {
			list = append(list, id[:]...)
		}
		if len(list) > volume.MaxBufferedRead {
			return nil, volume.ErrRange
		}
		err = c.write(func(h *volume.Handle) error {
			_, err := h.CAS(BucketCatalogKey(), 0, nil, list, volume.Options{WholeObject: true, ExpectedVersion: &version})
			return err
		})
		if errors.Is(err, volume.ErrPrecondition) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if c.onSave != nil {
			c.onSave(cloneBucketDescription(d))
		}
		return &Bucket{description: cloneBucketDescription(d)}, nil
	}
	return nil, ErrBucketConflict
}
func (c *BucketCatalog) persistDescription(d BucketDescription) (volume.ObjectID, error) {
	data, err := json.Marshal(d)
	if err != nil {
		return volume.ObjectID{}, err
	}
	if len(data) > volume.MaxBufferedRead {
		return volume.ObjectID{}, volume.ErrRange
	}
	hash := volume.ObjectID(sha256.Sum256(data))
	key := volume.Key{Bucket: systemBucket, Object: hash}
	err = c.write(func(h *volume.Handle) error {
		existing, _, err := h.Get(key)
		if err == nil {
			if !bytes.Equal(existing, data) {
				return ErrBucketCatalogCorrupt
			}
			return nil
		}
		if !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		_, err = h.CAS(key, 0, nil, data, volume.Options{WholeObject: true, CreateOnly: true})
		return err
	})
	return hash, err
}

// write releases every handle before growing a full regular admin file. It does
// not resize raw devices, clear a recovery fence, or update server config; later
// integration can publish the new capacity through its normal management path.
func (c *BucketCatalog) write(fn func(*volume.Handle) error) error {
	for {
		h, err := c.admin.Open(true)
		if err != nil {
			return err
		}
		err = fn(h)
		h.Close()
		if !errors.Is(err, volume.ErrFull) || errors.Is(err, volume.ErrRecovery) {
			return err
		}
		size := c.admin.Size()
		if size > math.MaxInt64/2 {
			return volume.ErrFull
		}
		if growErr := c.admin.Grow(int64(size * 2)); growErr != nil {
			return errors.Join(err, growErr)
		}
	}
}
