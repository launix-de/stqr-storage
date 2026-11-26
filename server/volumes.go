// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string           { return e.message }
func fail(status int, message string) error { return &apiError{status, message} }
func statusOf(e error) int {
	var aborted *volume.ReplicaAbortError
	if errors.As(e, &aborted) && aborted.Status >= 400 && aborted.Status <= 599 {
		return int(aborted.Status)
	}
	if e == nil {
		return 200
	}
	var a *apiError
	if errors.As(e, &a) {
		return a.status
	}
	switch {
	case errors.Is(e, ErrBucketCatalogMissing), errors.Is(e, ErrBucketCatalogCorrupt):
		return 503
	case errors.Is(e, ErrBucketForbidden):
		return 403
	case errors.Is(e, ErrBucketNotFound):
		return 404
	case errors.Is(e, ErrBucketConflict):
		return 409
	case errors.Is(e, ErrBucketInvalid):
		return 400
	case errors.Is(e, volume.ErrFull):
		return 507
	case errors.Is(e, volume.ErrNotFound):
		return 404
	case errors.Is(e, volume.ErrBusy), errors.Is(e, volume.ErrIdentity), errors.Is(e, volume.ErrPrecondition):
		return 409
	case errors.Is(e, volume.ErrUnsupported):
		return 501
	case errors.Is(e, volume.ErrAccountingNotReady), errors.Is(e, volume.ErrClosed), errors.Is(e, volume.ErrRecovery), errors.Is(e, volume.ErrDraining):
		return 503
	case errors.Is(e, volume.ErrRange):
		return 400
	default:
		return 500
	}
}

type AddVolumeRequest struct {
	PlacementGroups []string `json:"placement_groups,omitempty"`
	Name            string   `json:"name"`
	Source          uint8    `json:"source"`
	Path            string   `json:"path"`
	Class           string   `json:"class"`
	SizeBytes       int64    `json:"size_bytes"`
}
type Operation struct {
	ID        string `json:"operation_id"`
	State     string `json:"state"`
	VolumeID  string `json:"volume_id,omitempty"`
	BucketID  string `json:"bucket_id,omitempty"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Kind      string `json:"kind"`
	Digest    string `json:"digest"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

func copyConfig(c Config) Config {
	c.RecoveryVolumes = append([]VolumeConfig(nil), c.RecoveryVolumes...)
	c.Volumes = append([]VolumeConfig(nil), c.Volumes...)
	c.Peers = append([]PeerConfig(nil), c.Peers...)
	c.AllowSubnets = append([]string{}, c.AllowSubnets...)
	return c
}
func (s *Server) beginOperation(id, kind string, input any) (Operation, bool, error) {
	if _, e := parseID(id); e != nil {
		return Operation{}, false, fail(400, "a nonzero 128-bit operation ID is required")
	}
	if id == "00000000000000000000000000000000" {
		return Operation{}, false, fail(400, "zero operation ID is invalid")
	}
	b, e := json.Marshal(input)
	if e != nil {
		return Operation{}, false, e
	}
	digest := sha256.Sum256(append([]byte(kind+":"), b...))
	op := Operation{ID: id, Kind: kind, State: "requested", Status: 202, Digest: hex.EncodeToString(digest[:])}
	if kind == "remove" || kind == "rescan" {
		op.VolumeID, _ = input.(string)
	}
	var old Operation
	e = s.readBlob("admin-op/"+id, &old)
	if e == nil {
		if old.Digest != op.Digest {
			return old, true, fail(400, "operation ID reused with different input")
		}
		return old, true, nil
	}
	if !errors.Is(e, volume.ErrNotFound) {
		return op, false, e
	}
	e = s.writeBlob("admin-op/"+id, op)
	return op, false, e
}
func (s *Server) saveOperation(op Operation) error { return s.writeBlob("admin-op/"+op.ID, op) }
func (s *Server) operation(id string) (Operation, error) {
	var op Operation
	if _, e := parseID(id); e != nil {
		return op, fail(400, "invalid operation ID")
	}
	e := s.readBlob("admin-op/"+id, &op)
	if errors.Is(e, volume.ErrNotFound) {
		parsed, _ := parseID(id)
		if s.completedExclusionProposal(client.ID(parsed)) != nil {
			return Operation{ID: id, Kind: "force-disconnect", State: "fulfilled", Status: 200}, nil
		}
		if s.pendingExclusionClaim(client.ID(parsed)) != nil {
			return Operation{ID: id, Kind: "force-disconnect", State: "running", Status: 202}, nil
		}
		for _, d := range s.bucketSnapshots() {
			for _, r := range d.Receipts {
				if r.ID == id {
					state := "fulfilled"
					if r.Running {
						state = "running"
					}
					if r.Status >= 400 {
						state = "failed"
					}
					return Operation{ID: id, BucketID: d.ID, Kind: "bucket", Status: r.Status, State: state}, nil
				}
			}
		}
	}
	return op, e
}
func (s *Server) finishOperation(op Operation, e error) (Operation, error) {
	if e != nil {
		op.State = "failed"
		op.Status = statusOf(e)
		op.Detail = e.Error()
	} else {
		op.State = "fulfilled"
		op.Status = 200
		if op.Kind == "add" {
			op.Status = 201
		}
		op.Detail = "Abgeschlossen"
	}
	if saveErr := s.saveOperation(op); saveErr != nil {
		return op, saveErr
	}
	if e == nil {
		s.notifyStats()
	}
	return op, e
}

var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (s *Server) AddVolume(id string, r AddVolumeRequest) (op Operation, err error) {
	if s.storageWarming.Load() {
		return op, s.storageUnavailable()
	}
	s.opMu.Lock()
	s.dirtyVolumeAuthority()
	defer s.opMu.Unlock()
	if s.forceFrozen() {
		return op, fail(503, "administration is frozen for exclusion")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return op, fail(503, "server is stopping")
	}
	if err := s.validateVolumeGroupSelection(r.PlacementGroups); err != nil {
		return op, err
	}
	op, exists, e := s.beginOperation(id, "add", r)
	if e != nil || exists {
		return op, e
	}
	defer func() {
		if err != nil {
			op, err = s.finishOperation(op, err)
		}
	}()
	if !validName.MatchString(r.Name) || r.Name == "admin" {
		return op, fail(400, "choose a simple volume name (1–64 letters, numbers, dots, underscores or hyphens); admin is reserved")
	}
	class, e := classFromString(r.Class)
	if e != nil {
		return op, fail(400, e.Error())
	}
	s.mu.RLock()
	c := copyConfig(s.cfg)
	s.mu.RUnlock()
	for _, v := range c.Volumes {
		if v.Name == r.Name {
			return op, fail(409, "a volume with that name already exists")
		}
	}
	if r.Source > 2 {
		return op, fail(400, "invalid volume source")
	}
	path := ""
	switch r.Source {
	case 0:
		if class == volume.RAM {
			return op, fail(400, "use the RAM source for RAM volumes")
		}
		path = filepath.Join(s.volumesDir, r.Name)
		if r.Path != "" && filepath.Clean(r.Path) != path {
			return op, fail(400, "file volumes must be directly inside VOLUMES_DIR under their name")
		}
		if st, e := os.Lstat(path); e == nil {
			if !st.Mode().IsRegular() {
				return op, fail(400, "target must be a regular file, not a symlink")
			}
			if r.SizeBytes != 0 && r.SizeBytes != st.Size() {
				return op, fail(409, "an existing file cannot be resized implicitly")
			}
		} else if !os.IsNotExist(e) {
			return op, e
		} else {
			available, e := filesystemAvailable(s.volumesDir)
			if e != nil {
				return op, e
			}
			if r.SizeBytes > 0 && uint64(r.SizeBytes) > alignedCapacity(available) {
				return op, fail(507, "requested file volume exceeds available filesystem space")
			}
		}
	case 1:
		if class == volume.RAM {
			return op, fail(400, "a blockdevice must be HDD or SSD")
		}
		candidates, e := volume.ListBlockdevices(s.volumesDir)
		if e != nil {
			return op, e
		}
		for _, v := range candidates {
			if v.Type == "file" {
				continue
			}
			if v.Path == r.Path {
				path = v.Path
			}
			for _, alias := range v.Aliases {
				if alias == r.Path {
					path = v.Path
				}
			}
		}
		if path == "" {
			return op, fail(409, "blockdevice is mounted, claimed or no longer available; refresh device discovery")
		}
	case 2:
		if class != volume.RAM {
			return op, fail(400, "RAM source requires RAM class")
		}
		capacity, e := s.volumeCapacity()
		if e != nil {
			return op, e
		}
		if r.SizeBytes < volume.MinVolumeSize {
			return op, fail(400, "volume must be at least 256 KiB")
		}
		if uint64(r.SizeBytes) > alignedCapacity(capacity.RAMLimit) {
			return op, fail(507, "requested RAM volume exceeds available RAM or the configured remaining budget")
		}
		path = r.Name
	}
	if r.Source != 1 && r.SizeBytes < volume.MinVolumeSize {
		if st, e := os.Stat(path); e != nil || st.Size() < volume.MinVolumeSize {
			return op, fail(400, "volume must be at least 256 KiB")
		}
	}
	v, e := volume.New(path, r.SizeBytes, class)
	if e != nil {
		return op, e
	}
	keep := false
	defer func() {
		if !keep {
			v.Close()
		}
	}()
	node, _ := parseID(c.Node.ID)
	cluster, _ := parseID(c.ClusterID)
	if e = v.Bind(cluster, node); e != nil {
		return op, e
	}
	vid := v.Identity().VolumeID.String()
	if _, ok := s.volumes[vid]; ok {
		return op, fail(409, "volume is already attached")
	}
	actual := v.Identity().Class.String()
	vc := VolumeConfig{PlacementGroupsPending: append([]string(nil), r.PlacementGroups...), ID: vid, Name: r.Name, Path: path, Class: actual, SizeBytes: int64(v.Size()), State: "pending", OperationID: id}
	if e = v.SetState(volume.Pending); e != nil {
		return op, e
	}
	c.Volumes = append(c.Volumes, vc)
	// Persist PENDING first, then commit a trivial one-node placement generation.
	// TODO(multinode): offer rebuild, collect fragments and wait for committed NEXT
	// before making this volume a regular placement target.
	if e = saveConfig(s.configPath, c); e != nil {
		return op, e
	}
	c.Generation++
	c.Volumes[len(c.Volumes)-1].State = "active"
	vc.State = "active"
	if e = saveConfig(s.configPath, c); e != nil {
		return op, e
	}
	if e = v.SetState(volume.Active); e != nil {
		return op, e
	}
	s.mu.Lock()
	s.cfg = c
	s.volumes[vid] = &managedVolume{config: vc, store: v}
	s.mu.Unlock()
	keep = true
	op.VolumeID = vid
	op, err = s.finishOperation(op, nil)
	return op, err
}
func (s *Server) RemoveVolume(id, vid string) (Operation, error) {
	if s.storageWarming.Load() {
		return Operation{}, s.storageUnavailable()
	}
	s.opMu.Lock()
	s.dirtyVolumeAuthority()
	defer s.opMu.Unlock()
	if s.forceFrozen() {
		return Operation{}, fail(503, "administration is frozen for exclusion")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return Operation{}, fail(503, "server is stopping")
	}
	op, exists, e := s.beginOperation(id, "remove", vid)
	if e != nil || exists {
		return op, e
	}
	s.mu.RLock()
	m := s.volumes[vid]
	c := copyConfig(s.cfg)
	s.mu.RUnlock()
	if m == nil {
		return s.finishOperation(op, fail(404, "volume not found"))
	}
	if m.config.System {
		return s.finishOperation(op, fail(409, "the admin volume holds system accounts and must remain attached in this single-node version"))
	}
	if m.config.State == "draining" {
		return s.finishOperation(op, fail(409, "volume is already draining"))
	}
	for i := range c.Volumes {
		if c.Volumes[i].ID == vid {
			c.Volumes[i].State = "draining"
			c.Volumes[i].OperationID = id
		}
	}
	c.Generation++
	if e = saveConfig(s.configPath, c); e != nil {
		return s.finishOperation(op, e)
	}
	if s.controller == nil {
		if e = m.store.SetState(volume.Draining); e != nil {
			return s.finishOperation(op, e)
		}
	}
	s.mu.Lock()
	s.cfg = c
	m.config.State = "draining"
	m.config.OperationID = id
	s.mu.Unlock()
	op.VolumeID = vid
	op.State = "running"
	op.Detail = "Volume wird kooperativ geleert"
	if e = s.saveOperation(op); e != nil {
		return op, e
	}
	s.startDrain(id, vid)
	s.notifyStats()
	return op, nil
}
func (s *Server) candidates() ([]volume.BlockDevice, error) {
	return volume.ListBlockdevices(s.volumesDir)
}
func (s *Server) configView() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := copyConfig(s.cfg)
	restart := c.Node.Listen != s.running.Node.Listen || c.Node.DataDir != s.running.Node.DataDir || c.VolumesDir != s.running.VolumesDir || c.RAMBytes != s.running.RAMBytes || c.RAMCacheBytes != s.running.RAMCacheBytes
	mode := "single-node"
	if c.JoinPending {
		mode = "joining"
	} else if len(c.Peers) > 0 {
		mode = "cluster-preparation"
	}
	view := map[string]any{"config": c, "config_path": s.configPath, "active_listen": s.running.Node.Listen, "active_volumes_dir": s.volumesDir, "restart_required": restart, "mode": mode, "system_bucket_placement": "everywhere", "node_versions": s.versionsView()}
	if image := s.currentSystem.Load(); image != nil {
		members, excluded := []string{}, []string{}
		for _, member := range image.manifest.Membership.Members {
			members = append(members, member.ID.String())
		}
		for _, node := range image.manifest.Membership.Excluded {
			excluded = append(excluded, node.String())
		}
		view["member_nodes"], view["excluded_nodes"] = members, excluded
		view["membership_epoch"] = strconv.FormatUint(image.manifest.Membership.Epoch, 10)
		recoveries := []map[string]any{}
		for _, origin := range image.manifest.VolumeRecoveries {
			volumes := []string{}
			for _, id := range origin.Volumes {
				volumes = append(volumes, id.String())
			}
			recoveries = append(recoveries, map[string]any{"bucket": origin.Before.Bucket.String(), "source": origin.Before.Entries[0].Locations[0].Node.String(), "complete": origin.Complete, "target": origin.Target.String(), "volumes": volumes})
		}
		view["volume_recoveries"] = recoveries
		if len(members) > 1 {
			view["mode"] = "cluster"
		} else {
			view["mode"] = "single-node"
		}
	}
	return view
}
func (s *Server) updateConfig(patch map[string]json.RawMessage) error {
	if s.storageWarming.Load() {
		return s.storageUnavailable()
	}
	s.opMu.Lock()
	s.dirtyVolumeAuthority()
	defer s.opMu.Unlock()
	if s.forceFrozen() {
		return fail(503, "administration is frozen for exclusion")
	}
	s.mu.RLock()
	c := copyConfig(s.cfg)
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return fail(503, "server is stopping")
	}
	// Field updates are typed and allowlisted; immutable IDs and managed volumes
	// cannot be replaced through an arbitrary configuration document.
	for key, raw := range patch {
		if string(raw) == "null" {
			return fail(400, "null is not a configuration field value")
		}
		var dst any
		switch key {
		case "name":
			dst = &c.Node.Name
		case "listen":
			dst = &c.Node.Listen
		case "advertise":
			dst = &c.Node.Advertise
		case "data_dir":
			dst = &c.Node.DataDir
		case "volumes_dir":
			dst = &c.VolumesDir
		case "ram_bytes":
			dst = &c.RAMBytes
		case "ramcache_bytes":
			dst = &c.RAMCacheBytes
		case "system_volume_bytes":
			dst = &c.SystemVolumeBytes
		case "allow_subnets":
			dst = &c.AllowSubnets
		case "peers":
			return fail(501, "peer configuration requires the future multinode join implementation")
		default:
			return fail(400, fmt.Sprintf("unknown or immutable configuration field %q", key))
		}
		if e := json.Unmarshal(raw, dst); e != nil {
			return fail(400, "invalid value for "+key)
		}
	}
	if e := validateConfig(c); e != nil {
		return fail(400, e.Error())
	}
	if e := saveConfig(s.configPath, c); e != nil {
		return e
	}
	s.mu.Lock()
	s.cfg = c
	s.mu.Unlock()
	s.invalidateDisallowedConnections()
	s.notifyStats()
	return nil
}

// RescanVolume adopts capacity added externally (e.g. lvextend). It does not
// resize an LV, reread partition tables or format an already attached device.
func (s *Server) RescanVolume(id, vid string) (op Operation, err error) {
	if s.storageWarming.Load() {
		return op, s.storageUnavailable()
	}
	s.opMu.Lock()
	s.dirtyVolumeAuthority()
	defer s.opMu.Unlock()
	if s.forceFrozen() {
		return op, fail(503, "administration is frozen for exclusion")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return op, fail(503, "server is stopping")
	}
	op, exists, e := s.beginOperation(id, "rescan", vid)
	if e != nil || exists {
		return op, e
	}
	s.mu.RLock()
	m := s.volumes[vid]
	s.mu.RUnlock()
	if m == nil {
		return s.finishOperation(op, fail(404, "volume not found"))
	}
	if m.config.State != "active" {
		return s.finishOperation(op, fail(409, "only active volumes can be rescanned"))
	}
	size, e := m.store.Rescan()
	if e != nil {
		return s.finishOperation(op, e)
	}
	s.mu.Lock()
	c := copyConfig(s.cfg)
	for i := range c.Volumes {
		if c.Volumes[i].ID == vid {
			c.Volumes[i].SizeBytes = int64(size)
		}
	}
	c.Generation++
	e = saveConfig(s.configPath, c)
	if e == nil {
		s.cfg = c
		m.config.SizeBytes = int64(size)
	}
	s.mu.Unlock()
	op.VolumeID = vid
	return s.finishOperation(op, e)
}
