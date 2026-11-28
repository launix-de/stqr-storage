// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ClusterStopID          string `yaml:"cluster_stop_id,omitempty" json:"-"`
	ClusterResumedID       string `yaml:"cluster_resumed_id,omitempty" json:"-"`
	ClusterShutdownRequest string `yaml:"cluster_shutdown_request,omitempty" json:"-"`
	ShortOperation         string `yaml:"short_operation,omitempty" json:"short_operation"`
	DrainOperation         string `yaml:"drain_operation,omitempty" json:"drain_operation"`
	DrainComplete          bool   `yaml:"drain_complete,omitempty" json:"drain_complete"`
	LeaveCoordinator       string `yaml:"leave_coordinator,omitempty" json:"leave_coordinator"`
	Paused                 bool   `yaml:"paused,omitempty" json:"paused"`
	Draining               bool   `yaml:"draining,omitempty" json:"draining"`
	ID                     string `yaml:"id" json:"id"`
	Name                   string `yaml:"name" json:"name"`
	Listen                 string `yaml:"listen" json:"listen"`
	Advertise              string `yaml:"advertise" json:"advertise"`
	DataDir                string `yaml:"data_dir" json:"data_dir"`
}
type BootstrapAdmin struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"-"`
}
type AuthConfig struct {
	ClusterToken   string         `yaml:"cluster_token" json:"-"`
	BootstrapAdmin BootstrapAdmin `yaml:"bootstrap_admin" json:"bootstrap_admin"`
}
type PeerConfig struct {
	NodeID string `yaml:"node_id,omitempty" json:"node_id,omitempty"`
	URL    string `yaml:"url" json:"url"`
}
type VolumeConfig struct {
	PlacementGroupsPending []string `yaml:"placement_groups_pending,omitempty" json:"placement_groups_pending,omitempty"`
	OriginNode             string   `yaml:"origin_node,omitempty" json:"origin_node,omitempty"`
	RecoveryOnly           bool     `yaml:"recovery_only,omitempty" json:"recovery_only,omitempty"`
	ID                     string   `yaml:"id" json:"id"`
	Name                   string   `yaml:"name" json:"name"`
	Path                   string   `yaml:"path" json:"path"`
	Class                  string   `yaml:"class" json:"class"`
	SizeBytes              int64    `yaml:"size_bytes" json:"size_bytes"`
	State                  string   `yaml:"state" json:"state"`
	System                 bool     `yaml:"system,omitempty" json:"system"`
	OperationID            string   `yaml:"operation_id,omitempty" json:"operation_id,omitempty"`
}
type Config struct {
	PlacementStatistics    PlacementStatisticsConfig `yaml:"placement_statistics,omitempty" json:"placement_statistics"`
	RecoveryVolumes        []VolumeConfig            `yaml:"recovery_volumes,omitempty" json:"recovery_volumes,omitempty"`
	Node                   NodeConfig                `yaml:"node" json:"node"`
	ClusterID              string                    `yaml:"cluster_id" json:"cluster_id"`
	Generation             uint64                    `yaml:"generation" json:"generation"`
	BucketCatalogVersion   uint16                    `yaml:"bucket_catalog_version,omitempty" json:"-"`
	JoinPending            bool                      `yaml:"join_pending,omitempty" json:"join_pending"`
	PeerDirectoryVersion   uint16                    `yaml:"peer_directory_version,omitempty" json:"-"`
	PlacementGroupsVersion uint16                    `yaml:"placement_groups_version,omitempty" json:"-"`
	ControlVersion         uint16                    `yaml:"control_version,omitempty" json:"-"`
	QuotaVersion           uint16                    `yaml:"quota_version,omitempty" json:"-"`
	RangeVersion           uint16                    `yaml:"range_version,omitempty" json:"-"`
	Auth                   AuthConfig                `yaml:"auth" json:"auth"`
	VolumesDir             string                    `yaml:"volumes_dir" json:"volumes_dir"`
	SystemVolumeBytes      int64                     `yaml:"system_volume_bytes" json:"system_volume_bytes"`
	RAMBytes               int64                     `yaml:"ram_bytes" json:"ram_bytes"`
	RAMCacheBytes          int64                     `yaml:"ramcache_bytes" json:"ramcache_bytes"`
	AllowSubnets           []string                  `yaml:"allow_subnets" json:"allow_subnets"`
	Peers                  []PeerConfig              `yaml:"peers" json:"peers"`
	Volumes                []VolumeConfig            `yaml:"volumes" json:"volumes"`
}

// Options describes daemon startup overrides, not a separate administrative CLI.
// Only explicitly supplied flags override persistent settings. Runtime changes
// are saved atomically so the next start does not silently undo them.
type Options struct {
	ConfigPath    string
	values        map[string]string
	transportOnly bool // isolated legacy/transport tests; never a daemon flag
	manualRanges  bool // integration fixtures select explicit plans instead of auto planning

	// Isolated I/O fault injection; never a daemon flag.
	wrapDataVolumeIO func(volume.VolumeIO) volume.VolumeIO
}

func ParseOptions(args []string, output io.Writer) (Options, error) {
	f := flag.NewFlagSet("stqr", flag.ContinueOnError)
	f.SetOutput(output)
	o := Options{values: map[string]string{}}
	f.StringVar(&o.ConfigPath, "config", "stqr.yaml", "node configuration file (YAML)")
	defaults := map[string]string{"port": "7419", "listen": "", "connect": "", "advertise": "", "name": "", "data-dir": "", "volumes-dir": "", "ram-bytes": "", "ramcache-bytes": "", "system-volume-bytes": "", "allow-subnet": ""}
	descriptions := map[string]string{"port": "HTTP/WebSocket port", "listen": "HTTP bind address, e.g. 127.0.0.1:7419", "connect": "existing cluster seed URL (multinode stub)", "advertise": "advertised http:// address", "name": "display name of this node", "data-dir": "local node data directory", "volumes-dir": "volume directory (also VOLUMES_DIR)", "ram-bytes": "optional RAM budget in bytes; 0 uses available system RAM", "ramcache-bytes": "read-cache budget in bytes (reserved)", "system-volume-bytes": "initial admin volume size in bytes", "allow-subnet": "allowed IPv4/IPv6 CIDRs, comma separated; empty allows all"}
	refs := map[string]*string{}
	for k, v := range defaults {
		refs[k] = f.String(k, v, descriptions[k])
	}
	if e := f.Parse(args); e != nil {
		return o, e
	}
	if f.NArg() != 0 {
		return o, errors.New("stqr has no subcommands")
	}
	f.Visit(func(v *flag.Flag) {
		if v.Name != "config" {
			o.values[v.Name] = *refs[v.Name]
		}
	})
	if _, ok := o.values["volumes-dir"]; !ok {
		if v := os.Getenv("VOLUMES_DIR"); v != "" {
			o.values["volumes-dir"] = v
		}
	}
	return o, nil
}
func randomSecret() (string, error) {
	var b [24]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}
func newID() string {
	id, e := volume.NewID()
	if e != nil {
		panic(e)
	}
	return id.String()
}
func parseID(s string) (volume.ID, error) {
	var id volume.ID
	b, e := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if e != nil || len(b) != len(id) {
		return id, errors.New("invalid 128-bit ID")
	}
	copy(id[:], b)
	return id, nil
}
func classFromString(s string) (volume.Class, error) {
	switch s {
	case "hdd":
		return volume.HDD, nil
	case "ssd":
		return volume.SSD, nil
	case "ram":
		return volume.RAM, nil
	default:
		return 0, errors.New("class must be hdd, ssd or ram")
	}
}

func loadConfig(path string, o Options) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e != nil && !os.IsNotExist(e) {
		return c, e
	}
	if len(b) > 0 {
		d := yaml.NewDecoder(strings.NewReader(string(b)))
		d.KnownFields(true)
		if e = d.Decode(&c); e != nil {
			return c, e
		}
		var extra any
		if e = d.Decode(&extra); e != io.EOF {
			return c, errors.New("configuration must contain exactly one YAML document")
		}
	}
	if c.Node.ID == "" {
		c.Node.ID = newID()
	}
	if c.Node.Name == "" {
		c.Node.Name, _ = os.Hostname()
	}
	if c.Node.Listen == "" {
		c.Node.Listen = ":7419"
	}
	if c.Node.DataDir == "" {
		c.Node.DataDir = "."
	}
	if c.VolumesDir == "" {
		c.VolumesDir = "volumes"
	}
	if c.SystemVolumeBytes == 0 {
		c.SystemVolumeBytes = 1 << 20
	}
	if c.AllowSubnets == nil {
		c.AllowSubnets = []string{"127.0.0.0/8", "::1/128"}
	}
	for _, k := range []string{"listen", "port", "connect", "advertise", "name", "data-dir", "volumes-dir", "ram-bytes", "ramcache-bytes", "system-volume-bytes", "allow-subnet"} {
		v, ok := o.values[k]
		if !ok {
			continue
		}
		switch k {
		case "listen":
			c.Node.Listen = v
		case "port":
			p, e := strconv.Atoi(v)
			if e != nil || p < 1 || p > 65535 {
				return c, errors.New("invalid --port")
			}
			host, _, e := net.SplitHostPort(c.Node.Listen)
			if e != nil {
				return c, e
			}
			c.Node.Listen = net.JoinHostPort(host, v)
		case "name":
			c.Node.Name = v
		case "advertise":
			c.Node.Advertise = v
		case "data-dir":
			c.Node.DataDir = v
		case "volumes-dir":
			c.VolumesDir = v
		case "allow-subnet":
			c.AllowSubnets = []string{}
			for _, s := range strings.Split(v, ",") {
				if strings.TrimSpace(s) != "" {
					c.AllowSubnets = append(c.AllowSubnets, strings.TrimSpace(s))
				}
			}
		case "connect":
			if v != "" {
				if err := validateURL(v); err != nil {
					return c, err
				}
				c.Peers = append(c.Peers, PeerConfig{URL: v})
			}
		case "ram-bytes", "ramcache-bytes", "system-volume-bytes":
			n, e := strconv.ParseInt(v, 10, 64)
			if e != nil {
				return c, fmt.Errorf("%s: %w", k, e)
			}
			switch k {
			case "ram-bytes":
				c.RAMBytes = n
			case "ramcache-bytes":
				c.RAMCacheBytes = n
			default:
				c.SystemVolumeBytes = n
			}
		}
	}
	// Contacts are not membership or placement. A fresh candidate must retain a
	// preconfigured cluster identity/token and remain non-authoritative until the
	// durable membership/system bootstrap has finished. Never create a new cluster
	// when a seed is present but unavailable or its credentials are missing.
	if (len(c.Peers) > 0 || c.JoinPending) && c.ClusterID == "" {
		return c, errors.New("joining requires the existing cluster identity and token; no new cluster was created")
	}
	if c.JoinPending && c.Generation == 0 {
		c.Generation = 1
	}
	if c.PeerDirectoryVersion == 1 {
		// This is only a YAML cache. The durable system object is loaded after
		// opening its volume. A stale/missing cache must not pin the old initiator.
		c.Peers = nil
	}
	if c.ClusterID == "" {
		if len(c.Volumes) > 0 {
			return c, errors.New("volumes without cluster identity require recovery, not bootstrap")
		}
		c.ClusterID = newID()
		c.Generation = 1
		if c.Auth.ClusterToken, e = randomSecret(); e != nil {
			return c, e
		}
		if c.Auth.BootstrapAdmin.Password, e = randomSecret(); e != nil {
			return c, e
		}
		c.Auth.BootstrapAdmin.Username = "admin"
	}
	if e = validateConfig(c); e != nil {
		return c, e
	}
	return c, nil
}
func validateConfig(c Config) error {
	if err := c.PlacementStatistics.validate(); err != nil {
		return err
	}
	if _, e := parseID(c.Node.ID); e != nil {
		return e
	}
	if _, e := parseID(c.ClusterID); e != nil {
		return e
	}
	_, p, e := net.SplitHostPort(c.Node.Listen)
	if e != nil {
		return fmt.Errorf("listen: %w", e)
	}
	n, e := strconv.Atoi(p)
	if e != nil || n < 1 || n > 65535 {
		return errors.New("listen port must be 1..65535")
	}
	if c.Node.Name == "" || len(c.Node.Name) > 128 {
		return errors.New("node name must contain 1..128 bytes")
	}
	if c.Node.Advertise != "" {
		if e = validateURL(c.Node.Advertise); e != nil {
			return e
		}
	}
	if c.Node.DataDir == "" || c.VolumesDir == "" {
		return errors.New("data and volume directories cannot be empty")
	}
	if c.SystemVolumeBytes < volume.MinVolumeSize || c.RAMBytes < 0 || c.RAMCacheBytes < 0 {
		return errors.New("invalid storage budget")
	}
	if c.Auth.ClusterToken == "" || !c.JoinPending && c.ControlVersion == 0 && (c.Auth.BootstrapAdmin.Username == "" || c.Auth.BootstrapAdmin.Password == "") {
		return errors.New("existing cluster credentials are missing; refusing to recreate them")
	}
	for _, s := range c.AllowSubnets {
		if _, e = netip.ParsePrefix(s); e != nil {
			return fmt.Errorf("invalid subnet %q", s)
		}
	}
	for _, p := range c.Peers {
		if e = validateURL(p.URL); e != nil {
			return e
		}
		if p.NodeID != "" {
			if _, e = parseID(p.NodeID); e != nil || p.NodeID == c.Node.ID {
				return errors.New("peer identity must be valid and different from this node")
			}
		}
	}
	if c.JoinPending && len(c.Peers) == 0 && c.PeerDirectoryVersion == 0 {
		return errors.New("joining node requires at least one known seed")
	}
	return nil
}
func validateURL(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return errors.New("peer/advertise URL must be http://host:port without credentials or query")
	}
	return nil
}
func resolvePath(base, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(base, path)
}

// saveConfig commits a complete image with file+directory sync and mode 0600.
// It never exposes bootstrap credentials through JSON marshalling.
func saveConfig(path string, c Config) error {
	b, e := yaml.Marshal(c)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".stqr-config-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
