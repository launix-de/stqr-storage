// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
	"gopkg.in/yaml.v3"
)

// Native administrative messages; credentials never enter a query or config JSON.
const (
	clusterJoinStatus  uint16 = 0x203b
	clusterJoinInfo    uint16 = 0x203c
	clusterJoinPrepare uint16 = 0x203d
)

func (s *Server) joinEligibility() error {
	if !s.joinHosted || s.controller == nil {
		return fail(409, "Beitritt ist nur auf einer eigenständigen Node verfügbar.")
	}
	if s.joinRequested.Load() || s.joinPending() || s.shortStopping.Load() {
		return fail(409, "Die Node führt bereits einen Beitritt oder eine Wartung aus.")
	}
	s.mu.RLock()
	localID := s.cfg.Node.ID
	s.mu.RUnlock()
	view := s.currentSystem.Load()
	if view == nil || len(view.manifest.Membership.Members) != 1 || view.manifest.Membership.Members[0].ID.String() != localID || len(view.manifest.Membership.Excluded) != 0 {
		return fail(409, "Die Node gehört bereits zu einem Cluster.")
	}
	for _, b := range s.bucketSnapshots() {
		if b.ID != systemBucket.String() {
			return fail(409, "Beitritt erfordert eine Node ohne Anwendungs-Buckets.")
		}
	}
	s.mu.RLock()

	if len(s.cfg.RecoveryVolumes) != 0 || s.cfg.Node.Draining || s.cfg.Node.Paused {
		s.mu.RUnlock()
		return fail(409, "Wiederherstellung oder Wartung ist noch ausstehend.")
	}
	volumes := make([]managedVolume, 0, len(s.volumes))
	for _, v := range s.volumes {
		volumes = append(volumes, *v)
	}
	s.mu.RUnlock()
	for _, v := range volumes {
		if v.config.System {
			continue
		}
		if v.config.State != "active" || len(v.config.PlacementGroupsPending) != 0 || v.config.RecoveryOnly {
			return fail(409, "Alle Datenvolumes müssen aktiv und leer sein.")
		}
		if err := v.store.CheckEmpty(); err != nil {
			return fail(409, "Ein Datenvolume enthält noch Daten oder gespeicherte Operationen.")
		}
		record, err := readVolumeAuthority(v.store)
		if err != nil && !errors.Is(err, volume.ErrNotFound) {
			return err
		}
		if len(record.Entries) != 0 {
			return fail(409, "Ein Datenvolume enthält noch Platzierungsdaten.")
		}
	}
	l := s.control.ledger
	l.mu.Lock()
	busy := l.slot != nil || l.request != nil
	l.mu.Unlock()
	if busy {
		return fail(409, "Eine Clusteränderung ist noch ausstehend.")
	}
	return nil
}

func (s *Server) executeClusterJoin(m client.Message, actor principal) ([]byte, int, error) {
	if actor.User.Role != "ADMIN" || actor.ClusterPeer {
		return nil, 403, ErrBucketForbidden
	}
	s.opMu.Lock()
	authErr := s.requireAdmin(actor)
	s.opMu.Unlock()
	if authErr != nil {
		return nil, 403, authErr
	}
	d := client.Decoder{B: m.Body}
	if m.Kind != client.Request || m.ID == (client.ID{}) {
		return nil, 400, client.ErrProtocol
	}
	switch m.Opcode {
	case clusterJoinStatus:
		if d.Done() != nil {
			return nil, 400, client.ErrProtocol
		}
		s.opMu.Lock()
		err := s.joinEligibility()
		s.opMu.Unlock()
		e := client.Encoder{}
		e.Bool(err == nil)
		if err != nil {
			e.Text(err.Error())
		} else {
			e.Text("")
		}
		return e.B, 200, nil
	case clusterJoinInfo:
		candidate := d.ID()
		if d.Done() != nil || s.controller == nil || s.joinPending() || s.shortStopping.Load() {
			return nil, 409, fail(409, "Zielcluster ist noch nicht bereit.")
		}
		if view := s.currentSystem.Load(); view == nil || candidate == (client.ID{}) || hasControlNode(membershipIDs(view.manifest.Membership), candidate) || hasControlNode(view.manifest.Membership.Excluded, candidate) {
			return nil, 409, fail(409, "Diese Node-ID ist im Zielcluster bereits bekannt.")
		}
		s.mu.RLock()
		cfg := copyConfig(s.cfg)
		s.mu.RUnlock()
		e := client.Encoder{}
		cluster, _ := parseID(cfg.ClusterID)
		node, _ := parseID(cfg.Node.ID)
		e.ID(client.ID(cluster))
		e.ID(client.ID(node))
		e.Text(cfg.Auth.ClusterToken)
		addresses := []string{}
		if view := s.currentSystem.Load(); view != nil {
			for _, member := range view.manifest.Membership.Members {
				addresses = append(addresses, member.Addresses...)
			}
		}
		e.U32(uint32(len(addresses)))
		for _, address := range addresses {
			e.Text(address)
		}
		return e.B, 200, nil
	case clusterJoinPrepare:
		address, username, password := d.Text(), d.Text(), d.Text()
		if d.Done() != nil || len(address) > 2048 || len(username) > 255 || len(password) > 4096 || username == "" || password == "" {
			return nil, 400, client.ErrProtocol
		}
		s.opMu.Lock()
		eligible := s.joinEligibility()
		s.mu.RLock()
		preparedFrom := copyConfig(s.cfg)
		s.mu.RUnlock()
		s.opMu.Unlock()
		if eligible != nil {
			return nil, statusOf(eligible), eligible
		}
		ctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
		defer cancel()
		next, err := s.joinTarget(ctx, address, username, password, copyConfig(preparedFrom))
		if err != nil {
			return nil, statusOf(err), err
		}
		s.controller.mu.Lock()
		defer s.controller.mu.Unlock()
		s.opMu.Lock()
		defer s.opMu.Unlock()
		if err = s.requireAdmin(actor); err != nil {
			return nil, 403, err
		}
		if err = s.joinEligibility(); err != nil {
			return nil, statusOf(err), err
		}
		s.mu.RLock()
		previous := copyConfig(s.cfg)
		s.mu.RUnlock()
		if !reflect.DeepEqual(previous, preparedFrom) {
			return nil, 409, fail(409, "Die lokale Konfiguration wurde geändert. Bitte den Beitritt erneut starten.")
		}
		// Preserve local volumes and identity, but start a new metadata namespace.
		// The old system volume and its journals remain available for inspection.
		next.Node = NodeConfig{ID: previous.Node.ID, Name: previous.Node.Name, Listen: next.Node.Listen, Advertise: next.Node.Advertise}
		base := resolvePath(filepath.Dir(s.configPath), previous.Node.DataDir)
		next.Node.DataDir = filepath.Join(base, ".join-"+previous.Node.ID+"-"+m.ID.String())
		next.VolumesDir = resolvePath(base, previous.VolumesDir)
		next.Volumes = nil
		for _, v := range previous.Volumes {
			if !v.System {
				v.Path = resolvePath(base, v.Path)
				next.Volumes = append(next.Volumes, v)
			}
		}
		// Keep the configured data directory (which can be a separate disk).
		// Only metadata gets a private namespace and a fresh admin volume.
		if err = os.Mkdir(next.Node.DataDir, 0700); err != nil {
			return nil, 500, err
		}
		persisted := false
		defer func() {
			if !persisted {
				_ = os.RemoveAll(next.Node.DataDir)
			}
		}()
		adminPath := filepath.Join(next.Node.DataDir, "admin")
		admin, err := volume.New(adminPath, next.SystemVolumeBytes, volume.HDD)
		if err != nil {
			return nil, 500, err
		}
		cluster, _ := parseID(next.ClusterID)
		node, _ := parseID(next.Node.ID)
		err = admin.Bind(cluster, node)
		identity := admin.Identity()
		closeErr := admin.Close()
		if err != nil {
			return nil, 500, err
		}
		if closeErr != nil {
			return nil, 500, closeErr
		}
		next.Volumes = append(next.Volumes, VolumeConfig{ID: identity.VolumeID.String(), Name: "admin", Path: adminPath, Class: "hdd", SizeBytes: int64(identity.Size), State: "active", System: true})
		if err = validateConfig(next); err != nil {
			return nil, 400, err
		}
		if err = saveConfig(s.configPath+".before-join", previous); err != nil {
			return nil, 500, err
		}
		if err = saveConfig(s.configPath+".join", next); err != nil {
			// Rename can succeed before directory Sync reports an error. Once an
			// intent exists, never allow fresh local mutations behind that intent.
			if _, statErr := os.Stat(s.configPath + ".join"); statErr == nil {
				persisted = true
				s.joinRequested.Store(true)
			}
			return nil, 500, err
		}
		persisted = true
		s.joinRequested.Store(true)
		e := client.Encoder{}
		e.Text(next.ClusterID)
		return e.B, 202, nil
	}
	return nil, 400, client.ErrProtocol
}

func (s *Server) joinTarget(ctx context.Context, address, username, password string, next Config) (Config, error) {
	if err := validateURL(address); err != nil {
		return Config{}, fail(400, "Bitte eine gültige HTTP-URI der Zielnode eingeben.")
	}
	if address == s.localAddress() {
		return Config{}, fail(409, "Die Node kann ihrem eigenen Cluster nicht beitreten.")
	}
	u, _ := url.Parse(address)
	u.Path = "/api/message"
	u.RawQuery = ""
	u.Fragment = ""
	info := client.Encoder{}
	candidate, _ := parseID(next.Node.ID)
	info.ID(client.ID(candidate))
	m := client.Message{Kind: client.Request, Opcode: clusterJoinInfo, ID: client.NewRequestID(), Body: info.B}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(client.EncodeMessage(m)))
	if err != nil {
		return Config{}, err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(compatibilityHeader, profileHeader(s.protocolVersion()))
	var localIP net.IP
	tr := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			localIP = c.LocalAddr().(*net.TCPAddr).IP
		}
		return c, err
	}}
	defer tr.CloseIdleConnections()
	hc := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := hc.Do(req)
	if err != nil {
		return Config{}, fail(502, "Zielnode ist nicht erreichbar.")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return Config{}, fail(502, "Antwort der Zielnode konnte nicht gelesen werden.")
	}
	if response.StatusCode != 200 {
		message := "Anmeldung an der Zielnode fehlgeschlagen; ein Administratorkonto ist erforderlich."
		if response.StatusCode == 403 {
			message = "Die Zielnode verweigert den Zugriff. Adminrechte und erlaubte Quellnetze prüfen."
		}
		if response.StatusCode == 426 {
			message = "Die Protokollversion der Zielnode unterstützt diesen Dashboard-Beitritt nicht."
		}
		return Config{}, fail(response.StatusCode, message)
	}
	reply, err := client.DecodeMessage(raw)
	if err != nil || reply.Opcode != m.Opcode || reply.ID != m.ID || reply.Kind != client.Response {
		return Config{}, fail(502, "Zielnode unterstützt den Dashboard-Beitritt nicht.")
	}
	d := client.Decoder{B: reply.Body}
	status := d.V()
	d.Blob()
	if status != 200 {
		return Config{}, fail(int(status), "Zielcluster ist noch nicht bereit für den Beitritt.")
	}
	profile, err := decodeProfile(response.Header)
	if err != nil {
		return Config{}, fail(426, "Ungültiges Kompatibilitätsprofil der Zielnode.")
	}
	if err = checkPeerOperation(s.protocolVersion(), profile, clusterJoinInfo); err != nil {
		return Config{}, err
	}
	cluster, seed, token := d.ID(), d.ID(), d.Text()
	count := d.U32()
	if count > 10000 {
		return Config{}, client.ErrProtocol
	}
	addresses := []string{address}
	for i := uint32(0); i < count; i++ {
		addresses = append(addresses, d.Text())
	}
	if d.Done() != nil || cluster == (client.ID{}) || seed == (client.ID{}) || token == "" || cluster.String() == next.ClusterID {
		return Config{}, fail(409, "Ungültiger Zielcluster oder eigene Cluster-ID.")
	}
	next.ClusterID = cluster.String()
	next.Auth = AuthConfig{ClusterToken: token}
	next.JoinPending = true
	next.ControlVersion = 0
	next.RangeVersion = 0
	next.QuotaVersion = 0
	next.PeerDirectoryVersion = 0
	next.BucketCatalogVersion = 0
	next.PlacementGroupsVersion = 0
	next.Generation = 1
	next.RecoveryVolumes = nil
	next.Peers = []PeerConfig{{NodeID: seed.String(), URL: address}}
	// A default loopback-only installation becomes reachable through its routed
	// interface while retaining loopback dashboard access. Keep explicit addresses.
	next.Node.Listen, next.Node.Advertise, err = joinAddresses(next.Node.Listen, next.Node.Advertise, localIP)
	if err != nil {
		return Config{}, err
	}
	if len(next.AllowSubnets) > 0 {
		for _, address := range addresses {
			u, err := url.Parse(address)
			if err != nil {
				continue
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, u.Hostname())
			if err != nil {
				return Config{}, fail(502, "Eine Cluster-Adresse kann nicht aufgelöst werden.")
			}
			for _, ip := range ips {
				bits := 128
				if ip.IP.To4() != nil {
					bits = 32
				}
				next.AllowSubnets = append(next.AllowSubnets, fmt.Sprintf("%s/%d", ip.IP.String(), bits))
			}
		}
	}
	return next, nil
}

func joinAddresses(listen, advertise string, localIP net.IP) (string, string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || localIP == nil {
		return "", "", errors.New("cannot determine the joining node address")
	}
	advertised, err := url.Parse(advertise)
	if err != nil {
		return "", "", err
	}
	localAdvertise := advertise == "" || advertised.Hostname() == "localhost"
	if ip := net.ParseIP(advertised.Hostname()); ip != nil {
		localAdvertise = ip.IsLoopback()
	}
	ip := net.ParseIP(host)
	if (host == "localhost" || ip != nil && ip.IsLoopback()) && localAdvertise && !localIP.IsLoopback() {
		listen = net.JoinHostPort("", port)
		advertise = "http://" + net.JoinHostPort(localIP.String(), port)
	}
	if advertise == "" {
		advertise = "http://" + net.JoinHostPort(localIP.String(), port)
	}
	return listen, advertise, nil
}

// Called with the configuration lock held, before opening any configured volume.
// Publishing .join is the durable boundary: every subsequent start finishes the
// same transition, including a crash between two volume identity updates.
func resumeClusterJoin(path string) error {
	raw, err := os.ReadFile(path + ".join")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var next, previous, current Config
	if err = yaml.Unmarshal(raw, &next); err != nil {
		return err
	}
	raw, err = os.ReadFile(path + ".before-join")
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(raw, &previous); err != nil {
		return err
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(raw, &current); err != nil {
		return err
	}
	if next.Node.ID != previous.Node.ID || current.Node.ID != next.Node.ID || !next.JoinPending || (current.ClusterID != previous.ClusterID && current.ClusterID != next.ClusterID) {
		return errors.New("invalid persisted cluster join")
	}
	if current.ClusterID != next.ClusterID {
		old, err := parseID(previous.ClusterID)
		if err != nil {
			return err
		}
		cluster, err := parseID(next.ClusterID)
		if err != nil {
			return err
		}
		node, err := parseID(next.Node.ID)
		if err != nil {
			return err
		}
		for _, vc := range next.Volumes {
			if vc.Class == "ram" {
				continue
			}
			dev, err := volume.OpenFile(vc.Path)
			if err != nil {
				return err
			}
			v, err := volume.Open(dev, volume.OpenOptions{})
			if err != nil {
				dev.Close()
				return err
			}
			id, err := parseID(vc.ID)
			if err == nil {
				if vc.System {
					err = v.VerifyIdentity(cluster, node, id)
				} else {
					err = v.JoinEmptyCluster(old, cluster, node, id)
				}
			}
			closeErr := v.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
		if err = saveConfig(path, next); err != nil {
			return err
		}
	}
	if err = os.Remove(path + ".join"); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
