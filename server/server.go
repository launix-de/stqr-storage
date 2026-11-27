// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type managedVolume struct {
	config VolumeConfig
	store  *volume.Volume
}

// Server currently owns one node. opMu serializes local administrative commits;
// its shared side pins their lifetime for independently admitted object writes.
// It is not a distributed lock or a substitute for placement/Volume authority.
type Server struct {
	joinHosted      bool
	joinRequested   atomic.Bool
	joinSignal      chan struct{}
	leaseGate       chan struct{}
	copyCoordinator sync.Mutex
	cow             cowTransfers
	leaseMu         sync.Mutex
	activeLeases    map[volume.ID]*Bucket
	durability      durabilityState

	objectSubscriptions    objectSubscriptionRegistry
	recoveryVolumes        map[volume.ID]*stagedRecoveryVolume
	authority              *volumeAuthorityState
	mu                     sync.RWMutex
	opMu                   sync.RWMutex
	parallelQuota          parallelQuotaState
	cfg, running           Config
	configPath, volumesDir string
	volumes                map[string]*managedVolume
	system                 *volume.Volume
	buckets                *BucketCatalog
	bucketMu               sync.RWMutex
	bucketsByID            map[volume.ID]*Bucket
	bucketsByName          map[string]*Bucket
	bucketJobs             map[volume.ID]bool
	lock                   *os.File
	ctx                    context.Context
	cancel                 context.CancelFunc
	workers                sync.WaitGroup
	closeOnce              sync.Once
	stopRequest            chan struct{}
	stopOnce               sync.Once
	shortStopping          atomic.Bool
	drainClosing           atomic.Bool
	closed                 bool
	boot                   client.ID
	started                time.Time
	sequence               atomic.Uint64
	statsWake              chan struct{}
	systemMu               sync.Mutex
	authMu                 sync.Mutex
	sessions               map[string]session
	frontendGrants         map[client.ID]frontendGrant
	wsMu                   sync.Mutex
	sockets                map[*wsSession]struct{}
	latest                 atomic.Pointer[client.StatsSnapshot]
	retiredIO              client.IOStats
	userIO                 sync.Map
	ioPriority             ioPriorities
	cleanupMu              sync.Mutex
	cleanupJobs            map[volume.ID]*bucketCleanupJob
	cleanupView            *systemView
	cleanupOrder           []volume.ID
	cleanupCursor          int
	cleanupReceipts        map[volume.ID]*cleanupReceiptSweep
	cleanupLast            time.Time
	cleanupReceiptTurn     bool
	cleanupVolumeCursor    int
	peerStatsMu            sync.RWMutex
	peerStats              map[client.ID]*peerObservation
	peerManagers           map[string]bool
	compatibility          atomic.Pointer[protocolProfile]
	versions               versionObservations
	directory              atomic.Pointer[peerDirectoryImage]
	placementGroups        atomic.Pointer[placementGroupImage]
	accounts               atomic.Pointer[accountsIndex]
	control                *controlRuntime // initialized before accepting control RPCs
	metadataPreview        bool            // isolated metadata draft; never owns data volumes
	controller             *clusterController
	currentSystem          atomic.Pointer[systemView]
	placement              atomic.Pointer[placementImage]
	quotaNode              client.ID
	quotaUsage             atomic.Pointer[quotaUsageImage]
	quotaPolicies          atomic.Pointer[quotaPolicyImage]
	quotaRanges            atomic.Pointer[quotaRangeImage]
	quotaImports           atomic.Pointer[quotaImportImage]
	pendingQuotas          atomic.Pointer[systemView]
	quotaHooked            map[volume.ID]*volume.Volume
	quotas                 *quotaManager
	scopes                 *rangeManager
	replicas               *replicaManager
}

func Open(opts Options) (s *Server, err error) {
	if opts.ConfigPath == "" {
		opts.ConfigPath = "stqr.yaml"
	}
	path, e := filepath.Abs(opts.ConfigPath)
	if e != nil {
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, errors.New("this node configuration is already in use")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s = &Server{configPath: path, lock: lock, ctx: ctx, cancel: cancel, volumes: map[string]*managedVolume{}, sessions: map[string]session{}, sockets: map[*wsSession]struct{}{}, started: time.Now(), statsWake: make(chan struct{}, 1), stopRequest: make(chan struct{})}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	if e = resumeClusterJoin(path); e != nil {
		return s, e
	}
	c, e := loadConfig(path, opts)
	if e != nil {
		return s, e
	}
	s.cfg = c
	s.authority = &volumeAuthorityState{records: make(map[volume.ID]volumeAuthorityRecord)}
	s.authority.dirty.Store(true)
	s.running = c
	if c.Node.DrainComplete {
		return s, ErrNodeDrained
	}
	if e = saveConfig(path, c); e != nil {
		return s, e
	}
	dataDir := resolvePath(filepath.Dir(path), c.Node.DataDir)
	s.volumesDir = resolvePath(dataDir, c.VolumesDir)
	if e = os.MkdirAll(s.volumesDir, 0700); e != nil {
		return s, e
	}
	node, _ := parseID(c.Node.ID)
	cluster, _ := parseID(c.ClusterID)
	boot, _ := volume.NewID()
	s.boot = client.ID(boot)
	for i := range s.cfg.Volumes {
		vc := &s.cfg.Volumes[i]
		if vc.Name == "" {
			vc.Name = filepath.Base(vc.Path)
		}
		if vc.State == "" {
			vc.State = "active"
		}
		class, e := classFromString(vc.Class)
		if e != nil {
			return s, e
		}
		expected := volume.ID{}
		if vc.ID != "" {
			expected, e = parseID(vc.ID)
			if e != nil {
				return s, e
			}
		}
		var v *volume.Volume
		if class == volume.RAM {
			mem, er := volume.NewMemory(vc.SizeBytes)
			if er != nil {
				return s, er
			}
			v, e = volume.Format(mem, volume.FormatOptions{Class: class, VolumeID: expected, NodeID: node, ClusterID: cluster})
			if e != nil {
				mem.Close()
			}
		} else {
			// Restart must never reformat or recreate a configured volume.
			dev, er := volume.OpenFile(vc.Path)
			if er != nil {
				return s, fmt.Errorf("open configured volume %s: %w", vc.Name, er)
			}
			v, e = volume.Open(dev, volume.OpenOptions{})
			if e != nil {
				dev.Close()
			}
		}
		if e != nil {
			return s, e
		}
		if e = v.VerifyIdentity(cluster, node, expected); e != nil && class != volume.RAM {
			v.Close()
			return s, e
		}
		if class == volume.RAM {
			if e = v.Bind(cluster, node); e != nil {
				v.Close()
				return s, e
			}
			vc.ID = v.Identity().VolumeID.String()
		}
		if vc.ID == "" {
			vc.ID = v.Identity().VolumeID.String()
		}
		vc.SizeBytes = int64(v.Size())
		vc.Class = v.Identity().Class.String()
		if _, ok := s.volumes[vc.ID]; ok {
			v.Close()
			return s, errors.New("duplicate volume identity")
		}
		s.volumes[vc.ID] = &managedVolume{config: *vc, store: v}
		if vc.System {
			st, er := os.Stat(vc.Path)
			if er != nil || !st.Mode().IsRegular() {
				return s, errors.New("the auto-growing admin volume must be a regular file")
			}
			if s.system != nil || class == volume.RAM {
				return s, errors.New("exactly one persistent admin volume is required")
			}
			s.system = v
		}
		if vc.State == "draining" {
			if opts.transportOnly {
				v.SetState(volume.Draining)
			} else {
				v.SetState(volume.Active)
			}
		} else {
			v.SetState(volume.Active)
			vc.State = "active"
			s.volumes[vc.ID].config = *vc
		}
	}
	newSystem := s.system == nil
	if newSystem {
		v, e := volume.New(filepath.Join(s.volumesDir, "admin"), c.SystemVolumeBytes, volume.HDD)
		if e != nil {
			return s, e
		}
		if e = v.Bind(cluster, node); e != nil {
			v.Close()
			return s, e
		}
		id := v.Identity().VolumeID.String()
		vc := VolumeConfig{ID: id, Name: "admin", Path: filepath.Join(s.volumesDir, "admin"), Class: "hdd", SizeBytes: int64(v.Size()), State: "active", System: true}
		s.system = v
		s.volumes[id] = &managedVolume{config: vc, store: v}
		s.cfg.Volumes = append(s.cfg.Volumes, vc)
	}
	// Dashboard joins precreate an empty, identity-bound admin volume in their
	// private metadata directory while preserving the ordinary volume directory.
	if s.cfg.JoinPending && s.cfg.ControlVersion == 0 && s.system.Stats().Objects == 0 {
		newSystem = true
	}
	if e = s.openStagedRecoveryVolumes(); e != nil {
		return s, e
	}
	if e = s.bootstrapPlacementGroups(); e != nil {
		return s, e
	}
	if !opts.transportOnly {
		if e = s.initClusterControl(dataDir); e != nil {
			return s, e
		}
	}
	if s.cfg.JoinPending {
		// No second bootstrap administrator and no stale local account authority.
		// The pending node accepts peer traffic and delegated local-volume admin
		// only; ordinary authentication waits for committed system bootstrap.
		var users accountsImage
		e = s.loadAccounts(&users)
		if errors.Is(e, volume.ErrNotFound) && newSystem && s.system.Stats().Objects == 0 {
			e = s.writeBlob("accounts", accountsImage{Schema: 1, Users: []account{}})
		}
	} else {
		e = s.initAccounts(newSystem)
	}
	if e != nil {
		return s, e
	}
	if e = s.initPlacementGroups(); e != nil {
		return s, e
	}
	if e = s.initBuckets(); e != nil {
		return s, e
	}
	if e = s.upgradePGStorage(); e != nil {
		return s, e
	}
	if e = s.initPeerDirectory(); e != nil {
		return s, e
	}
	if s.controller != nil {
		if e = s.initQuota(dataDir); e != nil {
			return s, e
		}
		if e = s.controller.initialCheckpoint(); e != nil {
			return s, e
		}
		if e = s.initRanges(dataDir); e != nil {
			return s, e
		}
	}
	if e = s.restoreQuotaProtection(); e != nil {
		return s, e
	}
	if e = s.initReplicas(); e != nil {
		return s, e
	}
	if e = s.recoverOperations(); e != nil {
		return s, e
	}
	// A durable STOP received by the preceding process is fulfilled by this
	// restart. A merely prepared/paused process must continue waiting for STOP.
	if s.cfg.Node.ClusterStopID != "" {
		s.cfg.Node.ClusterResumedID = s.cfg.Node.ClusterStopID
		if s.cfg.Node.ClusterShutdownRequest == s.cfg.Node.ClusterStopID {
			s.cfg.Node.ClusterShutdownRequest = ""
		}
	}
	if e = saveConfig(path, s.cfg); e != nil {
		return s, e
	}
	// Header persistence may block on storage; the constructor owns no
	// admission lock while awaiting its explicit authority cut.
	e = s.ensureVolumeAuthority()
	if e != nil {
		return s, e
	}
	s.running = s.cfg
	s.publishStats()
	// All local journals/placement are restored before announcing return. A
	// paused marker never means a membership change or an implicit drain.
	if s.cfg.Node.Paused {
		s.cfg.Node.Paused = false
		if err := saveConfig(s.configPath, s.cfg); err != nil {
			return nil, err
		}
	}
	if e = s.loadBucketLeases(); e != nil {
		return s, e
	}
	s.startLeaseRecovery()
	s.startDurabilityCleanup()
	s.startBucketReclaimer()
	// Transport fixtures drive recovery explicitly; autonomous housekeeping
	// belongs to the production daemon lifecycle, including startup recovery.
	if !opts.transportOnly {
		s.startS3Maintenance()
	}
	s.workers.Add(1)
	go s.statsLoop()
	for _, d := range s.bucketSnapshots() {
		if !d.DeletionComplete && (d.State == BucketDeleting || d.DesiredPolicy != nil || d.CloneDetach) {
			id, _ := bucketID(d.ID)
			s.startBucketCleanup(id)
		}
	}
	for _, v := range s.running.Volumes {
		if v.State == "draining" {
			id := v.OperationID
			if id == "" {
				id = newID()
			}
			s.startDrain(id, v.ID)
		}
	}
	if s.quotas != nil {
		s.startQuotaWorker()
	}
	s.startKnownPeers()
	if s.controller != nil {
		s.replicas.start()
	}
	if s.controller != nil {
		s.controller.start()
	}
	if s.scopes != nil && !opts.manualRanges {
		s.scopes.start()
	}
	if hint := opts.values["connect"]; hint != "" {
		// An explicit helper is attempted once as a contact source. Only verified
		// discovered identities enter the durable directory; this is not a primary.
		s.mu.Lock()
		s.startPeerContactsLocked([]PeerConfig{{URL: hint}})
		s.mu.Unlock()
	}
	return s, nil
}
func (s *Server) Close() error {
	var result error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.cancel()
		s.wsMu.Lock()
		for ws := range s.sockets {
			ws.close()
		}
		s.wsMu.Unlock()
		s.workers.Wait()
		s.opMu.Lock()
		defer s.opMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, v := range s.volumes {
			if e := v.store.Close(); e != nil {
				result = errors.Join(result, e)
			}
		}
		for _, staged := range s.recoveryVolumes {
			result = errors.Join(result, staged.store.Close())
		}
		if s.control != nil {
			result = errors.Join(result, s.control.ledger.journal.close())
		}
		if s.quotas != nil {
			result = errors.Join(result, s.quotas.ledger.journal.close())
		}
		if s.scopes != nil {
			result = errors.Join(result, s.scopes.close())
		}
		if s.lock != nil {
			result = errors.Join(result, s.lock.Close())
		}
	})
	return result
}
func Run(ctx context.Context, opts Options, output io.Writer) error {
	for {
		restart, err := runNode(ctx, opts, output)
		if err != nil || !restart {
			return err
		}
		// The persisted join configuration replaces the original startup overrides.
		opts.values = nil
	}
}

func runNode(ctx context.Context, opts Options, output io.Writer) (restart bool, err error) {
	s, e := Open(opts)
	if errors.Is(e, ErrNodeDrained) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	defer s.Close()
	s.joinHosted = true
	s.joinSignal = make(chan struct{}, 1)
	ln, e := net.Listen("tcp", s.running.Node.Listen)
	if e != nil {
		return false, e
	}
	fmt.Fprintf(output, "STQR listening on http://%s · config %s\n", ln.Addr(), s.configPath)
	fmt.Fprintln(output, "Bootstrap credentials are in the auth.bootstrap_admin section of the config file.")
	h := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	done := make(chan error, 1)
	go func() { done <- h.Serve(ln) }()
	select {
	case <-s.joinSignal:
		s.cancel()
		if e = s.Close(); e != nil {
			h.Close()
			return false, e
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e = h.Shutdown(shutdownCtx); e != nil {
			h.Close()
			return false, e
		}
		return true, nil
	case e = <-done:
		if errors.Is(e, http.ErrServerClosed) {
			return false, nil
		}
		return false, e
	case <-s.stopRequest:
		s.cancel()
		return false, h.Shutdown(context.Background())
	case <-ctx.Done():
		// Process/supervisor/system shutdown is always SHORT (wait for return).
		// Never infer a drain from cancellation; only explicit admin intent may
		// initiate evacuation. An existing persisted drain remains resumable.
		if err := s.prepareShortShutdown(context.Background()); err != nil {
			return false, err
		}
		s.cancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e = h.Shutdown(shutdownCtx); e != nil {
			h.Close()
		}
		return false, e
	}
}
