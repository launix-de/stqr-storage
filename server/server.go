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
// it is not a distributed lock or a substitute for placement authority.
type Server struct {
	mu                     sync.RWMutex
	opMu                   sync.Mutex
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
	closed                 bool
	boot                   client.ID
	started                time.Time
	sequence               atomic.Uint64
	statsWake              chan struct{}
	systemMu               sync.Mutex
	authMu                 sync.Mutex
	sessions               map[string]session
	wsMu                   sync.Mutex
	sockets                map[*wsSession]struct{}
	latest                 atomic.Pointer[client.StatsSnapshot]
	retiredIO              client.IOStats
	peerStatsMu            sync.RWMutex
	peerStats              map[client.ID]*peerObservation
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
	s = &Server{configPath: path, lock: lock, ctx: ctx, cancel: cancel, volumes: map[string]*managedVolume{}, sessions: map[string]session{}, sockets: map[*wsSession]struct{}{}, started: time.Now(), statsWake: make(chan struct{}, 1)}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	c, e := loadConfig(path, opts)
	if e != nil {
		return s, e
	}
	s.cfg = c
	s.running = c
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
			v.SetState(volume.Draining)
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
	if e = s.initAccounts(newSystem); e != nil {
		return s, e
	}
	if e = s.initBuckets(); e != nil {
		return s, e
	}
	if e = s.recoverOperations(); e != nil {
		return s, e
	}
	if e = saveConfig(path, s.cfg); e != nil {
		return s, e
	}
	s.running = s.cfg
	s.publishStats()
	s.workers.Add(1)
	go s.statsLoop()
	for _, d := range s.bucketSnapshots() {
		if !d.DeletionComplete && (d.State == BucketDeleting || d.DesiredPolicy != nil) {
			id, _ := bucketID(d.ID)
			s.startBucketCleanup(id)
		}
	}
	for _, v := range s.cfg.Volumes {
		if v.State == "draining" {
			id := v.OperationID
			if id == "" {
				id = newID()
			}
			s.startDrain(id, v.ID)
		}
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
		if s.lock != nil {
			result = errors.Join(result, s.lock.Close())
		}
	})
	return result
}
func Run(ctx context.Context, opts Options, output io.Writer) error {
	s, e := Open(opts)
	if e != nil {
		return e
	}
	defer s.Close()
	ln, e := net.Listen("tcp", s.running.Node.Listen)
	if e != nil {
		return e
	}
	fmt.Fprintf(output, "STQR listening on http://%s · config %s\n", ln.Addr(), s.configPath)
	fmt.Fprintln(output, "Bootstrap credentials are in the auth.bootstrap_admin section of the config file.")
	h := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 64 << 10}
	done := make(chan error, 1)
	go func() { done <- h.Serve(ln) }()
	select {
	case e = <-done:
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
		// Process/supervisor/system shutdown is always SHORT (wait for return).
		// Never infer a drain from cancellation; only explicit admin intent may
		// initiate evacuation. An existing persisted drain remains resumable.
		// TODO(multinode): announce persistent SHORT and coordinate RESUME_STATE.
		// Do not implement a failure timeout or evict peers on process shutdown.
		s.cancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e = h.Shutdown(shutdownCtx); e != nil {
			h.Close()
		}
		return e
	}
}
