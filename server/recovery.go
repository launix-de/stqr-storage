// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/json"
	"errors"

	"github.com/launix-de/stqr-storage/volume"
)

// recoverOperations reconciles the local configuration commit point with its
// system-object operation records. It does not infer distributed ownership.
// Unpublished new files remain discoverable, never silently removed/formatted.
func (s *Server) recoverOperations() error {
	s.mu.RLock()
	cfg := copyConfig(s.cfg)
	stores := make(map[string]*managedVolume, len(s.volumes))
	for id, m := range s.volumes {
		stores[id] = m
	}
	s.mu.RUnlock()
	var after *volume.Key
	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		h, e := s.system.Open(false)
		if e != nil {
			return e
		}
		records, e := h.ScanObjects(after, 128)
		if e != nil {
			h.Close()
			return e
		}
		pending := []Operation{}
		for _, r := range records {
			if r.Info.Deleted || r.Info.Length > 1<<20 {
				continue
			}
			b, _, e := h.Get(r.Key)
			if e != nil {
				h.Close()
				return e
			}
			var op Operation
			if json.Unmarshal(b, &op) != nil || op.ID == "" || len(op.Digest) != 64 || op.Kind == "" {
				continue
			}
			if op.State == "requested" || op.State == "running" {
				pending = append(pending, op)
			}
		}
		h.Close()
		for _, op := range pending {
			switch op.Kind {
			case "recovery-stage":
				op.State, op.Status = "failed", 503
				for _, v := range append(append([]VolumeConfig(nil), cfg.RecoveryVolumes...), cfg.Volumes...) {
					if v.OperationID == op.ID {
						op.State, op.Status, op.VolumeID = "fulfilled", 201, v.ID
					}
				}
			case "recovery-release":
				op.State, op.Status = "failed", 503
				if op.VolumeID != "" {
					found := false
					for _, v := range cfg.RecoveryVolumes {
						found = found || v.ID == op.VolumeID
					}
					if !found {
						op.State, op.Status = "fulfilled", 200
					}
				}
			case "force-disconnect", "node-recovery", "volume-recovery":
				id, _ := parseID(op.ID)
				if s.completedExclusion([16]byte(id)) != nil {
					op.State, op.Status = "fulfilled", 200
					op.Detail = "Ausschluss beim Wiederanlauf rekonstruiert"
				} else {
					pending := s.controller.coordinator.requestSnapshot()
					if pending != nil && pending.Claim.Exclusion != nil && pending.Claim.Exclusion.Operation.String() == op.ID {
						op.State, op.Status = "running", 202
					} else {
						op.State, op.Status = "failed", 503
						op.Detail = "Ausschlussauftrag vor Bindung unterbrochen"
					}
				}
			case "cluster-shutdown":
				switch {
				case cfg.Node.ClusterStopID == op.ID:
					op.State, op.Status = "fulfilled", 200
					op.Detail = "Wiederanlauf nach Cluster-Shutdown"
				case cfg.Node.ClusterShutdownRequest == op.ID:
					op.State, op.Status = "running", 202
				default:
					op.State, op.Status = "failed", 503
					op.Detail = "Shutdown-Auftrag vor Speicherung unterbrochen"
				}
			case "node-short":
				if cfg.Node.ShortOperation == op.ID {
					op.State = "fulfilled"
					op.Status = 200
					op.Detail = "Wiederanlauf nach Wartungs-Shutdown"
				} else {
					op.State = "failed"
					op.Status = 503
					op.Detail = "Wartungsauftrag vor Speicherung unterbrochen"
				}
			case "node-drain":
				if cfg.Node.Draining {
					op.State = "running"
					op.Status = 202
				} else {
					op.State = "failed"
					op.Status = 503
				}
			case "add":
				found := false
				for _, v := range cfg.Volumes {
					if v.OperationID == op.ID {
						op.VolumeID = v.ID
						op.State = "fulfilled"
						op.Status = 201
						op.Detail = "Volume-Aktivierung beim Wiederanlauf rekonstruiert"
						found = true
						break
					}
				}
				if !found {
					op.State = "failed"
					op.Status = 503
					op.Detail = "Neuanlage vor Veröffentlichung unterbrochen. Eine vorhandene Datei bleibt unverändert und kann ausdrücklich erneut hinzugefügt werden."
				}
			case "remove":
				var configured *VolumeConfig
				for i := range cfg.Volumes {
					if cfg.Volumes[i].ID == op.VolumeID {
						configured = &cfg.Volumes[i]
						break
					}
				}
				if configured == nil && op.VolumeID != "" {
					op.State = "fulfilled"
					op.Status = 200
					op.Detail = "Freigabe beim Wiederanlauf rekonstruiert"
				} else if configured != nil && configured.State == "draining" && configured.OperationID == op.ID {
					op.State = "running"
					op.Status = 202
					op.Detail = "Kooperatives Aushängen wird fortgesetzt"
				} else {
					op.State = "failed"
					op.Status = 503
					op.Detail = "Entfernen wurde vor dem Draining-Commit unterbrochen; Volume bleibt eingebunden"
				}
			case "grow":
				m := stores[op.VolumeID]
				if m != nil && op.SizeBytes > 0 && m.store.Size() >= uint64(op.SizeBytes) {
					op.State = "fulfilled"
					op.Status = 200
					op.Detail = "Vergrößerung beim Wiederanlauf rekonstruiert"
				} else {
					op.State = "failed"
					op.Status = 503
					op.Detail = "Vergrößerung unterbrochen; aktuelle Kapazität prüfen und erneut anfordern"
				}
			case "rescan":
				m := stores[op.VolumeID]
				if m == nil {
					op.State = "failed"
					op.Status = 404
					op.Detail = "Volume nicht mehr eingebunden"
				} else {
					n, e := m.store.Rescan()
					if e != nil {
						op.State = "failed"
						op.Status = statusOf(e)
						op.Detail = e.Error()
					} else {
						s.mu.Lock()
						m.config.SizeBytes = int64(n)
						for i := range s.cfg.Volumes {
							if s.cfg.Volumes[i].ID == op.VolumeID {
								s.cfg.Volumes[i].SizeBytes = int64(n)
							}
						}
						err := saveConfig(s.configPath, s.cfg)
						s.mu.Unlock()
						if err != nil {
							return err
						}
						op.State = "fulfilled"
						op.Status = 200
						op.Detail = "Rescan beim Wiederanlauf abgeschlossen"
					}
				}
			default:
				return errors.New("unknown pending local operation kind")
			}
			if e = s.saveOperation(op); e != nil {
				return e
			}
		}
		if len(records) == 0 {
			return nil
		}
		last := records[len(records)-1].Key
		after = &last
	}
}
