// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"

	"github.com/launix-de/stqr-storage/volume"
)

type membershipJob struct{ cancel context.CancelFunc }

// This is optional background acceleration, never readiness/accounting/control
// authority. Keep only in-flight instances; no registry retains closed volumes
// or their 4MiB filter. Add and closed publication share s.mu with Close's Wait.
func (s *Server) startVolumeMembership(v *volume.Volume) {
	s.mu.Lock()
	if s.closed || v == s.system {
		s.mu.Unlock()
		return
	}
	attached := false
	for _, m := range s.volumes {
		attached = attached || m.store == v
	}
	if !attached {
		s.mu.Unlock()
		return
	}
	if s.membershipJobs == nil {
		s.membershipJobs = make(map[*volume.Volume]*membershipJob)
	}
	if s.membershipJobs[v] != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	job := &membershipJob{cancel: cancel}
	s.membershipJobs[v] = job
	s.workers.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workers.Done()
		defer cancel()
		defer func() {
			s.mu.Lock()
			if s.membershipJobs[v] == job {
				delete(s.membershipJobs, v)
			}
			if len(s.membershipJobs) == 0 {
				s.membershipJobs = nil
			}
			s.mu.Unlock()
		}()
		// ErrBusy (obsolete import/build), close/cancel and I/O failures all leave
		// conservative UNKNOWN hints. Explicit activation/import events may schedule
		// another attempt; no balancing/statistics timer ever retries a census.
		_ = v.WarmKeyMembership(ctx)
	}()
}
func (s *Server) startVolumeMemberships() {
	for _, v := range s.quotaVolumes() {
		s.startVolumeMembership(v)
	}
}
func (s *Server) stopVolumeMembership(v *volume.Volume) {
	s.mu.Lock()
	job := s.membershipJobs[v]
	delete(s.membershipJobs, v)
	if len(s.membershipJobs) == 0 {
		s.membershipJobs = nil
	}
	s.mu.Unlock()
	if job != nil {
		job.cancel()
	}
}
