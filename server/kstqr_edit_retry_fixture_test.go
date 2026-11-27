// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/launix-de/stqr-storage/client"
	"sync"
	"time"
)

// Inject short outages before or after one independent payload edit. The guest
// must retry that same REDO request, without starting its append again.
type kernelEditRetryFault struct {
	mu                                     sync.Mutex
	mode                                   string
	armed, faulted, dropped                bool
	id                                     client.ID
	raw                                    []byte
	retries                                int
	mismatch                               bool
	fatalHeld, fatalReleased, fatalTimeout bool
	fatalRelease                           chan struct{}
}

func (f *kernelEditRetryFault) begin(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
	f.armed = true
	f.faulted = false
	f.dropped = false
	f.retries = 0
	f.mismatch = false
	f.raw = nil
	f.fatalHeld, f.fatalReleased, f.fatalTimeout = false, false, false
	if mode == "fatal-caller" {
		f.fatalRelease = make(chan struct{})
	}
}
func (f *kernelEditRetryFault) request(raw []byte) (reject, disconnect bool) {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Kind != client.Request {
		return
	}
	op := m.Opcode
	if op == client.ObjectAccepted && len(m.Body) >= 82 {
		op = binary.LittleEndian.Uint16(m.Body[80:82])
	}
	if op != 0x102 && op != 0x103 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faulted && m.ID == f.id {
		f.retries++
		f.mismatch = f.mismatch || !bytes.Equal(raw, f.raw)
		return
	}
	if !f.armed {
		return
	}
	f.armed = false
	f.faulted = true
	f.id = m.ID
	f.raw = append([]byte(nil), raw...)
	return f.mode == "503", f.mode == "disconnect-before"
}
func (f *kernelEditRetryFault) response(raw []byte) bool {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Kind != client.Response {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode == "disconnect-after" && f.faulted && !f.dropped && m.ID == f.id {
		f.dropped = true
		return true
	}
	return false
}

// Hold a real accepted payload reply while the guest kills its submitting
// task. The server-side write has completed, but the filesystem must still
// finish its marked descriptor and release its exclusive claim safely.
func (f *kernelEditRetryFault) holdFatalReply(raw []byte) {
	m, err := client.DecodeMessage(raw)
	if err != nil || m.Kind != client.Response {
		return
	}
	f.mu.Lock()
	if f.mode != "fatal-caller" || !f.faulted || f.fatalHeld || m.ID != f.id {
		f.mu.Unlock()
		return
	}
	f.fatalHeld = true
	release := f.fatalRelease
	f.mu.Unlock()
	select {
	case <-release:
	case <-time.After(8 * time.Second):
		f.mu.Lock()
		f.fatalTimeout = true
		f.mu.Unlock()
	}
}
func (f *kernelEditRetryFault) fatalSeen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode == "fatal-caller" && f.fatalHeld
}
func (f *kernelEditRetryFault) releaseFatal() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mode != "fatal-caller" || !f.fatalHeld || f.fatalReleased {
		return fmt.Errorf("fatal caller reply is not held")
	}
	f.fatalReleased = true
	close(f.fatalRelease)
	return nil
}
func (f *kernelEditRetryFault) verify(mode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if mode == "fatal-caller" {
		if f.mode != mode || !f.fatalHeld || !f.fatalReleased || f.fatalTimeout {
			return fmt.Errorf("fatal caller completion not observed: held=%v released=%v timeout=%v", f.fatalHeld, f.fatalReleased, f.fatalTimeout)
		}
		return nil
	}
	if f.mode != mode || !f.faulted || f.retries < 1 || f.mismatch || mode == "disconnect-after" && !f.dropped {
		return fmt.Errorf("edit outage not recovered: mode=%s fault=%v retries=%d changed=%v dropped=%v", f.mode, f.faulted, f.retries, f.mismatch, f.dropped)
	}
	return nil
}
