// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

// Connection fences describe progress, not transactions or object versions.
// Admission is performed by the socket receiver before starting any worker.
// A fence retains the preceding in-flight jobs and their final touched-volume
// generations. Later messages cannot overtake that cut or extend its lifetime.
// The server interprets no client format and contacts no other node for a fence.
type connectionWork struct {
	session     *wsSession
	sequence    uint64
	done        chan struct{}
	touches     map[*volume.Volume]uint64 // session.fenceMu, immutable after done
	headerReady chan struct{}
	headerOnce  sync.Once
	failure     *connectionFailure
}
type connectionFailure struct {
	sequence uint64
	id       client.ID
	opcode   uint16
	status   int
	detail   string
}
type connectionFence struct {
	cut      uint64
	prior    []*connectionWork
	failures []connectionFailure
	touches  map[*volume.Volume]uint64
}
type connectionWorkKey struct{}

func (ws *wsSession) admitConnectionMessage(m client.Message) bool {
	ws.fenceMu.Lock()
	defer ws.fenceMu.Unlock()
	if ws.connectionJobs == nil {
		ws.connectionJobs = map[client.ID]*connectionWork{}
	}
	if ws.connectionTouches == nil {
		ws.connectionTouches = map[*volume.Volume]uint64{}
	}
	if ws.connectionFences == nil {
		ws.connectionFences = map[client.ID]*connectionFence{}
	}
	if ws.uploadTails == nil {
		ws.uploadTails = map[client.ID]chan struct{}{}
	}
	if _, exists := ws.connectionJobs[m.ID]; exists {
		return false
	}
	if _, exists := ws.connectionFences[m.ID]; exists {
		return false
	}
	if m.Opcode == client.ConnectionSync {
		f := &connectionFence{cut: ws.connectionSequence, touches: map[*volume.Volume]uint64{}}
		for v, g := range ws.connectionTouches {
			f.touches[v] = g
		}
		f.failures = append(f.failures, ws.connectionFailures...)
		for _, job := range ws.connectionJobs {
			f.prior = append(f.prior, job)
		}
		ws.connectionFences[m.ID] = f
		return true
	}
	ws.connectionSequence++
	ws.connectionJobs[m.ID] = &connectionWork{session: ws, sequence: ws.connectionSequence, done: make(chan struct{}), headerReady: make(chan struct{}), touches: map[*volume.Volume]uint64{}}
	return true
}
func (ws *wsSession) connectionContext(ctx context.Context, id client.ID) context.Context {
	ws.fenceMu.Lock()
	job := ws.connectionJobs[id]
	ws.fenceMu.Unlock()
	if job != nil {
		return context.WithValue(ctx, connectionWorkKey{}, job)
	}
	return ctx
}
func connectionWorkFrom(ctx context.Context) *connectionWork {
	job, _ := ctx.Value(connectionWorkKey{}).(*connectionWork)
	return job
}
func connectionFenced(ctx context.Context) bool {
	job := connectionWorkFrom(ctx)
	return job != nil && job.session.connectionFenceCap.Load()
}
func touchConnectionVolume(ctx context.Context, v *volume.Volume, g uint64) {
	job := connectionWorkFrom(ctx)
	if job == nil || !job.session.connectionFenceCap.Load() {
		return
	}
	ws := job.session
	ws.fenceMu.Lock()
	defer ws.fenceMu.Unlock()
	job.touches[v] = max(job.touches[v], g)
	ws.connectionTouches[v] = max(ws.connectionTouches[v], g)
}
func (ws *wsSession) connectionHeaderReady(id client.ID) {
	ws.fenceMu.Lock()
	job := ws.connectionJobs[id]
	ws.fenceMu.Unlock()
	if job != nil {
		job.headerOnce.Do(func() { close(job.headerReady) })
	}
}
func (ws *wsSession) completeConnectionMessage(m client.Message, status int, body []byte) {
	ws.fenceMu.Lock()
	defer ws.fenceMu.Unlock()
	if m.Opcode == client.ConnectionSync {
		delete(ws.connectionFences, m.ID)
		return
	}
	job := ws.connectionJobs[m.ID]
	if job == nil {
		return
	}
	if m.Kind == client.FireAndForget && ws.connectionFenceCap.Load() && status >= 300 {
		detail := "asynchronous object request failed"
		d := client.Decoder{B: body}
		d.V()
		headers := client.Decoder{B: d.Blob()}
		count := headers.V()
		for i := uint64(0); i < count && headers.Err == nil; i++ {
			tag := headers.V()
			data := headers.Blob()
			if tag == 3 {
				text := client.Decoder{B: data}
				if value := text.Text(); text.Done() == nil {
					detail = value
				}
			}
		}
		failure := connectionFailure{job.sequence, m.ID, m.Opcode, status, detail}
		job.failure = &failure
		ws.connectionFailures = append(ws.connectionFailures, failure)
	}
	delete(ws.connectionJobs, m.ID)
	job.headerOnce.Do(func() { close(job.headerReady) })
	close(job.done)
}

// Called in receiver order for upload frames. The predecessor is only the
// preceding frame of this transfer, never another object's request. It protects
// streaming offset/hash validation; file I/O occurs in the frame's goroutine.
func (ws *wsSession) connectionUploadFrame(id client.ID) (<-chan struct{}, chan struct{}) {
	ws.fenceMu.Lock()
	defer ws.fenceMu.Unlock()
	previous := ws.uploadTails[id]
	if previous == nil {
		if job := ws.connectionJobs[id]; job != nil {
			previous = job.headerReady
		}
	}
	done := make(chan struct{})
	ws.uploadTails[id] = done
	return previous, done
}
func (ws *wsSession) finishConnectionUploadFrames(id client.ID, tail chan struct{}) {
	close(tail)
	ws.fenceMu.Lock()
	if ws.uploadTails[id] == tail {
		delete(ws.uploadTails, id)
	}
	ws.fenceMu.Unlock()
}
func (ws *wsSession) executeConnectionSync(ctx context.Context, m client.Message) ([]byte, int) {
	if !ws.connectionFenceCap.Load() {
		return client.ResponseBody(501, nil, "connection sync capability was not negotiated"), 501
	}
	if len(m.Body) != 0 {
		return client.ResponseBody(400, nil, "connection sync body must be empty"), 400
	}
	ws.fenceMu.Lock()
	f := ws.connectionFences[m.ID]
	ws.fenceMu.Unlock()
	if f == nil {
		return client.ResponseBody(412, nil, "connection fence was not admitted"), 412
	}
	failures := append([]connectionFailure(nil), f.failures...)
	touches := make(map[*volume.Volume]uint64, len(f.touches))
	for v, g := range f.touches {
		touches[v] = g
	}
	for _, job := range f.prior {
		select {
		case <-job.done:
		case <-ctx.Done():
			return client.ResponseBody(499, nil, ctx.Err().Error()), 499
		}
		for v, g := range job.touches {
			touches[v] = max(touches[v], g)
		}
		if job.failure != nil {
			failures = append(failures, *job.failure)
		}
	}
	// Every volume may already have been checkpointed by another socket or the
	// background writer. CheckpointThrough then returns without issuing I/O.
	var wait sync.WaitGroup
	errors := make(chan error, len(touches))
	for v, g := range touches {
		wait.Add(1)
		go func(v *volume.Volume, g uint64) {
			defer wait.Done()
			if err := v.CheckpointThrough(g); err != nil {
				errors <- err
			}
		}(v, g)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		status := objectStatus(err)
		return client.ResponseBody(status, nil, err.Error()), status
	}
	// Remove only generations actually covered. Writes after the captured cut
	// remain for the next fence; they never delay this ACK indefinitely.
	ws.fenceMu.Lock()
	for v, g := range touches {
		if current := ws.connectionTouches[v]; current <= g {
			delete(ws.connectionTouches, v)
		}
	}
	var first *connectionFailure
	for i := range failures {
		failure := failures[i]
		if first == nil || failure.sequence < first.sequence {
			copy := failure
			first = &copy
		}
	}
	retained := ws.connectionFailures[:0]
	for i := range ws.connectionFailures {
		failure := ws.connectionFailures[i]
		if failure.sequence > f.cut {
			retained = append(retained, failure)
		}
	}
	ws.connectionFailures = retained
	ws.fenceMu.Unlock()
	if first != nil {
		return client.ResponseBody(first.status, nil, fmt.Sprintf("request %s opcode %04x: %s", first.id.String(), first.opcode, first.detail)), first.status
	}
	e := client.Encoder{}
	e.ID(ws.server.boot)
	e.U64(f.cut)
	return client.ResponseBody(200, e.B, ""), 200
}

// Unsolicited subscription updates can introduce a read dependency without a
// pending request. Track only the exact publishing local volume, not all volumes
// written by unrelated users of that bucket.
func (ws *wsSession) observeConnectionVolume(v *volume.Volume, g uint64) {
	if !ws.connectionFenceCap.Load() {
		return
	}
	ws.fenceMu.Lock()
	defer ws.fenceMu.Unlock()
	if ws.connectionTouches == nil {
		ws.connectionTouches = map[*volume.Volume]uint64{}
	}
	ws.connectionTouches[v] = max(ws.connectionTouches[v], g)
}
