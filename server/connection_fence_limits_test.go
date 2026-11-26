// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestConnectionFenceCancelFinishingUpload(t *testing.T) {
	ws := connectionTestSession(testServer(t))
	work := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	ws.admitConnectionMessage(work)
	job := ws.connectionJobs[work.ID]
	ctx := ws.connectionContext(ws.ctx, work.ID)
	u := &serverObjectUpload{op: &objectMutation{message: work}, finishing: true}
	ws.uploads = map[client.ID]*serverObjectUpload{work.ID: u}
	before := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(before)
	e := client.Encoder{}
	e.ID(work.ID)
	if err := ws.objectTransfer(client.Message{Opcode: 7, Body: e.B}, principal{}, nil); err != nil {
		t.Fatal(err)
	}
	after := connectionTestMessage(client.ConnectionSync, client.Request)
	ws.admitConnectionMessage(after)
	if ws.connectionJobs[work.ID] != job {
		t.Fatal("CANCEL retired commit owner's work")
	}
	select {
	case <-job.done:
		t.Fatal("commit completed prematurely")
	default:
	}
	for _, f := range []client.Message{before, after} {
		found := false
		for _, p := range ws.connectionFences[f.ID].prior {
			found = found || p == job
		}
		if !found {
			t.Fatal("fence omitted finishing upload")
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, status := ws.executeConnectionSync(cancelled, f); status != 499 {
			t.Fatalf("fence completed before owner: %d", status)
		}
	}
	v, err := volume.NewRAM(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	g := connectionTestPut(t, v, volume.Key{Object: volume.ObjectID{9}}, "late publication")
	touchConnectionVolume(ctx, v, g)
	ws.completeConnectionMessage(work, 201, client.ResponseBody(201, nil, ""))
	for _, f := range []client.Message{before, after} {
		if _, status := ws.executeConnectionSync(context.Background(), f); status != 200 {
			t.Fatal(status)
		}
	}
	if !v.IsDurableThrough(g) {
		t.Fatal("late commit touch was not checkpointed")
	}
}

func TestConnectionFenceFailureHistoryBound(t *testing.T) {
	ws := connectionTestSession(testServer(t))
	for i := 0; i <= maxConnectionFailures; i++ {
		m := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
		if !ws.admitConnectionMessage(m) {
			t.Fatal("admission")
		}
		ws.completeConnectionMessage(m, 409, client.ResponseBody(409, nil, strings.Repeat("x", 2*maxConnectionFailureDetail)))
	}
	if len(ws.connectionFailures) != maxConnectionFailures {
		t.Fatal(len(ws.connectionFailures))
	}
	for _, f := range ws.connectionFailures {
		if len(f.detail) > maxConnectionFailureDetail {
			t.Fatal("unbounded detail")
		}
	}
	select {
	case <-ws.done:
	default:
		t.Fatal("exhaustion did not close uncertain connection")
	}
}

func TestUploadFrameAdmissionBackpressure(t *testing.T) {
	for _, bytes := range []int{0, 1 << 20} {
		t.Run(map[bool]string{true: "bytes", false: "count"}[bytes != 0], func(t *testing.T) {
			ws := connectionTestSession(nil)
			limit := maxPendingUploadFrames
			if bytes != 0 {
				limit = maxPendingUploadFrameBytes / bytes
			}
			for i := 0; i < limit; i++ {
				if !ws.reserveUploadFrame(bytes) {
					t.Fatal("reservation")
				}
			}
			done := make(chan bool, 1)
			go func() { done <- ws.reserveUploadFrame(bytes) }()
			select {
			case <-done:
				t.Fatal("admission bypassed occupied budget")
			case <-time.After(20 * time.Millisecond):
			}
			ws.releaseUploadFrame(bytes)
			select {
			case ok := <-done:
				if !ok {
					t.Fatal("released budget rejected")
				}
			case <-time.After(time.Second):
				t.Fatal("release did not wake receiver")
			}
			if ws.pendingUploadFrames != limit || ws.pendingUploadFrameBytes != limit*bytes {
				t.Fatal("incorrect accounting")
			}
			blocked := make(chan bool, 1)
			go func() { blocked <- ws.reserveUploadFrame(bytes) }()
			ws.close()
			select {
			case ok := <-blocked:
				if ok {
					t.Fatal("closed full session admitted")
				}
			case <-time.After(time.Second):
				t.Fatal("disconnect did not unblock receiver")
			}
		})
	}
}

func TestUploadFrameBudgetIncludesWaitingCallbacks(t *testing.T) {
	ws := connectionTestSession(testServer(t))
	ws.authorization = &connectionAuthorization{}
	ws.requests = make(chan client.Message)
	work := connectionTestMessage(client.ObjectAccepted, client.FireAndForget)
	ws.admitConnectionMessage(work) // Keep headerReady blocked for every frame.
	e := client.Encoder{}
	e.ID(work.ID)
	frame := client.Message{Opcode: 5, Body: e.B}
	stopped := make(chan struct{})
	go func() { ws.requestLoop(); close(stopped) }()
	for i := 0; i < maxPendingUploadFrames; i++ {
		if !ws.reserveUploadFrame(len(frame.Body)) {
			t.Fatal("frame reservation")
		}
		ws.requests <- frame
	}
	// Unbuffered delivery proves the receive queue drained into callbacks, whose
	// blocked predecessor must still consume the same admission credits.
	blocked := make(chan bool, 1)
	go func() { blocked <- ws.reserveUploadFrame(len(frame.Body)) }()
	select {
	case <-blocked:
		t.Fatal("waiting callbacks escaped frame budget")
	case <-time.After(20 * time.Millisecond):
	}
	ws.close()
	select {
	case ok := <-blocked:
		if ok {
			t.Fatal("closed admission")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked receiver")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("waiting callbacks did not stop on disconnect")
	}
	ws.frameMu.Lock()
	defer ws.frameMu.Unlock()
	if ws.pendingUploadFrames != 0 || ws.pendingUploadFrameBytes != 0 {
		t.Fatal("callback reservations leaked")
	}
}
