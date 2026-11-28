// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

type testSocketOutputPort struct {
	conn     net.Conn
	capacity int64
	queued   atomic.Int64
	partial  bool
	observed chan struct{}
}

func (p *testSocketOutputPort) budget() (socketOutputBudget, error) {
	if p.observed != nil {
		select {
		case p.observed <- struct{}{}:
		default:
		}
	}
	return socketOutputBudget{p.capacity, p.queued.Load()}, nil
}
func (p *testSocketOutputPort) tryWrite(b []byte) (int, error) {
	if p.partial {
		b = b[:min(7, len(b))]
	}
	return p.conn.Write(b)
}

func TestMappedDownloadBackpressureReleasesVolume(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "opaque-owned", true: "partial-borrowed"}[borrowed], func(t *testing.T) {
			s, reader, writer, ctx := subscriptionClients(t)
			id := client.ObjectID{210}
			if _, err := writer.Put(ctx, id, make([]byte, 4096), client.PutOptions{Accepted: true}); err != nil {
				t.Fatal(err)
			}
			b, err := s.Bucket(volume.ID(reader.BucketID()))
			if err != nil {
				t.Fatal(err)
			}
			ws, _, blocked := mappedSocket(t, s)
			if borrowed {
				output := ws.conn.UnderlyingConn().(*socketOutputConn)
				output.port = &testSocketOutputPort{conn: blocked.Conn, capacity: 8 << 20, partial: true}
			}
			blocked.armed.Store(true)
			downloadCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				sent, _, e := ws.writeMappedObject(downloadCtx, client.Message{}, userPrincipal(t, s), &objectMutation{bucket: b, object: volume.ObjectID(id)}, math.MaxUint64, nil, client.ID{})
				if !sent {
					e = errors.New("partial response lost sent status")
				}
				done <- e
			}()
			select {
			case <-blocked.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("download never reached blocked socket")
			}
			mutation := make(chan error, 1)
			started := time.Now()
			go func() {
				_, e := writer.Put(ctx, client.ObjectID{211}, []byte("unrelated"), client.PutOptions{Accepted: true})
				mutation <- e
			}()
			select {
			case e := <-mutation:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("unrelated write blocked behind socket output")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled partial frame reported success")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancel did not release download")
			}

			select {
			case <-ws.done:
			default:
				t.Fatal("partial frame failure left websocket open")
			}

			t.Logf("unrelated PUT completed while download remained blocked after %s", time.Since(started))
		})
	}
}

func TestMappedDownloadAdmissionPrecedesVolumeRead(t *testing.T) {
	s, reader, writer, ctx := subscriptionClients(t)
	id := client.ObjectID{212}
	if _, err := writer.Put(ctx, id, []byte("before"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	b, err := s.Bucket(volume.ID(reader.BucketID()))
	if err != nil {
		t.Fatal(err)
	}
	ws, _, blocked := mappedSocket(t, s)
	p := &testSocketOutputPort{conn: blocked.Conn, capacity: 8 << 20, observed: make(chan struct{}, 1)}
	p.queued.Store(p.capacity)
	ws.socketOutput().port = p
	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	principal := userPrincipal(t, s)
	go func() {
		_, _, err := ws.writeMappedObject(downloadCtx, client.Message{}, principal, &objectMutation{bucket: b, object: volume.ObjectID(id)}, 100, nil, client.ID{})
		done <- err
	}()
	select {
	case <-p.observed:
	case <-time.After(time.Second):
		t.Fatal("admission not reached")
	}
	mutationCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := writer.Put(mutationCtx, id, []byte("after"), client.PutOptions{Accepted: true}); err != nil {
		t.Fatal("capacity wait acquired storage", err)
	}
	select {
	case err := <-done:
		t.Fatal("full kernel queue admitted read", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission ignored cancellation")
	}
}
