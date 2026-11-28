// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"time"

	"github.com/gorilla/websocket"
	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

var errObjectResponseSent = errors.New("object response already sent")

// Native clients cap fragmented WebSocket messages at64 frames. A segment can
// flush up to two frames plus a buffer boundary;16 segments leave room for
// headers and final framing. More fragmented views capture that checked snapshot.
const maxMappedDownloadSegments = 16

type borrowedWriteResult struct {
	sent   bool
	status int
	err    error
}
type borrowedWriteJob struct {
	run  func() borrowedWriteResult
	done chan borrowedWriteResult
}

// Queue only an operation, never a borrowed slice or an acquired read handle.
// The sole socket writer acquires and releases the volume handle inside run.
// In-place writes cannot change the borrowed bytes until frame output ends.
// Kernel output capacity is admitted before storage acquisition. Nonblocking
// writes capture any unsent suffix and flush it only after releasing storage.
// Unsupported transports and fragmented views use bounded owned capture.
func (ws *wsSession) directObjectDownload(ctx context.Context, m client.Message, p principal, op *objectMutation, length uint64, condition *volume.Options, runtime client.ID) (bool, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	job := borrowedWriteJob{done: make(chan borrowedWriteResult, 1)}
	job.run = func() borrowedWriteResult {
		sent, status, err := ws.writeMappedObject(ctx, m, p, op, length, condition, runtime)
		return borrowedWriteResult{sent, status, err}
	}
	select {
	case <-ctx.Done():
		return false, 0, ctx.Err()
	case <-ws.done:
		return false, 0, context.Canceled
	case ws.borrowedWrites <- job:
	}
	// Once admitted, wait for the writer to release its handle even if cancellation
	// races completion. Closing the connection interrupts its bounded socket write.
	result := <-job.done
	return result.sent, result.status, result.err
}

func (ws *wsSession) writeMappedObject(ctx context.Context, m client.Message, p principal, op *objectMutation, length uint64, condition *volume.Options, runtime client.ID) (sent bool, responseStatus int, resultErr error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	borrow := false
	if output := ws.socketOutput(); output != nil {
		var err error
		borrow, err = output.beginBorrow(ctx, inlineObjectDownload+objectDownloadHeader)
		if err != nil {
			return false, 0, err
		}
		if borrow {
			// Registered before h.Close so storage guards unwind first.
			defer func() {
				if err := output.finishBorrow(ctx); err != nil {
					ws.close()
					if resultErr == nil {
						resultErr, responseStatus = err, objectStatus(err)
					}
				}
			}()
		}
	}
	b := op.bucket
	// Match checked copies: local physical records remain authoritative even
	// when the bucket has clone dependents. Only missing inherited objects
	// defer to COW resolution after all local guards have unwound.
	v, h, description, prior, err := b.openSelectedRead(withMembershipRead(ctx), p, op.object)
	if err != nil {
		if errors.Is(err, volume.ErrNotFound) && len(description.ClonePeers) != 0 {
			return false, 0, nil
		}
		return false, 0, b.absenceError(op.object, prior, err)
	}
	defer h.Close()
	// Match checked copies: revalidate local ownership/epoch after pinning bytes.
	// This consults only the RAM placement image; it never contacts a peer.
	if err := b.server.checkObjectBinding(ctx, b.ID(), op.object, true); err != nil {
		return false, 0, err
	}
	key := volume.Key{Bucket: b.ID(), Object: op.object}
	info, err := h.Stat(key)
	if err != nil {
		return false, 0, err
	}
	// Preserve CopyTo RPC semantics: EOF itself is empty, offsets past it fail.
	if op.offset > info.Length {
		return false, 0, volume.ErrRange
	}
	n := uint64(0)
	if op.offset < info.Length {
		n = min(length, info.Length-op.offset)
	}
	if n > inlineObjectDownload {
		return false, 0, nil
	}
	if condition != nil {
		if err := h.Check(key, *condition); err != nil {
			return false, 0, err
		}
	}
	if g, err := h.Generation(); err == nil {
		touchConnectionVolume(ctx, v, g)
	}
	ws.server.ioPriority.users.Add(1)
	defer ws.server.ioPriority.users.Add(-1)
	status := 200
	if m.Opcode == 0x0101 {
		status = 206
	}
	started := false
	var transferred uint64
	var captured []byte
	var header []byte
	send := func(view volume.ReadView) error {
		deadline := time.Now().Add(10 * time.Second)
		if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
		ws.conn.SetWriteDeadline(deadline)
		stop := context.AfterFunc(ctx, ws.close)
		defer stop()
		if err := ctx.Err(); err != nil {
			return err
		}
		started = true
		writer, err := ws.conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			return err
		}
		transferred, err = writeMappedFrame(writer, header, view)
		return err
	}
	_, err = h.WithReadView(key, op.offset, length, func(view volume.ReadView) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		hash := sha256.New()
		if !borrow || len(view.Segments) > maxMappedDownloadSegments {
			// Keep this already checked snapshot. Do not release the handle and
			// ask dispatch to read/check it a second time. One owned segment also
			// stays within the native WebSocket fragmentation bound.
			capture := bytes.NewBuffer(make([]byte, 0, int(view.Length)))
			if _, err := view.WriteTo(bucketContextWriter{ctx: ctx, w: io.MultiWriter(capture, hash)}); err != nil {
				return err
			}
			captured = capture.Bytes()
		} else if _, err := view.WriteTo(hash); err != nil {
			return err
		}
		e := client.Encoder{B: make([]byte, 0, objectDownloadHeader)}
		encodeObjectInfo(&e, info, runtime)
		if m.Opcode == 0x0101 {
			e.U64(op.offset)
			e.U64(view.Length)
			e.U64(info.Length)
		}
		e.U8(0)
		e.U64(view.Length)
		e.B = append(e.B, hash.Sum(nil)...)
		e.V(view.Length)
		body := ws.server.durabilityResponse(status, e.B, b.ID())
		header = client.EncodeMessage(client.Message{Kind: client.Response, Opcode: m.Opcode, ID: m.ID, Body: body})
		if captured != nil {
			return nil
		}
		return send(view)
	})
	h.Close() // Never close a handle inside its WithReadView callback.
	if err == nil && captured != nil {
		// The captured view is now owned. Network backpressure no longer holds
		// the volume lock, and concurrent writes cannot alter this response.
		err = send(volume.ReadView{Length: uint64(len(captured)), Segments: []volume.ReadSegment{{Data: captured, Length: uint64(len(captured))}}})
	}
	b.recordTraffic(p, transferred, 0)
	b.recordRangeTraffic(p, op.object, transferred, 0)
	if started && err != nil {
		// No second error frame may follow a partially emitted success frame.
		ws.close()
		return true, objectStatus(err), err
	}
	return started, status, err
}

// The payload is passed straight to Write, without joining it to the header.
// On any error the caller closes the WebSocket; Close here would flush a partial
// frame as if it were complete. Each valid reply remains one WebSocket message.
func writeMappedFrame(writer io.WriteCloser, header []byte, view volume.ReadView) (uint64, error) {
	n, err := writer.Write(header)
	if err != nil {
		return 0, err
	}
	if n != len(header) {
		return 0, io.ErrShortWrite
	}
	count, err := view.WriteTo(writer)
	if err != nil {
		return uint64(count), err
	}
	return uint64(count), writer.Close()
}
