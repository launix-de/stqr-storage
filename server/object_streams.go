// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"

	"github.com/launix-de/stqr-storage/client"
)

const maxObjectStagingBytes = 1 << 30
const maxObjectStagingTransfers = 4

func objectUploadHeader(m client.Message) bool {
	if m.Opcode < 0x0102 || m.Opcode > 0x0104 {
		return false
	}
	d := client.Decoder{B: m.Body}
	d.Take(80) // bucket, object, map revision, owner epoch, runtime
	if m.Opcode == 0x0103 {
		d.U64()
	}
	decodeObjectOptions(&d, m.ID)
	mode := d.U8()
	return d.Err == nil && mode == 1
}

type serverObjectUpload struct {
	op         *objectMutation
	file       *os.File
	hash       hash.Hash
	next, part uint64
	finishing  bool // uploadMu; the commit goroutine owns the file while true
}

func (u *serverObjectUpload) close() {
	if u.file != nil {
		name := u.file.Name()
		u.file.Close()
		os.Remove(name)
		u.file = nil
	}
}
func (ws *wsSession) beginObjectUpload(op *objectMutation, p principal) error {
	ws.uploadMu.Lock()
	defer ws.uploadMu.Unlock()
	if err := ws.objectContext(op.message.ID).Err(); err != nil {
		return err
	}
	permission := BucketWrite
	if op.message.Opcode == 0x0104 {
		permission = BucketAppend
	}
	if _, err := op.bucket.authorize(p, permission); err != nil {
		return err
	}
	if len(ws.uploads) >= maxObjectStagingTransfers || op.length > maxObjectStagingBytes-ws.stagedBytes {
		return fail(507, "upload staging budget exhausted")
	}
	if ws.uploads == nil {
		ws.uploads = map[client.ID]*serverObjectUpload{}
	}
	if ws.uploads[op.message.ID] != nil {
		return fail(409, "upload already pending")
	}
	file, err := os.CreateTemp("", "stqr-server-upload-*")
	if err != nil {
		return err
	}
	ws.uploads[op.message.ID] = &serverObjectUpload{op: op, file: file, hash: sha256.New()}
	ws.stagedBytes += op.length
	return nil
}
func (ws *wsSession) discardUpload(id client.ID) {
	ws.uploadMu.Lock()
	defer ws.uploadMu.Unlock()
	if u := ws.uploads[id]; u != nil {
		if u.finishing {
			return
		} // CANCEL cancels its context; owner closes it
		ws.discardUploadLocked(id, u)
	}
}

func (ws *wsSession) discardUploadLocked(id client.ID, u *serverObjectUpload) {
	ws.stagedBytes -= u.op.length
	u.close()
	ws.releaseObjectContext(id)
	delete(ws.uploads, id)
}

// Retire the exact transfer before exposing its terminal response. A retry may
// immediately reuse the request ID; a deferred old cleanup must not erase it.
func (ws *wsSession) completeUpload(u *serverObjectUpload) {
	ws.uploadMu.Lock()
	defer ws.uploadMu.Unlock()
	id := u.op.message.ID
	if ws.uploads[id] == u {
		ws.discardUploadLocked(id, u)
	}
}
func (ws *wsSession) sendMessage(m client.Message) error { return ws.sendMessageContext(ws.ctx, m) }
func (ws *wsSession) sendMessageContext(ctx context.Context, m client.Message) error {
	select {
	case ws.responses <- client.EncodeMessage(m):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (ws *wsSession) finishUpload(u *serverObjectUpload, result []byte, status int, err error) error {
	var redirect *objectRedirect
	var body []byte
	if errors.As(err, &redirect) {
		status = 301
		body = redirectResponse(redirect.patch)
	} else {
		detail := ""
		if err != nil {
			status = objectStatus(err)
			result = nil
			detail = err.Error()
		}
		body = client.ResponseBody(status, result, detail)
	}
	defer ws.completeConnectionMessage(u.op.message, status, body)
	if u.op.message.Kind != client.Request && status < 300 {
		return nil
	}
	return ws.sendMessage(client.Message{Kind: client.Response, Opcode: u.op.message.Opcode, ID: u.op.message.ID, Body: body})
}
func (ws *wsSession) objectTransfer(m client.Message, p principal, startCall func(func()) bool) error {
	d := client.Decoder{B: m.Body}
	id := d.ID()
	ws.uploadMu.Lock()
	u := ws.uploads[id]
	finishing := u != nil && u.finishing
	ws.uploadMu.Unlock()
	if m.Opcode == 7 {
		if d.Done() != nil {
			return fail(400, "invalid CANCEL")
		}
		ws.discardUpload(id)
		if u != nil {
			ws.completeConnectionMessage(u.op.message, 499, client.ResponseBody(499, nil, "upload cancelled"))
		}
		return nil
	}
	if u == nil || finishing {
		return nil
	} // Late chunks after cancellation cannot recreate a request.
	var err error
	if m.Opcode == 5 {
		part, offset := d.U64(), d.U64()
		data := d.Blob()
		if d.Done() != nil || part != u.part || offset != u.next || len(data) > 1<<20 || uint64(len(data)) > u.op.length-u.next {
			err = fail(400, "invalid upload chunk")
		} else {
			_, err = u.file.Write(data)
			if err == nil {
				u.hash.Write(data)
				u.next += uint64(len(data))
				u.part++
			}
		}
		if err == nil {
			return nil
		}
	} else {
		length := d.U64()
		digest := d.Take(32)
		if d.Done() != nil || length != u.op.length || u.next != length || !equalObjectBytes(digest, u.op.digest[:]) || !equalObjectBytes(u.hash.Sum(nil), u.op.digest[:]) {
			err = fail(400, "upload length/digest mismatch")
		} else {
			_, err = u.file.Seek(0, io.SeekStart)
			if err == nil {
				runtime := client.ID{}
				if u.op.bucket.Describe().Policy.StorageStrategy == "ram" {
					runtime = ws.server.boot
					if ws.server.controller != nil {
						if patch, known := ws.server.routingPatch(u.op.bucket.ID(), u.op.object); known {
							runtime = patch.Entries[0].Runtime
						}
					}
				}
				ws.uploadMu.Lock()
				u.finishing = true
				ws.uploadMu.Unlock()
				if startCall(func() {
					defer ws.completeUpload(u)
					result, status, commitErr := u.op.commit(ws.objectContext(id), p, u.file, runtime)
					ws.completeUpload(u)
					if ws.finishUpload(u, result, status, commitErr) != nil {
						ws.close()
					}
				}) {
					return nil
				}
				ws.uploadMu.Lock()
				u.finishing = false
				ws.uploadMu.Unlock()
				err = fail(503, "client request concurrency budget exhausted")
			}
		}
	}
	ws.discardUpload(id)
	return ws.finishUpload(u, nil, 0, err)
}
func equalObjectBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func (ws *wsSession) sendObjectStream(id client.ID, src io.Reader, length uint64, digest [32]byte) error {
	ctx := ws.objectContext(id)
	e := client.Encoder{}
	e.ID(id)
	e.U64(length)
	e.B = append(e.B, digest[:]...)
	e.Bool(false)
	e.Bool(false)
	if err := ws.sendMessageContext(ctx, client.Message{Kind: client.Push, Opcode: 13, ID: client.NewRequestID(), Body: e.B}); err != nil {
		return err
	}
	buffer := make([]byte, 1<<20)
	var offset, part uint64
	for offset < length {
		count := int(min(uint64(len(buffer)), length-offset))
		if _, err := io.ReadFull(src, buffer[:count]); err != nil {
			return err
		}
		e = client.Encoder{}
		e.ID(id)
		e.U64(part)
		e.U64(offset)
		e.Blob(buffer[:count])
		if err := ws.sendMessageContext(ctx, client.Message{Kind: client.Push, Opcode: 5, ID: client.NewRequestID(), Body: e.B}); err != nil {
			return err
		}
		offset += uint64(count)
		part++
	}
	e = client.Encoder{}
	e.ID(id)
	e.U64(length)
	e.B = append(e.B, digest[:]...)
	return ws.sendMessageContext(ctx, client.Message{Kind: client.Push, Opcode: 6, ID: client.NewRequestID(), Body: e.B})
}
