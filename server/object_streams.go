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

type serverObjectUpload struct {
	op         *objectMutation
	file       *os.File
	hash       hash.Hash
	next, part uint64
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
	if u := ws.uploads[id]; u != nil {
		ws.stagedBytes -= u.op.length
		u.close()
		ws.releaseObjectContext(id)
		delete(ws.uploads, id)
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
	if errors.As(err, &redirect) && u.op.message.Kind == client.Request {
		return ws.sendMessage(client.Message{Kind: client.Response, Opcode: u.op.message.Opcode, ID: u.op.message.ID, Body: redirectResponse(redirect.patch)})
	}
	if err != nil {
		status = objectStatus(err)
		result = nil
	}
	detail := ""
	if err != nil {
		detail = err.Error()
	}
	if u.op.message.Kind != client.Request {
		return nil
	}
	return ws.sendMessage(client.Message{Kind: client.Response, Opcode: u.op.message.Opcode, ID: u.op.message.ID, Body: client.ResponseBody(status, result, detail)})
}
func (ws *wsSession) objectTransfer(m client.Message, p principal) error {
	d := client.Decoder{B: m.Body}
	id := d.ID()
	u := ws.uploads[id]
	if m.Opcode == 7 {
		if d.Done() != nil {
			return fail(400, "invalid CANCEL")
		}
		ws.discardUpload(id)
		return nil
	}
	if u == nil {
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
				result, status, commitErr := u.op.commit(ws.objectContext(id), p, u.file, runtime)
				sendErr := ws.finishUpload(u, result, status, commitErr)
				ws.discardUpload(id)
				return sendErr
			}
		}
	}
	sendErr := ws.finishUpload(u, nil, 0, err)
	ws.discardUpload(id)
	return sendErr
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
