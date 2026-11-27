// SPDX-License-Identifier: GPL-3.0-only
package server

import "os"

const inlineObjectDownload = 1 << 20

// Enough space for ObjectInfo, optional range metadata and the inline transfer
// header. Keeping it before the payload lets the final response own the same
// buffer, rather than copying the snapshot again during protocol encoding.
const objectDownloadHeader = 128

// objectDownload captures one checked object snapshot. Small replies stay in a
// bounded owned buffer; only crossing the inline limit creates a file. It never
// retains a borrowed volume slice and never sends to a socket under its lock.
type objectDownload struct {
	memory    []byte // header reserve followed by payload
	file      *os.File
	streaming bool // HTTP has no large-response chunk transport
}

func (d *objectDownload) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if d.file != nil {
		return d.file.Write(p)
	}
	used := max(0, len(d.memory)-objectDownloadHeader)
	if len(p) <= inlineObjectDownload-used {
		need := objectDownloadHeader + used + len(p)
		if cap(d.memory) < need {
			capacity := min(inlineObjectDownload+objectDownloadHeader, max(need, cap(d.memory)*2))
			memory := make([]byte, objectDownloadHeader+used, capacity)
			copy(memory, d.memory)
			d.memory = memory
		}
		d.memory = append(d.memory, p...)
		return len(p), nil
	}
	if !d.streaming {
		return 0, fail(400, "large GET requires WebSocket streaming")
	}
	f, err := os.CreateTemp("", "stqr-server-download-*")
	if err != nil {
		return 0, err
	}
	d.file = f // Close also cleans up a failed prefix write.
	if used != 0 {
		if _, err = f.Write(d.memory[objectDownloadHeader:]); err != nil {
			return 0, err
		}
	}
	d.memory = nil
	return f.Write(p)
}

func (d *objectDownload) inline(header []byte) []byte {
	if len(d.memory) == 0 {
		return header
	}
	// Keep future protocol extensions correct if their header grows beyond the
	// current reserve. Existing messages fit without a second payload copy.
	if len(header) > objectDownloadHeader {
		return append(header, d.memory[objectDownloadHeader:]...)
	}
	out := d.memory[objectDownloadHeader-len(header):]
	copy(out, header)
	return out
}

func (d *objectDownload) Close() {
	if d.file != nil {
		file := d.file
		d.file = nil
		file.Close()
		os.Remove(file.Name())
	}
}
