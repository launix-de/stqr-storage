// SPDX-License-Identifier: GPL-3.0-only
package server

// BucketReceipt is persisted atomically with the description it changes. It
// makes create/config/delete retries recoverable even if their response is lost.
type BucketReceipt struct {
	ID      string `json:"id"`
	Actor   string `json:"actor"`
	Digest  string `json:"digest"`
	Opcode  uint16 `json:"opcode"`
	Status  int    `json:"status"`
	Objects uint64 `json:"objects,omitempty"`
	Running bool   `json:"running,omitempty"`
}
