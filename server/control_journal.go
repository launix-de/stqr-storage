// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

const controlRecordHeader = 88

var controlLogKey = systemKey("node-control-journal-v1")
var controlMagic = [8]byte{'S', 'T', 'Q', 'R', 'C', 'T', 'L', '1'}

type controlRecord struct {
	Kind  uint16
	Proof client.Proof
	Data  []byte
}

// The private node journal uses the volume engine's atomic, flushed APPEND.
// Replaying never guesses that a corrupt/truncated tail was uncommitted: each
// append is already atomic at that layer. A failed instance is fenced until
// reopened/reconciled. Journal records are bounded control/transfer fragments;
// this bound does not restrict client object sizes or total journal length.
type controlJournal struct {
	mu            sync.Mutex
	store         *volume.Volume
	cluster, node volume.ID
	sequence      uint64
	last          [32]byte
	length        uint64
	failed        error
}

func openControlJournal(path string, cluster, node volume.ID, create bool, replay func(controlRecord) error) (_ *controlJournal, err error) {
	if cluster == (volume.ID{}) || node == (volume.ID{}) {
		return nil, volume.ErrIdentity
	}
	if _, err = os.Stat(path); errors.Is(err, os.ErrNotExist) && create {
		if err = createControlFile(path, cluster, node); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	dev, err := volume.OpenFile(path)
	if err != nil {
		return nil, err
	}
	v, err := volume.Open(dev, volume.OpenOptions{})
	if err != nil {
		dev.Close()
		return nil, err
	}
	j := &controlJournal{store: v, cluster: cluster, node: node}
	defer func() {
		if err != nil {
			v.Close()
		}
	}()
	if err = v.VerifyIdentity(cluster, node, volume.ID{}); err != nil {
		return nil, err
	}
	if err = j.replay(replay); err != nil {
		return nil, err
	}
	if !create && j.sequence == 0 {
		return nil, volume.ErrCorrupt
	}
	return j, nil
}

func createControlFile(path string, cluster, node volume.ID) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(parent, "stqr-control-init-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = f.Truncate(1 << 20)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	dev, err := volume.OpenFile(f.Name())
	if err != nil {
		return err
	}
	v, err := volume.Format(dev, volume.FormatOptions{Class: volume.HDD, ClusterID: cluster, NodeID: node})
	if err != nil {
		dev.Close()
		return err
	}
	err = v.Sync()
	closeErr = v.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	// Publish a complete formatted/bound file without overwriting an existing
	// journal. An interrupted initial format leaves only an unreferenced temp.
	if err = os.Link(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (j *controlJournal) recordDigest(header, payload []byte) (sum [32]byte) {
	h := sha256.New()
	h.Write([]byte("STQR node control record v1\x00"))
	h.Write(j.cluster[:])
	h.Write(j.node[:])
	h.Write(header[:56])
	h.Write(payload)
	copy(sum[:], h.Sum(nil))
	return
}

func (j *controlJournal) replay(consume func(controlRecord) error) error {
	h, err := j.store.Open(false)
	if err != nil {
		return err
	}
	defer h.Close()
	info, err := h.Stat(controlLogKey)
	if errors.Is(err, volume.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	j.length = info.Length
	// CopyTo walks the log's segments once, rather than restarting a span scan
	// for every record. Pipe backpressure bounds staging regardless of log size.
	r, w := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := h.CopyTo(controlLogKey, 0, info.Length, w)
		w.CloseWithError(err)
		done <- err
	}()
	finished := false
	defer func() {
		r.Close()
		if !finished {
			<-done
		}
	}()
	in := bufio.NewReaderSize(r, 1<<20)
	for offset := uint64(0); offset < info.Length; {
		var header [controlRecordHeader]byte
		if _, err := io.ReadFull(in, header[:]); err != nil {
			return errors.Join(volume.ErrCorrupt, err)
		}
		n := binary.LittleEndian.Uint32(header[12:])
		sequence := binary.LittleEndian.Uint64(header[16:])
		kind := binary.LittleEndian.Uint16(header[10:])
		if !bytes.Equal(header[:8], controlMagic[:]) || binary.LittleEndian.Uint16(header[8:]) != 1 || kind == 0 || n > client.MaxMessage || sequence == 0 || sequence != j.sequence+1 || !bytes.Equal(header[24:56], j.last[:]) || uint64(n)+controlRecordHeader > info.Length-offset {
			return volume.ErrCorrupt
		}
		payload := make([]byte, int(n))
		if _, err := io.ReadFull(in, payload); err != nil {
			return errors.Join(volume.ErrCorrupt, err)
		}
		digest := j.recordDigest(header[:], payload)
		if !bytes.Equal(header[56:], digest[:]) {
			return volume.ErrCorrupt
		}
		j.sequence, j.last = sequence, digest
		offset += controlRecordHeader + uint64(n)
		if consume != nil {
			if err := consume(controlRecord{Kind: kind, Proof: client.Proof{Node: client.ID(j.node), RecordSequence: sequence, Digest: digest}, Data: payload}); err != nil {
				return err
			}
		}
	}
	err = <-done
	finished = true
	if err != nil {
		return err
	}
	// Retained append receipts are independent evidence of the durable tail.
	// Replacing the log by a valid shorter prefix cannot erase this evidence.
	return h.ScanRequests(func(record volume.RequestRecord) error {
		if record.Key == controlLogKey && record.Result.Info.Length > j.length {
			return volume.ErrCorrupt
		}
		return nil
	})
}

func (j *controlJournal) append(kind uint16, payload []byte) (client.Proof, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return client.Proof{}, j.failed
	}
	if kind == 0 || len(payload) > client.MaxMessage || j.sequence == ^uint64(0) || j.length > ^uint64(0)-uint64(len(payload))-controlRecordHeader {
		return client.Proof{}, volume.ErrRange
	}
	frame := make([]byte, controlRecordHeader+len(payload))
	copy(frame, controlMagic[:])
	binary.LittleEndian.PutUint16(frame[8:], 1)
	binary.LittleEndian.PutUint16(frame[10:], kind)
	binary.LittleEndian.PutUint32(frame[12:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(frame[16:], j.sequence+1)
	copy(frame[24:], j.last[:])
	copy(frame[controlRecordHeader:], payload)
	digest := j.recordDigest(frame[:controlRecordHeader], payload)
	copy(frame[56:], digest[:])
	for {
		h, err := j.store.Open(true)
		if err != nil {
			j.failed = err
			return client.Proof{}, err
		}
		result, err := h.Append(controlLogKey, frame, volume.Options{ExpectedLength: &j.length, RequestID: volume.ID(digest[:16])})
		h.Close()
		if errors.Is(err, volume.ErrFull) && !errors.Is(err, volume.ErrRecovery) {
			current := j.store.Size()
			if current > uint64(^uint64(0)>>1)/2 {
				return client.Proof{}, volume.ErrFull
			}
			if err := j.store.Grow(int64(current * 2)); err != nil {
				j.failed = err
				return client.Proof{}, err
			}
			continue
		}
		if err == nil && (result.Offset != j.length || result.Info.Length != j.length+uint64(len(frame))) {
			err = volume.ErrCorrupt
		}
		if err != nil {
			j.failed = err
			return client.Proof{}, err
		}
		j.sequence++
		j.last, j.length = digest, result.Info.Length
		return client.Proof{Node: client.ID(j.node), RecordSequence: j.sequence, Digest: digest}, nil
	}
}

func (j *controlJournal) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.store.Close()
}
