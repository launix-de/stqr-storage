// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// RestartReceipt proves one sealed OS-backed accepted image, not durability.
// The caller must keep its node manifest and this immutable image together.
// Neither writing nor restoring it invokes VolumeIO.Sync or filesystem fsync.
type RestartReceipt struct {
	Handoff, Cluster, Node, Volume ID
	BaselineGeneration, Generation uint64
	ConfirmedGeneration            uint64
	HeaderDigest                   [32]byte
}

const restartMetadataBytes = 256

func writeRestartPage(f *os.File, off uint64, page []byte) error {
	n, err := f.WriteAt(page, int64(off))
	if err == nil && n != len(page) {
		err = io.ErrShortWrite
	}
	return err
}

// RetainAccepted streams the already-published dirty image to a new sealed file.
// It waits only for current readers/private publication/an already-requested
// checkpoint. It does not drain client connections or start a durability fence.
// path must be a fresh, node-owned file path; raw devices must use a separate
// node writeback directory. The previous complete image is never overwritten.
func (v *Volume) RetainAccepted(path string, handoff ID) (RestartReceipt, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.retainAcceptedLocked(path, handoff)
}
func (v *Volume) retainAcceptedLocked(path string, handoff ID) (receipt RestartReceipt, err error) {
	if handoff == (ID{}) || path == "" {
		return receipt, ErrRange
	}
	if v.sb.Identity.Class == RAM || v.sb.Identity.Class == RAMCache {
		return receipt, ErrUnsupported
	}
	for v.preparing || v.checkpointing {
		v.prepareCond.Wait()
	}
	if err = v.check(false); err != nil {
		return receipt, err
	}
	if v.staging != nil || v.overlay != nil || v.stageStore != nil {
		return receipt, ErrBusy
	}
	if _, err = os.Lstat(path); err == nil {
		return receipt, os.ErrExist
	} else if !os.IsNotExist(err) {
		return receipt, err
	}
	baseline, err := loadSuperblocks(v.dev)
	if err != nil {
		return receipt, err
	}
	if baseline.Generation < v.durableGeneration || baseline.Generation != v.durableGeneration && (!v.restartCanonicalPending || baseline.Generation != v.restartCanonicalGeneration) {
		return receipt, fmt.Errorf("%w: restart baseline changed", ErrRecovery)
	}
	baselineRaw, err := baseline.MarshalBinary()
	if err != nil {
		return receipt, err
	}
	acceptedHeader := v.sb
	acceptedHeader.RestartLineage = handoff
	acceptedRaw, err := acceptedHeader.MarshalBinary()
	if err != nil {
		return receipt, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return receipt, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".stqr-writeback-*")
	if err != nil {
		return receipt, err
	}
	temporary := f.Name()
	published := false
	defer func() {
		f.Close()
		if !published {
			os.Remove(temporary)
		}
	}()
	if err = f.Chmod(0600); err != nil {
		return receipt, err
	}
	if err = writeRestartPage(f, PageSize, acceptedRaw); err != nil {
		return receipt, err
	}
	if err = writeRestartPage(f, 2*PageSize, baselineRaw); err != nil {
		return receipt, err
	}
	meta := make([]byte, restartMetadataBytes)
	copy(meta, "RESTART1")
	copy(meta[8:24], handoff[:])
	copy(meta[24:40], v.sb.Identity.ClusterID[:])
	copy(meta[40:56], v.sb.Identity.NodeID[:])
	copy(meta[56:72], v.sb.Identity.VolumeID[:])
	binary.LittleEndian.PutUint64(meta[72:], baseline.Generation)
	binary.LittleEndian.PutUint64(meta[80:], v.sb.Generation)
	baseHash, acceptedHash := sha256.Sum256(baselineRaw), sha256.Sum256(acceptedRaw)
	copy(meta[88:120], baseHash[:])
	copy(meta[120:152], acceptedHash[:])
	binary.LittleEndian.PutUint64(meta[184:], v.durableGeneration)
	b := restartIndexBuilder{file: f, end: 3 * PageSize, headerData: meta}
	// Capture maintained aggregate maps only; never inspect object records.
	_, accounting, _ := v.encodeAccountingCheckpointCacheLocked()
	if len(accounting) > 0 {
		digest := sha256.Sum256(acceptedRaw)
		copy(accounting[8:40], digest[:])
		sum := sha256.Sum256(accounting[:len(accounting)-sha256.Size])
		copy(accounting[len(accounting)-sha256.Size:], sum[:])
		binary.LittleEndian.PutUint64(meta[192:], b.end)
		binary.LittleEndian.PutUint64(meta[200:], uint64(len(accounting)))
		sum = sha256.Sum256(accounting)
		copy(meta[208:240], sum[:])
		if n, e := f.WriteAt(accounting, int64(b.end)); e != nil {
			return receipt, e
		} else if n != len(accounting) {
			return receipt, io.ErrShortWrite
		}
		b.end += (uint64(len(accounting)) + PageSize - 1) / PageSize * PageSize
	}

	if v.deferred != nil {
		g := v.deferred
		binary.LittleEndian.PutUint64(meta[152:], g.journal.off)
		binary.LittleEndian.PutUint64(meta[160:], g.journal.n)
		binary.LittleEndian.PutUint64(meta[168:], g.protectedPages)
		binary.LittleEndian.PutUint64(meta[176:], g.baseFreeRoot)
		if err = g.store.eachSorted(b.add); err != nil {
			return receipt, err
		}
	}
	if b.count == 0 {
		clear(meta[152:184])
	}
	if err = b.seal(); err != nil {
		return receipt, err
	}
	header := make([]byte, PageSize)
	if _, err = f.ReadAt(header, 0); err != nil {
		return receipt, err
	}
	receipt = RestartReceipt{Handoff: handoff, Cluster: v.sb.Identity.ClusterID, Node: v.sb.Identity.NodeID, Volume: v.sb.Identity.VolumeID, BaselineGeneration: baseline.Generation, Generation: v.sb.Generation, ConfirmedGeneration: v.durableGeneration, HeaderDigest: sha256.Sum256(header)}
	if err = f.Close(); err != nil {
		return receipt, err
	}
	// Link gives exclusive publication and never replaces another sealed image.
	// Both paths are in one node-owned directory; removing the private name is
	// not a persistence claim. The sealed file remains immutable after publish.
	if err = os.Link(temporary, path); err != nil {
		return receipt, err
	}
	published = true
	if err = os.Remove(temporary); err != nil {
		return receipt, err
	}
	v.retainedGeneration = v.sb.Generation
	return receipt, nil
}

func openRestartImage(path string, receipt RestartReceipt) (*restartIndexReader, []byte, []byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}
	keep := false
	defer func() {
		if !keep {
			f.Close()
		}
	}()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		if err == nil {
			err = ErrCorrupt
		}
		return nil, nil, nil, err
	}
	if stat.Size() < 0 {
		return nil, nil, nil, ErrCorrupt
	}
	mapping, err := mapVolumeFile(f, stat.Size())
	if err != nil {
		return nil, nil, nil, err
	}
	mapped := &restartMappedSource{file: f, mapping: mapping, size: uint64(stat.Size())}
	mappedKeep := false
	defer func() {
		if !mappedKeep {
			unmapVolumeFile(mapping)
		}
	}()
	r, err := restartIndexOpen(mapped, uint64(stat.Size()))
	if err != nil {
		return nil, nil, nil, err
	}
	if sha256.Sum256(r.header) != receipt.HeaderDigest {
		return nil, nil, nil, ErrCorrupt
	}
	m := r.header[96 : 96+restartMetadataBytes]
	if string(m[:8]) != "RESTART1" || !bytes.Equal(m[8:24], receipt.Handoff[:]) || !bytes.Equal(m[24:40], receipt.Cluster[:]) || !bytes.Equal(m[40:56], receipt.Node[:]) || !bytes.Equal(m[56:72], receipt.Volume[:]) || binary.LittleEndian.Uint64(m[72:]) != receipt.BaselineGeneration || binary.LittleEndian.Uint64(m[80:]) != receipt.Generation || binary.LittleEndian.Uint64(m[184:]) != receipt.ConfirmedGeneration {
		return nil, nil, nil, ErrIdentity
	}
	accepted, baseline := make([]byte, PageSize), make([]byte, PageSize)
	if err = restartIndexRead(f, accepted, PageSize); err != nil {
		return nil, nil, nil, err
	}
	if err = restartIndexRead(f, baseline, 2*PageSize); err != nil {
		return nil, nil, nil, err
	}
	a, b := sha256.Sum256(accepted), sha256.Sum256(baseline)
	if !bytes.Equal(a[:], m[120:152]) || !bytes.Equal(b[:], m[88:120]) {
		return nil, nil, nil, ErrCorrupt
	}
	r.close = func() error { err := unmapVolumeFile(mapping); return errors.Join(err, f.Close()) }
	mappedKeep = true
	keep = true
	return r, accepted, baseline, nil
}

// restoreRestartLocked runs before root/index/accounting access. A different
// higher native generation is NOT ancestry proof. Only the exact baseline or
// retained checkpoint or an explicitly tagged canonical descendant is accepted.
func (v *Volume) restoreRestartLocked(path string, receipt RestartReceipt) error {
	if v.sb.Identity.ClusterID != receipt.Cluster || v.sb.Identity.NodeID != receipt.Node || v.sb.Identity.VolumeID != receipt.Volume {
		return ErrIdentity
	}
	if receipt.Handoff == (ID{}) || receipt.ConfirmedGeneration > receipt.BaselineGeneration || receipt.BaselineGeneration > receipt.Generation {
		return ErrCorrupt
	}
	if v.sb.RestartLineage == receipt.Handoff && v.sb.Generation >= receipt.Generation {
		v.restartCanonicalGeneration = v.sb.Generation
		v.durableGeneration = receipt.ConfirmedGeneration
		v.restartCanonicalPending = v.sb.Generation > v.durableGeneration
		return nil
	}
	r, acceptedRaw, baselineRaw, err := openRestartImage(path, receipt)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep && r.close != nil {
			r.close()
		}
	}()
	accepted, err := DecodeSuperblock(acceptedRaw)
	if err != nil {
		return err
	}
	baseline, err := DecodeSuperblock(baselineRaw)
	if err != nil {
		return err
	}
	if accepted.Identity != v.sb.Identity || baseline.Identity != v.sb.Identity || accepted.Generation != receipt.Generation || baseline.Generation != receipt.BaselineGeneration || accepted.Generation < baseline.Generation {
		return ErrIdentity
	}
	currentRaw, err := v.sb.MarshalBinary()
	if err != nil {
		return err
	}
	if bytes.Equal(currentRaw, acceptedRaw) {
		v.restartCanonicalGeneration = v.sb.Generation
		v.durableGeneration = receipt.ConfirmedGeneration
		v.restartCanonicalPending = v.sb.Generation > v.durableGeneration
		return nil
	}
	if !bytes.Equal(currentRaw, baselineRaw) {
		return fmt.Errorf("%w: unproven restart baseline", ErrRecovery)
	}
	v.restartCanonicalGeneration = baseline.Generation
	v.durableGeneration = receipt.ConfirmedGeneration
	v.restartCanonicalPending = baseline.Generation > v.durableGeneration
	if accepted.Generation == baseline.Generation {
		metadataOnly := baseline
		metadataOnly.RestartLineage = accepted.RestartLineage
		if accepted != metadataOnly || r.count != 0 {
			return ErrCorrupt
		}
		v.sb = accepted
		v.restartAccounting = readRestartAccounting(r)
		v.publishIdentity()
		return nil
	}
	if r.count == 0 || r.root.max >= accepted.Identity.Size {
		return ErrCorrupt
	}
	m := r.header[96:]
	journal := allocation{binary.LittleEndian.Uint64(m[152:]), binary.LittleEndian.Uint64(m[160:]), false}
	protected, baseRoot := binary.LittleEndian.Uint64(m[168:]), binary.LittleEndian.Uint64(m[176:])
	if journal.off%PageSize != 0 || journal.n%PageSize != 0 || journal.off < 4*PageSize || journal.n < 8*PageSize || journal.off > accepted.Identity.Size || journal.n > accepted.Identity.Size-journal.off || protected > r.count || baseRoot != baseline.FreeRoot {
		return ErrCorrupt
	}
	store := newDirtyStore(deferredPageLimit, false)
	store.retained = r
	v.deferred = &deferredGroup{journal: journal, pages: store.pages, pageLimit: deferredPageLimit, store: store, protectedPages: protected, baseFreeRoot: baseRoot, baseNodes: newNodeCache(128, 1<<20)}
	v.sb = accepted
	v.restartAccounting = readRestartAccounting(r)
	v.cache.clear()
	v.nodes.clear()
	v.publishIdentity()
	keep = true
	return nil
}

// The immutable index/payload mapping is demand-paged. Volume reader/handle
// lifetimes pin it; no borrowed slice survives Close or a completed checkpoint.
// Other platforms retain bounded positioned reads instead of eager loading.
type restartMappedSource struct {
	file    *os.File
	mapping []byte
	size    uint64
}

func (s *restartMappedSource) ReadAt(p []byte, off int64) (int, error) {
	if s.mapping == nil {
		return s.file.ReadAt(p, off)
	}
	if off < 0 || uint64(off) > s.size {
		return 0, io.EOF
	}
	n := copy(p, s.mapping[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (s *restartMappedSource) retainedPage(off uint64) ([]byte, error) {
	if off > s.size || PageSize > s.size-off {
		return nil, io.ErrUnexpectedEOF
	}
	if s.mapping != nil {
		return s.mapping[off : off+PageSize : off+PageSize], nil
	}
	p := make([]byte, PageSize)
	if err := restartIndexRead(s.file, p, off); err != nil {
		return nil, err
	}
	return p, nil
}

// A missing/corrupt optional aggregate cache is unknown accounting, never zero.
// The retained header binds its exact buffer; ConfigureAccounting additionally
// validates the accepted native header and immutable ownership predicate.
func readRestartAccounting(r *restartIndexReader) []byte {
	m := r.header[96:]
	off, n := binary.LittleEndian.Uint64(m[192:]), binary.LittleEndian.Uint64(m[200:])
	if n == 0 || n > accountingCacheLimit || off < 3*PageSize || off%PageSize != 0 || off > r.size-PageSize || n > r.size-PageSize-off {
		return nil
	}
	b := make([]byte, int(n))
	if restartIndexRead(r.source, b, off) != nil {
		return nil
	}
	sum := sha256.Sum256(b)
	if !bytes.Equal(sum[:], m[208:240]) {
		return nil
	}
	return b
}
