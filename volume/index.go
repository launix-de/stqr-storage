// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const deltaLimit = 128
const maxIndexRuns = 8
const indexEntrySize = 64
const entriesPerPage = pagePayload / indexEntrySize

type indexKey [49]byte

func objectKey(k Key) (x indexKey)  { copy(x[1:17], k.Bucket[:]); copy(x[17:], k.Object[:]); return }
func requestKey(id ID) (x indexKey) { x[0] = 1; copy(x[1:17], id[:]); return }
func (x indexKey) object() (k Key)  { copy(k.Bucket[:], x[1:17]); copy(k.Object[:], x[17:]); return }

type indexEntry struct {
	Key   indexKey
	Value uint64
}
type indexRun struct{ Location, Count uint64 }

func (r indexRun) pages() uint64 { return (r.Count + entriesPerPage - 1) / entriesPerPage }
func lessKey(a, b indexKey) bool { return bytes.Compare(a[:], b[:]) < 0 }

type cachedObject struct {
	key   Key
	value *object
	cost  uint64
}
type objectCache struct {
	mu           sync.Mutex
	items        map[Key]*list.Element
	lru          *list.List
	entries      int
	budget, used uint64
}

func newCache(n int, b uint64) *objectCache {
	if n == 0 {
		n = 256
	}
	if b == 0 {
		b = 16 << 20
	}
	return &objectCache{items: make(map[Key]*list.Element), lru: list.New(), entries: n, budget: b}
}
func (c *objectCache) get(k Key) *object {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[k]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cachedObject).value
	}
	return nil
}
func (c *objectCache) put(k Key, o *object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.items[k]; e != nil {
		c.used -= e.Value.(cachedObject).cost
		c.lru.Remove(e)
		delete(c.items, k)
	}
	cost := uint64(128 + len(o.Extents)*80)
	if c.entries < 0 || cost > c.budget {
		return
	}
	e := c.lru.PushFront(cachedObject{k, o, cost})
	c.items[k] = e
	c.used += cost
	for len(c.items) > c.entries || c.used > c.budget {
		e := c.lru.Back()
		x := e.Value.(cachedObject)
		delete(c.items, x.key)
		c.used -= x.cost
		c.lru.Remove(e)
	}
}
func (c *objectCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.items)
	c.lru.Init()
	c.used = 0
}

func (v *Volume) indexEntry(s Superblock, r indexRun, pos uint64) (indexEntry, error) {
	var e indexEntry
	if pos >= r.Count {
		return e, ErrRange
	}
	page := pos / entriesPerPage
	slot := pos % entriesPerPage
	b := make([]byte, PageSize)
	if err := readFullAt(v.dev, b, s.bankStart()+r.Location+page*PageSize); err != nil {
		return e, err
	}
	p, err := decodePage(b, "IDX1", s.BankEpoch, page, s.Identity.VolumeID)
	if err != nil || p.Generation > s.Generation {
		return e, ErrCorrupt
	}
	at := int(slot) * indexEntrySize
	if uint64(p.Used) < uint64(at+indexEntrySize) {
		return e, ErrCorrupt
	}
	copy(e.Key[:], p.Data[at:at+49])
	e.Value = diskOrder.Uint64(p.Data[at+56:])
	if e.Key[0] > 1 {
		return e, ErrCorrupt
	}
	return e, nil
}
func (v *Volume) lookup(k indexKey) (uint64, error) {
	if x, ok := v.delta[k]; ok {
		return x, nil
	}
	for i := len(v.runs) - 1; i >= 0; i-- {
		r := v.runs[i]
		lo, hi := uint64(0), r.Count
		for lo < hi {
			mid := lo + (hi-lo)/2
			e, err := v.indexEntry(v.sb, r, mid)
			if err != nil {
				return 0, err
			}
			if lessKey(e.Key, k) {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < r.Count {
			e, err := v.indexEntry(v.sb, r, lo)
			if err != nil {
				return 0, err
			}
			if e.Key == k {
				return e.Value, nil
			}
		}
	}
	return 0, nil
}
func sortedDelta(m map[indexKey]uint64) []indexEntry {
	x := make([]indexEntry, 0, len(m))
	for k, v := range m {
		x = append(x, indexEntry{k, v})
	}
	sort.Slice(x, func(i, j int) bool { return lessKey(x[i].Key, x[j].Key) })
	return x
}

// walkIndex merges a bounded number of sorted immutable on-volume runs and the
// bounded delta. No object-count-sized map is required, including during GC.
func (v *Volume) walkIndex(fn func(indexEntry) error) error {
	delta := sortedDelta(v.delta)
	pos := make([]uint64, len(v.runs))
	heads := make([]indexEntry, len(v.runs))
	valid := make([]bool, len(v.runs))
	for i, r := range v.runs {
		if r.Count > 0 {
			e, err := v.indexEntry(v.sb, r, 0)
			if err != nil {
				return err
			}
			heads[i] = e
			valid[i] = true
		}
	}
	dp := 0
	for {
		found := false
		var key indexKey
		for i, e := range heads {
			if valid[i] && (!found || lessKey(e.Key, key)) {
				found = true
				key = e.Key
			}
		}
		if dp < len(delta) && (!found || lessKey(delta[dp].Key, key)) {
			found = true
			key = delta[dp].Key
		}
		if !found {
			return nil
		}
		var chosen indexEntry
		for i := range heads {
			if valid[i] && heads[i].Key == key {
				chosen = heads[i]
				pos[i]++
				if pos[i] >= v.runs[i].Count {
					valid[i] = false
				} else {
					e, err := v.indexEntry(v.sb, v.runs[i], pos[i])
					if err != nil {
						return err
					}
					heads[i] = e
				}
			}
		}
		if dp < len(delta) && delta[dp].Key == key {
			chosen = delta[dp]
			dp++
		}
		if chosen.Value != 0 {
			if err := fn(chosen); err != nil {
				return err
			}
		}
	}
}
func (v *Volume) writeIndex(s *Superblock, entries []indexEntry) (indexRun, error) {
	r := indexRun{Count: uint64(len(entries))}
	if r.Count == 0 {
		return r, nil
	}
	n := r.pages() * PageSize
	off, err := reserve(s, n)
	if err != nil {
		return r, err
	}
	r.Location = off
	for page := uint64(0); page < r.pages(); page++ {
		data := make([]byte, pagePayload)
		start := int(page) * entriesPerPage
		end := min(start+entriesPerPage, len(entries))
		for j := start; j < end; j++ {
			at := (j - start) * indexEntrySize
			copy(data[at:], entries[j].Key[:])
			binary.LittleEndian.PutUint64(data[at+56:], entries[j].Value)
		}
		p := cowPage{VolumeID: s.Identity.VolumeID, Epoch: s.BankEpoch, Index: page, Generation: s.Generation + 1, Used: uint32((end - start) * indexEntrySize), Data: data}
		if err = v.write(encodePage("IDX1", p), s.bankStart()+off+page*PageSize); err != nil {
			return r, err
		}
	}
	s.IndexBytes += n
	return r, nil
}
func (v *Volume) readRecord(s Superblock, off uint64) (record, error) {
	var out record
	if off > s.JournalBytes || s.JournalBytes-off < recordHeader {
		return out, ErrCorrupt
	}
	b := make([]byte, recordHeader)
	if err := v.readMeta(s, b, off); err != nil {
		return out, err
	}
	n := diskOrder.Uint64(b[8:])
	if n < recordHeader+36 || n > s.JournalBytes-off || n > uint64(^uint(0)>>1) {
		return out, ErrCorrupt
	}
	b = make([]byte, int(n))
	if err := v.readMeta(s, b, off); err != nil {
		return out, err
	}
	r, err := decodeRecord(b)
	if err != nil {
		return r, err
	}
	if r.Previous != 0 && r.Previous-1 >= off {
		return r, ErrCorrupt
	}
	if r.Arena.Pages == 0 {
		if r.Arena.Base != 0 || r.Arena.Used != 0 {
			return r, ErrCorrupt
		}
	} else if r.Arena.Pages > 256 || r.Arena.Pages&(r.Arena.Pages-1) != 0 || r.Arena.Used > r.Arena.capacity() || r.Arena.Base < s.DataTop || r.Arena.Base > s.bankSize() || uint64(r.Arena.Pages)*2*PageSize > s.bankSize()-r.Arena.Base {
		return r, ErrCorrupt
	}
	for _, x := range r.Extents {
		if x.Location < s.DataTop || x.Location%PageSize != 0 || x.Location > s.bankSize() {
			return r, ErrCorrupt
		}
		if x.Arena {
			if x.ArenaOffset > s.bankSize() || uint64(x.Stored) > s.bankSize()-x.ArenaOffset {
				return r, ErrCorrupt
			}
			last := (x.ArenaOffset + uint64(x.Stored) - 1) / pagePayload
			if last > (s.bankSize()-x.Location)/(2*PageSize) || 2*PageSize > s.bankSize()-x.Location-last*2*PageSize {
				return r, ErrCorrupt
			}
		} else if align(uint64(x.Stored)) > s.bankSize()-x.Location {
			return r, ErrCorrupt
		}
	}
	return r, nil
}
func (v *Volume) readMeta(s Superblock, dst []byte, off uint64) error {
	if off > s.JournalBytes || uint64(len(dst)) > s.JournalBytes-off {
		return ErrCorrupt
	}
	for len(dst) > 0 {
		p, _, e := v.readMetaPage(s, off/pagePayload)
		if e != nil {
			return e
		}
		at := int(off % pagePayload)
		n := min(len(dst), pagePayload-at)
		if at+n > int(p.Used) {
			return ErrCorrupt
		}
		copy(dst[:n], p.Data[at:at+n])
		dst = dst[n:]
		off += uint64(n)
	}
	return nil
}
func (v *Volume) loadObject(k Key) (*object, error) {
	if o := v.cache.get(k); o != nil {
		return o, nil
	}
	return v.loadObjectUncached(k)
}
func (v *Volume) loadObjectUncached(k Key) (*object, error) {
	p, e := v.lookup(objectKey(k))
	if e != nil {
		return nil, e
	}
	if p == 0 {
		return nil, ErrNotFound
	}
	var chain []record
	for p != 0 {
		r, err := v.readRecord(v.sb, p-1)
		if err != nil {
			return nil, err
		}
		if r.Key != k || r.Op == opIndex || r.Op == opDedupe || r.Op == opForget {
			return nil, ErrCorrupt
		}
		chain = append(chain, r)
		if r.Op == opPut || r.Op == opSnapshot || r.Replace || r.Op == opDelete {
			break
		}
		p = r.Previous
	}
	o := &object{}
	for i := len(chain) - 1; i >= 0; i-- {
		if e := applyObject(o, chain[i]); e != nil {
			return nil, e
		}
	}
	v.cache.put(k, o)
	return o, nil
}
func (v *Volume) loadDedupe(id ID) (dedupe, bool, error) {
	var d dedupe
	if id == (ID{}) {
		return d, false, nil
	}
	p, e := v.lookup(requestKey(id))
	if e != nil || p == 0 {
		return d, false, e
	}
	r, e := v.readRecord(v.sb, p-1)
	if e != nil {
		return d, false, e
	}
	if r.Request != id {
		return d, false, fmt.Errorf("%w: retry index", ErrCorrupt)
	}
	return dedupe{r.Key, r.Fingerprint, r.Result}, true, nil
}

// sortedIndexWriter builds one sorted run using a single page of RAM. Callers
// know the entry count from a first streaming pass or a snapshot's entry count.
// This avoids an object-count-sized merge directory during full compaction.
type sortedIndexWriter struct {
	v             *Volume
	s             *Superblock
	run           indexRun
	written, page uint64
	used          int
	buf           []byte
	previous      indexKey
}

func (v *Volume) newIndexWriter(s *Superblock, count uint64) (*sortedIndexWriter, error) {
	w := &sortedIndexWriter{v: v, s: s, run: indexRun{Count: count}, buf: make([]byte, pagePayload)}
	if count == 0 {
		return w, nil
	}
	if count > s.bankSize()/indexEntrySize {
		return nil, ErrFull
	}
	n := w.run.pages() * PageSize
	off, e := reserve(s, n)
	if e != nil {
		return nil, e
	}
	w.run.Location = off
	s.IndexBytes += n
	return w, nil
}
func (w *sortedIndexWriter) append(e indexEntry) error {
	if w.written >= w.run.Count || (w.written > 0 && !lessKey(w.previous, e.Key)) {
		return ErrCorrupt
	}
	if w.used+indexEntrySize > pagePayload {
		if err := w.flush(); err != nil {
			return err
		}
	}
	copy(w.buf[w.used:], e.Key[:])
	diskOrder.PutUint64(w.buf[w.used+56:], e.Value)
	w.previous = e.Key
	w.used += indexEntrySize
	w.written++
	return nil
}
func (w *sortedIndexWriter) flush() error {
	if w.used == 0 {
		return nil
	}
	p := cowPage{VolumeID: w.s.Identity.VolumeID, Epoch: w.s.BankEpoch, Index: w.page, Generation: w.s.Generation + 1, Used: uint32(w.used), Data: w.buf}
	if e := w.v.write(encodePage("IDX1", p), w.s.bankStart()+w.run.Location+w.page*PageSize); e != nil {
		return e
	}
	w.page++
	w.used = 0
	clear(w.buf)
	return nil
}
func (w *sortedIndexWriter) finish() error {
	if w.written != w.run.Count {
		return ErrCorrupt
	}
	return w.flush()
}
