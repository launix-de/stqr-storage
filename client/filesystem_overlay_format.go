// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"bytes"
	"errors"
	"path"
	"sort"
	"strings"
)

var ErrOverlayNeedsLower = errors.New("client: overlay bucket requires its readonly base bucket")

func ParseFilesystemWhitelist(value string) ([]string, error) {
	if value == "" {
		return nil, errors.New("whitelist cannot be empty")
	}
	return canonicalWhitelist(strings.Split(value, ","))
}

// STOR v1 is the physical upper root: lower bucket ID and an ordinary segmented
// blob containing STOM v1. STOM holds a persisted write policy and sparse directory
// records. A zero entry OID is a whiteout. Non-opaque records merge a lower
// directory by OID; opaque records are independent upper directories. Root CAS
// publishes a whole namespace change, including both halves of rename.
type overlayState struct {
	orphaned      []ObjectID // transient publication participants; never encoded
	orphanMembers []fsTxMember
	rootRaw       []byte // exact bytes observed before editing this namespace
	policy        []string
	dirs          map[ObjectID]*overlayDirectory
}
type overlayDirectory struct {
	lower            ObjectID
	opaque, metadata bool
	node             fsNode
	entries          map[string]ObjectID
}

func validOverlayPath(p string) bool {
	if p == "" || len(p) > 4096 || path.IsAbs(p) || path.Clean(p) != p {
		return false
	}
	for _, name := range strings.Split(p, "/") {
		if !validFSName(name) {
			return false
		}
	}
	return true
}
func canonicalWhitelist(entries []string) ([]string, error) {
	if len(entries) > 4096 {
		return nil, errors.New("whitelist is limited to 4096 paths")
	}
	result := []string{}
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry != strings.TrimSpace(entry) {
			return nil, errors.New("whitelist paths cannot have surrounding whitespace")
		}
		p := strings.TrimSuffix(entry, "/")
		if !validOverlayPath(p) {
			return nil, errors.New("whitelist requires relative file paths or directory paths ending in /")
		}
		if !seen[entry] {
			result = append(result, entry)
			seen[entry] = true
		}
	}
	sort.Strings(result)
	return result, nil
}
func overlayAllowed(policy []string, p string, directory bool) bool {
	if len(policy) == 0 {
		return true
	}
	for _, rule := range policy {
		if strings.HasSuffix(rule, "/") {
			base := strings.TrimSuffix(rule, "/")
			if directory && p == base || strings.HasPrefix(p, rule) {
				return true
			}
		} else if !directory && p == rule {
			return true
		}
	}
	return false
}
func encodeOverlayState(s *overlayState) ([]byte, error) {
	e := Encoder{B: []byte("STOM")}
	e.U16(1)
	e.U32(uint32(len(s.policy)))
	for _, p := range s.policy {
		e.Text(p)
	}
	ids := make([]ObjectID, 0, len(s.dirs))
	for id := range s.dirs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	e.U32(uint32(len(ids)))
	for _, id := range ids {
		dir := s.dirs[id]
		e.B = append(e.B, id[:]...)
		e.B = append(e.B, dir.lower[:]...)
		e.Bool(dir.opaque)
		e.Bool(dir.metadata)
		e.Blob(encodeFSNode(dir.node))
		names := make([]string, 0, len(dir.entries))
		for name := range dir.entries {
			if !validFSName(name) {
				return nil, ErrFSFormat
			}
			names = append(names, name)
		}
		sort.Strings(names)
		e.U32(uint32(len(names)))
		for _, name := range names {
			e.Text(name)
			child := dir.entries[name]
			e.B = append(e.B, child[:]...)
		}
	}
	return e.B, nil
}
func decodeOverlayState(raw []byte) (*overlayState, error) {
	s := &overlayState{dirs: map[ObjectID]*overlayDirectory{}}
	d := Decoder{B: raw}
	if string(d.Take(4)) != "STOM" || d.U16() != 1 {
		return nil, ErrFSFormat
	}
	count := d.U32()
	if count > 4096 || uint64(count) > uint64(len(d.B)) {
		return nil, ErrFSFormat
	}
	for i := uint32(0); i < count; i++ {
		s.policy = append(s.policy, d.Text())
	}
	canonical, err := canonicalWhitelist(s.policy)
	if err != nil || len(canonical) != len(s.policy) {
		return nil, ErrFSFormat
	}
	for i, p := range canonical {
		if p != s.policy[i] {
			return nil, ErrFSFormat
		}
	}
	count = d.U32()
	if count == 0 || uint64(count) > uint64(len(d.B))/70 {
		return nil, ErrFSFormat
	}
	var previous ObjectID
	for i := uint32(0); i < count; i++ {
		var id, lower ObjectID
		copy(id[:], d.Take(32))
		copy(lower[:], d.Take(32))
		if id == (ObjectID{}) || i > 0 && bytes.Compare(previous[:], id[:]) >= 0 {
			return nil, ErrFSFormat
		}
		previous = id
		dir := &overlayDirectory{lower: lower, opaque: d.Bool(), metadata: d.Bool(), entries: map[string]ObjectID{}}
		dir.node, err = decodeFSNode(d.Blob())
		if err != nil || dir.node.kind != fsDirectory || dir.node.blob.lower || dir.node.blob.root != (ObjectID{}) {
			return nil, ErrFSFormat
		}
		if dir.opaque && lower != (ObjectID{}) || !dir.opaque && lower == (ObjectID{}) {
			return nil, ErrFSFormat
		}
		n := d.U32()
		if uint64(n) > uint64(len(d.B))/34 {
			return nil, ErrFSFormat
		}
		last := ""
		for j := uint32(0); j < n; j++ {
			name := d.Text()
			var child ObjectID
			copy(child[:], d.Take(32))
			if !validFSName(name) || j > 0 && name <= last {
				return nil, ErrFSFormat
			}
			last = name
			dir.entries[name] = child
		}
		s.dirs[id] = dir
	}
	if d.Done() != nil || s.dirs[FilesystemRootID()] == nil || s.dirs[FilesystemRootID()].node.removed {
		return nil, ErrFSFormat
	}
	return s, nil
}
func encodeOverlayRoot(lower BucketID, blob fsBlob) []byte {
	e := Encoder{B: []byte("STOR")}
	e.U16(1)
	e.ID(lower)
	e.Blob(encodeFSNode(fsNode{kind: fsFile, blob: blob}))
	return e.B
}
func decodeOverlayRoot(raw []byte) (BucketID, fsBlob, error) {
	d := Decoder{B: raw}
	if len(raw) > FSObjectSize || string(d.Take(4)) != "STOR" || d.U16() != 1 {
		return BucketID{}, fsBlob{}, ErrFSFormat
	}
	id := d.ID()
	node, err := decodeFSNode(d.Blob())
	if err != nil || d.Done() != nil || id == (BucketID{}) || node.kind != fsFile || node.blob.lower {
		return id, fsBlob{}, ErrFSFormat
	}
	return id, node.blob, nil
}
