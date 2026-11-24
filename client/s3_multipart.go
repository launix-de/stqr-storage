// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	S3UploadOpen uint32 = iota
	S3UploadPublishing
	S3UploadDone
	S3UploadFailed
)
const s3UploadHeader = 32
const s3UploadMetadataLimit = 4 << 20

var ErrS3Upload = errors.New("client: no such multipart upload")
var ErrS3Part = errors.New("client: invalid multipart part")
var ErrS3PartOrder = errors.New("client: invalid multipart part order")
var ErrS3PartSmall = errors.New("client: multipart part smaller than 5 MiB")
var ErrS3UploadBusy = errors.New("client: multipart completion in progress")

type S3Part struct {
	Number    uint32
	Size      uint64
	ETag      string
	Modified  time.Time
	Checksums map[string]string
}
type S3CompletedPart struct {
	Number    uint32
	ETag      string
	Checksums map[string]string
}
type S3Upload struct {
	PublicRead                          bool
	ID, Key, Owner                      string
	State                               uint32
	Created, Updated                    time.Time
	Headers, Metadata                   map[string]string
	Algorithm, ChecksumType             string
	Parts                               []S3Part
	Selected                            []uint32
	Recipe                              [32]byte
	Publication                         PutOptions
	PublishTime                         time.Time
	FinalChecksums                      map[string]string
	ResultETag                          string
	ResultSize                          uint64
	ResultHeaders                       map[string]string
	Version, PayloadOffset, PayloadSize uint64
}

func s3UploadID(id string) (ObjectID, error) {
	b, e := hex.DecodeString(id)
	if e != nil || len(b) != 16 || id != strings.ToLower(id) {
		return ObjectID{}, ErrS3Upload
	}
	return sha256.Sum256([]byte("stqr:s3:upload:v1:\x00" + id)), nil
}
func s3WriteMap(e *Encoder, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.V(uint64(len(keys)))
	for _, k := range keys {
		e.Text(k)
		e.Text(m[k])
	}
}
func s3ReadMap(d *Decoder) map[string]string {
	m := map[string]string{}
	previous := ""
	for n := d.Count(4096); n > 0; n-- {
		k, v := d.Text(), d.Text()
		if k == "" || previous != "" && k <= previous {
			d.Err = ErrS3Format
		}
		m[k] = v
		previous = k
	}
	return m
}
func encodeS3Upload(u S3Upload) ([]byte, error) {
	e := Encoder{B: []byte("STMU")}
	e.U16(1)
	if u.PublicRead {
		e.U16(1)
	} else {
		e.U16(0)
	}
	e.U32(u.State)
	e.U32(0)
	e.U64(uint64(u.Updated.UnixNano()))
	e.U64(uint64(u.Created.UnixNano()))
	e.Text(u.ID)
	e.Text(u.Key)
	e.Text(u.Owner)
	s3WriteMap(&e, u.Headers)
	s3WriteMap(&e, u.Metadata)
	e.Text(u.Algorithm)
	e.Text(u.ChecksumType)
	e.V(uint64(len(u.Parts)))
	for _, p := range u.Parts {
		e.U32(p.Number)
		e.U64(p.Size)
		e.Text(p.ETag)
		e.U64(uint64(p.Modified.UnixNano()))
		s3WriteMap(&e, p.Checksums)
	}
	e.V(uint64(len(u.Selected)))
	for _, n := range u.Selected {
		e.U32(n)
	}
	e.B = append(e.B, u.Recipe[:]...)
	e.ID(u.Publication.RequestID)
	e.Bool(u.Publication.Absent)
	e.Bool(u.Publication.ExpectedVersion != nil)
	if u.Publication.ExpectedVersion != nil {
		e.U64(*u.Publication.ExpectedVersion)
	}
	e.U64(uint64(u.PublishTime.UnixNano()))
	s3WriteMap(&e, u.FinalChecksums)
	e.Text(u.ResultETag)
	e.U64(u.ResultSize)
	s3WriteMap(&e, u.ResultHeaders)
	if len(e.B) > s3UploadMetadataLimit {
		return nil, ErrRange
	}
	putU32(e.B[12:16], uint32(len(e.B)-s3UploadHeader))
	return e.B, nil
}
func putU32(b []byte, n uint32) {
	for i := 0; i < 4; i++ {
		b[i] = byte(n >> (8 * i))
	}
}
func decodeS3Upload(raw []byte, info ObjectInfo) (S3Upload, error) {
	var u S3Upload
	d := Decoder{B: raw}
	if string(d.Take(4)) != "STMU" || d.U16() != 1 {
		return u, ErrS3Format
	}
	flags := d.U16()
	if flags&^uint16(1) != 0 {
		return u, ErrS3Format
	}
	u.PublicRead = flags&1 != 0
	u.State = d.U32()
	size := d.U32()
	u.Updated = time.Unix(0, int64(d.U64())).UTC()
	u.Created = time.Unix(0, int64(d.U64())).UTC()
	if size > s3UploadMetadataLimit-s3UploadHeader || uint64(s3UploadHeader)+uint64(size) > uint64(len(raw)) {
		return u, ErrS3Format
	}
	d.B = d.B[:size]
	u.PayloadOffset = uint64(s3UploadHeader) + uint64(size)
	u.ID = d.Text()
	u.Key = d.Text()
	u.Owner = d.Text()
	u.Headers = s3ReadMap(&d)
	u.Metadata = s3ReadMap(&d)
	u.Algorithm = d.Text()
	u.ChecksumType = d.Text()
	for n := d.Count(10000); n > 0; n-- {
		p := S3Part{Number: d.U32(), Size: d.U64(), ETag: d.Text(), Modified: time.Unix(0, int64(d.U64())).UTC(), Checksums: s3ReadMap(&d)}
		if p.Number == 0 || p.Number > 10000 || len(u.Parts) > 0 && u.Parts[len(u.Parts)-1].Number >= p.Number || p.Size > 5<<30 || len(p.ETag) != 32 {
			return u, ErrS3Format
		}
		u.PayloadSize += p.Size
		u.Parts = append(u.Parts, p)
	}
	for n := d.Count(10000); n > 0; n-- {
		u.Selected = append(u.Selected, d.U32())
	}
	copy(u.Recipe[:], d.Take(32))
	u.Publication.RequestID = d.ID()
	u.Publication.Absent = d.Bool()
	if d.Bool() {
		v := d.U64()
		u.Publication.ExpectedVersion = &v
	}
	u.PublishTime = time.Unix(0, int64(d.U64())).UTC()
	u.FinalChecksums = s3ReadMap(&d)
	u.ResultETag = d.Text()
	u.ResultSize = d.U64()
	u.ResultHeaders = s3ReadMap(&d)
	u.Version = info.Version
	if d.Done() != nil || !validS3Key(u.Key) || u.State > S3UploadFailed || u.PayloadSize+u.PayloadOffset != info.Length {
		return u, ErrS3Format
	}
	if _, e := s3UploadID(u.ID); e != nil {
		return u, ErrS3Format
	}
	return u, nil
}
func (s *S3) readUpload(ctx context.Context, oid ObjectID) (S3Upload, error) {
	for attempt := 0; attempt < 128; attempt++ {
		header, info, e := s.store.GetRange(ctx, oid, 0, s3UploadHeader)
		if errors.Is(e, ErrNotFound) {
			e = ErrS3Upload
		}
		if e != nil {
			return S3Upload{}, e
		}
		if len(header) != s3UploadHeader || string(header[:4]) != "STMU" {
			return S3Upload{}, ErrS3Format
		}
		size := binary.LittleEndian.Uint32(header[12:16])
		if size > s3UploadMetadataLimit-s3UploadHeader {
			return S3Upload{}, ErrS3Format
		}
		raw, next, e := s.store.GetRange(ctx, oid, 0, s3UploadHeader+uint64(size))
		if errors.Is(e, ErrNotFound) {
			e = ErrS3Upload
		}
		if e != nil {
			return S3Upload{}, e
		}
		if next.Version != info.Version {
			continue
		}
		return decodeS3Upload(raw, next)
	}
	return S3Upload{}, ErrPrecondition
}
func (s *S3) Multipart(ctx context.Context, id string) (S3Upload, error) {
	oid, e := s3UploadID(id)
	if e != nil {
		return S3Upload{}, e
	}
	u, e := s.readUpload(ctx, oid)
	if e == nil && u.ID != id {
		e = ErrS3Format
	}
	return u, e
}

func (s *S3) saveUpload(ctx context.Context, u S3Upload, body io.Reader, absent bool) (S3Upload, error) {
	raw, e := encodeS3Upload(u)
	if e != nil {
		return u, e
	}
	oid, _ := s3UploadID(u.ID)
	opt := PutOptions{RequestID: NewRequestID(), Absent: absent}
	if !absent {
		opt.ExpectedVersion = &u.Version
	}
	info, e := s.store.PutFrom(ctx, oid, io.MultiReader(strings.NewReader(string(raw)), body), uint64(len(raw))+u.PayloadSize, opt)
	u.Version = info.Version
	u.PayloadOffset = uint64(len(raw))
	return u, e
}
func (s *S3) CreateMultipart(ctx context.Context, key, owner, algorithm, checksumType string, opt S3PutOptions) (S3Upload, error) {
	if !validS3Key(key) {
		return S3Upload{}, ErrRange
	}
	if algorithm != "" {
		if _, e := S3Checksum(algorithm); e != nil {
			return S3Upload{}, e
		}
		if checksumType == "" {
			checksumType = "COMPOSITE"
			if algorithm == "CRC64NVME" {
				checksumType = "FULL_OBJECT"
			}
		}
		if checksumType != "COMPOSITE" && checksumType != "FULL_OBJECT" || algorithm == "CRC64NVME" && checksumType != "FULL_OBJECT" || checksumType == "FULL_OBJECT" && algorithm != "CRC64NVME" && algorithm != "CRC32" && algorithm != "CRC32C" {
			return S3Upload{}, ErrRange
		}
	} else if checksumType != "" {
		return S3Upload{}, ErrRange
	}
	now := time.Now().UTC()
	u := S3Upload{PublicRead: opt.PublicRead, ID: NewRequestID().String(), Key: key, Owner: owner, Created: now, Updated: now, Headers: opt.Headers, Metadata: opt.Metadata, Algorithm: algorithm, ChecksumType: checksumType}
	return s.saveUpload(ctx, u, strings.NewReader(""), true)
}
func multipartTemp() (*os.File, error) {
	f, e := os.CreateTemp("", "stqr-s3-multipart-*")
	if e != nil {
		return nil, e
	}
	if e = os.Remove(f.Name()); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
func (s *S3) uploadSnapshot(ctx context.Context, u S3Upload) (*os.File, error) {
	f, e := multipartTemp()
	if e != nil {
		return nil, e
	}
	oid, _ := s3UploadID(u.ID)
	_, e = s.store.GetRangeToVersion(ctx, oid, u.PayloadOffset, u.PayloadSize, u.Version, f)
	if e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
func copyS3Map(m map[string]string) map[string]string {
	n := map[string]string{}
	for k, v := range m {
		n[k] = v
	}
	return n
}
func (s *S3) UploadPart(ctx context.Context, id string, number uint32, src io.Reader, size uint64, opt S3PutOptions) (S3Part, error) {
	if number == 0 || number > 10000 || size > 5<<30 {
		return S3Part{}, ErrRange
	}
	f, e := multipartTemp()
	if e != nil {
		return S3Part{}, e
	}
	defer f.Close()
	md := md5.New()
	hashes := map[string]string{}
	writers := []io.Writer{f, md}
	u, e := s.Multipart(ctx, id)
	if e != nil {
		return S3Part{}, e
	}
	if u.State != S3UploadOpen {
		return S3Part{}, ErrS3UploadBusy
	}
	algorithms := copyS3Map(opt.Checksums)
	if u.Algorithm != "" {
		if _, ok := algorithms[u.Algorithm]; !ok {
			algorithms[u.Algorithm] = ""
		}
	}
	sums := map[string]interface{ Sum([]byte) []byte }{}
	for algo := range algorithms {
		h, e := S3Checksum(algo)
		if e != nil {
			return S3Part{}, e
		}
		writers = append(writers, h)
		sums[algo] = h
	}
	sha := sha256.New()
	writers = append(writers, sha)
	if _, e = io.CopyN(io.MultiWriter(writers...), contextReader{ctx, src}, int64(size)); e != nil {
		return S3Part{}, e
	}
	for algo, h := range sums {
		value := base64.StdEncoding.EncodeToString(h.Sum(nil))
		if algorithms[algo] != "" && algorithms[algo] != value {
			return S3Part{}, ErrS3Checksum
		}
		hashes[algo] = value
	}
	etag := hex.EncodeToString(md.Sum(nil))
	if opt.SHA256 != "" && opt.SHA256 != hex.EncodeToString(sha.Sum(nil)) || opt.ContentMD5 != "" && opt.ContentMD5 != etag {
		return S3Part{}, ErrS3Checksum
	}
	part := S3Part{Number: number, Size: size, ETag: etag, Modified: time.Now().UTC(), Checksums: hashes}
	for attempt := 0; attempt < 128; attempt++ {
		u, e = s.Multipart(ctx, id)
		if e != nil {
			return part, e
		}
		if u.State != S3UploadOpen {
			return part, ErrS3UploadBusy
		}
		snapshot, e := s.uploadSnapshot(ctx, u)
		if errors.Is(e, ErrPrecondition) {
			continue
		}
		if e != nil {
			return part, e
		}
		readers := []io.Reader{}
		parts := []S3Part{}
		var offset int64
		inserted := false
		for _, old := range u.Parts {
			if !inserted && old.Number >= number {
				readers = append(readers, io.NewSectionReader(f, 0, int64(size)))
				parts = append(parts, part)
				inserted = true
			}
			if old.Number != number {
				readers = append(readers, io.NewSectionReader(snapshot, offset, int64(old.Size)))
				parts = append(parts, old)
			}
			offset += int64(old.Size)
		}
		if !inserted {
			readers = append(readers, io.NewSectionReader(f, 0, int64(size)))
			parts = append(parts, part)
		}
		u.Parts = parts
		u.PayloadSize = 0
		for _, p := range parts {
			u.PayloadSize += p.Size
		}
		u.Updated = time.Now().UTC()
		_, e = s.saveUpload(ctx, u, io.MultiReader(readers...), false)
		snapshot.Close()
		if errors.Is(e, ErrPrecondition) {
			continue
		}
		return part, e
	}
	return part, ErrPrecondition
}
func completeRecipe(parts []S3CompletedPart, opt S3PutOptions) [32]byte {
	e := Encoder{}
	e.V(uint64(len(parts)))
	for _, p := range parts {
		e.U32(p.Number)
		e.Text(strings.Trim(p.ETag, "\""))
		s3WriteMap(&e, p.Checksums)
	}
	e.Text(opt.IfMatch)
	e.Bool(opt.IfNoneMatch)
	s3WriteMap(&e, opt.Checksums)
	return sha256.Sum256(e.B)
}
func (s *S3) CompleteMultipart(ctx context.Context, id string, parts []S3CompletedPart, opt S3PutOptions) (S3Object, error) {
	recipe := completeRecipe(parts, opt)
	for attempt := 0; attempt < 128; attempt++ {
		u, e := s.Multipart(ctx, id)
		if e != nil {
			return S3Object{}, e
		}
		if u.State != S3UploadOpen {
			if u.Recipe != recipe {
				return S3Object{}, ErrS3Part
			}
			return s.FinishMultipart(ctx, u)
		}
		if len(parts) == 0 || len(parts) > 10000 {
			return S3Object{}, ErrS3Part
		}
		selected := []uint32{}
		for i, p := range parts {
			if p.Number == 0 || i > 0 && parts[i-1].Number >= p.Number {
				return S3Object{}, ErrS3PartOrder
			}
			j := sort.Search(len(u.Parts), func(j int) bool { return u.Parts[j].Number >= p.Number })
			if j == len(u.Parts) || u.Parts[j].Number != p.Number || u.Parts[j].ETag != strings.Trim(p.ETag, "\"") {
				return S3Object{}, ErrS3Part
			}
			stored := u.Parts[j]
			if i < len(parts)-1 && stored.Size < 5<<20 {
				return S3Object{}, ErrS3PartSmall
			}
			for algo, value := range p.Checksums {
				if stored.Checksums[algo] != value {
					return S3Object{}, ErrS3Checksum
				}
			}
			if u.Algorithm != "" && (p.Number != uint32(i+1) || stored.Checksums[u.Algorithm] == "") {
				return S3Object{}, ErrS3PartOrder
			}
			selected = append(selected, p.Number)
		}
		old, e := s.Stat(ctx, u.Key)
		absent := errors.Is(e, ErrNotFound)
		if e != nil && !absent {
			return S3Object{}, e
		}
		if opt.IfNoneMatch && !absent || opt.IfMatch != "" && (absent || opt.IfMatch != "*" && strings.Trim(opt.IfMatch, "\"") != old.ETag) {
			return S3Object{}, ErrPrecondition
		}
		snapshot, e := s.uploadSnapshot(ctx, u)
		if errors.Is(e, ErrPrecondition) {
			continue
		}
		if e != nil {
			return S3Object{}, e
		}
		u.State = S3UploadPublishing
		u.Selected = selected
		u.Recipe = recipe
		u.Publication = PutOptions{RequestID: NewRequestID(), Absent: absent}
		if !absent {
			v := old.Version
			u.Publication.ExpectedVersion = &v
		}
		u.PublishTime = time.Now().UTC()
		u.Updated = u.PublishTime
		u.FinalChecksums = opt.Checksums
		u, e = s.saveUpload(ctx, u, io.NewSectionReader(snapshot, 0, int64(u.PayloadSize)), false)
		snapshot.Close()
		if errors.Is(e, ErrPrecondition) {
			continue
		}
		if e != nil {
			return S3Object{}, e
		}
		return s.FinishMultipart(ctx, u)
	}
	return S3Object{}, ErrPrecondition
}
func (s *S3) FinishMultipart(ctx context.Context, u S3Upload) (S3Object, error) {
	result := S3Object{Key: u.Key, ETag: u.ResultETag, Size: u.ResultSize, Modified: u.PublishTime, Headers: u.ResultHeaders}
	if u.State == S3UploadDone {
		return result, nil
	}
	if u.State == S3UploadFailed {
		return result, ErrPrecondition
	}
	if u.State != S3UploadPublishing {
		return result, ErrS3UploadBusy
	}
	snapshot, e := s.uploadSnapshot(ctx, u)
	if e != nil {
		if errors.Is(e, ErrFull) || errors.Is(e, syscall.ENOSPC) || errors.Is(e, syscall.EDQUOT) || errors.Is(e, ErrTooLarge) {
			return result, s.restoreMultipartOpen(ctx, u, nil, e)
		}
		return result, e
	}
	defer snapshot.Close()
	readers := []io.Reader{}
	md := md5.New()
	checks := map[string][]byte{}
	var offset int64
	var size uint64
	index := 0
	for _, p := range u.Parts {
		if index < len(u.Selected) && p.Number == u.Selected[index] {
			readers = append(readers, io.NewSectionReader(snapshot, offset, int64(p.Size)))
			digest, _ := hex.DecodeString(p.ETag)
			md.Write(digest)
			for algo, value := range p.Checksums {
				b, _ := base64.StdEncoding.DecodeString(value)
				checks[algo] = append(checks[algo], b...)
			}
			size += p.Size
			index++
		}
		offset += int64(p.Size)
	}
	if index != len(u.Selected) || index == 0 {
		return result, ErrS3Format
	}
	headers := copyS3Map(u.Headers)
	expected := copyS3Map(u.FinalChecksums)
	if u.Algorithm != "" {
		headers["X-Amz-Checksum-Type"] = u.ChecksumType
		if u.ChecksumType == "COMPOSITE" {
			h, _ := S3Checksum(u.Algorithm)
			h.Write(checks[u.Algorithm])
			value := base64.StdEncoding.EncodeToString(h.Sum(nil)) + fmt.Sprintf("-%d", index)
			if expected[u.Algorithm] != "" && expected[u.Algorithm] != value {
				return result, s.restoreMultipartOpen(ctx, u, snapshot, ErrS3Checksum)
			}
			delete(expected, u.Algorithm)
			headers["X-Amz-Checksum-"+strings.ToLower(u.Algorithm)] = value
		} else {
			if _, ok := expected[u.Algorithm]; !ok {
				expected[u.Algorithm] = ""
			}
		}
	}
	etag := hex.EncodeToString(md.Sum(nil)) + fmt.Sprintf("-%d", index)
	result, e = s.Put(ctx, u.Key, io.MultiReader(readers...), size, S3PutOptions{PublicRead: u.PublicRead, Headers: headers, Metadata: u.Metadata, Checksums: expected, Publication: &u.Publication, Modified: u.PublishTime, ETag: etag})
	if errors.Is(e, ErrS3Checksum) || errors.Is(e, ErrRange) || errors.Is(e, ErrFull) || errors.Is(e, syscall.ENOSPC) || errors.Is(e, syscall.EDQUOT) {
		return result, s.restoreMultipartOpen(ctx, u, snapshot, e)
	}
	if e != nil && !errors.Is(e, ErrPrecondition) {
		return result, e
	}
	failed := errors.Is(e, ErrPrecondition)
	u.State = S3UploadDone
	if failed {
		u.State = S3UploadFailed
	}
	u.ResultETag = etag
	u.ResultSize = size
	u.ResultHeaders = copyS3Map(result.Headers)
	u.Parts = nil
	u.PayloadSize = 0
	u.Updated = time.Now().UTC()
	_, saveErr := s.saveUpload(ctx, u, strings.NewReader(""), false)
	if errors.Is(saveErr, ErrPrecondition) {
		current, err := s.Multipart(ctx, u.ID)
		if err != nil {
			return result, err
		}
		if current.State == S3UploadDone {
			return S3Object{Key: current.Key, ETag: current.ResultETag, Size: current.ResultSize, Modified: current.PublishTime, Headers: current.ResultHeaders}, nil
		}
		if current.State == S3UploadFailed {
			return result, ErrPrecondition
		}
	}
	if saveErr != nil {
		return result, saveErr
	}
	if failed {
		return result, ErrPrecondition
	}
	return result, nil
}
func (s *S3) AbortMultipart(ctx context.Context, id string) error {
	for i := 0; i < 128; i++ {
		u, e := s.Multipart(ctx, id)
		if errors.Is(e, ErrS3Upload) {
			return nil
		}
		if e != nil {
			return e
		}
		if u.State == S3UploadPublishing {
			return ErrS3UploadBusy
		}
		oid, _ := s3UploadID(id)
		e = s.store.Delete(ctx, oid, DeleteOptions{RequestID: NewRequestID(), ExpectedVersion: &u.Version})
		if errors.Is(e, ErrPrecondition) {
			continue
		}
		return e
	}
	return ErrPrecondition
}
func (s *S3) MultipartUploads(ctx context.Context) ([]S3Upload, error) {
	out := []S3Upload{}
	var after *ObjectID
	for {
		records, next, e := s.store.ScanObjects(ctx, after, 512)
		if e != nil {
			return nil, e
		}
		for _, r := range records {
			raw, _, e := s.store.GetRange(ctx, r.ID, 0, s3UploadHeader+33)
			if errors.Is(e, ErrNotFound) {
				continue
			}
			if e != nil {
				return nil, e
			}
			if len(raw) < s3UploadHeader+33 || string(raw[:4]) != "STMU" {
				continue
			}
			identity := Decoder{B: raw[s3UploadHeader:]}
			candidate, e := s3UploadID(identity.Text())
			if identity.Err != nil || e != nil || candidate != r.ID {
				continue
			}
			u, e := s.readUpload(ctx, r.ID)
			if errors.Is(e, ErrS3Upload) {
				continue
			}
			if e != nil {
				return nil, e
			}
			id, _ := s3UploadID(u.ID)
			if id != r.ID {
				return nil, ErrS3Format
			}
			out = append(out, u)
		}
		if next == nil {
			break
		}
		after = next
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key == out[j].Key {
			return out[i].ID < out[j].ID
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// Recover requested completions; expire idle uploads and short-lived receipts.
// Version checks fence a concurrently committed part update before deletion.
func (s *S3) MaintainMultipart(ctx context.Context, now time.Time, idle, receipts time.Duration) error {
	return s.maintainMultipart(ctx, now, idle, receipts, true)
}

// ExpireMultipart only reclaims idle uploads/receipts; the server supplies an
// owner-authorized completion path instead of elevating old writes to admin.
func (s *S3) ExpireMultipart(ctx context.Context, now time.Time, idle, receipts time.Duration) error {
	return s.maintainMultipart(ctx, now, idle, receipts, false)
}
func (s *S3) maintainMultipart(ctx context.Context, now time.Time, idle, receipts time.Duration, recover bool) error {
	uploads, e := s.MultipartUploads(ctx)
	if e != nil {
		return e
	}
	var failures error
	for _, u := range uploads {
		if u.State == S3UploadPublishing && !recover {
			continue
		}
		if u.State == S3UploadPublishing {
			_, e = s.FinishMultipart(ctx, u)
			if e != nil && !errors.Is(e, ErrPrecondition) {
				failures = errors.Join(failures, e)
			}
			continue
		}
		age := idle
		if u.State == S3UploadDone || u.State == S3UploadFailed {
			age = receipts
		}
		if now.Sub(u.Updated) < age {
			continue
		}
		oid, _ := s3UploadID(u.ID)
		e = s.store.Delete(ctx, oid, DeleteOptions{RequestID: NewRequestID(), ExpectedVersion: &u.Version})
		if e != nil && !errors.Is(e, ErrPrecondition) && !errors.Is(e, ErrNotFound) {
			failures = errors.Join(failures, e)
		}
	}
	return failures
}

func (s *S3) restoreMultipartOpen(ctx context.Context, u S3Upload, _ *os.File, reason error) error {
	// Keep the parts intact and re-open the upload with a four-byte CAS. This
	// also works when there is insufficient disk/quota to rewrite its data.
	id, _ := s3UploadID(u.ID)
	before, next := make([]byte, 4), make([]byte, 4)
	putU32(before, S3UploadPublishing)
	putU32(next, S3UploadOpen)
	_, e := s.store.CAS(ctx, id, 8, before, next, CASOptions{Scope: CASRange, MutationOptions: MutationOptions{RequestID: NewRequestID(), ExpectedVersion: &u.Version}})
	if e != nil && !errors.Is(e, ErrPrecondition) {
		return e
	}
	return reason
}

// DiscardMultipart records a failed pending completion and frees its parts.
// Used by administrative maintenance after the owner's write grant is revoked.
func (s *S3) DiscardMultipart(ctx context.Context, u S3Upload) error {
	if u.State != S3UploadPublishing {
		return ErrRange
	}
	u.State = S3UploadFailed
	u.Parts = nil
	u.PayloadSize = 0
	u.Updated = time.Now().UTC()
	_, e := s.saveUpload(ctx, u, strings.NewReader(""), false)
	if errors.Is(e, ErrPrecondition) {
		return nil
	}
	return e
}
