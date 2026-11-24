// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// S3Store is bucket-bound. Metadata and payload share one atomic native object.
// Native replacement/deletion owns reclamation; there is no second object graph.
type S3Store interface {
	FilesystemStore
	GetRange(context.Context, ObjectID, uint64, uint64) ([]byte, ObjectInfo, error)
	PutFrom(context.Context, ObjectID, io.Reader, uint64, PutOptions) (ObjectInfo, error)
	GetRangeToMatching(context.Context, ObjectID, uint64, uint64, ReadCondition, io.Writer) (ObjectInfo, error)
	Delete(context.Context, ObjectID, DeleteOptions) error
	ScanObjects(context.Context, *ObjectID, uint32) ([]ObjectEntry, *ObjectID, error)
}
type S3 struct{ store S3Store }

func NewS3(store S3Store) *S3 { return &S3{store: store} }

type S3Object struct {
	PublicRead    bool
	Schema        int
	Key           string
	Payload       ObjectID
	Size          uint64
	ETag          string
	Modified      time.Time
	Headers       map[string]string
	Metadata      map[string]string
	header        []byte
	PayloadOffset uint64
	Deleted       bool
}
type S3PutOptions struct {
	PublicRead         bool
	Headers, Metadata  map[string]string
	Checksums          map[string]string // algorithm (CRC32/CRC32C/SHA1/SHA256) -> base64
	SHA256, ContentMD5 string            // lowercase hex; empty means no supplied checksum
	IfMatch            string
	IfNoneMatch        bool
	// Used by durable multipart completion: all fields and native preconditions
	// remain identical across recovery, including after a later overwrite.
	Publication *PutOptions
	Modified    time.Time
	ETag        string
}

var ErrS3Format = errors.New("client: corrupt S3 metadata")
var ErrS3Checksum = errors.New("client: S3 payload checksum mismatch")

func S3MetadataID(key string) ObjectID { return sha256.Sum256([]byte("stqr:s3:meta:v1:\x00" + key)) }
func validS3Key(key string) bool       { return len(key) > 0 && len(key) <= 1024 && utf8.ValidString(key) }
func (s *S3) load(ctx context.Context, key string) (S3Object, ObjectInfo, error) {
	var o S3Object
	if !validS3Key(key) {
		return o, ObjectInfo{}, ErrRange
	}
	raw, info, err := s.store.GetRange(ctx, S3MetadataID(key), 0, 64<<10)
	if err != nil {
		return o, info, err
	}
	o, err = decodeS3Object(raw)
	if err != nil || o.Key != key || o.Payload != S3MetadataID(key) || o.PayloadOffset+o.Size != info.Length {
		return o, info, ErrS3Format
	}
	o.header = append([]byte(nil), raw[:o.PayloadOffset]...)
	return o, info, nil
}
func (s *S3) Head(ctx context.Context, key string) (S3Object, error) {
	o, _, err := s.load(ctx, key)
	if err == nil && o.Deleted {
		err = ErrNotFound
	}
	return o, err
}

// Stat reads only the fixed binary header, for listing and metadata-only
// control paths. It does not decode the variable key/header maps.
func (s *S3) Stat(ctx context.Context, key string) (S3Object, error) {
	if !validS3Key(key) {
		return S3Object{}, ErrRange
	}
	raw, info, err := s.store.GetRange(ctx, S3MetadataID(key), 0, S3HeaderSize)
	if err != nil {
		return S3Object{}, err
	}
	o, err := decodeS3Header(raw)
	o.Key = key
	o.header = append([]byte(nil), raw...)
	if err == nil && (o.Payload != S3MetadataID(key) || o.PayloadOffset+o.Size != info.Length) {
		err = ErrS3Format
	}
	if err == nil && o.Deleted {
		err = ErrNotFound
	}
	return o, err
}

func (s *S3) Put(ctx context.Context, key string, src io.Reader, size uint64, opt S3PutOptions) (S3Object, error) {
	if !validS3Key(key) {
		return S3Object{}, ErrRange
	}
	opt.Headers = copyS3Map(opt.Headers)
	opt.Metadata = copyS3Map(opt.Metadata)
	opt.Checksums = copyS3Map(opt.Checksums)
	// Stage before touching the native object. Checksums and the complete header
	// are known before publication; cancellation/failure removes staging only.
	staged, err := os.CreateTemp("", "stqr-s3-body-*")
	if err != nil {
		return S3Object{}, err
	}
	// Unlink immediately: the open descriptor remains usable, and even SIGKILL
	// cannot strand this staging file in the filesystem.
	if err = os.Remove(staged.Name()); err != nil {
		staged.Close()
		return S3Object{}, err
	}
	defer staged.Close()
	md, sha := md5.New(), sha256.New()
	writers := []io.Writer{md, sha}
	checks := map[string]hash.Hash{}
	for algorithm := range opt.Checksums {
		h, e := S3Checksum(algorithm)
		if e != nil {
			return S3Object{}, e
		}
		checks[algorithm] = h
		writers = append(writers, h)
	}
	if size > uint64(^uint64(0)>>1) {
		return S3Object{}, ErrRange
	}
	n, err := io.CopyN(io.MultiWriter(append(writers, staged)...), contextReader{ctx, src}, int64(size))
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return S3Object{}, err
	}
	if uint64(n) != size {
		return S3Object{}, io.ErrUnexpectedEOF
	}
	for algorithm, h := range checks {
		expected, e := base64.StdEncoding.DecodeString(opt.Checksums[algorithm])
		if opt.Checksums[algorithm] != "" && (e != nil || !hmac.Equal(expected, h.Sum(nil))) {
			return S3Object{}, ErrS3Checksum
		}
		if opt.Headers == nil {
			opt.Headers = map[string]string{}
		}
		opt.Headers["X-Amz-Checksum-"+strings.ToLower(algorithm)] = base64.StdEncoding.EncodeToString(h.Sum(nil))
	}
	etag := hex.EncodeToString(md.Sum(nil))
	digest := hex.EncodeToString(sha.Sum(nil))
	if opt.ContentMD5 != "" && !strings.EqualFold(opt.ContentMD5, etag) || opt.SHA256 != "" && !strings.EqualFold(opt.SHA256, digest) {
		return S3Object{}, ErrS3Checksum
	}
	o := S3Object{Schema: 2, PublicRead: opt.PublicRead, Key: key, Payload: S3MetadataID(key), Size: size, ETag: etag, Modified: time.Now().UTC(), Headers: opt.Headers, Metadata: opt.Metadata}
	if opt.ETag != "" {
		o.ETag = opt.ETag
	}
	if !opt.Modified.IsZero() {
		o.Modified = opt.Modified
	}
	raw, err := encodeS3Object(o)
	if err != nil {
		return o, err
	}
	if opt.Publication != nil {
		if opt.Publication.RequestID == (ID{}) || opt.Modified.IsZero() {
			return o, ErrRange
		}
		if _, err = staged.Seek(0, io.SeekStart); err != nil {
			return o, err
		}
		_, e := s.store.PutFrom(ctx, o.Payload, io.MultiReader(strings.NewReader(string(raw)), staged), uint64(len(raw))+size, *opt.Publication)
		o.header = append([]byte(nil), raw...)
		o.PayloadOffset = uint64(len(raw))
		return o, e
	}
	for attempt := 0; attempt < 128; attempt++ {
		old, info, err := s.load(ctx, key)
		absent := errors.Is(err, ErrNotFound)
		if err != nil && !absent {
			return o, err
		}
		if opt.IfNoneMatch && !absent && !old.Deleted || opt.IfMatch != "" && (absent || old.Deleted || opt.IfMatch != "*" && strings.Trim(opt.IfMatch, "\"") != old.ETag) {
			return o, ErrPrecondition
		}
		co := PutOptions{RequestID: NewRequestID(), Absent: absent}
		if !absent {
			co.ExpectedLength = &info.Length
			co.Compare = old.header
		}
		if _, err = staged.Seek(0, io.SeekStart); err != nil {
			return o, err
		}
		_, err = s.store.PutFrom(ctx, S3MetadataID(key), io.MultiReader(strings.NewReader(string(raw)), staged), uint64(len(raw))+size, co)
		o.header = append([]byte(nil), raw...)
		o.PayloadOffset = uint64(len(raw))
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return o, err
	}
	return o, ErrPrecondition
}
func (s *S3) Delete(ctx context.Context, key string) error {
	for attempt := 0; attempt < 128; attempt++ {
		o, err := s.Stat(ctx, key)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		err = s.store.Delete(ctx, S3MetadataID(key), DeleteOptions{RequestID: NewRequestID(), ExpectedLength: uint64Pointer(o.PayloadOffset + o.Size), Compare: o.header})
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	return ErrPrecondition
}

// ReadTo checks the native version before emitting any bytes. Callers can retry
// Head on ErrPrecondition; a snapshot already being copied cannot mix versions.
func (s *S3) ReadTo(ctx context.Context, o S3Object, off, length uint64, dst io.Writer) error {
	if off > o.Size || length > o.Size-off {
		return ErrRange
	}
	_, err := s.store.GetRangeToMatching(ctx, o.Payload, o.PayloadOffset+off, length, ReadCondition{Bytes: o.header, Length: uint64Pointer(o.PayloadOffset + o.Size)}, dst)
	return err
}

// Keys enumerates native live objects. No separately persisted name index can
// retain deleted names or lose discoverability around a crashed publication.
func (s *S3) Keys(ctx context.Context, prefix string) ([]string, error) {
	keys := []string{}
	var after *ObjectID
	for {
		records, next, err := s.store.ScanObjects(ctx, after, 512)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			raw, info, err := s.store.GetRange(ctx, record.ID, 0, 64<<10)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if len(raw) < 4 || string(raw[:4]) != "STS3" {
				continue
			}
			o, err := decodeS3Object(raw)
			if err != nil || o.Payload != record.ID || S3MetadataID(o.Key) != record.ID || o.PayloadOffset+o.Size != info.Length {
				return nil, ErrS3Format
			}
			if !o.Deleted && strings.HasPrefix(o.Key, prefix) {
				keys = append(keys, o.Key)
			}
		}
		if next == nil {
			break
		}
		after = next
	}
	sort.Strings(keys)
	return keys, nil
}
func (s *S3) List(ctx context.Context, prefix string) ([]S3Object, error) {
	keys, err := s.Keys(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]S3Object, 0, len(keys))
	for _, key := range keys {
		o, err := s.Head(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func uint64Pointer(n uint64) *uint64 { return &n }
