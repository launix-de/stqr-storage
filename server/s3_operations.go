// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

func s3XML(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(value)
}
func s3ReadXML(r *http.Request, dst any, limit int64) error {
	if r.ContentLength > limit {
		return client.ErrRange
	}
	d := xml.NewDecoder(io.LimitReader(r.Body, limit+1))
	if e := d.Decode(dst); e != nil {
		return e
	}
	for {
		token, e := d.Token()
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if text, ok := token.(xml.CharData); !ok || strings.TrimSpace(string(text)) != "" {
			return client.ErrRange
		}
	}
}
func (s *Server) s3ListBuckets(w http.ResponseWriter, r *http.Request, p principal) {
	if r.Method != "GET" {
		s3Error(w, 501, "NotImplemented", "Unsupported service operation")
		return
	}
	type bucket struct {
		Name         string
		CreationDate string
	}
	result := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		XMLNS   string   `xml:"xmlns,attr"`
		Buckets []bucket `xml:"Buckets>Bucket"`
	}{XMLNS: s3XMLNS}
	id, _ := bucketID(p.User.ID)
	for _, d := range s.bucketSnapshots() {
		if d.ID == systemBucket.String() || d.DeletionComplete || d.State == BucketDeleting {
			continue
		}
		b, e := s.BucketByName(d.Name)
		if e != nil {
			continue
		}
		info, e := b.Info(id, p.isAdmin())
		if e == nil && info.Permissions&BucketRead != 0 {
			result.Buckets = append(result.Buckets, bucket{d.Name, "1970-01-01T00:00:00Z"})
		}
	}
	s3XML(w, result)
}
func (s *Server) s3BucketProbe(w http.ResponseWriter, r *http.Request, bucket string) bool {
	q := r.URL.Query()
	if _, ok := q["location"]; ok {
		if r.Method != "GET" {
			s3Error(w, 501, "NotImplemented", "Read-only bucket location")
			return true
		}
		s3XML(w, struct {
			XMLName xml.Name `xml:"LocationConstraint"`
			XMLNS   string   `xml:"xmlns,attr"`
			Region  string   `xml:",chardata"`
		}{XMLNS: s3XMLNS, Region: "eu-west-1"})
		return true
	}
	if _, ok := q["versioning"]; ok {
		if r.Method != "GET" {
			s3Error(w, 501, "NotImplemented", "Versioning is not supported")
			return true
		}
		s3XML(w, struct {
			XMLName xml.Name `xml:"VersioningConfiguration"`
			XMLNS   string   `xml:"xmlns,attr"`
		}{XMLNS: s3XMLNS})
		return true
	}
	return false
}
func (s *Server) s3DeleteObjects(w http.ResponseWriter, r *http.Request, api *client.S3) {
	var request struct {
		XMLName xml.Name `xml:"Delete"`
		Objects []struct {
			Key       string
			VersionID string `xml:"VersionId"`
		} `xml:"Object"`
		Quiet bool
	}
	if e := s3ReadXML(r, &request, 2<<20); e != nil || len(request.Objects) == 0 || len(request.Objects) > 1000 {
		s3Error(w, 400, "MalformedXML", "Delete requires 1 to 1000 keys")
		return
	}
	digest := r.Header.Get("Content-MD5") != ""
	for _, algo := range []string{"crc32", "crc32c", "crc64nvme", "sha1", "sha256"} {
		digest = digest || r.Header.Get("X-Amz-Checksum-"+algo) != ""
	}
	if !digest {
		s3Error(w, 400, "MissingContentMD5", "A request checksum is required")
		return
	}
	type deleted struct{ Key string }
	type failure struct{ Key, Code, Message string }
	result := struct {
		XMLName xml.Name `xml:"DeleteResult"`
		XMLNS   string   `xml:"xmlns,attr"`
		Deleted []deleted
		Errors  []failure `xml:"Error"`
	}{XMLNS: s3XMLNS}
	for _, o := range request.Objects {
		if o.VersionID != "" && o.VersionID != "null" {
			result.Errors = append(result.Errors, failure{o.Key, "NotImplemented", "Versioning is not supported"})
			continue
		}
		if e := api.Delete(r.Context(), o.Key); e != nil {
			code := "InternalError"
			if errors.Is(e, client.ErrRange) {
				code = "InvalidArgument"
			} else if statusOf(e) == 403 {
				code = "AccessDenied"
			}
			result.Errors = append(result.Errors, failure{o.Key, code, "Object deletion failed"})
		} else if !request.Quiet {
			result.Deleted = append(result.Deleted, deleted{o.Key})
		}
	}
	s3XML(w, result)
}
func (s *Server) s3CopySource(w http.ResponseWriter, r *http.Request, p principal) (*client.S3, string, func(), bool) {
	raw := r.Header.Get("X-Amz-Copy-Source")
	path, query, _ := strings.Cut(raw, "?")
	if query != "" {
		q, e := url.ParseQuery(query)
		if e != nil || q.Get("versionId") != "" && q.Get("versionId") != "null" {
			s3Error(w, 501, "NotImplemented", "Versioned copy is not supported")
			return nil, "", nil, false
		}
	}
	path, e := url.PathUnescape(strings.TrimPrefix(path, "/"))
	if e != nil {
		s3Error(w, 400, "InvalidArgument", "Invalid copy source")
		return nil, "", nil, false
	}
	name, key, ok := strings.Cut(path, "/")
	if !ok || key == "" {
		s3Error(w, 400, "InvalidArgument", "Copy source requires bucket/key")
		return nil, "", nil, false
	}
	b, e := s.BucketByName(name)
	if e != nil {
		s3Error(w, 404, "NoSuchBucket", "Source bucket does not exist")
		return nil, "", nil, false
	}
	p.frontendPermission = 0
	if _, e = b.authorize(p, BucketRead); e != nil {
		s3Failure(w, e)
		return nil, "", nil, false
	}
	p.frontendPermission = BucketRead
	api, closeAPI, e := s.openS3(r.Context(), b, p)
	if e != nil {
		s3Failure(w, e)
		return nil, "", nil, false
	}
	return api, key, closeAPI, true
}
func s3CopyConditions(r *http.Request, o client.S3Object) error {
	etag := "\"" + o.ETag + "\""
	if v := r.Header.Get("X-Amz-Copy-Source-If-Match"); v != "" && !s3ETagMatch(v, etag, false) {
		return client.ErrPrecondition
	}
	if v := r.Header.Get("X-Amz-Copy-Source-If-None-Match"); v != "" && s3ETagMatch(v, etag, true) {
		return client.ErrPrecondition
	}
	if r.Header.Get("X-Amz-Copy-Source-If-Match") == "" {
		if t, e := http.ParseTime(r.Header.Get("X-Amz-Copy-Source-If-Unmodified-Since")); e == nil && o.Modified.Truncate(time.Second).After(t) {
			return client.ErrPrecondition
		}
	}
	if r.Header.Get("X-Amz-Copy-Source-If-None-Match") == "" {
		if t, e := http.ParseTime(r.Header.Get("X-Amz-Copy-Source-If-Modified-Since")); e == nil && !o.Modified.Truncate(time.Second).After(t) {
			return client.ErrPrecondition
		}
	}
	return nil
}
func (s *Server) s3Copy(w http.ResponseWriter, r *http.Request, p principal, dest *client.S3, key, upload string, part uint32) {
	source, sourceKey, closeSource, ok := s.s3CopySource(w, r, p)
	if !ok {
		return
	}
	defer closeSource()
	for attempt := 0; attempt < 128; attempt++ {
		o, e := source.Head(r.Context(), sourceKey)
		if e != nil {
			s3Failure(w, e)
			return
		}
		if e = s3CopyConditions(r, o); e != nil {
			s3Failure(w, e)
			return
		}
		off, n := uint64(0), o.Size
		if upload != "" {
			if header := r.Header.Get("X-Amz-Copy-Source-Range"); header != "" {
				off, n, e = s3Range(header, o.Size)
				if e != nil {
					s3Error(w, 416, "InvalidRange", "Invalid copy range")
					return
				}
			}
		} else if o.Size > 5<<30 {
			s3Error(w, 400, "EntityTooLarge", "Use multipart copy above 5 GiB")
			return
		}
		snapshot, e := s3Temp("stqr-s3-copy-*")
		if e != nil {
			s3Failure(w, e)
			return
		}
		e = source.ReadTo(r.Context(), o, off, n, snapshot)
		if errors.Is(e, client.ErrPrecondition) {
			snapshot.Close()
			continue
		}
		if e != nil {
			snapshot.Close()
			s3Failure(w, e)
			return
		}
		snapshot.Seek(0, io.SeekStart)
		if upload != "" {
			partResult, e := dest.UploadPart(r.Context(), upload, part, snapshot, n, client.S3PutOptions{})
			snapshot.Close()
			if e != nil {
				s3Failure(w, e)
				return
			}
			s3XML(w, struct {
				XMLName           xml.Name `xml:"CopyPartResult"`
				XMLNS             string   `xml:"xmlns,attr"`
				LastModified      string
				ETag              string
				ChecksumCRC32     string `xml:",omitempty"`
				ChecksumCRC32C    string `xml:",omitempty"`
				ChecksumCRC64NVME string `xml:",omitempty"`
				ChecksumSHA1      string `xml:",omitempty"`
				ChecksumSHA256    string `xml:",omitempty"`
			}{XMLNS: s3XMLNS, LastModified: partResult.Modified.Format(time.RFC3339Nano), ETag: "\"" + partResult.ETag + "\"", ChecksumCRC32: partResult.Checksums["CRC32"], ChecksumCRC32C: partResult.Checksums["CRC32C"], ChecksumCRC64NVME: partResult.Checksums["CRC64NVME"], ChecksumSHA1: partResult.Checksums["SHA1"], ChecksumSHA256: partResult.Checksums["SHA256"]})
			return
		}
		opt, e := s3PutOptions(r)
		if e != nil {
			snapshot.Close()
			s3Failure(w, e)
			return
		}
		directive := r.Header.Get("X-Amz-Metadata-Directive")
		if directive == "" || directive == "COPY" {
			opt.Headers = map[string]string{}
			for k, v := range o.Headers {
				if !strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-") {
					opt.Headers[k] = v
				}
			}
			opt.Metadata = o.Metadata
		} else if directive != "REPLACE" {
			snapshot.Close()
			s3Error(w, 400, "InvalidArgument", "Invalid metadata directive")
			return
		}
		opt.ContentMD5 = ""
		opt.SHA256 = ""
		opt.Checksums = map[string]string{}
		if algorithm := r.Header.Get("X-Amz-Checksum-Algorithm"); algorithm != "" {
			if _, e = client.S3Checksum(algorithm); e != nil {
				snapshot.Close()
				s3Failure(w, e)
				return
			}
			opt.Checksums[algorithm] = ""
		}
		result, e := dest.Put(r.Context(), key, snapshot, n, opt)
		snapshot.Close()
		if e != nil {
			s3Failure(w, e)
			return
		}
		s3XML(w, struct {
			XMLName      xml.Name `xml:"CopyObjectResult"`
			XMLNS        string   `xml:"xmlns,attr"`
			LastModified string
			ETag         string
		}{XMLNS: s3XMLNS, LastModified: result.Modified.Format(time.RFC3339Nano), ETag: "\"" + result.ETag + "\""})
		return
	}
	s3Error(w, 503, "SlowDown", "Source changed repeatedly")
}
func s3MaxQuery(r *http.Request, name string) (int, error) {
	if raw := r.URL.Query().Get(name); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 0 || n > 1000 {
			return 0, client.ErrRange
		}
		return n, nil
	}
	return 1000, nil
}

// Tags cannot be created by this release. Reporting an empty set is truthful
// and lets copy tools discover that an ordinary untagged object has no tags.
func s3EmptyTagging(w http.ResponseWriter, r *http.Request, api *client.S3, key string) {
	if _, e := api.Head(r.Context(), key); e != nil {
		s3Failure(w, e)
		return
	}
	if r.Method == "PUT" {
		var body struct {
			XMLName xml.Name                      `xml:"Tagging"`
			Tags    []struct{ Key, Value string } `xml:"TagSet>Tag"`
		}
		if e := s3ReadXML(r, &body, 16384); e != nil {
			s3Error(w, 400, "MalformedXML", "Invalid tagging document")
			return
		}
		if len(body.Tags) > 0 {
			s3Error(w, 501, "NotImplemented", "Nonempty object tags are not supported")
			return
		}
		w.WriteHeader(200)
		return
	}
	if r.Method == "DELETE" {
		w.WriteHeader(204)
		return
	}
	if r.Method != "GET" {
		s3Error(w, 501, "NotImplemented", "Unsupported tagging operation")
		return
	}
	s3XML(w, struct {
		XMLName xml.Name `xml:"Tagging"`
		XMLNS   string   `xml:"xmlns,attr"`
		TagSet  struct{}
	}{XMLNS: s3XMLNS})
}
