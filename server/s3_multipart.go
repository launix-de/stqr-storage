// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

func s3UploadAccess(w http.ResponseWriter, r *http.Request, p principal, api *client.S3, id, key string) (client.S3Upload, bool) {
	u, e := api.Multipart(r.Context(), id)
	if e != nil {
		s3Failure(w, e)
		return u, false
	}
	if u.Key != key {
		s3Failure(w, client.ErrS3Upload)
		return u, false
	}
	if u.Owner != p.User.ID && !p.isAdmin() {
		s3Error(w, 403, "AccessDenied", "Upload belongs to another user")
		return u, false
	}
	return u, true
}
func (s *Server) serveS3Multipart(w http.ResponseWriter, r *http.Request, p principal, api *client.S3, bucket, key string) bool {
	q := r.URL.Query()
	_, uploads := q["uploads"]
	id := q.Get("uploadId")
	if !uploads && id == "" {
		return false
	}
	if uploads && id != "" {
		s3Error(w, 400, "InvalidArgument", "Conflicting upload selectors")
		return true
	}
	if uploads {
		if r.Method == "GET" && key == "" {
			s.s3ListUploads(w, r, p, api, bucket)
			return true
		}
		if r.Method != "POST" || key == "" {
			s3Error(w, 400, "InvalidRequest", "Invalid multipart operation")
			return true
		}
		opt, e := s3PutOptions(r)
		if e != nil {
			s3Failure(w, e)
			return true
		}
		u, e := api.CreateMultipart(r.Context(), key, p.User.ID, r.Header.Get("X-Amz-Checksum-Algorithm"), r.Header.Get("X-Amz-Checksum-Type"), opt)
		if e != nil {
			s3Failure(w, e)
			return true
		}
		if u.Algorithm != "" {
			w.Header().Set("X-Amz-Checksum-Algorithm", u.Algorithm)
			w.Header().Set("X-Amz-Checksum-Type", u.ChecksumType)
		}
		s3XML(w, struct {
			XMLName               xml.Name `xml:"InitiateMultipartUploadResult"`
			XMLNS                 string   `xml:"xmlns,attr"`
			Bucket, Key, UploadID string   `xml:"-"`
			B                     string   `xml:"Bucket"`
			K                     string   `xml:"Key"`
			ID                    string   `xml:"UploadId"`
		}{XMLNS: s3XMLNS, B: bucket, K: key, ID: u.ID})
		return true
	}
	u, ok := s3UploadAccess(w, r, p, api, id, key)
	if !ok {
		return true
	}
	switch r.Method {
	case "PUT":
		number, e := strconv.ParseUint(q.Get("partNumber"), 10, 32)
		if e != nil || number == 0 || number > 10000 {
			s3Failure(w, client.ErrRange)
			return true
		}
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			s.s3Copy(w, r, p, api, key, id, uint32(number))
			return true
		}
		opt, e := s3PutOptions(r)
		if e != nil {
			s3Failure(w, e)
			return true
		}
		part, e := api.UploadPart(r.Context(), id, uint32(number), r.Body, uint64(r.ContentLength), opt)
		if e != nil {
			s3Failure(w, e)
			return true
		}
		w.Header().Set("ETag", "\""+part.ETag+"\"")
		for algo, v := range part.Checksums {
			w.Header().Set("X-Amz-Checksum-"+strings.ToLower(algo), v)
		}
		w.WriteHeader(200)
	case "GET":
		s3ListParts(w, r, u, bucket)
	case "DELETE":
		if e := api.AbortMultipart(r.Context(), id); e != nil {
			s3Failure(w, e)
		} else {
			w.WriteHeader(204)
		}
	case "POST":
		var request struct {
			XMLName xml.Name    `xml:"CompleteMultipartUpload"`
			Parts   []s3XMLPart `xml:"Part"`
		}
		if e := s3ReadXML(r, &request, 4<<20); e != nil {
			s3Error(w, 400, "MalformedXML", "Invalid completion document")
			return true
		}
		parts := make([]client.S3CompletedPart, len(request.Parts))
		for i, v := range request.Parts {
			parts[i] = client.S3CompletedPart{Number: v.PartNumber, ETag: v.ETag, Checksums: v.checksums()}
		}
		opt := client.S3PutOptions{IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match") == "*", Checksums: map[string]string{}}
		if value := r.Header.Get("If-None-Match"); value != "" && value != "*" {
			s3Failure(w, client.ErrRange)
			return true
		}
		// Complete headers describe the final object, not the XML request body.
		for _, algo := range []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"} {
			if v := r.Header.Get("X-Amz-Checksum-" + strings.ToLower(algo)); v != "" {
				opt.Checksums[algo] = v
			}
		}
		result, e := api.CompleteMultipart(r.Context(), id, parts, opt)
		if e != nil {
			s3Failure(w, e)
			return true
		}
		out := struct {
			XMLName                     xml.Name `xml:"CompleteMultipartUploadResult"`
			XMLNS                       string   `xml:"xmlns,attr"`
			Location, Bucket, Key, ETag string
			ChecksumCRC32               string `xml:",omitempty"`
			ChecksumCRC32C              string `xml:",omitempty"`
			ChecksumCRC64NVME           string `xml:",omitempty"`
			ChecksumSHA1                string `xml:",omitempty"`
			ChecksumSHA256              string `xml:",omitempty"`
			ChecksumType                string `xml:",omitempty"`
		}{XMLNS: s3XMLNS, Location: "/s3/" + bucket + "/" + s3Escape(key, true), Bucket: bucket, Key: key, ETag: "\"" + result.ETag + "\""}
		for k, v := range result.Headers {
			switch strings.ToLower(k) {
			case "x-amz-checksum-crc32":
				out.ChecksumCRC32 = v
			case "x-amz-checksum-crc32c":
				out.ChecksumCRC32C = v
			case "x-amz-checksum-crc64nvme":
				out.ChecksumCRC64NVME = v
			case "x-amz-checksum-sha1":
				out.ChecksumSHA1 = v
			case "x-amz-checksum-sha256":
				out.ChecksumSHA256 = v
			case "x-amz-checksum-type":
				out.ChecksumType = v
			}
		}
		s3XML(w, out)
	default:
		s3Error(w, 501, "NotImplemented", "Unsupported multipart operation")
	}
	return true
}

type s3XMLPart struct {
	PartNumber        uint32
	ETag              string
	Size              uint64
	LastModified      string `xml:",omitempty"`
	ChecksumCRC32     string `xml:",omitempty"`
	ChecksumCRC32C    string `xml:",omitempty"`
	ChecksumCRC64NVME string `xml:",omitempty"`
	ChecksumSHA1      string `xml:",omitempty"`
	ChecksumSHA256    string `xml:",omitempty"`
}

func (p s3XMLPart) checksums() map[string]string {
	m := map[string]string{}
	for k, v := range map[string]string{"CRC32": p.ChecksumCRC32, "CRC32C": p.ChecksumCRC32C, "CRC64NVME": p.ChecksumCRC64NVME, "SHA1": p.ChecksumSHA1, "SHA256": p.ChecksumSHA256} {
		if v != "" {
			m[k] = v
		}
	}
	return m
}
func s3ListParts(w http.ResponseWriter, r *http.Request, u client.S3Upload, bucket string) {
	if u.State == client.S3UploadDone || u.State == client.S3UploadFailed {
		s3Failure(w, client.ErrS3Upload)
		return
	}
	limit, e := s3MaxQuery(r, "max-parts")
	if e != nil {
		s3Failure(w, e)
		return
	}
	marker := uint64(0)
	if raw := r.URL.Query().Get("part-number-marker"); raw != "" {
		marker, e = strconv.ParseUint(raw, 10, 32)
	}
	if e != nil {
		s3Failure(w, client.ErrRange)
		return
	}
	result := struct {
		XMLName              xml.Name `xml:"ListPartsResult"`
		XMLNS                string   `xml:"xmlns,attr"`
		Bucket, Key          string
		UploadID             string `xml:"UploadId"`
		PartNumberMarker     uint64
		NextPartNumberMarker uint32
		MaxParts             int
		IsTruncated          bool
		StorageClass         string
		Parts                []s3XMLPart `xml:"Part"`
		ChecksumAlgorithm    string      `xml:",omitempty"`
		ChecksumType         string      `xml:",omitempty"`
	}{XMLNS: s3XMLNS, Bucket: bucket, Key: u.Key, UploadID: u.ID, PartNumberMarker: marker, MaxParts: limit, StorageClass: "STANDARD", ChecksumAlgorithm: u.Algorithm, ChecksumType: u.ChecksumType}
	if limit == 0 {
		s3XML(w, result)
		return
	}
	for _, p := range u.Parts {
		if uint64(p.Number) <= marker {
			continue
		}
		if len(result.Parts) == limit {
			result.IsTruncated = true
			break
		}
		v := s3XMLPart{PartNumber: p.Number, ETag: "\"" + p.ETag + "\"", Size: p.Size, LastModified: p.Modified.Format(time.RFC3339Nano), ChecksumCRC32: p.Checksums["CRC32"], ChecksumCRC32C: p.Checksums["CRC32C"], ChecksumCRC64NVME: p.Checksums["CRC64NVME"], ChecksumSHA1: p.Checksums["SHA1"], ChecksumSHA256: p.Checksums["SHA256"]}
		result.Parts = append(result.Parts, v)
		result.NextPartNumberMarker = p.Number
	}
	s3XML(w, result)
}
func (s *Server) s3ListUploads(w http.ResponseWriter, r *http.Request, p principal, api *client.S3, bucket string) {
	limit, e := s3MaxQuery(r, "max-uploads")
	if e != nil {
		s3Failure(w, e)
		return
	}
	uploads, e := api.MultipartUploads(r.Context())
	if e != nil {
		s3Failure(w, e)
		return
	}
	type upload struct {
		Key          string
		ID           string `xml:"UploadId"`
		Initiated    string
		StorageClass string
	}
	q := r.URL.Query()
	prefix, delimiter, keyMarker, idMarker := q.Get("prefix"), q.Get("delimiter"), q.Get("key-marker"), q.Get("upload-id-marker")
	result := struct {
		XMLName                              xml.Name `xml:"ListMultipartUploadsResult"`
		EncodingType                         string   `xml:",omitempty"`
		XMLNS                                string   `xml:"xmlns,attr"`
		Bucket, Prefix, Delimiter, KeyMarker string
		UploadIDMarker                       string `xml:"UploadIdMarker"`
		NextKeyMarker                        string
		NextUploadIDMarker                   string `xml:"NextUploadIdMarker"`
		MaxUploads                           int
		IsTruncated                          bool
		Uploads                              []upload `xml:"Upload"`
		CommonPrefixes                       []s3ListPrefix
	}{XMLNS: s3XMLNS, Bucket: bucket, Prefix: prefix, Delimiter: delimiter, KeyMarker: keyMarker, UploadIDMarker: idMarker, MaxUploads: limit}
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		s3Failure(w, client.ErrRange)
		return
	}
	encode := func(v string) string {
		if encoding == "url" {
			return s3Escape(v, false)
		}
		return v
	}
	result.Prefix = encode(result.Prefix)
	result.Delimiter = encode(result.Delimiter)
	result.KeyMarker = encode(result.KeyMarker)
	result.EncodingType = encoding
	if limit == 0 {
		s3XML(w, result)
		return
	}
	seen := map[string]bool{}
	count := 0
	for _, u := range uploads {
		if u.State != client.S3UploadOpen && u.State != client.S3UploadPublishing || u.Owner != p.User.ID && !p.isAdmin() || !strings.HasPrefix(u.Key, prefix) || u.Key < keyMarker || u.Key == keyMarker && (idMarker == "" || u.ID <= idMarker) {
			continue
		}
		key := u.Key
		common := false
		if delimiter != "" {
			if i := strings.Index(strings.TrimPrefix(key, prefix), delimiter); i >= 0 {
				key = key[:len(prefix)+i+len(delimiter)]
				common = true
			}
		}
		if common && (seen[key] || key <= keyMarker) {
			continue
		}
		if count == limit {
			result.IsTruncated = true
			break
		}
		if common {
			seen[key] = true
			result.CommonPrefixes = append(result.CommonPrefixes, s3ListPrefix{Prefix: encode(key)})
		} else {
			result.Uploads = append(result.Uploads, upload{encode(u.Key), u.ID, u.Created.Format(time.RFC3339Nano), "STANDARD"})
		}
		count++
		result.NextKeyMarker = encode(key)
		result.NextUploadIDMarker = u.ID
	}
	s3XML(w, result)
}
