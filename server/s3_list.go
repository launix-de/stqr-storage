// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type s3ListContent struct {
	Key          string
	LastModified string
	ETag         string
	Size         uint64
	StorageClass string
}
type s3ListPrefix struct{ Prefix string }
type s3ListResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name                  string
	Prefix                string
	Marker                *string `xml:",omitempty"`
	NextMarker            string  `xml:",omitempty"`
	ContinuationToken     string  `xml:",omitempty"`
	NextContinuationToken string  `xml:",omitempty"`
	StartAfter            string  `xml:",omitempty"`
	KeyCount              *int    `xml:",omitempty"`
	MaxKeys               int
	Delimiter             string `xml:",omitempty"`
	EncodingType          string `xml:",omitempty"`
	IsTruncated           bool
	Contents              []s3ListContent
	CommonPrefixes        []s3ListPrefix
}

func (s *Server) serveS3List(w http.ResponseWriter, r *http.Request, api *client.S3, bucket string) {
	q := r.URL.Query()
	version := q.Get("list-type")
	if version != "" && version != "2" {
		s3Error(w, 400, "InvalidArgument", "Invalid list type")
		return
	}
	maxKeys := 1000
	if raw := q.Get("max-keys"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 0 {
			s3Error(w, 400, "InvalidArgument", "Invalid max-keys")
			return
		}
		maxKeys = min(n, 1000)
	}
	prefix, delimiter, marker := q.Get("prefix"), q.Get("delimiter"), q.Get("marker")
	if version == "2" {
		marker = q.Get("start-after")
		if token := q.Get("continuation-token"); token != "" {
			raw, e := base64.RawURLEncoding.DecodeString(token)
			if e != nil {
				s3Error(w, 400, "InvalidArgument", "Invalid continuation token")
				return
			}
			marker = string(raw)
		}
	}
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		s3Error(w, 400, "InvalidArgument", "Invalid encoding type")
		return
	}
	encode := func(v string) string {
		if encoding == "url" {
			return s3Escape(v, false)
		}
		return v
	}
	result := s3ListResult{XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: encode(prefix), MaxKeys: maxKeys, Delimiter: encode(delimiter), EncodingType: encoding}
	if version == "2" {
		result.ContinuationToken = q.Get("continuation-token")
		result.StartAfter = encode(q.Get("start-after"))
		count := 0
		result.KeyCount = &count
	} else {
		m := encode(marker)
		result.Marker = &m
	}
	if maxKeys > 0 {
		keys, err := api.Keys(r.Context(), prefix)
		if err != nil {
			s3Failure(w, err)
			return
		}
		type entry struct {
			key    string
			prefix bool
			object client.S3Object
		}
		entries := []entry{}
		seen := map[string]bool{}
		for _, originalKey := range keys {
			key := originalKey
			isPrefix := false
			if delimiter != "" {
				if i := strings.Index(strings.TrimPrefix(key, prefix), delimiter); i >= 0 {
					key = key[:len(prefix)+i+len(delimiter)]
					isPrefix = true
				}
			}
			if key <= marker || seen[key] {
				continue
			}
			o, err := api.Stat(r.Context(), originalKey)
			if errors.Is(err, client.ErrNotFound) {
				continue
			}
			if err != nil {
				s3Failure(w, err)
				return
			}
			seen[key] = true
			entries = append(entries, entry{key, isPrefix, o})
			if len(entries) > maxKeys {
				break
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
		result.IsTruncated = len(entries) > maxKeys
		if result.IsTruncated {
			entries = entries[:maxKeys]
		}
		for _, e := range entries {
			if e.prefix {
				result.CommonPrefixes = append(result.CommonPrefixes, s3ListPrefix{encode(e.key)})
			} else {
				o := e.object
				result.Contents = append(result.Contents, s3ListContent{encode(o.Key), o.Modified.Format(time.RFC3339Nano), "\"" + o.ETag + "\"", o.Size, "STANDARD"})
			}
		}
		if result.KeyCount != nil {
			*result.KeyCount = len(entries)
		}
		if result.IsTruncated {
			last := entries[len(entries)-1].key
			if version == "2" {
				result.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(last))
			} else {
				result.NextMarker = encode(last)
			}
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(result)
}
