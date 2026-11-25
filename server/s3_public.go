// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"

	"github.com/launix-de/stqr-storage/client"
)

type publicS3Context struct{}

// Internal object reads are bucket-bound, READ-only and never returned as a
// native capability. The object's ACL is checked on each exact read snapshot.
func (s *Server) publicS3Principal(b *Bucket) (principal, error) {
	d := b.Describe()
	if d.State != BucketActive || b.ID() == systemBucket {
		return principal{}, ErrBucketForbidden
	}
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		return principal{}, err
	}
	for _, u := range image.Users {
		if u.Disabled {
			continue
		}
		if u.Role == "USER" {
			for _, g := range d.Grants {
				if g.UserID == u.ID && g.Permissions&BucketRead != 0 {
					return principal{User: u}, nil
				}
			}
		}
	}
	for _, u := range image.Users {
		if !u.Disabled && u.Role == "ADMIN" {
			return principal{User: u}, nil
		}
	}
	return principal{}, ErrBucketForbidden
}
func (s *Server) servePublicS3(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead || len(r.Header.Values("Authorization")) != 0 {
		return false
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false
	}
	for k := range q {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			return false
		}
	}
	name, key, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
	if !ok || key == "" {
		return false
	}
	for _, k := range []string{"acl", "uploadId", "uploads", "tagging", "policy", "delete", "versionId", "versions"} {
		if _, ok := q[k]; ok {
			return false
		}
	}
	b, err := s.BucketByName(name)
	if err != nil {
		return false
	}
	// Anonymous requests cannot select an application format for a bucket.
	if b.Describe().Format != "s3" {
		return false
	}
	p, err := s.publicS3Principal(b)
	if err != nil {
		s3Failure(w, err)
		return true
	}
	p.frontendPermission = BucketRead
	api, closeAPI, err := s.openS3(r.Context(), b, p)
	if err != nil {
		s3Failure(w, err)
		return true
	}
	defer closeAPI()
	r = r.WithContext(context.WithValue(r.Context(), publicS3Context{}, true))
	s.serveS3Read(w, r, api, key)
	return true
}

const allUsersGroup = "http://acs.amazonaws.com/groups/global/AllUsers"

type s3ACLGrantee struct {
	Type string `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	ID   string `xml:",omitempty"`
	URI  string `xml:",omitempty"`
}
type s3ACLGrant struct {
	Grantee    s3ACLGrantee
	Permission string
}
type s3ACLPolicy struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Owner   struct{ ID string }
	Grants  []s3ACLGrant `xml:"AccessControlList>Grant"`
}

func (s *Server) serveS3ACL(w http.ResponseWriter, r *http.Request, api *client.S3, b *Bucket, key string) {
	if r.Method == http.MethodGet {
		o, err := api.Head(r.Context(), key)
		if err != nil {
			s3Failure(w, err)
			return
		}
		policy := s3ACLPolicy{XMLNS: s3XMLNS}
		policy.Owner.ID = b.ID().String()
		policy.Grants = []s3ACLGrant{{Grantee: s3ACLGrantee{Type: "CanonicalUser", ID: policy.Owner.ID}, Permission: "FULL_CONTROL"}}
		if o.PublicRead {
			policy.Grants = append(policy.Grants, s3ACLGrant{Grantee: s3ACLGrantee{Type: "Group", URI: allUsersGroup}, Permission: "READ"})
		}
		s3XML(w, policy)
		return
	}
	if r.Method != http.MethodPut {
		s3Error(w, 501, "NotImplemented", "Unsupported ACL operation")
		return
	}
	public := false
	if acl := r.Header.Get("X-Amz-Acl"); acl != "" {
		if acl != "private" && acl != "public-read" {
			s3Error(w, 501, "NotImplemented", "Supported canned ACLs are private and public-read")
			return
		}
		if r.ContentLength != 0 {
			s3Error(w, 400, "InvalidRequest", "Use a canned ACL or an XML policy, not both")
			return
		}
		public = acl == "public-read"
	} else {
		var policy s3ACLPolicy
		if err := s3ReadXML(r, &policy, 64<<10); err != nil {
			s3Error(w, 400, "MalformedACLError", "Invalid ACL XML")
			return
		}
		if policy.Owner.ID != "" && policy.Owner.ID != b.ID().String() {
			s3Error(w, 501, "NotImplemented", "Only the bucket owner and public READ grants are supported")
			return
		}
		for _, g := range policy.Grants {
			if g.Grantee.Type == "Group" && g.Grantee.URI == allUsersGroup && g.Permission == "READ" {
				public = true
				continue
			}
			if g.Grantee.Type == "CanonicalUser" && g.Grantee.ID == b.ID().String() && g.Permission == "FULL_CONTROL" {
				continue
			}
			s3Error(w, 501, "NotImplemented", "Unsupported ACL grant")
			return
		}
	}
	for _, h := range []string{"X-Amz-Grant-Read", "X-Amz-Grant-Read-Acp", "X-Amz-Grant-Write-Acp", "X-Amz-Grant-Full-Control"} {
		if r.Header.Get(h) != "" {
			s3Error(w, 501, "NotImplemented", "Explicit grant headers are unsupported")
			return
		}
	}
	if err := api.SetPublicRead(r.Context(), key, public); err != nil {
		s3Failure(w, err)
		return
	}
	w.WriteHeader(200)
}
