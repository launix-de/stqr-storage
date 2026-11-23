// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

const s3IdleUploadLifetime = 24 * time.Hour
const s3CompletionReceiptLifetime = time.Hour

func (s *Server) maintainS3(ctx context.Context, now time.Time) error {
	if s.joinPending() {
		return nil
	}
	var accounts accountsImage
	if e := s.readBlob("accounts", &accounts); e != nil {
		return e
	}
	var admin account
	for _, u := range accounts.Users {
		if u.Role == "ADMIN" && !u.Disabled {
			admin = u
			break
		}
	}
	if admin.ID == "" {
		return ErrBucketForbidden
	}
	for _, d := range s.bucketSnapshots() {
		if d.State != BucketActive || d.ID == systemBucket.String() {
			continue
		}
		if e := s.maintainS3Bucket(ctx, now, d, accounts, admin); e != nil {
			return e
		}
	}
	return nil
}
func (s *Server) maintainS3Bucket(ctx context.Context, now time.Time, d BucketDescription, accounts accountsImage, admin account) error {
	id, e := bucketID(d.ID)
	if e != nil {
		return e
	}
	b, e := s.Bucket(id)
	if e != nil {
		return e
	}
	api, closeAPI, e := s.openS3(ctx, b, principal{User: admin, frontendPermission: BucketWrite})
	if e != nil {
		return e
	}
	defer closeAPI()
	uploads, e := api.MultipartUploads(ctx)
	if e != nil {
		return e
	}
	for _, upload := range uploads {
		if upload.State != client.S3UploadPublishing {
			continue
		}
		var owner account
		for _, user := range accounts.Users {
			if user.ID == upload.Owner {
				owner = user
				break
			}
		}
		revoked := owner.ID == "" || owner.Disabled || (owner.Role != "ADMIN" && owner.Role != "USER")
		if !revoked {
			id, _ := bucketID(owner.ID)
			info, err := b.Info(id, owner.Role == "ADMIN")
			revoked = err != nil || info.Permissions&BucketWrite == 0
		}
		if revoked {
			if e = api.DiscardMultipart(ctx, upload); e != nil {
				return e
			}
			continue
		}
		ownerAPI, closeOwner, e := s.openS3(ctx, b, principal{User: owner, frontendPermission: BucketWrite})
		if e != nil {
			return e
		}
		_, e = ownerAPI.FinishMultipart(ctx, upload)
		closeOwner()
		if e != nil && !errors.Is(e, client.ErrPrecondition) {
			continue
		}
	}
	if e = api.ExpireMultipart(ctx, now, s3IdleUploadLifetime, s3CompletionReceiptLifetime); e != nil {
		return e
	}
	return nil
}
func (s *Server) startS3Maintenance() {
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			if s.ctx.Err() != nil {
				return
			}
			_ = s.maintainS3(s.ctx, time.Now())
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
