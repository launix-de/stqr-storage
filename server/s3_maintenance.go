// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"errors"
	"sort"
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
		if d.State != BucketActive || d.ID == systemBucket.String() || d.Format != "s3" {
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
	return s.maintainS3Uploads(ctx, now, b, accounts, api, uploads)
}
func (s *Server) maintainS3Uploads(ctx context.Context, now time.Time, b *Bucket, accounts accountsImage, api *client.S3, uploads []client.S3Upload) error {
	var e error
	for _, upload := range uploads {
		if upload.State != client.S3UploadPublishing {
			if e := api.ExpireMultipartUpload(ctx, upload, now, s3IdleUploadLifetime, s3CompletionReceiptLifetime); e != nil {
				return e
			}
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
			fresh, err := api.Multipart(ctx, upload.ID)
			if err != nil && !errors.Is(err, client.ErrS3Upload) {
				return err
			}
			if err == nil {
				if err = api.ExpireMultipartUpload(ctx, fresh, now, s3IdleUploadLifetime, s3CompletionReceiptLifetime); err != nil {
					return err
				}
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
		fresh, err := api.Multipart(ctx, upload.ID)
		if err != nil && !errors.Is(err, client.ErrS3Upload) {
			return err
		}
		if err == nil {
			if err = api.ExpireMultipartUpload(ctx, fresh, now, s3IdleUploadLifetime, s3CompletionReceiptLifetime); err != nil {
				return err
			}
		}
	}
	return nil
}

// Discovery is round-robin across buckets and resumes inside each bucket. One
// page per interval bounds housekeeping even for buckets with millions of native
// filesystem/data objects. No client bucket marker or persisted format is added.
type s3MaintenanceCursor struct {
	after map[string]*client.ObjectID
	last  string
}

const s3MaintenancePageObjects = 128
const s3MaintenancePageBytes = 8 << 20

func (s *Server) maintainS3Page(ctx context.Context, now time.Time, cursor *s3MaintenanceCursor) error {
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
	descriptions := s.bucketSnapshots()
	active := descriptions[:0]
	for _, d := range descriptions {
		if d.State == BucketActive && d.ID != systemBucket.String() && d.Format == "s3" {
			active = append(active, d)
		}
	}
	if cursor.after == nil {
		cursor.after = map[string]*client.ObjectID{}
	}
	existing := map[string]bool{}
	for _, d := range active {
		existing[d.ID] = true
	}
	for id := range cursor.after {
		if !existing[id] {
			delete(cursor.after, id)
		}
	}
	if len(active) == 0 {
		cursor.last = ""
		return nil
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	selected := 0
	for i, d := range active {
		if d.ID > cursor.last {
			selected = i
			break
		}
	}
	d := active[selected]
	cursor.last = d.ID
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
	uploads, next, e := api.MultipartUploadsPage(ctx, cursor.after[d.ID], s3MaintenancePageObjects, s3MaintenancePageBytes)
	if e != nil {
		return e
	}
	if e = s.maintainS3Uploads(ctx, now, b, accounts, api, uploads); e != nil {
		return e
	}
	cursor.after[d.ID] = next
	return nil
}
func (s *Server) startS3Maintenance() {
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		cursor := s3MaintenanceCursor{}
		for {
			if s.ctx.Err() != nil {
				return
			}
			_ = s.maintainS3Page(s.ctx, time.Now(), &cursor)
			// Start the interval after the bounded work: a slow step cannot accumulate
			// ticker debt and immediately begin another scan while foreground I/O waits.
			timer := time.NewTimer(time.Second)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}
