// SPDX-License-Identifier: GPL-3.0-only
package server

// Account removal and these catalog intents are published together by the
// global administrative draft. Physical cleanup uses the regular resumable
// bucket-deletion worker, including preservation of COW neighbours.
func (s *Server) deleteExclusiveUserBuckets(user string) error {
	var changes []BucketDescription
	for _, d := range s.bucketSnapshots() {
		if d.ID == systemBucket.String() || d.State == BucketDeleting || d.DeletionComplete {
			continue
		}
		owned, shared := false, false
		for _, grant := range d.Grants {
			if grant.Permissions == 0 {
				continue
			}
			if grant.UserID == user {
				owned = true
			} else {
				shared = true
			}
		}
		if !owned || shared {
			continue
		}
		bucket, err := s.Bucket(newVolumeID(d.ID))
		if err != nil {
			return err
		}
		if bucket.underMaintenance() {
			return fail(409, "bucket is under exclusive maintenance")
		}
		d.State = BucketDeleting
		changes = append(changes, d)
	}
	// Validate every candidate before changing any of them. One catalog CAS also
	// avoids rewriting a thousand-bucket catalog once for every deleted bucket.
	if err := s.buckets.saveBatch(changes); err != nil {
		return err
	}
	for _, d := range changes {
		s.startBucketCleanup(newVolumeID(d.ID))
	}
	return nil
}
