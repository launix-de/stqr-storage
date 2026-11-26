// SPDX-License-Identifier: GPL-3.0-only
package server

// Called under opMu on the global administration draft. Account removal and
// the DELETING bucket intent are published by the same ALL metadata commit.
// Only members of this bucket's ACL are candidates; admins are never removed.
// A locked bucket remains a valid readable namespace, a deleting one does not.
func (s *Server) bucketOrphanAccounts(deleting BucketDescription) (*accountsImage, []string, error) {
	candidates := make(map[string]bool, len(deleting.Grants))
	for _, grant := range deleting.Grants {
		if grant.Permissions != 0 {
			candidates[grant.UserID] = true
		}
	}
	if len(candidates) == 0 {
		return nil, nil, nil
	}
	for _, other := range s.bucketSnapshots() {
		if other.ID == deleting.ID || other.ID == systemBucket.String() || other.DeletionComplete || other.State == BucketDeleting {
			continue
		}
		for _, grant := range other.Grants {
			if grant.Permissions != 0 {
				delete(candidates, grant.UserID)
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil, nil
	}
	var image accountsImage
	if err := s.readBlob("accounts", &image); err != nil {
		return nil, nil, err
	}
	keep := image.Users[:0]
	var removed []string
	for _, user := range image.Users {
		if user.Role == "USER" && candidates[user.ID] {
			removed = append(removed, user.ID)
		} else {
			keep = append(keep, user)
		}
	}
	if len(removed) == 0 {
		return nil, nil, nil
	}
	image.Users = keep
	return &image, removed, nil
}
