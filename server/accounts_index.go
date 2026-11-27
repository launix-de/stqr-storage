// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"github.com/launix-de/stqr-storage/volume"
)

// accountsIndex is the immutable serving view of the persistent accounts object.
// The admin volume is read at recovery/bootstrap, never for a runtime lookup.
// Writers build an owned snapshot, persist it, then publish the complete index.
// Private admin drafts own a separate index; staging a cluster proposal does not
// change serving authority. A committed system-view installation publishes it.
// Readers must not mutate image or its slices; mutable admin operations receive
// a deep copy through readBlob. No failed persistence may publish a new index.
type accountsIndex struct {
	image                accountsImage
	byName               map[string]int
	byID                 map[string]int
	byAccessKey          map[string]accountAccessKey
	byReceipt            map[string]int
	admin                int
	referenceAuth        []*referenceAccountStamp
	referenceOwners      map[volume.ID]struct{}
	referenceOwnersKnown bool
}

type accountAccessKey struct {
	user int
	key  int
}

func cloneAccount(a account) account {
	a.S3Keys = append([]s3AccessKey(nil), a.S3Keys...)
	return a
}

func cloneAccountsImage(image accountsImage) accountsImage {
	image.Users = append([]account(nil), image.Users...)
	for i := range image.Users {
		image.Users[i] = cloneAccount(image.Users[i])
	}
	image.Receipts = append([]accountReceipt(nil), image.Receipts...)
	return image
}

func newAccountsIndex(image accountsImage) (*accountsIndex, error) {
	if image.Schema != 1 {
		return nil, errors.New("invalid system account image")
	}
	x := &accountsIndex{referenceOwners: make(map[volume.ID]struct{}, len(image.Users)), referenceOwnersKnown: true, referenceAuth: make([]*referenceAccountStamp, len(image.Users)), image: cloneAccountsImage(image), byName: make(map[string]int, len(image.Users)), byID: make(map[string]int, len(image.Users)), byAccessKey: make(map[string]accountAccessKey), byReceipt: make(map[string]int, len(image.Receipts)), admin: -1}
	for i, u := range x.image.Users {
		x.referenceAuth[i] = new(referenceAccountStamp)
		if id, err := parseID(u.ID); err == nil {
			x.referenceOwners[id] = struct{}{}
		} else {
			x.referenceOwnersKnown = false
		}
		if _, exists := x.byName[u.Name]; exists {
			return nil, errors.New("duplicate account name")
		}
		if _, exists := x.byID[u.ID]; exists {
			return nil, errors.New("duplicate account ID")
		}
		x.byName[u.Name], x.byID[u.ID] = i, i
		if x.admin < 0 && u.Role == "ADMIN" && !u.Disabled {
			x.admin = i
		}
		for j, k := range u.S3Keys {
			if _, exists := x.byAccessKey[k.ID]; exists {
				return nil, errors.New("duplicate S3 access key")
			}
			x.byAccessKey[k.ID] = accountAccessKey{i, j}
		}
	}
	for i, receipt := range x.image.Receipts {
		if _, exists := x.byReceipt[receipt.ID]; exists {
			return nil, errors.New("duplicate account receipt")
		}
		x.byReceipt[receipt.ID] = i
	}
	return x, nil
}

func (s *Server) accountIndex() (*accountsIndex, error) {
	if image := s.accounts.Load(); image != nil {
		return image, nil
	}
	return nil, errors.New("system account index is not loaded")
}

func (s *Server) findAccountID(id string) (account, error) {
	x, err := s.accountIndex()
	if err != nil {
		return account{}, err
	}
	if i, exists := x.byID[id]; exists && !x.image.Users[i].Disabled {
		return cloneAccount(x.image.Users[i]), nil
	}
	return account{}, errors.New("unknown account")
}
