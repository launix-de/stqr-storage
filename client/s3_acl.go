// SPDX-License-Identifier: GPL-3.0-only
package client

import (
	"context"
	"encoding/binary"
	"errors"
)

// SetPublicRead changes only the fixed ACL flag with byte-comparing ranged CAS.
// Payload, ETag and Last-Modified remain unchanged. Private is the default ACL.
func (s *S3) SetPublicRead(ctx context.Context, key string, public bool) error {
	for n := 0; n < 128; n++ {
		o, err := s.Head(ctx, key)
		if err != nil {
			return err
		}
		if o.PublicRead == public {
			return nil
		}
		expected, replacement := make([]byte, 4), make([]byte, 4)
		if o.PublicRead {
			binary.LittleEndian.PutUint32(expected, 2)
		}
		if public {
			binary.LittleEndian.PutUint32(replacement, 2)
		}
		_, err = s.store.CAS(ctx, S3MetadataID(key), S3FlagsOffset, expected, replacement, CASOptions{Scope: CASRange, MutationOptions: MutationOptions{RequestID: NewRequestID()}})
		if errors.Is(err, ErrPrecondition) {
			continue
		}
		return err
	}
	return ErrPrecondition
}
