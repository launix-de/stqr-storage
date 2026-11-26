// SPDX-License-Identifier: GPL-3.0-only
package client

import "context"

// Native filesystems wait for visibility/CAS results, not physical persistence.
// The wrapper is owned by the filesystem; the original Client retains its
// default durable API (including S3 calls using that same native connection).
// Custom stores keep their own acknowledgement contract.
type filesystemNativeStore struct{ *Client }

func filesystemDeferredStore(store FilesystemStore) FilesystemStore {
	switch s := store.(type) {
	case *Client:
		return &filesystemNativeStore{s}
	case *SubscriptionStore:
		s.mu.Lock()
		s.deferred = true
		s.accepted = true
		s.mu.Unlock()
	}
	return store
}
func filesystemDeferredContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, filesystemDeferredKey{}, true)
}
func (s *filesystemNativeStore) CAS(ctx context.Context, id ObjectID, off uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error) {
	opt.Accepted = true
	return s.Client.CAS(filesystemDeferredContext(ctx), id, off, expected, replacement, opt)
}
func (s *filesystemNativeStore) Put(ctx context.Context, id ObjectID, data []byte, opt PutOptions) (ObjectInfo, error) {
	opt.Accepted = true
	return s.Client.Put(filesystemDeferredContext(ctx), id, data, opt)
}
func (s *filesystemNativeStore) PutRange(ctx context.Context, id ObjectID, off uint64, data []byte, opt PutOptions) (ObjectInfo, error) {
	opt.Accepted = true
	return s.Client.PutRange(filesystemDeferredContext(ctx), id, off, data, opt)
}
func (s *filesystemNativeStore) Delete(ctx context.Context, id ObjectID, opt DeleteOptions) error {
	opt.Accepted = true
	return s.Client.Delete(filesystemDeferredContext(ctx), id, opt)
}
func (s *filesystemNativeStore) PinObject(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.PinObject(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) UnpinObject(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.UnpinObject(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) ClaimObjectRetirement(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.ClaimObjectRetirement(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) ReleaseObjectRetirement(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.ReleaseObjectRetirement(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) PinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.PinObjectDependency(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) UnpinObjectDependency(ctx context.Context, id ObjectID, token ID) error {
	return s.Client.UnpinObjectDependency(filesystemDeferredContext(ctx), id, token)
}
func (s *filesystemNativeStore) ResetAbandonedObjectReferences(ctx context.Context) error {
	return s.Client.ResetAbandonedObjectReferences(filesystemDeferredContext(ctx))
}
func (s *filesystemNativeStore) ReclaimBucketMetadata(ctx context.Context) error {
	return s.Client.ReclaimBucketMetadata(filesystemDeferredContext(ctx))
}
func (s *filesystemNativeStore) CanReclaimDependencies(ctx context.Context) (bool, error) {
	return s.Client.CanReclaimDependencies(filesystemDeferredContext(ctx))
}

func (s *SubscriptionStore) filesystemContext(ctx context.Context) context.Context {
	s.mu.Lock()
	deferred := s.deferred
	s.mu.Unlock()
	if deferred {
		return filesystemDeferredContext(ctx)
	}
	return ctx
}
func (s *SubscriptionStore) acceptedWrites() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}
