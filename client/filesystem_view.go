// SPDX-License-Identifier: GPL-3.0-only
package client

import "context"

// FilesystemView is the common client API of ordinary and native overlay
// filesystems. Bucket selection stays in the constructors.
type FilesystemView interface {
	Stat(context.Context, ObjectID) (FSInfo, error)
	Lookup(context.Context, ObjectID, string) (FSInfo, error)
	ReadDir(context.Context, ObjectID) ([]FSEntry, error)
	Create(context.Context, ObjectID, string, uint32, uint32, uint32, bool) (FSInfo, error)
	ReadAt(context.Context, ObjectID, []byte, int64) (int, error)
	WriteAt(context.Context, ObjectID, []byte, int64) (int, error)
	Append(context.Context, ObjectID, []byte) (int, error)
	Truncate(context.Context, ObjectID, uint64) (FSInfo, error)
	SetAttributes(context.Context, ObjectID, FSAttributes) (FSInfo, error)
	Unlink(context.Context, ObjectID, string) error
	Rmdir(context.Context, ObjectID, string) error
	RenameWithFlags(context.Context, ObjectID, string, ObjectID, string, uint32) error
	Sync(context.Context, ObjectID) error
	CheckWrite(context.Context, ObjectID) error
	PrepareWrite(context.Context, ObjectID) error
}

func (f *Filesystem) PrepareWrite(ctx context.Context, id ObjectID) error {
	return f.CheckWrite(ctx, id)
}
func (o *OverlayFilesystem) PrepareWrite(ctx context.Context, id ObjectID) error {
	return o.copyUp(ctx, id)
}

var _ FilesystemView = (*Filesystem)(nil)
var _ FilesystemView = (*OverlayFilesystem)(nil)
