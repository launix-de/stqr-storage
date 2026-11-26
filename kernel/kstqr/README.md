# kstqr

Standalone Linux kernel filesystem client for STQR, using the native C protocol
core. The server may run on the same host. The mount helper exits after mounting;
no resident client daemon, Go runtime or FUSE process is required.

Build from the repository root with `make kernel` (module and mount helper),
or use `make kernel-module` and `make kernel-helper` individually. Matching
Linux 7.0 headers are required; select them with `KDIR=/path/to/kernel/build`.

See [the kernel client guide](../../docs/kstqr.md) for credentials, mount syntax,
implemented filesystem operations, current limits and isolated QEMU tests.

`-o bd` exports an existing named block-device bucket as real Linux block nodes;
`-o bd,device=NAME` selects one device and exposes it as `bd`. It shares the NBD
stripe format and exclusive object lease, without running an NBD process. See the
[block-device and fstab instructions](../../docs/kstqr.md#native-block-device-buckets--o-bd).

Filesystem payload and index pages use the same stable addresses and recoverable
inode intents as the Go client. Replaced/truncated upper pages are retired by
the decided intent; lower overlay pages are never deleted by an upper mutation.
With the object-reference capability, regular-file/directory opens acquire durable
upper/lower inode pins, including readonly server grants. Dup/fork and mmap keep
the same file reference alive. Final release flushes dirty pages before dropping
the pin. Unlink and rename-overwrite atomically enroll zero-link inodes in the
shared Go/kernel orphan queue. A writable mounted client retries bounded cleanup
after the last cooperating handle closes; it never performs an online namespace
sweep. Virtual removed-directory metadata is pruned under the same exclusive
claim, retaining parent whiteouts. Legacy/shared data blocks whose ownership is
not provable remain for offline collection.
Symlink, FIFO and socket metadata are retained for offline collection because
special-file operations bypass these open hooks. O_PATH handles also bypass
these hooks: their metadata lifetime and reopening an unlinked O_PATH target
are not covered by the online-reclamation guarantee. Ordinary read/write file
handles, directory handles and mmap lifetimes are covered.

Crashes and failed releases retain durable pins; exclusive offline filesystem
recovery handles abandoned open references. Persistent lower dependency pins
remain distinct and must preserve the referenced graph, including bucket COW
snapshots. Their canonical tokens are reserved in the shared STLA lower-anchor ledger
before publication and recorded in existing trusted xattrs. Failed copy-up
retries reuse the same owner/target token. Offline graph recovery releases an
unused anchor only when the server proves no live bucket clone still inherits
it; deleting one upper inode alone never releases it. Bucket-level
COW and replica lifetime remain server-owned. Servers without the capability
use the earlier conservative offline-only inode lifecycle. Existing overlay
metadata created by clients without dependency anchors is not automatically
migrated by the kernel; validate such dependencies during an offline upgrade
before enabling lower online reclamation.

Modern inode intents pin their temporary decision object until the origin has
observed its outcome. One exclusive finalizer applies all redo, publishes inode
heads and retires the temporary decision; delayed helpers cannot overwrite a
subsequent mutation. A WRITE-only account lacking DELETE can retain compact
receipts for an authorized later collector.
The VFS exposes ordinary file operations, not a separate user-visible CAS or
snapshot-creation API; its internal inode publication uses native object CAS.

Named block devices keep deterministic stripe object IDs. Overwrites update the
same stripe objects; full-stripe discard deletes them and partial discard zeros
the addressed range. This adapter does not implement a filesystem collector for
the filesystem inside a block device, nor a user-visible block CAS operation.
