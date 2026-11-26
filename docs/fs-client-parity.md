# Filesystem client parity and production-readiness audit

This audit compares the Linux kernel client (`kernel/kstqr`), Linux FUSE adapter
(`client`) and Windows WinFsp adapter (`windows`). The original audit used the
frozen integration baseline `969f705` on 2026-11-26. Its reference/reclamation
protocol was not all present in the older master `18216ce`; it was subsequently
merged. The matrix below also includes the Windows follow-up in PR #50.
A green result does not make an older mounted binary equivalent. Upgrade all
writers together.

No finite test run proves bug freedom. The supported common file operations are
tested below; platform differences and remaining production limits are explicit.
No production bucket was written, no cluster node restarted, and no module loaded
into the host kernel for this audit.

## Feature matrix

| Feature | Linux kernel | Linux FUSE | Windows WinFsp |
| --- | --- | --- | --- |
| Same STFS v7 / STIX v3 / STFT v3 representation | Yes | Yes | Shared Go implementation |
| Create, read/write, sparse extension, truncate/regrow | Yes | Yes | Yes |
| Concurrent append | Atomic per chunk, up to 1 MiB | Shared inode CAS retry | Shared inode CAS retry |
| Rename/replace, no-replace, exchange | Yes | Yes | Adapter callbacks; native Windows API semantics differ |
| Open file after rename/unlink | Retained inode, including mmap/dup/fork | Retained file/directory handles | Retained callback handles; native Windows sharing/delete rules also apply |
| Mode, UID/GID, explicit atime/mtime | Yes | Yes | Stored Unix metadata; mounting-user access model |
| Setgid-directory group inheritance | Existing VFS support | Corrected by this audit | Corrected by this audit |
| Symlinks | Yes | Yes | Callbacks implemented; native reparse-point behavior requires real-driver testing |
| Create hardlinks | Yes | Yes | Explicitly unsupported by the current WinFsp adapter |
| Access existing Linux hardlinks | Yes | Yes | Shared inode/content |
| FIFO and Unix socket nodes | Yes, IPC local to a mount | Yes, IPC local to a mount | No equivalent native creation/open support |
| `user.*` / privileged `trusted.*` xattrs | Yes | Yes | `user.*` callbacks; privileged `trusted.*` access rejected |
| POSIX ACLs, `security.*` / capability xattrs | Unsupported | Unsupported | No full Windows ACL/security-descriptor implementation |
| Character/block special files inside FS format | Unsupported | Unsupported | Unsupported |
| Explicit filesystem durability barrier | fsync/fdatasync/msync/syncfs | fsync/fsyncdir | Fsync/Fsyncdir callbacks |
| Ordinary close is a durability barrier | No blanket guarantee | No | No |
| Shared writable mmap | Supported within a mount, with writeback | Writable opens use direct I/O; unsupported | No distributed writable-mapping coherence guarantee |
| Native lower/upper overlay view | Yes | Yes | Shared overlay core, CLI and WinUI options |
| Overlay symlink/xattr reads | Yes | Yes | Shared overlay read callbacks |
| Overlay hardlink/special-file creation and xattr mutation | Unsupported | Unsupported | Unsupported |
| Native bucket-COW snapshots | Server-owned objects, transparent | Server-owned objects, transparent | Server-owned objects, transparent |
| Subscription/cache invalidation | Separate reader, bounded metadata cache and page invalidation | Subscription cache and FUSE invalidation | Shared subscription cache and WinFsp directory notifications; native cache timeouts zero |
| Transport failure | Hard wait/reconnect; interrupt/forced-unmount cancellation | Go client request/reconnect semantics | Go client request/reconnect semantics |
| Distributed application file locks | Not implemented; kernel locks are local | Not implemented | Not implemented |
| Cooperative last-close reclamation | Regular files/directories and mmap lifetimes | Shared reference/orphan protocol | Shared reference/orphan protocol |
| Crashed/abandoned reference collection | Exclusive offline recovery | Exclusive offline fsck | Linux offline fsck is the fallback |
| Kernel/OS integration support proven here | Linux 7.0.0-38 generic in disposable QEMU | Actual FUSE/VFS mounts | Portable and native callback tests, plus explicitly recorded WinFsp mount result |

The common API and wire representation are in `client/filesystem_view.go`,
`client/filesystem_format.go` and `client/filesystem_transaction.go`. Linux
callbacks are in `client/filesystem_fuse_linux.go`,
`client/filesystem_posix_fuse_linux.go` and `kernel/kstqr/vfs.c`. Windows callbacks
are in `windows/adapter_windows.go`; inode/handle management is in
`windows/filesystem.go`. Platform caveats are detailed in
[the Linux guide](kstqr.md), [POSIX semantics](filesystem-posix.md),
[Windows guide](../windows/README.md) and [reference/reclamation work](https://github.com/launix-de/stqr-storage/pull/39).

## Reproduced defects and corrections

1. **Incorrect group ownership under setgid directories.** FUSE used the caller's
   GID and Windows used the configured default GID, even when the parent directory
   had a different group and the setgid bit. Child directories also lost setgid.
   Both adapters now inherit the parent's GID and propagate setgid to new
   directories. FUSE regular files, directories, symlinks, FIFOs and socket nodes,
   and Windows regular files, directories and symlinks have regression tests.
   The kernel's existing behavior is checked explicitly in the VM smoke test.
   The tests failed on the frozen source before the fix, then passed.
2. **Concurrent removal could fail directory enumeration.** FUSE reads a directory
   snapshot, then obtains each child's attributes. A child reclaimed by another
   client between these steps caused the whole enumeration to return ENOENT.
   The kernel's `ks_iterate` had the same error branch. Both now skip only missing
   children and continue; the kernel also advances its cursor over the removed
   entry. Other backend errors remain errors. FUSE has deterministic missing-child
   injection and an authorization-error control. Windows already skipped missing
   entries. The kernel correction is source-reviewed and exercised by the full
   VM suite; no claim is made that a particular concurrent kernel race occurred
   in the live cluster.

## Verification and limits

The complete shared Go-client suite and Windows portable suite pass with the race
detector. Existing tests cover namespace crash cut points, CAS conflicts, sparse
 tails, CoW/overlay guards, concurrent writes and cooperative handle lifetime.
The new negative controls fail for the intended setgid/readdir defects.

Real FUSE mounts cover Git, rsync with hardlinks, POSIX metadata/symlink/FIFO/socket
operations, open-unlink and rename replacement. Native server fixtures verify
subscription invalidation of existing file descriptors, negative entries and
native overlay copy-up. Kernel QEMU checks separately cover ordinary files,
shared mmap/SQLite/writeback, subscription coherence, interrupted-intent recovery,
native overlays and a backend outage with wait/reconnect instead of panic.

`TestWindowsMountedFilesystem` is an explicit opt-in test for a disposable Windows
machine with the actual signed WinFsp driver (`STQR_TEST_WINFSP=1`). Callback tests
under Wine and portable Go tests do not load that driver and cannot substitute
for this test. The mount test covers native file APIs, Go-created metadata,
existing Linux hardlinks/xattrs, sparse I/O, sync, truncate/regrow, namespace
changes and reopening the resulting graph through Go.

Production release constraints:

- Pin the complete server and client build set, including the reference protocol
  capability and format revisions. Do not infer compatibility from the label
  `master` while open PRs are deployed in the cluster.
- The matrix is not full feature parity. Windows hardlink creation, privileged xattr access, ACLs and distributed file locks are not implemented. Shared writable
  mmap coherence across independent mounts is not promised.
- VM disk images or databases accessed concurrently from multiple mounts require
  external exclusive ownership. Successful per-inode CAS and subscriptions do
  not implement distributed application locks.
- Kernel operation is tested against the stated kernel version. Other VFS/kernel
  versions, long-duration hardware/power-loss testing, full desktop/Explorer
  application compatibility and every Windows sharing/locking mode are outside
  the demonstrated test coverage.
- Backends without cooperative references retain conservative offline-only
  reclamation. Special-node and O_PATH lifetimes have explicit limitations in
  the kernel/reclamation guides. Abandoned durable pins require exclusive offline
  recovery; they must not be expired by guessing that a client is dead.

These are release criteria and known limits, not newly invented corruption
findings. Passing the listed checks supports a bounded trial with the tested
builds; it does not justify an unconditional bug-free or production-ready claim.

## WinUI and parity follow-up

The Windows follow-up replaces the default setup frontend with native WinUI 3,
while retaining the Go CLI, DPAPI profiles and WinFsp backend. The Windows adapter
now accepts the common filesystem-view interface, enforces `PrepareWrite` at
writable open, exposes `user.*` extended attributes, and mounts native overlays
with their persisted whitelist. The overlay core has an explicit readonly option
that rejects copy-up/mutation and suppresses automatic orphan cleanup. Windows
also enables WinFsp delete-access preflight: readonly/whitelist failures are
returned before Windows acknowledges deletion, because the driver ignores unlink
errors during its later cleanup callback. Windows uses the same accepted-write mode as FUSE, with explicit durability barriers and
a final bounded synchronization during orderly unmount.

These changes do not add native Windows hardlink creation, POSIX IPC nodes,
Windows ACL translation, distributed application locks or coherent shared writable
mappings across independent clients. Those require capabilities or protocols
beyond adapter parity; supported operations retain the existing on-disk format.
Validation of this follow-up is recorded separately in its PR.
