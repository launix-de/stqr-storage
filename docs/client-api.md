# Bucket-bound object connection

The network constructor always receives URL, username, password and a bucket ID:

```go
conn, err := client.Dial(ctx, nodeURL, username, password, bucketID)
if err != nil {
    return err
}
defer conn.Close()
info, err := conn.Put(ctx, objectID, payload, client.PutOptions{
    RequestID: client.NewRequestID(),
})
```

`Client` exposes only object operations inside that bucket: Get/GetRange,
Put/PutRange, Append, Trunc, Delete, Stat, CAS, the necessary streaming variants
and Close. Object IDs are native `[32]byte` addresses. No per-call bucket argument,
Bucket(name), global multi-bucket handle, key-address alias API, administration
or cluster metrics are offered. Another bucket requires another connection.
The shared wire codec/statistics types remain in this package to avoid server /
client import cycles; they do not add administrative methods to the connection.

FUSE uses the already bound Get/CAS subset:

```go
filesystem, err := client.OpenFilesystem(ctx, conn, client.FSOptions{
    Create: true,
})
```

The in-memory filesystem backend is an explicitly volatile development backend,
not an unauthenticated network connection. Real network connections always require
all constructor arguments.

Build the separate Linux FUSE client with:

```sh
go build -o stqr-fuse ./cmd/stqr-fuse
mkdir -p /mnt/stqr
./stqr-fuse -uri http://localhost:7419 -username alice -password 'PASSWORD' \
    -bucket 0123456789abcdef0123456789abcdef -init /mnt/stqr
```

The bucket must already exist and grant the user the required object permissions.
`-init` creates a missing filesystem root; it never reformats an existing root.
Omit it when opening an existing filesystem. A new root belongs to the mounting
process's UID/GID and has mode 0755. Options precede the final mountpoint argument.
The process runs in the foreground and handles SIGINT/SIGTERM by unmounting before
closing its bucket connection. External `fusermount3 -u /mnt/stqr` also stops it.
If signal-triggered unmount fails, the process keeps serving until an external
unmount succeeds. `-connect-timeout` defaults to 30s and only bounds startup;
`-debug` enables FUSE protocol logging. The adapter uses go-fuse's direct Linux
kernel interface; it does not link to C libfuse. Linux FUSE support, `/dev/fuse`
and an appropriate fusermount helper are required for an unprivileged mount.
On such a host, `STQR_TEST_FUSE=1 go test ./client -run '^TestFilesystemKernelMount$' -v`
exercises a real mount, syscall reads/writes, fsync, rename and an open unlinked file.
This opt-in kernel test uses the memory backend; the server integration tests
separately exercise the native network and persistent volume path.

Unmount every client of a filesystem bucket before collecting unreachable objects:

```sh
./stqr-fuse -uri http://localhost:7419 -username alice -password 'PASSWORD' \
    -bucket 0123456789abcdef0123456789abcdef --fsck
```

`--fsck` validates and cleans the bucket, prints counts and removed payload bytes,
then exits without mounting. An optional final mountpoint argument is accepted but
unused. It cannot be combined with `-init`, and requires read and delete grants.
The bucket must be dedicated to this filesystem: every live object not reachable
from its fixed root is a collection candidate, including unrelated application
objects placed in the same bucket.

The client obtains an exclusive, socket-scoped single-node maintenance session.
The server rejects it while another bucket connection exists; during maintenance,
new connections, other HTTP object accesses, reference-creating writes and bucket
configuration mutations are blocked. The maintenance owner may read, scan, run
recovery CAS and delete. Recovery CAS requires the normal write grant; a clean
graph needs only read/delete. Closing the socket releases ownership after its pending work ends.
The run never reconnects or replays after losing that session; rerun from the
beginning after a disconnect or cancellation. This is a maintenance operation,
not a persistent write on each normal read.

Fsck first helps any reachable pending namespace intents to completion and repeats
until the namespace is stable; only then does it inventory and mark objects.
It enumerates live objects in bounded pages, marks from the root through inode,
directory and segment/index objects, and verifies all reachable payloads. Shared
segments and sparse spans are preserved. Any missing reference, invalid format,
inconsistent version or directory cycle aborts before any deletion. Directory
materialization is bounded to 64 MiB and nesting to 1024; exceeding either limit
also aborts without deleting. Only after the full mark succeeds are unreferenced
objects deleted, each with its scanned expected version and a fresh request ID.
A second run completes a partially interrupted sweep and leaves a clean graph
unchanged. Missing/malformed roots are never created or repaired automatically.

Removed bytes count object payload, not shrinking the underlying volume file.
Tombstones, retry receipts and their metadata are retained by the native volume;
they are not filesystem orphans. Both server and client must support maintenance
opcodes 0x0115/0x0116. The SDK entrypoint is `Client.CheckFilesystem(ctx)` on a
dedicated connection, which must be closed after the call.

Empty-directory removal, cross-directory rename and compatible replacement are
implemented through durable multi-inode CAS intents. Rename also supports
`RENAME_NOREPLACE` and `RENAME_EXCHANGE`; incompatible replacements and nonempty
destination directories are rejected. Directory moves verify and protect the
destination ancestor path against concurrent moves that could create a cycle.
Directory lookup for that check currently walks from the root and may be costly
in large trees. Read-only inode participants also receive new native versions.
There is no global single-writer lock; ordinary writes retain per-inode CAS.

The intent descriptor and inode markers are ordinary bucket objects. Another
writer or reader with write permission helps complete a stalled operation. A
read-only client may read the old committed image of a pending intent or the new
image of a committed one without modifying its marker. Removed/replaced directory
inodes are fenced so stale handles cannot create names in them. Open files remain
readable after unlink or replacement. Completed descriptors, old segments and
unlinked inodes are collected by `--fsck`; automatic online reclamation remains
unfinished. Live inode v1 stays compatible, but removed directories use v2 and
namespace markers/intents are new: upgrade all mounts before using these operations.
This remains a POSIX subset; symlinks, hardlinks and complete timestamp/locking
semantics are not claimed.

The constructor supplies `X-STQR-Bucket` during the authenticated upgrade.
The server validates the bucket and grant before accepting the socket: `Dial`
already fails for missing buckets or denied access. Its live connection stores
that Bucket instance and authenticated capabilities; later data operations do
not repeat account or grant lookups. Revocation/password changes invalidate the
connection, and reconnect performs the complete check again.

The transport uses HTTP Basic for the `stqr.v1` WebSocket upgrade, HELLO on each
new session, bounded send/pending queues and correlated responses. Data transfers
above 1 MiB use 1-MiB chunks and SHA-256. Streaming uses temporary files and bounded
buffers instead of buffering a complete large object; temporary files are removed
on success, cancellation and invalid transfers. `Get` convenience allocations are
limited to 16 MiB; use `GetTo` or ranged reads for larger payloads. This is not a
logical object-size limit. Inputs must remain unchanged until a call returns;
blocking reader/writer implementations are responsible for their own I/O cancellation.

On transport loss, operations reconnect and reuse their request ID. Cancellation
of a potentially sent mutation returns `UnknownOutcomeError` with that ID; retry
it unchanged. `StatusError` preserves server status/follow headers and maps 404,
412, 507 and 501 to ErrNotFound, ErrPrecondition, ErrFull and ErrNotImplemented.
No blind HTTP redirect forwards credentials to another host. Segmented routing,
learned ownership/RAM runtime IDs and replica routing are explicit multinode TODOs.

The server dispatches object operations to its RAM-resident `Bucket` instances,
with current account/bucket authorization and persistent descriptions in the
admin bucket. Get/GetRange, Put/PutRange, Append, Trunc, Delete, Stat, CAS and
streaming work end to end on single-node volumes, including the FUSE core.
The client learns the single-node RAM runtime from successful object replies and
carries it across reconnects. HTTP/protocol 410 maps to `ErrGone`; an application
must explicitly accept a reset, e.g. by opening a new connection, rather than
silently replaying pre-reset mutations into empty RAM.

Tests cover real client/server streaming (including transfers above the receive
queue size), conditional writes, durable retries after drain/restart, bucket
isolation, quota limits, grant revocation, codec/tier changes and RAM resets.
Protocol-peer tests additionally cover request multiplexing, cancellation and a
committed mutation whose connection fails before the reply. Multinode routing,
multiple-node replicas, guaranteed reservations and finite physical quotas remain
explicit unsupported features; the server never acknowledges those guarantees.

## Native readonly and overlay mounts

`stqr-fuse` now has three modes. `--readonly-bucket ID` alone mounts the base
read-only, `--bucket ID` alone mounts an ordinary writable filesystem, and both
options mount a native overlay. An overlay uses two ordinary authenticated SDK
connections; it does not invoke Linux OverlayFS or fuse-overlayfs.

```sh
# Shared base, with no writable bucket:
./stqr-fuse --uri http://shared:7419 --username reader --password PASSWORD \
    --readonly-bucket BASE_ID /srv/code-base

# Node-local upper combined with a remote shared base:
./stqr-fuse --uri http://localhost:7419 --username local-user --password LOCAL_PASSWORD \
    --bucket LOCAL_UPPER_ID --readonly-bucket BASE_ID \
    --readonly-uri http://shared:7419 \
    --readonly-username reader --readonly-password READ_PASSWORD \
    --whitelist=conf.json,fop-files/ --init /srv/php-code
```

The readonly URI/credentials default to `--uri`, `--username` and `--password`.
Every connection constructor still requires all four URL/user/password/bucket
arguments. The base can have only a READ grant. Readonly mode sets the kernel
`ro` mount option and rejects mutations in the client library before issuing an
RPC, including when reading an interrupted namespace intent. It does not use
persistent synchronization writes for reads. `--init` initializes only a missing
writable root and is rejected with a readonly-only mount.

Use one upper bucket per PHP node. Uppers are permanently identified as overlays
and bound to the readonly bucket ID. They cannot be mounted as a standalone
ordinary RW filesystem or as a readonly base. Existing ordinary filesystem roots
are not automatically converted into overlays. Mount the service under the PHP
worker UID or use `--allow-other` when host FUSE policy permits it; stored Unix
ownership/mode checks remain enabled. `allow_other` does not grant Unix write
permission on its own.

The whitelist is relative to the mount root. A file rule such as `conf.json`
permits changes only to that file. A directory rule such as `fop-files/` permits
that directory and its complete subtree. Existing lower directories also accept
`fop-files` without the slash; new directories must be explicitly declared with a
trailing slash. No globs, absolute paths, `.`/`..` components, empty rules or
surrounding whitespace are accepted. There are at most 4096 rules. Reads remain
unrestricted. Both ends of rename/exchange must be writable, and the actual
Create/Write/Truncate/Setattr/Unlink/Rmdir operations enforce the policy. Attempts
to alter protected PHP code return `EROFS`. Whitelisted temporary files must be
inside an allowed subtree; a file outside the policy cannot be used as an atomic
save temporary. For example, stage a new configuration inside `fop-files/` and
rename it onto `conf.json`.

The canonical policy is persisted with the upper. Omitting `--whitelist` on a
later mount adopts it; explicitly supplying a different policy is rejected rather
than widening access. Use a separate upper for a different policy. This policy
controls this frontend, not arbitrary direct SDK access by a user with writable
bucket credentials, and is not a process sandbox.

Overlay directory records hold whiteouts directly as zero-OID entries. They are
merged with the lower directory by OID. Recreated directories are opaque and do
not resurrect removed lower children. Cross-directory rename, replacement and
exchange publish their namespace changes through one upper-root CAS. No global
writer mutex is used; file updates have independent inode CAS. The current sparse
namespace manifest is segmented but materialized in memory with a 64-MiB limit;
namespace updates rewrite that manifest. This favors small per-node configuration
and upload overlays over huge mutable namespaces.

Metadata-only copy-up stores an inode containing the overridden attributes and a
versioned reference to the original lower inode; it does not copy file payload.
Partial writes store only new data plus extent references to untouched lower
ranges. Offsets are 64-bit, including sparse files. The lower inode version is
checked before reading an inherited range. If that inode is changed in place,
those references return `ESTALE`; they never silently read a different version.
Keep referenced base files stable (or use versioned base buckets). Untouched
lower files remain pass-through. A complete overwrite/truncate can remove an
inode's lower-data dependency. Moving to a different lower bucket requires a new
upper; automatic overlay rebasing is not implemented.

Readonly opens allow page-cache reads and mmap, including PHP streams/OPcache.
Writable opens retain direct I/O and CAS-based append. File timestamps support
explicit atime/mtime changes (`touch`), with ctime updated on mutation. Reads do
not persist atime. Kernel-local `flock` works for PHP processes using the same
mount; no distributed or cross-mount POSIX locking is promised. Local inode
identity entries use weak references and cleanup, while live nodes/open handles
retain their identity; the table no longer retains every historically seen OID.

Overlay cleanup uses both bucket options:

```sh
./stqr-fuse --uri http://localhost:7419 --username local-user --password LOCAL_PASSWORD \
    --bucket LOCAL_UPPER_ID --readonly-bucket BASE_ID \
    --readonly-uri http://shared:7419 \
    --readonly-username reader --readonly-password READ_PASSWORD --fsck
```

Close all mounts using that upper first. The upper maintenance session is
exclusive; the lower connection stays read-only. Fsck validates visible merged
references and the referenced lower versions, prunes explicitly removed
upper-directory records, then deletes only unreachable upper objects. It never sweeps the lower
bucket. Stale/missing base references abort cleanup before any object deletion.
The plain `--bucket UPPER --fsck` form rejects an overlay without its lower.
Automatic online reclamation is still deferred to fsck.

Finish lower maintenance before running upper fsck: there must be no concurrent
lower writes during reconciliation. The upper maintenance lock does not freeze
lower writers. Redundant whiteouts are removed when their name is absent in the
current lower directory; opaque directories need no whiteouts at all. A name
that exists in the lower is still hidden, even if it was deleted and recreated
before the check. Removing an obsolete whiteout deliberately means that a later
new lower entry under that name becomes visible. Fsck reports whiteouts removed
and directory records pruned separately from deleted payload objects.

An independent upper file survives deletion of its lower counterpart. Missing or
version-changed content needed by metadata-only copy-up or partial ranges still
aborts cleanup. A live upper directory record becoming unreachable after lower
delete/recreation is also an error: fsck preserves its manifest and local data
instead of mistaking them for garbage. Automatic directory rebinding or content
rebasing is not implemented. In particular, lower fsck does not enumerate offline
upper dependencies: do not collect former lower source objects still required by
an upper. Supporting arbitrary mutable-base maintenance needs dependency-aware
collection/reconciliation beyond redundant-whiteout cleanup.

`STQR_TEST_FUSE=1 go test -race ./... -count=1` includes a PHP-cluster integration
with two temporary servers, two readonly mounts, two isolated uppers, OPcache,
mmap, LOCK_EX, touch, whiteouts, remount and overlay fsck. With a freshly built
binary, `STQR_FUSE_BINARY=/absolute/path/stqr-fuse` runs those mounts through the
actual CLI and its separate readonly endpoint/credentials. All fixtures use
temporary volumes/buckets; they do not use the developer's `fuse` bucket.

## Subscription-backed metadata caching

`stqr-fuse` uses a bounded object-subscription cache (default 32 MiB per bucket).
Use `--metadata-cache-bytes=0` to disable it. Changes, permission loss and owner
handoffs invalidate dependent snapshots; caching does not depend on a TTL.
Readonly closes avoid redundant Stat calls and FUSE requests can reach 1 MiB.
See [object subscriptions](object-subscriptions.md) for the wire contract,
resnapshot behavior, memory limits and Go GC/AddCleanup lifetime rules.

## Linux application inode features

Normal filesystem views expose `Symlink`/`Readlink`, `Link`, `Mknod` for regular
files/FIFOs/Unix sockets, and `GetXattr`, `SetXattr`, `ListXattr`, `RemoveXattr`.
`FSInfo.Kind` distinguishes node types, and `FSInfo.Nlink` reports link counts.
The FUSE adapter implements the corresponding Linux operations. These changes
use STFS v5 and require upgrading every client accessing a bucket; see the
[shared format and semantics guide](filesystem-posix.md) before deploying them.
