# Standalone Linux kernel client: kstqr

`kstqr.ko` is a network filesystem client for STQR. It implements object RPCs,
WebSocket framing, authentication, filesystem metadata and file I/O in the kernel.
It uses the same protocol core as the [C library](../client/c/README.md), compiled
against kernel sockets, the kernel crypto API and kernel allocators. Neither Go,
OpenSSL, FUSE nor a resident userspace client is required on the mounting host.
The STQR server remains a separate process and may run on that same host.

`mount.kstqr` is a **one-shot mount helper**. It resolves the server name, opens a
credential file and passes its descriptor through the Linux filesystem mount API.
The helper exits once the mount is attached. Reconnects are handled by the module,
using credentials retained in private mount state. There are no per-read credential
file accesses or persistent read-coordination writes.

## Build and mount

The current adapter targets Linux 7.0 and has been compiled and VM-tested against
Ubuntu `7.0.0-38-generic`. Internal VFS interfaces change between kernel versions;
other versions need compatibility work and their own tests.

```sh
make -C kernel/kstqr
make -C kernel/kstqr helper
```

The module build requires the matching kernel headers and toolchain. Set `KDIR`
when building for another installed kernel. Module loading and mounting require
administrative privileges. On a host enforcing module signatures, sign the module
according to that host's policy.

For a test mount, install/load the module and create an existing STQR bucket with
a suitable grant. Use a dedicated filesystem bucket:

```sh
sudo insmod kernel/kstqr/kstqr.ko
sudo install -d -m 0755 /etc/stqr /mnt/stqr-test
sudo install -m 0755 kernel/kstqr/mount.kstqr /sbin/mount.kstqr
sudoedit /etc/stqr/test.credentials
sudo chmod 0600 /etc/stqr/test.credentials
```

The root-owned credentials file contains exactly two LF-terminated entries:

```ini
username=alice
password=YOUR_PASSWORD
```

Then mount, replacing `BUCKET_ID` with the 32-character hexadecimal bucket ID:

```sh
sudo mount.kstqr --uri http://127.0.0.1:7419 \
  --bucket BUCKET_ID --credentials /etc/stqr/test.credentials \
  --init /mnt/stqr-test
```

`--init` creates a missing root, owned by the mounting UID/GID with mode 0755.
It preserves an existing filesystem. Omit it for subsequent mounts. Set directory
ownership/permissions for the intended local users after initializing the root.
For a readonly mount, omit `--init` and use `--readonly`.

The usual mount-helper syntax also works once the helper is installed:

```sh
sudo mount -t kstqr http://127.0.0.1:7419 /mnt/stqr-test \
  -o bucket=BUCKET_ID,credentials=/etc/stqr/test.credentials
sudo umount /mnt/stqr-test
sudo rmmod kstqr
```

`mount.kstqr --help` lists the direct helper options. `--timeout-ms` / the
`timeout_ms=` mount option defaults to 30000. Standard helper options include
`ro`, `rw`, `nosuid`, `nodev`, `noexec`, `noatime`, `_netdev` and `defaults`.
The socket timeout is not a total syscall deadline.

## Credentials and access

The helper opens the credential file with `O_NOFOLLOW`. Both helper and module
require a regular file owned by the mounting identity with no group/other access.
The kernel reads it through the supplied descriptor; passwords are absent from
mount arguments and `/proc/mounts`. Temporary plaintext copies and retained
credentials are cleared before freeing. This implementation uses a private
credential copy, not Linux keyring integration.

Every mount has its own URL, bucket and authenticated connection. The server
validates the account and grant during upgrade and retains the connection's
bucket capability. A readonly mount does not perform namespace recovery writes
and works with a read-only server grant. Local processes share that STQR account
and are subject to the filesystem's Unix permissions; there is no automatic
per-local-user STQR login.

The kernel socket adapter supports numeric IPv4/IPv6 endpoints in the host network
namespace. The helper resolves hostnames at mount time; reconnect uses that same
resolved address. Socket allocation and connect run in a kernel worker, so a
service's cgroup IP filter cannot capture a shared storage connection during a
redirect/reconnect. The service's own sockets remain filtered. Object operations
follow validated range-owner redirects;
cluster nodes must advertise numeric addresses usable by the kernel. Before a
serial RPC connection changes owner, it synchronizes accepted writes to its old
node. Each RPC slot and the subscription client retains endpoint sessions, with
a durability barrier covering every retained session. Subscription polling reads
all owners; visiting a different owner does not invalidate existing inode proofs. Other network namespaces and automatic address failover are not
implemented. Transport is plain HTTP/WebSocket, matching the current
server; use it on loopback or an appropriately protected network.

## Filesystem behavior

The module reads and writes the normal Go/FUSE filesystem format: `STFS` inodes,
`STDR` directories, `STIX` segment indexes and `STFT`/`STFL` namespace
intents. No import/conversion is needed between upgraded normal FUSE and kernel mounts.
STFS v7 includes node types, link counts, xattrs and read-only lower references:
**upgrade all clients before writing v7 metadata**. See the [format and semantics guide](filesystem-posix.md).

Implemented operations include regular files, directories, symlinks, hardlinks,
FIFO/Unix-socket nodes, user/trusted xattrs, lookup/readdir,
read/write, atomic per-chunk append, truncate, sparse spans, chmod/chown,
explicit timestamp changes, fsync, unlink, rmdir and rename with replacement,
`NOREPLACE` or `EXCHANGE`. Cross-directory rename/rmdir use durable multi-inode
CAS intents. Destination ancestry is validated and enrolled when moving directories
so competing clients cannot silently introduce a directory cycle. Pending intents
can be helped after interruption without recursive kernel-stack traversal.

Ordinary data and index objects retain their IDs and are edited in place under
recoverable inode intents. A byte-compared inode CAS publishes the decided
metadata image. Partial writes preserve unaffected data; sparse extensions store
zero spans. Read-only lower references remain protected by the overlay protocol. Open-unlinked files remain usable until close.
Successful `write`/`pwrite` chunks are atomically accepted by the server before
returning; on capable nodes their persistence can be deferred until a barrier.
`fsync`, `fdatasync`, `msync(MS_SYNC)`, synchronous writes and waiting `syncfs`
confirm the observed bucket prefix across the mount's connections. Shared writable mappings use the kernel page cache: `msync(MS_SYNC)`
and `fsync` flush dirty pages through the same durable object CAS path. Background
writeback and unmount also flush these pages; `munmap` alone is not a durability
barrier. Failed writeback reports an error and retains dirty pages for retry.
A transport interruption during CAS is retried once with the same request ID
after reconnect, preserving server
retry deduplication. An unresolved outcome is returned as an I/O error.

Each mount uses four authenticated RPC connections, selected by object ID and
serialized independently. Operations on independent objects can proceed in
parallel; CAS conflicts reload the inode and retry. A separate subscription
connection and kernel thread receive changes even when applications are idle.
No resident userspace process is introduced. Normal writes publish chunks of up
to 1 MiB with synchronous acknowledgements. Competing larger append syscalls may
interleave at chunk boundaries; they do not reserve an entire syscall as one
operation. Mapped writeback collects contiguous dirty folios into batches of
at most 1 MiB. Gaps and the requested writeback range split batches; partial EOF
pages are clipped. Pages remain under writeback until their batch is acknowledged.
A failed batch redirties all of its pages and reports the error, preserving them
for a later retry. Writeback completion alone is an accepted ACK on capable nodes; filesystem sync
also confirms the server checkpoint. Nodes lacking the capability retain the
previous durable-per-mutation path. `MAP_SHARED` supports reads and writes;
`MAP_PRIVATE` uses normal copy-on-write pages without modifying stored content.
Mappings and ordinary I/O share the page cache within a mount. Truncation flushes
dirty data before resizing; access to pages beyond the new EOF raises `SIGBUS`.
`O_DIRECT` is unsupported. Locks are local kernel locks, not distributed locks.

Ordinary reads validate their inode through a live object subscription. Inode
snapshots are retained in a bounded LRU: 8 MiB for snapshots and their entry descriptors, with at most 2048
entries per mount (the current server subscription ceiling). Configure it with
`--metadata-cache-bytes N` or `-o metadata_cache_bytes=N`; zero disables it and
the maximum is 64 MiB. Warm positive and negative dentries reuse their lookup
only while the parent subscription ID and stream cursor match; positive dentries also require
a current subscribed child. There is no metadata TTL. Pending namespace intents
are resolved through the existing recovery path and cannot prove cached lookups.
Overlay name bindings and virtual directory attributes additionally track both
the upper root and the lower directory subscription identities. A changed cursor,
eviction or disconnect forces a fresh lookup; unchanged directories are not
repeatedly merged and sorted during stat calls. These proofs live only in RAM
and do not change the object format.

The protocol validates update hashes. Cursor gaps, multipart or unsupported
changes and socket loss invalidate cached snapshots. Eviction unsubscribes; unmount
stops the reader, drains invalidation work and closes all connections. The reader
reconnects after loss, but old snapshots remain invalid and resubscribe on demand.
Reconnection also invalidates pages refaulted through the uncached fallback
while subscription coverage was unavailable.
An initial notification racing snapshot receipt conservatively falls back to an
uncached read. Unsupported servers or snapshots which do not fit the budget also
fall back to native reads.

Notification handling queues targeted inode invalidation on a separate worker;
it never waits for page-cache locks while receiving a protocol message. Clean
mmap pages are unmapped and invalidated after updates. Dirty local mapped pages
are preserved for writeback. Notifications are asynchronous: a writer ACK on one
connection does not imply another mount has already processed its update.
Mappings without subscription coverage (cache disabled, unsupported server or
an uncacheable inode) retain the previous limitation: a long-lived mapping may
remain stale until another operation revalidates it. This is not distributed
shared memory; coordinate writable mappings across mounts externally.

Read-ahead fills contiguous windows up to 1 MiB from one inode snapshot instead
of issuing a metadata/index read for every 4-KiB page. Already cached file pages
are reused. Normal reads do not persist atime. `/proc/self/mountstats` includes
per-mount cache hit/miss, native GET/range/CAS and read-ahead-window counters.
The native counters count logical calls before reconnect retries; subscription
traffic is represented by cache misses rather than added to the native counters.
Writeback batch/page/byte/error counters and cumulative nanosecond timings split
native writes into inode snapshot, data/index preparation (including their RPCs)
and inode publication. These are elapsed phase times, not CPU samples; concurrent
writers' times can overlap. Ranged reads reconnect once on transport loss, just
as full reads do. The C core submits each complete masked WebSocket frame in one
send operation to avoid delaying a small payload behind its separate header.

## Current boundaries

- **This is not yet a complete developer-home filesystem.** POSIX ACLs and broader
  application compatibility testing remain. User/trusted xattrs
  and security/capability xattrs work; device nodes are not implemented. Keep a
  real home on its existing filesystem while evaluating this client.
- Native overlays use the same STOR/STOM format as Go/FUSE. Hardlink creation
  through an overlay is unsupported; untouched hardlinks in the lower remain
  readable. Copy-up or rename of a multiply linked lower file is rejected.
- Namespace editing materializes directory contents, limited to 16 MiB per
  directory; an intent is limited to 1 MiB and 1024 participants. Very large
  edits can fail with `E2BIG`. Index flattening needs further performance work.
- `statfs`/`df` report live bucket usage and available eligible cluster storage,
  capped by a finite bucket quota. Without a quota the remaining cluster pool
  is available. Measurements are advisory and do not reserve space; objects
  awaiting offline fsck still count as used. See [capacity reporting](space.md).
- Online object reclamation is pending. Unmount **all** clients of the bucket and
  use `stqr-fuse ... --fsck` for cleanup. The server's bucket-bound connection
  tracking prevents fsck maintenance while kernel mounts remain connected.
- There is no remote replication, automatic failover or TLS provided by this
  adapter. It inherits the implemented server's durability and availability.

## Serving and mounting on the same host

The intended boot sequence is:

```text
Local base filesystem / storage devices
└── STQR server service
    └── kstqr mount
        └── Applications / user sessions
```

For an eventual home mount, keep the daemon binary, configuration, credential
file, logs, temporary staging files and backing volumes outside that home.
Typical locations are `/usr/local/bin`, `/etc/stqr`, `/var/lib/stqr` and separate
local storage devices. The daemon must start without touching the mount it serves.
A volume file must never be placed inside the bucket-backed mount.

Use explicit systemd ordering between the server service and mount, and reverse
that order at shutdown. Credentials must be available before user login. Test
reconnect, resource exhaustion and the applications actually used in the home
before migrating data. Migrating a full filesystem also needs additional physical
capacity; the mount does not create storage space.

## Isolated kernel tests

The opt-in integration test builds a small initramfs and starts QEMU. It never
loads the module into the host kernel, changes `/home`, or uses existing buckets.
It requires a matching readable kernel image, headers, QEMU, a static BusyBox,
cpio and a C toolchain with static libc and SQLite development libraries.

```sh
STQR_TEST_KERNEL=1 \
STQR_KERNEL_IMAGE=/path/to/vmlinuz-7.0.0-38-generic \
STQR_KERNEL_LOG=/tmp/kstqr-vm.log \
go test -race ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v
```

KVM is used by default; set `STQR_KERNEL_TCG=1` for software emulation. Set
`STQR_KERNEL_HEADERS` to override the kernel build directory. The guest kernel
needs builtin virtio PCI/network, IPv4, devtmpfs, SHA-1 and SHA-256 support.

Tests include an existing Go filesystem, a kernel-initialized filesystem,
readonly-only server grants, interrupted intent recovery, concurrent append
through independent mounts, readonly/private/shared writable mmap, sparse
1-TiB logical files, permissions, symlink/hardlink/xattr interoperability, local sockets/FIFOs,
open-unlink, rename flags, rmdir, remount and clean module unload.
Mmap tests cover forked shared updates, SQLite WAL with concurrent processes,
I/O buffers backed by mappings, truncation/EOF, writeback failure and recovery,
explicit sync and unmount persistence. Writeback cases additionally exercise
noncontiguous pages, partial msync ranges, a 1 MiB boundary, partial EOF and
multi-batch failure/retry; the Go client checks all recovered bytes after VM exit. Subscription tests also cover external
Go-client updates to existing mmap pages, permissions, negative dentries,
forced socket loss/reconnect, explicit resync and unlink; a 2-KiB cache exercises eviction and a disabled-cache mount checks fallback.

An isolated VM microbenchmark is available with `STQR_KERNEL_BENCHMARK=1` using
the same test command. It seeds disposable data and prints JSON timing lines for
client-cold/warm sequential reads, existing/missing stat, and sequential/parallel
small-file reads. `STQR_KERNEL_MODULE=/absolute/path/kstqr.ko` selects a separately
built baseline module; its wire format must be compatible with the seeded data.
See [the measurements](../benchmarks/2026-11-23-kernel-throughput/README.md) for
method, raw samples and the distinction between VM/RAM and SSD throughput.
After the VM exits, the Go client reads the kernel-written data and its fsck
validates and cleans the published object graph.

`STQR_KERNEL_WRITE_BENCHMARK=1` runs the isolated VM write benchmark instead of
the read benchmark: ordinary durable writes, dense/sparse mmap+msync and repeated
4 KiB commits. It also records per-case CAS counts and phase timings. See
[the writeback measurements](../benchmarks/2026-11-23-writeback/README.md).

The [explicit durability contract](durability.md) describes accepted writes,
checkpoint bounds, cross-volume ordering and unconfirmed marks after server loss.
After a rollback, inode sizes can decrease to the recovered checkpoint;
reads refresh that state, while fsync of the lost incarnation reports ESTALE.

## Native networkboot root

The kernel reads and writes the same STOR/STOM overlay format as Go/FUSE. A
single mount merges an STFS lower bucket with an STOR upper bucket:

```sh
mount.kstqr --credentials /run/stqr/credentials --bucket UPPER_ID \
  --readonly-bucket LOWER_ID --init /mnt/root
```

`--init` initializes a fresh upper; existing uppers retain their stored lower ID
and whitelist. An upper cannot be mounted alone. `--whitelist conf.json,work/`
restricts changes, including metadata and writable mmap. On remount an explicitly
supplied whitelist must match the stored policy. With only `--readonly-bucket`,
the helper mounts the lower read-only without an overlay.

Directory records contain deletion markers and metadata overrides. File copy-up
retains references to unchanged read-only lower content, so a ranged change does
not copy the entire file. A writable origin needs a separate COW bucket as its
lower. Data fingerprints reject violations of that contract with `ESTALE`.
Ordinary own data/index blocks retain their IDs across writes. Missing lower
data is an error; lower garbage collection must respect dependent upper buckets.
Each bucket has its own RPC connections and subscription cache. Changes from
other clients invalidate the merged namespace and cached file data.

The networkboot adapter builds the module for the installed guest kernel. Its
one-shot helper mounts the immutable release and temporary upper directly as
one native kstqr root, without a resident userspace filesystem client.

The internal `--boot-lifetime-fd FD` mount-helper option passes a connected
socket into the upper superblock. The kernel holds it until unmount, after data
connections have closed, so the gateway can revoke and collect the temporary
identity/bucket without a resident userspace root client. It is not a data proxy.
See [networkboot](../networkboot/README.md) for provisioning and recovery.

The current native wire schema is `stqr.v2`; object contents have no version
counter. STFS v7 stores representation flags, namespace integrity digests and
optional read-only lower anchors. STIX v3 uses explicit lower-data flags.
STFT v3 compares exact before/after metadata and records idempotent in-place edits.
Both normal inode and STOR namespace readers resolve interrupted intents;
read-only mounts apply committed edits virtually. Ordinary older inode layouts
remain readable. Old lower-inode references require verified conversion or a
recreated upper before kernel use. See [the shared contract](filesystem-posix.md).

Kernel transport transfers run in NOFS workers, independent of the task issuing filesystem I/O. Nonfatal process signals (including SIGCHLD and restartable timers) do not break partial shared RPC frames or invalidate other tasks' sessions. Socket timeouts still bound stalled transfers; fatal cancellation is observed between dispatches, and dispatched buffers remain owned until the worker has returned. The VM regression suite exercises repeated reads, writes and fsync under a 500-microsecond restartable timer.

Overlay invalidation preserves file pages when their current inode still has the same continuously covered subscription snapshot. When namespace changes invalidate the cached overlay lookup, the worker may resolve a regular file that has cached pages before taking the mapping lock. It runs outside notification callbacks and subscription wire locks. Missing or changed coverage takes the conservative invalidation path. The worker never publishes asynchronously copied inode metadata: foreground access refreshes pending state before mmap checks EOF, and fault-around defers while invalidation is pending. A two-mount VM regression uses a 2-KiB cache with 48 files, checks that idle cache/RPC counters stop growing, then truncates an evicted mapped inode through the second mount and verifies SIGBUS.

### Read-ahead and storage fences

Readahead queues bounded windows up to 1 MiB on a per-mount background workqueue.
Transport and fence jobs use separate unbound `WQ_MEM_RECLAIM` queues: read/writeback workers may synchronously wait on them under reclaim. Fence jobs depend on the transport queue, so they must not share its single reclaim rescuer. Module initialization creates both queues before registering the filesystem; teardown drains fences before transport.
Only running workers allocate payload buffers; queued work retains locked folios.
Unmount drains this queue before closing caches or RPC sessions. `mountstats`
reports `read_windows`, successfully read `read_bytes`, `read_ns` and
`read_queue_ns`; speculative errors fall back to ordinary page reads.

Fsync captures preceding accepted-write marks under each RPC slot's framing lock
and the subscription reader's wire lock. It merges marks for the same owner and
bucket, then waits on independent, cached control sessions with those data locks
released. Different owners confirm in parallel. Applying the results confirms
only the returned durable prefixes; later writes and other buckets remain
unconfirmed. The mount checkpoint mutex serializes competing fsync calls, not
ordinary object requests. Whole WebSocket frames and TCP_NODELAY keep small
notification ACKs from delaying the next request.

Volume checkpoints pin the existing accepted-write buffer while physical WAL
writes and flushes run outside the metadata RWMutex. Readers continue to use
those same published bytes. In-place writers wait until that WAL space is safe
to reuse. This does not create alternate object IDs, object revisions, MVCC or
user transactions; the on-disk encoding and required persistence ordering are
unchanged. The server waits for volume-switch and buffer-pressure fences outside
node admission and reruns authorization, placement and quota checks before using
the untouched incoming payload.

Kernel VM regressions set `panic_on_warn=1` and reject every `WARNING:`, including Linux 7 diagnostics of the form `WARNING: kernel/workqueue.c`. A workqueue warning is a failed test even if the VM later reaches its success marker.


The cache-retention VM regression reads a 64-KiB inherited file through mmap,
queries 2,112 unrelated lower inodes and creates unrelated upper files. It uses
`mincore` to reject page eviction without memory pressure, then changes that
same file through a second mount and requires the mapping to observe the update.
Run it with `STQR_TEST_KERNEL=1 STQR_KERNEL_CACHE_RETENTION=1` and the target
kernel image/header settings used by the ordinary VM tests. Inherited data reads
switch to the lower bucket before both payload access and integrity-guard checks;
a lower guard must never be looked up in the upper bucket.

## Native block-device buckets (`-o bd`)

The same module can expose existing named STQR block devices as **real Linux
block devices**. It reads the existing `STNBD001` descriptors and systematically
addressed 1-MiB stripes used by [QEMU/NBD](../qemu-plugin-nbd/README.md).
No format conversion, segment list, server-side filesystem/block implementation,
NBD exporter or resident userspace helper is involved.

Create the bucket and grants using the dashboard, and named devices using `stqr-nbd`
first. `bd` exports existing devices; it does not initialize or format them.
A bucket containing an STFS root is rejected. Do not combine `bd` with `init`,
native filesystem overlay options, `whitelist` or `nodev`.

```sh
sudo mkdir -p /qstr/bucket1 /srv/data
sudo mount -t kstqr http://127.0.0.1:7419 /qstr/bucket1 \
  -o bd,bucket=BUCKET_ID,credentials=/etc/stqr/disks.credentials
```

A bucket containing devices named `system` and `data` then exposes
`/qstr/bucket1/system` and `/qstr/bucket1/data`. The directory and device nodes
initially allow root only. The disk also registers with the Linux block layer,
including sysfs, dynamic `/dev/kstqr*` nodes and partition discovery. Use the
stable exported names in configuration; dynamically assigned kernel disk numbers
are not persistent identities. The inventory is fixed for the mount lifetime;
remount after adding, removing or resizing a device. At most 256 devices are
exported by one mount. Automatic exports require device names that are valid
single filenames; use `device=NAME` to select a name containing a slash or a
reserved directory name and expose it under the safe alias `bd`.

To select one existing device and expose it as a single node named `bd`:

```sh
sudo mount -t kstqr http://127.0.0.1:7419 /qstr/bucket1 \
  -o bd,device=system,bucket=BUCKET_ID,credentials=/etc/stqr/disks.credentials
sudo mount -t ext4 /qstr/bucket1/bd /srv/data
```

The last command assumes that the selected **whole device already contains an
ext4 filesystem**. A partitioned disk needs the appropriate discovered partition
instead. Mounting/exporting does not create an ext4 filesystem. For an explicitly
readonly export add `ro`; the block layer also enforces readonly access.

For a systemd-based `/etc/fstab`, make the dependency explicit rather than relying
on the order of lines:

```fstab
http://127.0.0.1:7419 /qstr/bucket1 kstqr _netdev,bd,device=system,bucket=BUCKET_ID,credentials=/etc/stqr/disks.credentials 0 0
/qstr/bucket1/bd /srv/data ext4 _netdev,x-systemd.requires-mounts-for=/qstr/bucket1 0 0
```

The second mount requires the export mount and starts after it; stopping follows
the reverse ordering. See [systemd.mount](https://www.freedesktop.org/software/systemd/man/latest/systemd.mount.html).
The example disables automatic boot-time fsck: schedule offline filesystem checks
only after exporting the device and before mounting its contents. If the STQR
server runs on this host, add `x-systemd.requires=YOUR-STQR-SERVICE.service` to the
first line, using the actual local service unit name. Load `kstqr` through the
normal module installation/modules-load configuration and keep its binary matched
to the running kernel.

### Sectors, I/O and persistence

- **512-byte logical sectors** preserve compatibility with existing devices,
  partition tables and small writes. **4-KiB physical/minimum-I/O hints** encourage
  normal filesystem alignment. These hints do not expand a 512-byte write.
- **1 MiB is the stripe size and optimal-I/O hint**, not the logical sector size.
  A request is split at stripe boundaries. Partial reads/writes use ranged object
  operations; a complete stripe write uses PUT. No read-modify-write of a full
  stripe is required for a small write.
- Discard alignment follows 1-MiB allocation units, so Linux splits long trim
  requests at stripe boundaries. Small trims remain supported and zero bytes.
- Missing objects and short stripe tails read as zeros. A full-stripe discard
  deletes the object; boundary discards zero only stored bytes. A WRITE grant
  without DELETE permission falls back to an empty payload for a full stripe.
- Writes use the existing accepted-write protocol when supported. PREFLUSH, FLUSH
  and FUA confirm persistence across all owner sessions. Requests are processed
  on one ordered queue per exported bucket, with bounded per-request payload
  buffers and no additional stripe-content cache. Linux and the mounted filesystem
  retain their usual caching.

The mount acquires the same exclusive opaque-object lease as the NBD exporter,
including for readonly exports. A second exporter/client is rejected while it is
held. A routing redirect carries its authorization to the other owner; the
original lease-owning session is retained. A lost lease/uncertain transport error
fails I/O rather than silently reacquiring ownership. Unmount the filesystem,
close device users and remount the export after restoring connectivity.

Unmount the contents before the export:

```sh
sudo umount /srv/data
sudo umount /qstr/bucket1
```

Linux block users do not necessarily retain the pathname mount. If the export
namespace is detached first, an already-open device pins the backend superblock,
connection and lease until its last close. Final disk deletion runs outside the
block open mutex. This keeps mounted filesystems alive and avoids releasing the
lease while they can still write; it does not permit another exporter to open the
same bucket early. Lazy namespace detachment is not an I/O cancellation mechanism.

The isolated `TestKSTQRBlockVirtualMachine` verifies Go/NBD-format interoperability,
512-byte and cross-stripe I/O, sparse tails, full and partial discard, flush,
lease exclusion, readonly selection, actual filesystem mount/remount, namespace
detachment while the filesystem is active, and clean module unload. The test does
not load the module into the host kernel or touch existing buckets.
