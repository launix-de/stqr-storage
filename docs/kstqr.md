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
resolved address. Other network namespaces, address failover and cluster routing
are not implemented. Transport is plain HTTP/WebSocket, matching the current
server; use it on loopback or an appropriately protected network.

## Filesystem behavior

The module reads and writes the normal Go/FUSE filesystem format: `STFS` inodes,
`STDR` directories, `STIX` immutable segment indexes and `STFT`/`STFL` namespace
intents. No import/conversion is needed between upgraded normal FUSE and kernel mounts.
STFS v5 adds node types, link counts and xattrs: **upgrade all clients before
writing v5 metadata**. See the [format and semantics guide](filesystem-posix.md).

Implemented operations include regular files, directories, symlinks, hardlinks,
FIFO/Unix-socket nodes, user/trusted xattrs, lookup/readdir,
read/write, atomic per-chunk append, truncate, sparse spans, chmod/chown,
explicit timestamp changes, fsync, unlink, rmdir and rename with replacement,
`NOREPLACE` or `EXCHANGE`. Cross-directory rename/rmdir use durable multi-inode
CAS intents. Destination ancestry is validated and enrolled when moving directories
so competing clients cannot silently introduce a directory cycle. Pending intents
can be helped after interruption without recursive kernel-stack traversal.

Data and index objects are immutable. A version-checked inode CAS publishes each
new content image. Partial writes preserve unmodified segment references; sparse
extensions store zero spans. Open-unlinked files remain usable until close.
All successful `write`/`pwrite` chunks are acknowledged by the server before
returning. Shared writable mappings use the kernel page cache: `msync(MS_SYNC)`
and `fsync` flush dirty pages through the same durable object CAS path. Background
writeback and unmount also flush these pages; `munmap` alone is not a durability
barrier. Failed writeback reports an error and retains dirty pages for retry.
A transport interruption during CAS is retried once with the same request ID
after reconnect, preserving server
retry deduplication. An unresolved outcome is returned as an I/O error.

The module currently serializes RPCs within each mount. Multiple mounts use
independent connections and CAS conflicts are retried against fresh snapshots.
Large read/write syscalls are processed in 64-KiB chunks. Competing multi-chunk
append syscalls may interleave at those boundaries; they do not reserve an entire
large append as one operation. `MAP_SHARED` supports reads and writes;
`MAP_PRIVATE` uses normal copy-on-write pages without modifying stored content.
Mappings and ordinary I/O share the page cache within a mount. Truncation flushes
dirty data before resizing; access to pages beyond the new EOF raises `SIGBUS`.
`O_DIRECT` is unsupported. Locks are local kernel locks, not distributed locks.

Ordinary reads of clean files fetch a fresh inode snapshot and invalidate cached
pages when its version changes. Reads of locally dirty mappings preserve those
changes. Dentries are revalidated through lookup. Normal reads do not persist
atime. Long-lived mmap pages can remain cached after a change through another
mount; cross-host/page-cache invalidation still needs subscriptions. Applications
sharing writable mappings must use the same mount, or coordinate updates
externally; this does not provide distributed shared memory.

## Current boundaries

- **This is not yet a complete developer-home filesystem.** POSIX ACLs and broader
  application compatibility testing remain. User/trusted xattrs
  work; security/capability xattrs and device nodes are not implemented. Keep a
  real home on its existing filesystem while evaluating this client.
- Native overlay buckets require lower/upper handling and are currently rejected
  by the kernel client. Use `stqr-fuse` for overlays.
- Namespace editing materializes directory contents, limited to 16 MiB per
  directory; an intent is limited to 1 MiB and 1024 participants. Very large
  edits can fail with `E2BIG`. Index flattening needs further performance work.
- `statfs` currently reports the filesystem type/block size/name limit, but zero
  capacity values; use the server dashboard for actual storage accounting.
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
explicit sync and unmount persistence.
After the VM exits, the Go client reads the kernel-written data and its fsck
validates and cleans the published object graph.
