# Client compatibility with the current master

Audited against master `3f75e48` (after the PG-local rebuild and PXE retry fixes).
The checks below cover the source and newly built artifacts; installed binaries
are not automatically updated when the server or repository changes.

| Client | Relevant implementation | Result |
|---|---|---|
| Go SDK and Linux FUSE | Shared object protocol, filesystem format, subscription cache and owner routing | Compatible; ordinary and native-overlay FUSE mounts pass against disposable server fixtures. |
| Native C SDK | `client/c/src/core.c` | A large-upload routing/retry gap was reproduced and fixed in this audit. |
| Linux `kstqr` | Reuses the C core; native STFS and STOR/STOM implementations | Builds and passes the isolated QEMU integration test against the current server. |
| Windows WinFsp | Uses the shared Go SDK and filesystem implementation | Portable filesystem tests pass with the race detector; native callback/setup tests pass under an isolated Wine prefix and the GUI executable cross-compiles. A real Windows/WinFsp drive session was not run on this Linux host. |
| QEMU/NBD | Uses the shared Go object SDK; device descriptors and 1-MiB stripes remain client formats | Device/exporter tests pass with the race detector. |

## Compatibility findings

PG-local rebuild generations and their new peer operations are node-internal.
Clients continue using the existing v2 object protocol, range-owner epochs and
routing patches. They must not order independent PG progress using a single
cluster generation counter. No object content versions are needed or introduced.

Recent master changes already updated the native C subscription session handling
and the kernel socket adapter. Switching between owners preserves independent
subscriptions; a lost connection invalidates its snapshots. Kernel RPC transfers
are isolated from ordinary process signals. FUSE and Windows consume the Go SDK
through their existing module dependencies, rather than maintaining duplicate
wire implementations.

One remaining C-library defect was confirmed: payloads above 1 MiB bypassed the
common request path. A PUT to the wrong owner failed instead of following its
redirect, and a temporary 503 was returned without the normal bounded retry.
The fixed path shares redirect/retry handling with small operations, retransmits
the complete rejected upload under the same request ID, and preserves
`-EINPROGRESS` for uncertain transport outcomes. Regression tests cover two real
owners, complete streamed retries after repeated 503 replies, and interrupted
uploads. The multi-owner regression also checks streamed ranged writes and
appends, including preserved prefixes and the returned append offset. Default
durable ACK behavior is unchanged.

The first PR CI run also exposed an EINTR-sensitive FIFO test: a raw `open(2)`
could be interrupted while FUSE resolved its path. The test now uses `os.File`,
which retries signal interruptions for open/read/write, like normal Go callers.
The FIFO assertions remain enabled; no driver error or test is suppressed.

## Validation

- Go SDK and NBD package tests with the race detector; the two-node QEMU/NBD
  integration test also verifies ranged I/O, flush, discard, publication and clone/clear.
- Portable Windows filesystem/setup tests with the race detector; Windows
  callback/setup tests executed under isolated Wine, including DPAPI profiles,
  startup registration, session exclusion and shutdown. The GUI executable
  cross-compiles; the optional interactive dialog test remains skipped.
- C integration tests with AddressSanitizer, UBSan and a race-instrumented server,
  including multi-owner subscriptions and streamed upload fault injection.
- Actual FUSE/VFS mounts: namespace operations, POSIX/git operations, rsync
  hardlinks, readonly/private native overlays and external-update invalidation.
- `TestKSTQRVirtualMachine` with Linux 7.0.0-38: builds the module and runs it only
  in a disposable QEMU guest. Its checks include Go interoperability, native
  overlays, subscription coherence, mmap/SQLite, writeback, process signals,
  reconnect, namespace operations and clean module unload.
- The pre-change master format checkpoint is read successfully twice in fresh
  processes. The C change does not alter stored bytes or the wire layout.

## Performance opportunities, separate from compatibility

FUSE still uses zero metadata TTLs and direct I/O on writable opens. Subscriptions
already support reuse of readonly file pages; longer metadata caching or buffered
writes need dedicated coherence/writeback work, rather than a blanket mount flag.
The kernel client already uses page-cache folios, readahead and writeback.

Windows has subscription-backed metadata but conservative metadata timeouts. Its
notification worker refreshes snapshots of visited directories after an update.
Targeting only affected directories/inodes and adding a byte budget to those
snapshots would reduce work for large namespaces. These are optimization
opportunities, not evidence that the current server wire format is incompatible.

For this C-core fix, rebuild native C consumers and the kernel module. Existing
installed copies retain the old code until replaced/reloaded; the kernel test
above does not replace the host's loaded module or any user's mount.
