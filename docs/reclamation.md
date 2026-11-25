# Object lifetime and allocation reuse

This is the lifetime and regression contract for issue #32. PUT and CAS overwrite
an object; GET reads its current contents. The engine has no transaction API or
object-version history. Existing filesystem recovery intents coordinate
single-object operations in clients; ordinary reads need no permanent receipt.

## Implemented client inventory

| Client | Mutation and reclamation |
|---|---|
| Go object SDK | PUT, PUT_RANGE, range/whole-object CAS, APPEND, TRUNC and DELETE use supplied object IDs. Eligible replacement payload allocations are reused. |
| Go filesystem/FUSE | Mutable STFS inode/index/data objects retain their IDs. Retain/Release protect open files and directories across cooperating mounts; the orphan queue allows collection after last close. Exclusive fsck handles abandoned references and unreachable objects. |
| Native C library | Native object operations retain the supplied OID; reference pin/claim APIs interoperate with Go. C is an object library, without its own filesystem/fsck. |
| Linux kernel filesystem | Same STFS/STIX, orphan queue and lower-anchor ledger as Go. File/directory open and mmap file lifetime hold durable pins, including readonly handles. A bounded worker retires eligible unlinked upper objects after last close. Go provides offline fsck. |
| Linux kernel block devices | Deterministic stripe IDs; overwrite existing stripes, remove full discarded stripes and zero partial discards. No guest-filesystem collector or guest-snapshot implementation. |
| QEMU/NBD and Linux NBD helper | Deterministic stripe IDs and fixed descriptor ID. TRIM releases eligible stripes. Explicit offline Catalog.Collect preserves supplied opaque roots and rejects collection while local readonly or writable exports exist. Native exclusive maintenance is still required across processes. The helper forwards NBD requests. |
| JavaScript native test fixture | Test-only STAT, GET, SCAN and ranged CAS on stable object IDs; no dashboard object/filesystem management or collector. |
| Windows/WinFsp | Shared Go filesystem and Retain/Release. Close waits for in-flight handle operations. Unsupported overlays, alternate streams, security descriptors and hardlink creation remain documented in the Windows README. |

## Cooperative open and orphan lifetime

Reference-aware clients first open the bucket with its explicit opaque format
(`fs` for the supplied filesystem clients, `bd` for block clients). Reference
coordination negotiates capability bit 18 and uses opcode 0x0121, independently
of format binding at opcode 0x0120 / capability bit 17. The
bucket-local coordination root stores caller-owned tokens, bound to their account
and target object. A pin survives reconnect, node restart and ownership transfer;
it is not released by a socket timeout. An exclusive retirement claim succeeds
only when no pin/other claim exists and prevents a new pin until release. Claims
coordinate clients; they grant no permission to change or delete user data.
READ permits coordination; ordinary data writes/deletes retain their normal ACL.
The root is bounded to 8192 entries per bucket and 1024 per account, including
claims and persistent dependencies. Exhaustion is rejected, never silently evicted.

Clients retain open files/directories before publishing handles. Unlink places a
zero-link upper inode in the shared STOL orphan queue. Closing the final holder
allows an eligible writer to claim and collect it. A readonly holder may unpin,
but cannot delete data; cleanup waits for a writer with the required permissions.
The Go native worker and kernel worker perform bounded cleanup. Filesystem
operation helpers also pin temporary descriptors until their operations drain;
a finalizer obtains exclusive ownership before deleting completed recovery state.
No ordinary PUT/CAS response history is needed for those completed descriptors.

Uncooperative/crashed holders require offline fsck. Close all mounts, helpers and
old request holders, obtain a dedicated exclusive native maintenance connection,
and run CheckFilesystem/CheckOverlayFilesystem. Maintenance resets abandoned open
pins/claims, recovers reachable intents, validates/marks the graph, and sweeps only
after validation succeeds. Losing the maintenance connection aborts the run;
restart collection from the beginning. Do not retry operations from before a
completed offline reclamation boundary. Filesystem repair CAS still requires
WRITE, and collection requires READ/DELETE. A client without cleanup permissions
keeps recoverable state instead of claiming to have reclaimed it.

Old servers/backends without reference support retain conservative lifetimes;
they cannot provide cooperative online reclamation. New servers reject legacy
collectors when persistent dependency pins require a reference-aware collector.
Upgrade all mounts/helpers sharing a filesystem together before using online
reclamation: an old or uncooperative client that publishes no open pin cannot
participate in its lifetime guarantee. Close such clients before offline fsck.

## Overlays, COW and snapshots

Lower overlay objects are not disposable upper blocks. Clients reserve a canonical
STLA ledger entry before pinning an external lower inode. Offline lower fsck
preserves these dependency roots and their reachable children, even at zero links;
resetting crashed open holders deliberately preserves dependency pins.
Older overlay metadata without anchors is not automatically migrated by a
background graph sweep. Its external lower roots must remain protected during
upgrade/offline maintenance; absence of a new pin does not establish that old
upper metadata has no dependency.

Bucket clones can copy an upper ledger, so ending one upper reference cannot
alone authorize releasing a shared lower pin. Clone receipts on both endpoints
preserve the family relationship through detach. Under exclusive maintenance,
CanReclaimDependencies permits release only for the sole surviving family member;
live or detached surviving copies keep it conservative. Offline upper fsck then
releases anchors no longer needed by its reachable graph or fully materialized
upper data and compacts its ledger, using lower credentials. Deleting all other
clones must complete before that proof can succeed. Preserve/supply existing
backup and overlay roots through the appropriate collector API.

Online collection accepts only owner-derived upper-page identities. Legacy/shared
or unproven block identities are retained for offline graph analysis. The server
stores coordination bytes but does not interpret filesystem graphs.
Symlink/FIFO/socket inode heads are also retained for offline collection because
kernel special-file handles bypass the ordinary open callback. Linux O_PATH
metadata/reopen semantics after unlink are outside the online lifetime guarantee;
normal data descriptors, directories and mmap references are covered.

## Retry state and physical cleanup

For negotiated reference-aware clients, local ordinary non-APPEND writes do not
retain permanent result receipts. Replicated writes retain recovery records until
all copies publish, then retire settled non-APPEND records. A mixed legacy replica
set conservatively retains the old result contract. Legacy clients and the S3
frontend retain their existing behavior.

APPEND retains request-ID deduplication and its original returned offset, including
when the reply was lost. Reuse the same ID when retrying that operation. These
records last until explicit exclusive offline metadata reclamation ends all old
operation lifetimes; there is no timer that silently removes them.

Eligible local DELETE markers are retired after publication when no clone, lost
range or overlapping handoff needs them. Offline ReclaimBucketMetadata first
removes fenced inactive copies on every participant, then retires eligible
canonical tombstones and old retry records. Clone deletion masks and lost-range
markers remain until their owning protection ends. Fsck invokes this step only
after successful recovery and graph sweep. Existing WAL, per-object atomicity and
replication publication still apply; no multi-object engine transaction is added.

## Physical allocation and evidence

Volume tests inspect actual dataSpan.Location and capacity after every equal-size
PUT, ranged CAS and whole-object CAS. The near-capacity fixture keeps a 900 KiB
filler on a 1 MiB volume, with less than 128 KiB free, updates an 8192-byte target
100 times and reopens every 25 operations. Fault injection verifies old-or-new
recovery, acknowledged data, retries and stable addresses. Existing coverage also
checks growing/shorter RAW writes, compression, COW and replica publication.

A changed codec, encoded-size growth, shared allocation, relocation or compaction
can require new ranges. WAL/staging/metadata still require working space; an
otherwise fitting large overwrite may fail without it. Reuse is not a promise
that physical addresses are externally pinned or recovery needs no extra space.

Native filesystem tests run 100 overwrite/collection cycles and independently
bound live objects, payload and metadata bytes, then verify deletion. C and JS
native-server loops verify constant payload and current contents read by Go.
The counters describe allocated payload/metadata, not shrinking the fixed-size
host volume file. Persistent live dependencies and APPEND results before the
explicit offline boundary consume real space; no unconditional bound is claimed
for an unlimited number of live holders, clones or unretired APPEND operations.

## Checks and runtime limits

- go test ./volume -count=1 exercises allocator, WAL/recovery and replica storage.
- go test ./client -count=1 and targeted race tests cover shared lifetimes,
  helpers, crash cutpoints, lost replies, dependency ledgers and FUSE callbacks.
- STQR_TEST_C=1 go test ./server -run '^TestCClient' -count=1 executes the C library.
- go test ./server -run '^TestTicket32' -count=1 covers native allocation,
  filesystem metadata reclamation, references, COW proof, replication and Node CAS.
- go test -race ./qemu-plugin-nbd/... -count=1 and, in windows,
  go test -race ./... -count=1 exercise portable adapters.
- Actual kernel FS/BD tests require STQR_TEST_KERNEL=1, QEMU and readable matching
  kernel/header paths. Windows callback/setup cross-compilation is build evidence,
  not a live WinFsp test. Real FUSE mount tests require /dev/fuse; on a host without
  it, only bridge/callback coverage is claimed.

Disposable volume images and transfer staging require adequate temporary-storage
quota. EDQUOT is an environment failure, not successful reclamation evidence.
