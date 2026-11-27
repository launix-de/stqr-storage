# Volume library and on-volume format 5

The implementation is the Go package `github.com/launix-de/stqr-storage/volume`.
There are no UI handlers, CLI commands, or networking code. Runtime files live in
ignored `volumes/`; that is a different directory from library source `volume/`.
The library requires Go 1.24 or later for `runtime.AddCleanup`.

## Software contract and API

A volume belongs to one node exclusively. File constructors hold an OS flock;
Linux raw block devices also use an exclusive kernel device claim. Mounted
partitions, mounted devices, active swap, and claimed LVM/DM inputs are excluded.
`ListBlockdevices()` lists eligible paths and unclaimed files from `VOLUMES_DIR`
(default `volumes/`). Its exported `BlockDevice` values have JSON tags. Discovery
is advisory; opening rechecks eligibility. A caller must not externally mount or
modify backing bytes while the volume is in use.

`New(filename, sizeBytes, class)` opens an existing formatted file/device or
creates/formats an absent or unformatted target. Size is bytes, never `50M` text.
Creation rounds regular-file sizes to 4 KiB. Existing sizes/classes come from the
header. An empty existing regular file needs a size. Recognizable damaged or
unsupported formats are not silently reformatted. `NewRAM(sizeBytes)` starts an
empty volatile store. Minimum size is 256 KiB. Raw devices retain their full
physical size; logical sector sizes dividing 4096 (including 512/4096) are
supported, larger incompatible sectors are rejected.

`Volume` must not be copied after use. Its RWMutex protects every local operation;
readers run concurrently, writes/compaction/close are exclusive. Callbacks and
writers invoked under a lock must not reenter the same volume. When composing
operations on different volumes, callers must establish a consistent lock order.
The server is responsible for cluster ownership, replication, bucket access and
quotas across volumes. `Bind` persists/checks the node/cluster identity.

Private payload staging and physical durability fences release the metadata
lock. A complete published image is readable before its durable ACK. Later
bounded mutations use a separate overlay while an earlier image is being
checkpointed; an old fence confirms only the image it wrote. Both WAL allocations
and page images remain protected until completion. Closing rejects new
publications, waits for an already requested fence, and discards a later unfenced
overlay. See [write fences](write-fences.md)
for acknowledgement ordering, Linux persistence primitives and resource bounds.

`Options.Accepted=true` never starts a persistence fence, even for oversized
requests, fragmented metadata, resource exhaustion or rejected input. Accepted
builders (including trusted restore/receipt helpers) share bounded writeback
staging. File/block backends may spill to private UNSYNCED files with a disk-backed
page directory; RAM classes backpressure rather than spill. On-volume redo
reservations grow with protected dirty bytes when safe free space exists;
otherwise `ErrWritebackPressure` rejects before publication. An explicit Sync is
the only way to confirm those bytes. Handle/connection/volume Close does not ask
for Sync, and an unrequested accepted tail is discarded even at clean process
restart. See [write fences](write-fences.md) for the exact limits and recovery
contract. Default library durable operations and explicit control/admin history
remain separate requests for durability.

`Volume.Stat` is an independent general metadata query. All other object access
is exclusively through a handle from `Volume.Open(write bool)`; there is no
second, automatically locking object API on `Volume`. Use a read session to
build response headers and stream the matching contents:

```go
h, err := v.Open(false) // true acquires the exclusive write lock
if err != nil {
    return err
}
defer h.Close()
info, err := h.Stat(key)
if err != nil {
    return err
}
_, err = h.CopyTo(key, 0, info.Length, dst)
return err
```

`Handle.CopyTo` may be called repeatedly (also for multiple ranges/objects), and
never closes the handle, even on errors or panics. Only `Handle.Close` ends the
session. A read session allows other readers and prevents writes/GC/volume close;
a write session excludes them all and also exposes `Put`/`PutReader`,
`PutRange`/`PutRangeReader`, `Append`/`AppendReader`, `Trunc`, `Delete` and `CAS`.
Both modes expose `Stat`, `ReadAt`, `WithReadView` and `CopyTo`. Read-handle mutations return
`ErrReadOnly`; operations after close return `ErrClosed`. Separate writes remain
separate durable commits, not a multi-operation transaction.

The lock, backend and lease remain private. Handle methods serialize concurrent
use and close; close waits for an active transfer. Do not copy a handle or call
ordinary volume methods while it is open; use handle methods to avoid recursive
locking. Callbacks must not reenter either the handle or its volume.
Linux regular-file and raw-block backends use read-only `MAP_SHARED` mappings
of their physical address space. No pages are eagerly populated. Object/request
B-tree lookups and walks parse validated mapped page bytes directly instead of
allocating a decoded node for each search. Writer-side nodes and the object cache
remain bounded. Positioned writes, WAL ordering and persistence fences are
unchanged; the on-disk format version is unchanged.

`Handle.WithReadView(key, offset, length, callback)` lends read-only `ReadSegment`
values for the callback's duration. `Data == nil` represents `Length` sparse zero
bytes. RAW bytes borrow mapped or native RAM pages; compressed chunks may use
bounded decoded buffers. `Borrowed` distinguishes those cases. Slices must never
be modified or retained after the callback, and callbacks must not reenter the
handle/volume. A range is clipped at EOF and bounded by `MaxBufferedRead`.
`ReadView.WriteTo` sends its segments without a second payload copy; `CopyTo`
streams larger ranges through bounded views. `Get`/`GetRange` retain owned-copy
semantics.

The private handle lock and `KeepAlive` protect an active callback from concurrent
Close/cleanup. Its Volume lock pins the mapping until the handle closes, so
Grow/Rescan/Shrink/Close cannot invalidate a borrowed address. Accepted/staging
and immutable in-flight fence pages override mapped physical bytes: readers
never borrow a stale map page while a checkpoint writes it. A growing mapping is
prepared before the previous mapping is removed; a failed remap retains the old
one. Internal shrink unmaps the retired address range before truncation. External
truncation of an exclusively owned, active backend violates the software contract.

Native sparse RAM backends lend their existing pages; unsupported/custom
providers keep aligned, bounded positioned reads. No all-object RAM directory is
introduced by either path.

`IOStats.ReadOps/ReadBytes` count positioned backend reads. Separate
`ViewReadOps/ViewReadBytes` count mapped/RAM access requests, not physical I/O.
Neither measures OS page-cache misses or device traffic. Existing wire
`PhysicalReadBytes` is a legacy name: volume/node read rates sum positioned and
view bytes exactly once and are labeled **storage accesses**, including RAM/mmap,
metadata and migration. They are not physical-device throughput. Retired volume
history uses the same conversion. Backend operation counts stay separate from
object operation counts; logical bucket/user traffic still counts delivered
payload. No statistics wire layout change is required.

`runtime.AddCleanup` releases the lock for a collected, abandoned handle as a
fallback. Its independent lease does not retain the handle; `runtime.KeepAlive`
protects active operations. Explicit close cancels the cleanup and releases the
lock synchronously and exactly once, including repeated/concurrent closes.
The Go runtime does **not** guarantee cleanup execution or timing, particularly
before process exit: `defer h.Close()` is part of the caller's contract.

- Both handle modes: `Stat`, `Get`, `GetRange`, `ReadAt`, `WithReadView`, `CopyTo`, `Extents`, `ScanObjects`,
  hash-ordered `Scan`, `OperationStatus`, `ScanRequests`, `Close`.
- Write handles additionally: `Put`, `PutReader`, `PutRange`, `PutRangeReader`,
  `Append`, `AppendReader`, `Trunc`, `Delete`, `CAS`, `Recompress`, `Restore`,
  `Repair`, `RestoreRequest`, `ForgetRequest`, `SyncThrough`. The flush barrier
  also needs a write handle because a failed sync fences the volume.
- Volume management and maintenance, called outside handle sessions: `Open`,
  standalone `Stat`, `Identity`, `Name`, `Header`, `Size`, `State`, `SetState`,
  `Describe`, `Stats`, `IOStats`, `SetCacheLimits`, `SetGuaranteedReserve`, `Sync`,
  `Bind`, `VerifyIdentity`, `Grow`, `Rescan`, `Shrink`, `Checkpoint`, `Compact`, `Scrub`, `Export`, `Import`,
  `Detach`, `Close`. These operate on the volume itself and manage their own
  locking. Volume `Close` closes the backend; handle `Close` ends only its session.

Nonzero RequestIDs persist until explicitly forgotten; zero opts out of
deduplication. Inputs and options must remain unchanged until their call returns.

`Get`/`GetRange` cap convenience allocations at 16 MiB. `ReadAt`/`CopyTo` and
reader-based writes support larger objects without whole-object payload buffers.
`GetRange` clips at EOF, `ReadAt` follows the usual partial-read/EOF convention.
`Trunc` can create/extend sparse objects. Gaps use logical lengths and no payload
allocation. CAS compares up to 1 MiB of actual previous bytes under the volume write lock.
Whole-object CAS also checks the exact length; ranged CAS patches in place.
Optional length and byte predicates share that lock with PUT and DELETE. There
are no object-content counters. `Recompress` preserves the logical bytes.

`Import` atomically installs a complete export in an empty destination.
`Restore` installs one object in an already populated target;
`RestoreRequest` transfers original retry results before activation. `Repair`
can replace a corrupted physical representation without changing its logical bytes.
These are trusted server primitives, not authority to perform a cluster cutover.

## Packed format 5

The packed object layout from format 2 is retained. Format 3 added a private
management-root pointer and length at superblock offsets 160 and 168. Format-2
volumes open without copying data and are upgraded on the next transaction.
Private replication records remain checksummed: index kind 2 stores the
original request/result, epoch, sequence, replica-set hash, protocol state and
staged descriptor/receipt references; kind 3 indexes pending decisions by object.
Prepared payload is not reachable through the visible kind-0 object record.
Publication replaces the object pointer and installs its kind-1 retry receipt in
one transaction. Unchanged encoded allocations remain shared until retirement;
allocation accounting and freeing use the union of referenced intervals.
Readers accept known formats 2/3/4/5 and write format 5 on the next mutation.
Obsolete counter slots in old records are ignored, never used as mutation
conditions. Legacy completed receipts remain recognizable for idempotent retries.
Unknown formats are rejected without reformatting. Downgrades are unsupported;
format 5 fences binaries which do not understand the unversioned record contract.

The physical layout starts with two alternating 4096-byte superblocks, two
commit-anchor pages and eight redo scratch pages (32 KiB). The initial object
and free-space tree roots occupy two further pages. Remaining bytes belong to
the allocator. There is no permanently reserved second bank or half-volume GC
reserve. Headers carry volume identity/configuration, generations, B+-tree root
pointers, exact physical accounting and an optional resumable relocation cursor.

### Object representation

The object descriptor explicitly distinguishes CONTIGUOUS and SEGMENTED.
A complete PUT reserves **one contiguous physical range of its exact stored
length**, regardless of object size. Large payload ranges start on a page
boundary. Smaller payloads and descriptors are byte-packed into remaining gaps;
page alignment does not round their allocation length. A zero-byte object has
an index entry and descriptor, but no payload allocation.

A normal descriptor has a 40-byte header and 64 bytes per physical/logical span.
An ordinary PUT needs one span (104 descriptor bytes). Sparse PUT_RANGE/TRUNC
represent zero intervals implicitly. PUT_RANGE preserves existing allocations
and only allocates payload for newly written holes/extensions. A noncontiguous
result uses the segment directory; holes are never materialized as zero bytes.

The first APPEND keeps a PUT's existing data in place and switches the layout
flag. Segment lists use linked 512-byte blocks with seven entries each. List
blocks are themselves byte-allocated within page-aligned storage pages. APPEND
reserves a page-sized tail arena, grows subsequent arenas in steps, and reuses
an existing tail for small writes. Only the changed descriptor/list block and
tail page are updated; the full directory/list is not copied per append. A full
PUT replaces the segment representation again and releases the old arenas/list.

For complete PUT, compression chunks (up to 1 MiB) are positions **inside**
the one allocation. Raw 1-MiB PUTs are one uninterrupted payload
range, with their checksum in the descriptor. Larger raw PUTs have a separate
small checksum array. Compressed containers hold a compact chunk table so
ranged reads need only decode affected chunks. Incompressible chunks use raw
encoding. Hot APPEND arenas store raw bytes for efficient small updates; explicit
recompression can compress them without changing the logical object bytes.
Compression staging uses at most 4 MiB of RAM before spilling transient encoded
input to a temporary file. Committed data always lives in the volume.

### In-place ranged updates

PUT_RANGE and range CAS overwrite RAW data at the existing physical address,
retaining the allocation and all unrelated spans. Only addressed payload pages,
the affected checksums and object/request metadata enter redo. RAW checksums
cover at most 1 MiB, so a partial update verifies/recalculates that bounded block;
it does not read the whole native object or rewrite its untouched bytes.

On the first partial write to a packed compressed span, the chunk table is
replaced by individually owned chunk descriptors. Unchanged encoded chunks
remain at exactly their old addresses; table bytes/padding are returned to the
free tree. Only affected chunks are decoded/reencoded. If their encoded result
fits, it overwrites in place and frees any unused tail. Growth relocates only
that chunk. A subsequent full PUT restores the compact contiguous layout.

A 64-byte span uses byte 61 as the standalone-chunk flag (0 legacy packed/raw,
1 standalone; bytes 62–63 remain zero). For standalone chunks, `Checksums` holds
decoded length in its high u32 and encoded CRC32C in its low u32, rather than a
checksum-array pointer. `CRC` remains the decoded checksum, `Skip` the visible
logical offset. Existing format-2 objects remain readable; older pre-release
binaries do not understand the standalone representation and must not be used
to downgrade modified volumes.

Large ranged writes spill dirty page images and redo assembly after about
4 MiB to temporary files. Recovery validates the complete journal digest before
applying any image and also uses bounded payload buffers. A large atomic range
still needs transient on-volume journal space for its changed pages, not a second
copy of the whole object. Read handles and the private write lock provide local
isolation; no historical payload versions are retained for readers. Full PUT reuses independent allocations of the same codec, retaining fitting
encoded chunks and relocating only chunks that grow. TRUNC trims or extends the
logical extents. Replicated publication uses the same in-place path after its
durable ALL decision; private staging is not retained object history.

### Page-sized B+-trees

The object/request index and byte-exact free-space index are mutable 4-KiB
B+-trees. Object/request keys use 49 bytes and their value pointer 8 bytes;
free-space leaves use `(offset:u64,length:u64)` entries. Free-tree internal
entries additionally contain maximum byte/aligned allocation sizes for subtree
pruning. Splits and merges update affected pages. An unchanged parent/root is
not rewritten just because an object descriptor changes. Neighbor lookups and
coalescing use search paths, not directory scans. Object and decoded tree caches
are bounded; a decoded tree page is budgeted at 8 KiB of RAM.

Returned bytes are immediately merged with adjacent free ranges. Even one-byte
holes remain represented externally in the free tree, rather than needing room
for a free-list header inside the hole. Physical counters include descriptors,
request receipts, tree pages and reserved arena capacity. Every byte is accounted
as allocation, redo scratch or free space.

### Transaction and recovery order

1. Reserve new ranges in the transaction's free tree and stage new payload.
2. Persist redo images of modified occupied/shared pages and the new superblock.
   Entirely free old pages can be staged directly; they contain no confirmed
   neighbors. Larger redo records use additional free pages temporarily.
3. Sync payload/log, write and sync a checksummed commit anchor containing the
   complete log digest and generation.
4. Install changed pages and sync them; publish and sync the alternate root.

Before the anchor commits, the old tree remains authoritative. After it commits,
recovery must finish that transaction before serving clients. Redo includes full
shared-page images, so a torn write cannot damage adjacent small objects. Old log
pages become reusable after checkpoint publication. Ambiguous I/O errors fence
the open volume until recovery. Nonzero RequestIDs retain independently indexed
results until explicitly forgotten.

Free-tree edits that only shift a range boundary update its key in place,
without merging and splitting nodes unnecessarily. Finalization claims newly
allocated tree pages before processing the next retired range, including pages
allocated by a preceding free-tree edit. This preserves allocation accounting
when coalescing retires a page created within the same transaction.

### Defragmentation and capacity changes

Defragmentation moves allocations leftward and coalesces released ranges. A
move may overlap its source: only the free destination prefix is reserved, and
each moved page plus cursor is journalled. Startup resumes a committed cursor
before exposing any object. This needs bounded redo scratch, not a second copy
of the largest object. Index pointers switch only when the move is complete.

`Grow(bytes)` extends regular files/RAM; `Rescan()` adopts an externally enlarged
blockdevice. Both add the new bytes to the free tree without rewriting a bank.
`Shrink(bytes)` defragments and shortens regular file/RAM volumes only when the
whole removed tail is free. If live data or metadata cannot be evacuated, it
returns an error. The smaller header commits before the backend is truncated.

Snapshot Export/Import carries logical ranges, bytes, tombstones, layout flags
and request receipts. Import validates the complete stream and publishes the
final snapshot in one destination transaction. Restore/Repair additionally support
already populated destinations under the existing handle API.

### Planning estimates and verification

The bucket UI selects a typical object-size range instead of asking an admin
for a metadata percentage. `EstimatePutOverheadPPM` budgets packed descriptors,
one retained request per PUT, half-full index leaves, free-tree entries and
amortized inner nodes. The already withheld roots/WAL scratch are not charged
again. Compression savings and repeated-request/update history are not guessed.
These are workload estimates, not guaranteed fill ratios.

Tests cover 0–5-byte packing, contiguous multi-MiB PUTs, PUT/APPEND transitions,
tail reuse, sparse ranges, codecs, bounded caches, B+-tree splits/merges, exact
free-space accounting, growth/shrink, overlapping defrag and crashes at individual
writes/syncs. Hardware power-loss behavior and benchmarks still need separate
measurement. There is intentionally no README or performance claim yet.

## Private recovery provenance

`Handle.LocalMetadata` / `SetLocalMetadata` read and atomically replace a private
management record, limited to 64 MiB. The record is not a client object and does
not alter object counts, logical bytes, payload addresses or retry
records. Its pages count as physical metadata. A page is 4096 bytes: magic
`STQRMET1` at 0, volume ID at 8, next-page offset at 24, payload length at 32,
payload at 40, and CRC32C at 4092. Each page holds at most 4048 payload bytes;
unused bytes are zero. Existing pages are updated through redo, allowing changes
without allocating a duplicate chain on a full volume. Clearing/shrinking the
record releases surplus pages. Scrub checks it and Export/Import preserve it.

`ReassignIdentityWithMetadata(expected, newNode, record)` atomically publishes
the new owner and provenance through the same redo transaction, with exact
identity comparison and an unchanged cluster/volume identity. The server calls
this only after its durable ALL-approved takeover. `ReassignIdentity` is a
lower-level identity-only primitive and cannot establish cluster authority.

The server records authoritative ranges and their entire physical volume cohort
before admitting further writes. It invalidates source claims before a migration
FENCE. A disk without this provenance, an incomplete cohort or an unresolved
source fence cannot be adopted merely because it contains an object index.
See specification §8.2.3 for the administrative recovery protocol.


## Bucket accounting and trusted admission

`SetAdmission` reconstructs five counters per bucket from the canonical index;
transactions subsequently update changed object/request keys, not a complete RAM
directory or a full scan per write. `Handle.StorageUsage` reports actual private
allocations and deterministic shared metadata shares. `SetOwnershipFilter` and
`OwnedStorageUsage` distinguish authoritative copies from migration operating
space; the server supplies a comparable placement stamp and a pure predicate.
Callbacks must not acquire node/config/range locks or reenter volumes. The server
uses immutable accounting/range images to preserve the statistics lock order.
The optional post-commit observer receives immutable All/Owned snapshots.

Startup can instead call `ConfigureAccounting(AccountingOptions{CachePath, Stamp,
Include, Admission, Observer})`: this never traverses object records. A validated
empty index root provides an immediate constant-time bootstrap proof. Otherwise
an optional private sidecar restores all five counters per bucket against the
SHA-256 of the **exact durable encoded superblock**, volume UUID, cache schema
and checksum. Owned counters additionally require an immutable
`OwnershipCheckpointKey() [32]byte` covering the actual normalized ownership
predicate (node, volume, ranges and recovery/import masks). An unrelated user
configuration edit must not change this key. Matching global counters can be
restored independently when the ownership key changes.

Missing, stale, truncated, oversized or corrupt sidecars are only cache misses;
they cannot turn unknown counters into zero quota or hide corrupt canonical
records. `AccountingReady()` is lock-free. Servers expose reads and general
`Stats` while cold, and reject mutations/control **before** queuing a write lock.
`WarmAccounting(ctx)` runs a background census under a shared lock into private
bounded page/object caches and O(buckets) counters. It checks cancellation before
every backend page read/index entry and takes the exclusive lock only for final
publication/observer notification. Errors leave ownership/accounting unknown;
`OwnedStorageUsage` returns `ErrAccountingNotReady` until ready. Compatibility
`SetAdmission`/`SetOwnershipFilter` remain synchronous for unconfigured callers.

`SaveAccountingCache()` only snapshots an already durable generation; it does
not flush an accepted write tail. Warm completion and clean close save a cache
best-effort. Explicit maintenance may save after an existing checkpoint. The
32-MiB bounded, mode-0600 cache is atomically replaced; no userdata Sync or cache
fsync is introduced, and no cache operation occurs for each counter increment.
Interrupted/lost cache writes simply require another census. Cache failure never
fails an acknowledged user mutation or clean close. RAM buckets never save this
cache. The volume format and object IDs are unchanged.

Admission inspects the completed allocation plan before WAL publication and can
reject it without publishing bytes, index entries, a request receipt or counters.
It returns the required physical guarantee remainder; import allowances remain
separate. Extra redo pages beyond fixed scratch must also fit unprotected space.
`EnsurePhysicalBacking` conservatively preallocates Linux file volumes using
KEEP_SIZE before persistent guarantee READY; size and payload stay unchanged.

The OBJ2 flag byte at offset 25 uses bit 0 for tombstones, bit 1 for segmented
layout and now bit 2 for a trusted imported copy. REQ2 byte 102 bit 0 marks an
imported retry record; it participates in the existing CRC. `Restore` and
`RestoreRequestWithOptions` set these markers via `Options.ImportShadow`.
An unpinned local cohort must not select these copies as writers/snapshot sources
or count them as additional required copies. Committed explicit placement at that
volume makes them canonical. This is important for local volume migrations near
an absolute quota, including restart before COMMIT. Public object Info and the
native object wire format stay unchanged. Whole-volume Export/Import preserves
both marker types and replaces initialized accounting counters atomically with
the imported root; omitted frame fields are reset before decoding the next frame.


### Compact range-authority metadata

The server reads the legacy schema-1 JSON management record and the compact
`STVA` encoding. New compact records begin with `STVA`, encoding revision
`u16=1`, cluster/node/volume IDs and a `u32` entry count. Each entry contains its
bucket ID, range start/prefix, policy and owner epochs, state/runtime, `u16`
location count and locations, `u16` cohort count and volume IDs, `active:Bool` and
a 32-byte handoff fence. Fixed-width fields use the normal little-endian codec.
Only these format/placement identifiers are present; no new object-content
counter is introduced.

The active flag and fence have a constant encoded size. Fencing one range
therefore changes only its pages instead of shifting the remainder of a JSON
document and requiring a large temporary redo allocation on a full source disk.
The native volume format remains unchanged. Existing JSON records are converted
atomically by the normal metadata transaction, and compact records are never
expanded back to JSON on a peer disconnect.

Initial conversion waits until every admitted member is online and advertises
`CapabilityCompactAuthority` (32768). Upgrade all nodes before activation. Older
binaries cannot interpret compact authority metadata and reject that metadata;
downgrading after conversion requires a separate verified conversion rather
than silently reusing old recovery proofs.

Authority records retain stable physical entry slots across range splits and
retirement. A zero bucket ID denotes an unused slot and confers no authority;
logical readers omit these slots and return canonical range ordering. A new
entry reuses a vacant slot of the same encoded size before extending the record.
This avoids shifting unrelated claims during a handoff's final commit.
