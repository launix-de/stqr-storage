# Volume library and on-volume format 2

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

`Volume.Stat` is an independent general metadata query. All other object access
is exclusively through a handle from `Volume.Open(write bool)`; there is no
second, automatically locking object API on `Volume`. Use a read session to
build response headers and stream the matching version:

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
Both modes expose `Stat`, `ReadAt` and `CopyTo`. Read-handle mutations return
`ErrReadOnly`; operations after close return `ErrClosed`. Separate writes remain
separate durable commits, not a multi-operation transaction.

The lock, backend and lease remain private. Handle methods serialize concurrent
use and close; close waits for an active transfer. Do not copy a handle or call
ordinary volume methods while it is open; use handle methods to avoid recursive
locking. Callbacks must not reenter either the handle or its volume.
The injected writer prepares the interface for future direct I/O transfers;
current transfers still verify/decompress bounded blocks.

`runtime.AddCleanup` releases the lock for a collected, abandoned handle as a
fallback. Its independent lease does not retain the handle; `runtime.KeepAlive`
protects active operations. Explicit close cancels the cleanup and releases the
lock synchronously and exactly once, including repeated/concurrent closes.
The Go runtime does **not** guarantee cleanup execution or timing, particularly
before process exit: `defer h.Close()` is part of the caller's contract.

- Both handle modes: `Stat`, `Get`, `GetRange`, `ReadAt`, `CopyTo`, `Extents`, `ScanObjects`,
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
allocation. CAS is bounded to 1 MiB and supports ranges or version-conditional
whole-object replacement. Versions are monotonic across tombstones/recreation.
An explicit codec change uses `Recompress`, preserving the logical version.

`Import` atomically installs a complete export in an empty destination.
`Restore` installs one object/version in an already populated target;
`RestoreRequest` transfers original retry results before activation. `Repair`
can replace a corrupted physical representation at the same logical version.
These are trusted server primitives, not authority to perform a cluster cutover.

## Packed format 2

The pre-release format was replaced, not kept as a runtime compatibility mode.
Existing local data was exported with the old binary and rebuilt separately.

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
represent zero intervals implicitly; their actual nonzero ranges are packed
into a single allocation, rather than materializing holes.

The first APPEND keeps a PUT's existing data in place and switches the layout
flag. Segment lists use linked 512-byte blocks with seven entries each. List
blocks are themselves byte-allocated within page-aligned storage pages. APPEND
reserves a page-sized tail arena, grows subsequent arenas in steps, and reuses
an existing tail for small writes. Only the changed descriptor/list block and
tail page are updated; the full directory/list is not copied per append. A full
PUT replaces the segment representation again and releases the old arenas/list.

Compression chunks (up to 1 MiB) are positions **inside** the one allocation,
not independently allocated extents. Raw 1-MiB PUTs are one uninterrupted payload
range, with their checksum in the descriptor. Larger raw PUTs have a separate
small checksum array. Compressed containers hold a compact chunk table so
ranged reads need only decode affected chunks. Incompressible chunks use raw
encoding. Hot APPEND arenas store raw bytes for efficient small updates; explicit
recompression can compress them without changing the logical object version.
Compression staging uses at most 4 MiB of RAM before spilling transient encoded
input to a temporary file. Committed data always lives in the volume.

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

Snapshot Export/Import carries logical ranges, versions, tombstones, layout flags
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
