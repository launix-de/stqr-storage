# Volume library and on-volume format v1

The implementation is the Go package `github.com/launix-de/stqr-storage/volume`.
There are no UI handlers, CLI commands, or networking code. Runtime files live in
ignored `volumes/`; that is a different directory from library source `volume/`.

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

- Objects: `Put`, `PutReader`, `PutRange`, `PutRangeReader`, `Append`,
  `AppendReader`, `Trunc`, `Delete`, `CAS`, `Stat`, `Get`, `GetRange`, `ReadAt`.
- Streaming/layout: `CopyTo`, `Extents`, hash-ordered `Scan`, `Recompress`.
- Recovery/migration: `Checkpoint`, `Compact`, `Scrub`, `Export`, `Import`,
  `Restore`, `Repair`, `ScanRequests`, `RestoreRequest`.
- Retry: `OperationStatus`, `ForgetRequest`. Nonzero RequestIDs persist until
  explicitly forgotten; zero opts out of deduplication. Inputs and options must
  remain unchanged until their call returns.
- Management: `Identity`, `Name`, `Header`, `Size`, `State`, `SetState`, `Describe`,
  `Stats`, `IOStats`, `SetCacheLimits`, `SetGuaranteedReserve`, `Sync`,
  `SyncThrough`, `Detach`, `Close`.

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

## Physical layout and commit

All integers are little endian. Pages are 4096 bytes and carry CRC32C checksums.
The first two pages are alternating commit roots. The remaining space consists
of two equal page-aligned banks; trailing sub-page bytes are unused. Within the
active bank metadata page pairs grow upward and payload allocations downward.

**The current implementation reserves one complete bank (approximately half the
volume) for crash-safe compaction.** Admission uses the active-bank free gap,
subtracting guaranteed reserves. This is a deliberate initial implementation
trade-off, not a claimed optimal space overhead. A later segment-local collector
can reduce this reserve after measurements and a format revision. Reported
physical bytes include index, headers, alignment, arena space and GC reserve;
logical object length is a separate value.

Commit order:

1. Stage immutable payload blocks or the alternate copy of an append-arena page.
2. Append the mutation record and optional index checkpoint to COW journal pages.
3. Sync all new payload and metadata bytes.
4. Write the other physical superblock slot and sync it.
5. Publish the new in-memory root and return success.

The physical root slot alternates independently of generation numbers, because
one commit can add multiple metadata records. Multiple modifications of a tail
page within an unpublished transaction reuse its staged copy, never the copy
still referenced by the committed root. An I/O/sync failure fences the instance;
reopen decides whether the operation committed. A short input reader does not
publish a partial object. RAM uses the same local format/atomics on a volatile
paged backend; `Sync` does not make RAM persistent.

## Root encoding

| Offset | Encoding | Meaning |
|---:|---|---|
| 0 | 8 bytes | `STQRVOL1` |
| 8 | u16 | format version 1 |
| 10, 11, 12 | u8 each | class, active bank, journal tail copy |
| 16, 32, 48 | 16 bytes each | cluster, node, volume identity |
| 64 | fourteen u64s | physical size, generation, bank epoch, mutation sequence, journal byte length, data top, live object count, logical bytes, allocated index bytes, full-index location, full-index count, replay start, live payload allocation, arena slack |
| 4092 | u32 | CRC32C over bytes 0..4091 |

Zero node/cluster IDs are initially unbound. Volume IDs are generated, nonzero and
stable. The highest valid root is chosen; a supported older root is not used to
ignore a valid unsupported-format root. Corruption in a committed journal or
payload is reported, never interpreted as a missing/uncommitted object.

## Pages, journal and disk index

Page magic is `JPG1` (journal), `APG1` (append data), or `IDX1` (index).
Offsets: magic 0, bank epoch 8:u64, page/pair index 16:u64, generation 24:u64,
used bytes 32:u32, payload CRC 36:u32, full volume ID 44:16 bytes, header CRC
60:u32 over the preceding 60 bytes. Payload starts at 64 (4032 usable bytes).
Volume ID and bank epoch exclude stale pages after reformat or bank reuse.

Mutation records (`TXN1`) have a 184-byte header, 32-byte result/arena section,
56 bytes per extent, and a trailing CRC32C. They encode operation, codec,
sequence/version, range/new length, key, RequestID, request fingerprint,
previous record pointer, arena description, and new extents. The codec is per
object version; compression blocks are independently decodable. `raw`, gzip,
LZ4 frames and xz streams use at most 64 KiB decoded bytes per payload block.
XZ dictionaries are bounded to 1 MiB. Checksums cover stored and decoded bytes.

The in-memory index delta is bounded (128 keys, also flushed every 64 mutations).
Immutable sorted runs hold 64-byte entries: a one-byte namespace plus 48-byte
key (object or RequestID), then the journal pointer at offset 56. Lookup binary
searches at most eight recent runs plus the delta. Metadata LRU defaults to 256
objects / 16 MiB, configurable; oversized entries are not cached. Full checkpoint
roots point to a sorted on-volume directory and replay only the later journal.
Compaction builds its new sorted directory with one page of RAM after a first
streaming count pass. It does not build a whole-volume hash map.

Raw PUT blocks allocate only encoded payload/alignment, no append arena. APPEND
reserves paired pages; capacity doubles up to 256 page pairs. A touched tail page
is rewritten to its alternate copy, preserving the confirmed prefix under torn
writes. Compaction coalesces contiguous live data, drops dead allocations, and
retains tombstones and non-forgotten retry records. Recompression and export
preserve gaps without materializing large zero ranges.

## Verification boundary

Tests cover temporary files and bounded RAM/mock devices, 4-KiB-aligned I/O,
codec/range/CAS/sparse/model histories, many-object index spill, concurrent
requests, discovery fixtures, every simulated write/sync failure at commit and
compaction, payload/header corruption, migration and retry recovery. Format
parsers have fuzz targets. Real raw-device writes, actual controller power-loss
behavior, SMART integration and performance/space-efficiency claims require
separate hardware testing/review. There is intentionally no project README or
benchmark claim yet.
