# Published writes and durability acknowledgements

A write becomes readable when its complete payload and metadata image is
published under the volume metadata lock. Its durable acknowledgement waits for
the corresponding physical checkpoint after releasing that lock. Another reader
can therefore observe the new value before the writer receives its durable ACK.

A checkpoint owns an immutable page image and its WAL allocation. Later bounded
writes publish into a separate overlay. Reads select the newest overlay, then
the checkpoint's published image, then the backing file/device. The old checkpoint
never installs its stale accounting or cache descriptors over a later write, and
its completion advances durability only through the image it actually wrote.
Waiters for an already confirmed image share that fence instead of unnecessarily
flushing a newer overlay.

This uses the existing WAL encoding, four ordered persistence barriers and volume
format. It adds no wire-protocol fields, object versions, or database transaction
API. Default write replies remain durable; accepted replies retain their weaker
contract. The server completes ordinary single-copy durable acknowledgements
outside node admission and never replays an already published APPEND/CAS merely
because its physical acknowledgement is pending.

Both the checkpoint and the next overlay have bounded resources. The existing
4-MiB accepted-page budget applies to each overlay; temporary WAL allocation
adapts to available space and excludes the in-flight journal. Native-sized writes
up to 1 MiB can publish while a fence is running. Oversized writes or exhausted
buffers apply backpressure. Explicit volume resizing/defragmentation keeps its
allocation-management exclusion while allowing reads at physical fences. Clean
close stops new publications before flushing its final buffer. An ambiguous
checkpoint failure fences the volume until recovery.

## Reclamation

Bucket deletion first publishes its existing persistent DELETING intent. Physical
cleanup remains background work. Range-source cleanup scans bounded object and
receipt-index pages instead of building a temporary complete Request-ID list.
Eligible records are removed in batches; their metadata is published before the
checkpoint, and the normal buffered path waits for that checkpoint outside both
volume and node admission locks. Bucket cleanup retains a pending checkpoint
until its durable completion, so an empty volatile view cannot prematurely
complete the persistent deletion intent. Restart reconstructs cleanup from the
existing catalog/index state.

Idle work yields between bounded pages. A tiny/full volume that cannot fit a
buffered metadata page uses the existing bounded durable WAL path; its backend
fences also run without the volume metadata lock. That fallback may still hold
node admission. Range retirement's existing distributed completion point remains
after confirmed source cleanup; this patch does not change the PG wire protocol.

## Linux operating-system fences

Linux FileIO uses `fdatasync` through `os.File.SyscallConn().Control`. This pins the
file descriptor against concurrent Close without an application-wide ReadAt/
WriteAt lock. Other threads can issue positioned reads and writes while the
calling thread waits for persistence. The volume supplies the immutable image
and ordering needed for a meaningful acknowledgement; the syscall alone is not
an atomic multi-page publication mechanism.

`fdatasync` persists data and metadata required to retrieve it, including relevant
file-size changes. Creation and directory-entry publication retain their separate
full fsync handling. Non-Linux FileIO retains `os.File.Sync`. Filesystem-internal
locks, queue contention, writeback throttling and device flush latency can still
delay concurrent I/O; no universal zero-latency guarantee is implied.

`sync_file_range` is not a replacement durability fence: it does not persist all
required filesystem metadata or flush volatile device write caches. Kernel
`REQ_PREFLUSH` and `REQ_FUA` provide ordering/persistence at the block layer, but
are not flags that Go's buffered FileIO can independently apply to its metadata
writes. Per-write `RWF_DSYNC` does not replace the existing multi-step WAL order.

Sources: [Linux fsync/fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html),
[Linux sync_file_range](https://man7.org/linux/man-pages/man2/sync_file_range.2.html),
[kernel writeback-cache control](https://docs.kernel.org/block/writeback_cache_control.html),
[Linux pwritev2 flags](https://man7.org/linux/man-pages/man2/pwritev2.2.html).

Regression tests block physical Sync with channels, require same-volume reads
and later writes to complete before release, check old/new checkpoint images
across simulated crashes, preserve cleanup receipts of unrelated buckets and
verify that 128 forgotten receipts share four barriers. A real Linux file test
runs positioned I/O concurrently with fdatasync and checks its data after reopen.
