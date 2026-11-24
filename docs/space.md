# Bucket capacity in df

FUSE and the kernel client implement `statfs`, so `df -h /mountpoint` shows
bucket usage and available space. A native overlay reports its **upper** bucket;
the lower's contents and quota are not added to the writable pool. Readonly
mounts report their bucket's storage figures; this does not grant write access.

Without an absolute quota, available space is the remaining free space on
eligible cluster volumes. Storage strategy, placement groups, volume state,
physical reservations and other buckets' unused guaranteed reservations are
respected. System/admin volumes and unrelated membership entries do not add
capacity. With a finite quota, available space is additionally capped at
`max(0, quota - used)`. A finite zero quota means zero, not unlimited.

`Used` counts live object bytes in this bucket: file data, filesystem metadata,
and retained objects awaiting offline fsck. It is not the sum of visible file
sizes or a measurement of compressed physical allocation. `Total = Used + Free`
and `Available = Free`; the implementation has no extra root-only filesystem
reserve. Total is therefore dynamic and can be smaller than a configured quota
when the physical pool is insufficient. Compression and future metadata growth
can change actual write admission; these figures are advisory, not reservations.

The wire operation is `BUCKET_STATFS` (`0x011b`), request `bucket:ID`, result
`used:u64,total:u64,free:u64,available:u64,quota:u64,quota_limited:Bool`. A valid
bucket grant with any object permission suffices; administrative privileges are
not required. Bucket-bound connections cannot inspect another bucket. SDKs expose
`Client.Space(ctx)` and `stqr_bucket_space(client, &space)`. Filesystem adapters
report byte values in 4 KiB units, rounded down. Inode counts are not estimated.

Cluster aggregation uses maintained owner counters and validated member volume
statistics. Internal owner-usage requests use schema 2 of `BUCKET_OWNER_USAGE`,
restricted to authenticated current member nodes and carrying no user identity.
Ordinary clients receive only their authorized bucket's aggregate. Unknown or
disconnected member statistics, unavailable owners, recovery ranges or changing
placement/configuration fail the measurement rather than reporting a partial
cluster as empty or full. Statistics are snapshots and can lag concurrent writes.
The Go SDK bounds a capacity probe to five seconds, or the earlier caller
deadline, so `df` does not retry an unavailable owner's 503 indefinitely.

Old servers without this operation return an unsupported error. Upgrade the
node and rebuild/remount the client to enable the display on existing mounts.
