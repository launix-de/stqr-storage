# Subscription-backed FUSE throughput improvements

Local measurements on 2026-11-23, branch `throughput-improvements`, based on
`9eecb96`. This is the first Go/FUSE implementation of object subscriptions;
the kernel-module and C SDK clients are unchanged.

Object subscriptions cover cached metadata versions. There is **no metadata
TTL** in these measurements. The source registry uses weak pointers and
AddCleanup; bounded LRU eviction also releases subscriptions explicitly.

## Before and after

All values are medians of three repetitions, on the same host, filesystem and
local benchmark node. Baseline is the original FUSE binary built from `9eecb96`.
The final binary adds subscriptions, bounded decoded-directory caches,
root-version-validated overlay misses, cheap readonly close, version-checked
KEEP_CACHE and 1 MiB FUSE requests.

### Readonly mount

| Workload | Baseline | Improved | Ratio |
| --- | ---: | ---: | ---: |
| Sequential reopen read, MiB/s | 55.33 | 5,055 | 91.4× |
| Existing stat, ops/s | 254 | 13,346 | 52.6× |
| Missing stat, ops/s | 359 | 16,787 | 46.7× |
| Open/read/close 4 KiB, files/s | 162 | 6,842 | 42.3× |

### Read/write mount

| Workload | Baseline | Improved | Ratio |
| --- | ---: | ---: | ---: |
| Sequential reopen read, MiB/s | 44.72 | 111 | 2.5× |
| Existing stat, ops/s | 227 | 13,666 | 60.3× |
| Missing stat, ops/s | 289 | 17,379 | 60.2× |
| Open/read/close 4 KiB, files/s | 147 | 6,073 | 41.4× |
| Sequential write + fsync, MiB/s | 2.83 | 15.85 | 5.6× |
| Create/close 4 KiB, files/s | 23.41 | 37.41 | 1.6× |
| Create/fsync/close 4 KiB, files/s | 24.86 | 37.92 | 1.5× |
| Unlink, files/s | 60.54 | 106 | 1.8× |

### Native overlay

| Workload | Baseline | Improved | Ratio |
| --- | ---: | ---: | ---: |
| Sequential reopen read, MiB/s | 52.96 | 108 | 2.0× |
| Existing stat, ops/s | 145 | 9,607 | 66.3× |
| Missing stat, ops/s | 185 | 19,680 | 106.5× |
| Open/read/close 4 KiB, files/s | 67.78 | 6,607 | 97.5× |
| Sequential write + fsync, MiB/s | 3.73 | 15.69 | 4.2× |
| Create/close 4 KiB, files/s | 4.17 | 17.29 | 4.2× |
| Create/fsync/close 4 KiB, files/s | 3.49 | 17.15 | 4.9× |
| Unlink, files/s | 26.09 | 38.28 | 1.5× |

The very high readonly reopen bandwidth is **kernel page-cache/RAM throughput**,
not SSD or network bandwidth. The initial improved readonly pass still reads
through the protocol; later passes reuse pages with a live inode subscription.
RW and overlay repetitions modify their namespaces between reads, which
conservatively invalidates more pages. They should not be compared with the
readonly warm-cache number as if all three were physical-device throughput.

The ext4 comparison from the original experiment remains a separate measurement;
this run compares client implementations against the same new server.

## PHP metadata workload

128 independent 4 KiB scripts returning 7; PHP CLI with OPcache enabled and
file_update_protection=0. Four include rounds per process, three processes per
configuration. Warm times aggregate the three rounds after initial compilation.
Timestamp validation is forced on every include (revalidate_freq=0), and the PHP
stat cache is cleared between rounds. Smaller values are better.

| Mount | Baseline, ms per 128 includes | Improved | Ratio |
| --- | ---: | ---: | ---: |
| ro | 606.88 | 8.64 | 70.2× |
| rw | 410.54 | 8.99 | 45.7× |
| overlay | 1585.24 | 11.16 | 142.0× |

OPcache with revalidate_freq=2 also runs in the harness. Warm rounds before the
next validation interval largely bypass the filesystem and are intentionally
not used as evidence of storage performance. This is a CLI include microbenchmark,
not measured PHP-FPM application requests/second or a concurrent cluster benchmark.

## Method and limits

- AMD Ryzen 9 7900X3D, 24 logical CPUs, approximately 62 GiB RAM, Linux
  7.0.0-38-generic and PHP 8.5.4.
- Both storage backing and extant test volumes were on `/home` ext4, on a
  Crucial CT4000MX500SSD1 SATA SSD, approximately 98% full. `/tmp` is tmpfs and
  held binaries, logs and mountpoints rather than the benchmark data volume.
- Dedicated local node on 127.0.0.1:17421; 2 GiB sparse data volume, raw codec,
  single primary, synchronous persistent acknowledgements. The existing cluster
  and `./mnt` were not used for writes. The powered-off remote machine was excluded.
- Both baseline and improved clients use this same server. Each writable case
  uses a separate bucket; each overlay has a fresh upper and the same stable lower.
- 32 MiB sequential files, application writes of 1 MiB; baseline FUSE negotiates
  128 KiB and the improved adapter negotiates 1 MiB. Each sequential write ends
  in fsync. Small-file create cases use 64 files with close, or 32 with fsync.
- Stat/open cases use 128 files, 4 KiB each. Read contents and PHP return values
  are checked. Timed reads reopen the file; backing-store cache is not dropped.
- Ext4 close-only buffering is not equivalent to STQR's durable per-mutation ACK.
  Within this run both client implementations retain STQR's synchronous contract;
  no writeback caching was enabled to obtain the improvement.
- The workstation was not isolated from background activity. Reported values are
  microbenchmarks with min/max retained in summary.json, not production SLAs.
- `improved_*` samples retain the first subscribed-cache iteration; `final_*`
  adds immutable decoded-directory reuse, direct overlay child lookup and
  namespace-versioned negative upper probes. Both are retained for audit.
- Final cancellation, queue-admission and lifecycle review fixes were validated
  functionally after the timed run. They do not add a TTL or change the serial
  file layout. Cold misses now coalesce by object and wait for capacity on their
  own routed transport; a paused owner does not consume another owner's slots.

## Coherence and remaining costs

Subscriptions are asynchronous notifications, not linearizable reads across
independent connections. Observed socket loss, revocation, cursor gaps, budget
overflow or range fencing invalidate their snapshots. Migration uses explicit
resynchronization before cutover; restart/resume obtains a fresh authoritative
snapshot rather than claiming persistent delta replay.

Kernel entry, attribute and negative timeouts stay zero. Kernel invalidations are
performed by a separate bounded worker, preventing notification deadlocks in the
WebSocket reader. Page-cache reuse is allowed only for a version covered by a live
subscription. A namespace/root update conservatively invalidates all live nodes;
targeted namespace notifications are a future opportunity, especially with large
mutable overlays. Overlay namespace writes still rebuild a manifest; the new
caches do not eliminate that scaling cost or rebase a changed lower filesystem.

The cache has a 32 MiB raw-snapshot budget per bucket by default, plus at most
4 MiB of decoded directory entries per filesystem and a decoded overlay manifest
limited to a 1 MiB encoded body. Negative upper probes have a hard 4,096-entry
limit and are cleared by namespace version/runtime changes. Unlinked open upper
inodes remain readable. The raw cache retains at most 1,024 subscriptions per
bucket and falls back to uncached reads on server budget/capability limitations.

## Validation

See [validation.json](validation.json) and retained logs. Tests cover streamed and
ranged snapshots, concurrent registration/mutation barriers, retry deduplication,
DELETE/recreation, retention overflow, cursor gaps, permission revocation, GC
cleanup, bounded LRU eviction, canceled close, disconnect cleanup, concurrent cold
readers, independent ranges with a paused owner, range handoff/resume, and real
kernel mounts with content/namespace/permission/copy-up invalidation.

An initial all-package race run hit its five-minute overall server-package limit.
The 197 then-existing server tests were split into three independent groups.
An old role test's synthetic socket exposed overly broad session closure; the
fix limits immediate closure to subscribed sessions and rejects registrations
after revocation. Its failed/remaining cases were rerun successfully. Additional
lifecycle and concurrent-reader tests were then run against the final code.
Optional external browser/PHP-S3/CLI/SSH acceptance tests remain skipped unless
their prerequisites and opt-in environment are supplied.

## Artifacts

- [Raw samples](results.json), [summary](summary.json), [PHP timings](php/),
  [source/binary manifest](source-manifest.json).
- [Original run](logs/run.log), [final run](logs/final-run.log), and [test logs](logs/).
- [Recorded harness](harness/) uses the environment-specific paths documented in
  its constants. Build the baseline with `git archive 9eecb96` in a temporary
  directory and `go build -buildvcs=false`; build the improved daemon/FUSE client
  from this worktree. Start a fresh isolated node with a 32 MiB system volume,
  2 GiB data volume and the shown loopback endpoint before invoking the harness.
- [Subscription contract and resource limits](../../docs/object-subscriptions.md).

All benchmark mounts, the private daemon and its disposable data volume are
removed after measurement. Results contain no bootstrap passwords or cluster tokens.
