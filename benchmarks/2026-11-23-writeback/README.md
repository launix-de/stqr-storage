# Kernel writeback batching and native frame latency

Measured locally on 2026-11-23, branch `kernel-throughput`. Baseline is the
subscription/readahead kernel implementation at `e400b8f` (already pushed).
This change retains durable per-mutation ACKs while reducing the number of
mutations and framing overhead.

## Measurements

Medians of three rounds, with a fresh file for each workload/round. One isolated
VM run per implementation; samples/min/max are in [summary.json](summary.json).

| Workload | Baseline | Batching only | Batching + complete-frame send | Final / baseline |
| --- | ---: | ---: | ---: | ---: |
| Normal write + fsync, MiB/s | 49.021 | 47.364 | 96.283 | 2.0× |
| Dense mmap + msync/fsync, MiB/s | 0.329 | 39.840 | 82.678 | 251.3× |
| Sparse mmap + msync/fsync, dirty MiB/s | 0.314 | 0.296 | 1.211 | 3.9× |
| 4 KiB mmap + msync, commits/s | 299.411 | 254.600 | 733.841 | 2.5× |

Dense 1 MiB mmap writeback originally published 256 pages individually, requiring
**768 native CAS calls** (data + index + inode per page). The final implementation
publishes one contiguous batch with **3 CAS calls**. Sparse writeback deliberately
keeps 32 separated pages separate: 32 batches and 96 CAS calls, preserving holes
rather than overwriting clean intervening data. The 4 KiB inline case retains
one inode CAS per commit; its improvement comes from reduced frame latency.

The batching-only iteration regressed the 4 KiB case and did little for scattered
writes. Profiling highlighted inode subscription/round-trip latency. The shared
C core previously sent each WebSocket header and masked payload separately; it
now submits them together in one send. The final iteration improves all four
measured cases. The intermediate measurements are retained rather than omitted.

## Write-path profile

Per-mount counters measure elapsed nanoseconds in three phases: obtaining the
inode snapshot; preparing data/index objects (including their durable RPCs); and
publishing the inode (including subscription cleanup and its CAS). These are
wall times rather than CPU samples, and can overlap under concurrent writers.
The baseline module did not have these counters; its zero-valued JSON fields
mean unavailable, not free work.

- **8 MiB normal write:** median total 83.09 ms; phase medians snapshot 0.005 ms, data/index preparation 71.44 ms, publication 6.50 ms. Phase medians are computed independently.
- **1 MiB dense mmap:** median total 12.10 ms; phase medians snapshot 0.001 ms, data/index preparation 8.78 ms, publication 0.55 ms. Phase medians are computed independently.
- **32 scattered dirty pages:** median total 103.22 ms; phase medians snapshot 16.585 ms, data/index preparation 59.30 ms, publication 23.11 ms. Phase medians are computed independently.
- **32 individual 4 KiB commits:** median total 43.61 ms; phase medians snapshot 14.853 ms, data/index preparation 0.02 ms, publication 25.84 ms. Phase medians are computed independently.

For ordinary writes, data/index preparation remains the dominant phase. Batching
at the client and a future protocol durability barrier address different costs:
the former reduces individual mutations, while an accepted-versus-durable ACK
mode could group persistence across multiple object mutations. Such a protocol
change is **not implemented here**. Crash-safe reference ordering, durable
checkpoints, retry identity and barriers covering all participating owners would
need their own contract and tests.

## Implementation and correctness

- A writeback call collects ascending contiguous dirty folios into batches of at
  most 1 MiB. Gaps, capacity, iterator completion and the requested range bound
  batches. Valid bytes at EOF are clipped; the tail is zeroed while locked.
- Each folio is referenced and marked under writeback before unlocking. The
  shared buffer remains unchanged during its RPC. Successful ACK completes all
  folios; a failure redirties the entire affected batch and reports the error.
- Completion does not reacquire folio locks: truncate/page_mkwrite can already
  hold those locks while waiting for writeback. The writeback flag prevents
  removal before completion. Iterator accounting and background writeback limits
  are respected; WB_SYNC_ALL drains its tagged pages even after errors.
- The collection follows the [Linux 7.0 writeback iterator contract](https://github.com/torvalds/linux/blob/v7.0/mm/page-writeback.c#L2303).
- The C framing change uses the same bytes, mask and hashes, combining header
  and payload into one send. Calls may still need multiple underlying sends if
  the socket accepts only part of the buffer.
- Ranged reads now reconnect once on transport loss, matching full reads. The
  expanded bucket-lock/recovery scenario exposed a stale pooled read socket.
- Cross-mount tests use bounded observation waits for asynchronous notifications;
  local syscall checks remain immediate. There is no metadata TTL or changed
  synchronous-ACK guarantee.

## Method and limits

- Same local QEMU/KVM fixture as the preceding kernel report: 2 guest vCPUs,
  512 MiB RAM, Linux 7.0.0-38-generic, virtio-net with QEMU user networking to a
  temporary host server. Host Ryzen 9 7900X3D, approximately 62 GiB RAM.
- Server volume is a disposable 192 MiB file under `/tmp` on **tmpfs**. These
  numbers measure client/protocol/commit-path overhead, not SSD persistence speed
  or a remote physical cluster. Host background workloads were not isolated.
- Normal writes: 8 MiB in 1 MiB application writes, then fsync.
- Dense mapping: a sparse 1 MiB file, memset over all 256 pages, msync(MS_SYNC),
  then fsync. Timing includes page faults and copying, not mapping/truncate setup.
- Sparse mapping: 32 dirty 4 KiB pages, each 32 KiB apart in a 1 MiB file. Rates
  count the 128 KiB dirty payload rather than the logical file length.
- Commit latency: 32 modifications of the same mapped 4 KiB file, each followed
  by msync(MS_SYNC), with a final fsync.
- No PHP application, SSD, physical-network or whole-cluster durability throughput
  is inferred. Each scenario checks contents, and the Go client verifies stored
  results and runs fsck after the VM has unmounted and unloaded the module.
- The final read-reconnect hardening and test observation waits were added after
  timed runs; they do not alter successful write paths. Source hashes identify
  the reviewed final tree, not an assertion that every timed binary was identical.

## Validation and reproduction

[Logs](logs/) retain baseline, intermediate and final samples plus functional
tests. VM serial log line endings/trailing whitespace are normalized only.
C API tests run under AddressSanitizer/UBSan with Go's race detector. Kernel VM
cases cover mmap/SQLite WAL, multi-batch failure/retry with every recovered byte
verified externally after VM exit, gaps, a partial msync range, a 1 MiB boundary,
partial EOF, namespace/permission notifications, reconnect/resync and clean unload.

Build a baseline module from `e400b8f` using `git archive e400b8f client/c kernel`
in a disposable directory, then run the current harness:

```sh
STQR_TEST_KERNEL=1 STQR_KERNEL_WRITE_BENCHMARK=1 \
STQR_KERNEL_IMAGE=/path/to/vmlinuz-7.0.0-38-generic \
STQR_KERNEL_MODULE=/path/to/baseline/kstqr.ko \
STQR_KERNEL_LOG=/tmp/kstqr-writeback-baseline.log \
go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=4m
```

Repeat without STQR_KERNEL_MODULE for the current module. Normal functional tests
omit STQR_KERNEL_WRITE_BENCHMARK and enable STQR_TEST_C=1 STQR_C_SANITIZE=1 with
`go test -race ./server -run '^Test(CClient|KSTQRVirtualMachine)$'`.
Existing production mounts/buckets and the host kernel module are untouched.

[Raw samples](results.json), [summary](summary.json), [source manifest](source-manifest.json),
[kernel guide](../../docs/kstqr.md).
