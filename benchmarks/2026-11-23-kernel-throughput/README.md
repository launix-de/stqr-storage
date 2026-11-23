# Standalone kernel client: subscriptions, metadata cache and readahead

Local isolated-VM measurements on 2026-11-23. This work combines the existing
`native-clients` implementation (`fa63967`) with the object-subscription server
and Go/FUSE work (`84fbbef`), on branch `kernel-throughput`.

## Changes

- The shared C protocol core implements subscribe/unsubscribe, validated streamed
  and ranged snapshots, update callbacks, ACKs, polling and resume/resnapshot.
- The kernel caches up to 8 MiB of raw inode snapshots and 512 descriptors by
  default, using subscriptions rather than a TTL. Eviction unsubscribes; a
  separate kernel reader receives notifications and reconnects after loss.
- Positive/negative dentries reuse lookup results only while the parent's live
  version matches; positive dentries also refresh the covered child inode.
- A bounded separate worker invalidates affected clean file/mmap pages. Dirty
  local mapped pages are preserved. Disconnect and resync invalidate snapshots;
  reconnect additionally drops temporarily uncovered pages refaulted during loss.
- Four RPC connections with locks selected by object ID replace the mount-wide
  operation mutex. Independent objects can proceed concurrently.
- Readahead reads contiguous windows up to 1 MiB from one snapshot, rather than
  repeatedly fetching an inode/index for each page. Normal durable writes can
  publish up to 1 MiB per chunk; mapped writeback still commits individual folios.
- The integration retains the native branch's freelist boundary-update
  optimization and adds its inherited-hardlink check to the fast overlay lookup.

## Before and after

| Workload | Existing kernel module | Improved kernel module | Ratio |
| --- | ---: | ---: | ---: |
| Client-cold sequential read, MiB/s | 0.75 | 46.60 | 61.8× |
| Warm sequential reopen read, MiB/s | 30.33 | 11,523.91 | 380.0× |
| Existing stat, ops/s | 608.48 | 456,000.41 | 749.4× |
| Repeated missing stat, ops/s | 1,861.47 | 1,031,862.79 | 554.3× |
| Open/read/close 4 KiB, files/s | 584.02 | 231,616.78 | 396.6× |
| Four-process warm open/read/close, aggregate files/s | 577.70 | 236,388.83 | 409.2× |

The client-cold sequential result is **one first pass** per module, not a median
of independently cold runs. Warm sequential values are the median of the two
remaining rounds. Other rows are medians of all three rounds in one VM run per
module; their first round can include metadata warm-up. Raw min/max and individual
timings are retained in [summary.json](summary.json) and [results.json](results.json).
The four-process workload measures the warmed filesystem plus process startup;
its variation is substantial and it does not isolate RPC-pool scalability.

**Warm multi-GiB/s reads and hundreds of thousands of metadata operations per
second measure guest kernel-cache/RAM paths.** They are not SSD, network-link or
PHP application throughput. The original module still performed inode/network
validation during warm reads; the improved module can prove cached versions
from subscriptions. The first improved sequential pass continues to read through
the native protocol, with larger windows and fewer metadata/index RPCs.

The improved run ended with 7,389 cache hits, 34 cache misses, 22 logical native
GET calls, 29 range calls and 86 readahead windows. Native counters exclude
subscription commands/ACKs and retry attempts. The 34 initial subscriptions
cover the directory, sequential file and 32 small files; the hot loops do not
keep re-fetching their metadata.

## Method

- Baseline module built independently from `fa63967` using `git archive`, not the
  developer's loaded kernel. Improved module built from this worktree. Both used
  the same test harness and new server implementation in isolated fresh fixtures.
- QEMU/KVM with 2 vCPUs and 512 MiB guest RAM, Linux 7.0.0-38-generic, virtio-net
  with QEMU user networking to a local host server. Host: Ryzen 9 7900X3D and
  approximately 62 GiB RAM; the workstation also runs unrelated background work.
- Dedicated test node and 192 MiB disposable volume. Test paths are under `/tmp`
  **on tmpfs**, and seeded data is already warm on the server. This intentionally
  measures protocol/client costs; it does not measure physical-disk throughput.
- Readonly mount; 8 MiB sequential file stored as 1 MiB segments, and 32 files
  of 4 KiB containing checked bytes. Application read buffers are 64 KiB for the
  sequential case. Each round performs 512 stat calls, 256 missing stat calls,
  128 sequential small-file reads and four processes each doing 128 reads.
- No cache dropping between rounds. No OPcache or PHP interpreter runs in the
  guest: small-file/metadata patterns are relevant to PHP but are synthetic.
- No comparison with the earlier host FUSE/SSD measurements is implied. Different
  backing storage, virtual networking and workload sizes prevent that comparison.
- Benchmarks use a non-race server for both modules. Functional tests additionally
  use Go's race detector, C AddressSanitizer/UBSan and actual kernel VM mounts.
- The last reconnect-invalidation refinement and integration compatibility fixes
  were validated after the timed run. They do not change the hot measured paths.

## Reproduce

Build a compatible baseline module in a disposable directory:

```sh
mkdir -p /tmp/stqr-kernel-baseline
git archive fa63967 client/c kernel | tar -x -C /tmp/stqr-kernel-baseline
make -C /tmp/stqr-kernel-baseline/kernel/kstqr
```

Run the harness from the worktree (matching headers, readable guest kernel image,
QEMU/KVM, static BusyBox and development libraries are required):

```sh
STQR_TEST_KERNEL=1 STQR_KERNEL_BENCHMARK=1 \
STQR_KERNEL_IMAGE=/path/to/vmlinuz-7.0.0-38-generic \
STQR_KERNEL_MODULE=/tmp/stqr-kernel-baseline/kernel/kstqr/kstqr.ko \
STQR_KERNEL_LOG=/tmp/kstqr-baseline-vm.log \
go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=4m
```

Repeat without `STQR_KERNEL_MODULE` to measure the current module. The harness
uses temporary nodes/buckets and automatically unmounts and unloads in the guest.
The host module and existing production buckets/mounts are untouched.

## Validation and remaining boundaries

VM serial logs normalize CRLF and trailing whitespace; timing samples are
unchanged. [Validation logs](logs/) include C integration/fault tests with ASan/UBSan,
Go race tests for client/volume/FUSE, server subscription integration and a kernel
VM run covering ordinary I/O, mmap, SQLite WAL, dirty-page failure recovery,
large and unaligned durable writes, external content/permission/namespace changes,
forced disconnect/reconnect, resync, open-unlink, a tiny evicting cache, a disabled
cache, remount and module unload. Functional VM runs finish with post-exit Go
interoperability and fsck checks; benchmark runs unmount and unload in the guest. The initial combined regression run exposed the integration
issues described above; they were corrected and rerun successfully.

Caching remains asynchronous, not linearizable across independent connections.
Uncovered mappings (cache disabled, unsupported server or uncacheable inode) can
retain the previous long-lived-mmap staleness limitation. Cross-mount shared
writable mappings still require application coordination. Directory edits and
index flattening retain their existing scaling limits. Dirty-folio batching,
native kernel overlay support, C/kernel cluster redirect routing and a broad
home-directory compatibility qualification remain separate work.

See [the kernel guide](../../docs/kstqr.md), [the C API](../../client/c/README.md)
and [the subscription contract](../../docs/object-subscriptions.md).

A subsequent change on this branch adds dirty-folio batching and complete-frame
sends. Its [separate writeback report](../2026-11-23-writeback/README.md) records
those measurements; the results above remain the original read-cache experiment.
