# Explicit bucket durability barriers

Measured locally on 2026-11-23, branch `kernel-throughput`. The baseline kernel
module is `14470de`, including mmap batching and complete-frame sends. Both
variants use the same new server; the baseline continues to request durable
per-mutation ACKs. The new module requests accepted writes and checkpoints its
observed bucket prefix at filesystem durability boundaries.

## SSD results

Medians of three fresh-file rounds per workload, one isolated VM run per variant.
The server's temporary volume file is on ext4 on a Crucial CT4000MX500SSD1 SATA
SSD. These workloads include their final durability barrier.

| Workload | Durable per mutation | Accepted + barrier | Ratio | Backend sync calls, before → after |
| --- | ---: | ---: | ---: | ---: |
| Normal write + fsync, MiB/s | 27.779 | 49.183 | 1.77× | 96 → 20 |
| Dense mmap + msync/fsync, MiB/s | 26.712 | 39.801 | 1.49× | 12 → 4 |
| Sparse mmap + msync/fsync, dirty MiB/s | 0.149 | 1.505 | 10.10× | 384 → 4 |
| 4 KiB mmap + msync, commits/s | 116.704 | 115.823 | 0.99× | 128 → 128 |

Grouping removes repeated durable commits across the data/index/inode mutations
of one logical operation. The scattered-page workload retains 96 CAS calls, but
usually needs only one grouped checkpoint. A 4 KiB modification followed
immediately by synchronous msync still needs its own checkpoint, so its SSD
performance is essentially unchanged.

Sync counts are medians of actual volume backend `SyncOps` deltas, not inferred
from RPC counts. Capacity pressure and the background checkpoint worker can add
checkpoints: sparse improved samples use 12, 4 and 4 syncs, and normal improved
samples use 20, 20 and 28. Three samples are indicative, not a latency SLA.

## tmpfs control

The same benchmark on a disposable tmpfs-backed volume distinguishes persistence
latency from protocol and staging overhead.

| Workload | Durable per mutation | Accepted + barrier | Ratio |
| --- | ---: | ---: | ---: |
| Normal write + fsync, MiB/s | 90.421 | 82.696 | 0.91× |
| Dense mmap + msync/fsync, MiB/s | 67.482 | 73.910 | 1.10× |
| Sparse mmap + msync/fsync, dirty MiB/s | 1.222 | 1.515 | 1.24× |
| 4 KiB mmap + msync, commits/s | 748.843 | 763.288 | 1.02× |

Normal writes regress about 9% on tmpfs: the bounded overlay adds memory/copying
cost while flushes are cheap there. The SSD improvement must not be interpreted
as a universal throughput increase. No physical-network or PHP application
throughput is inferred from these kernel write workloads.

## Contract and scope

Native clients and S3 retain durable ACKs by default. Accepted ACKs are opt-in,
negotiated through HELLO capability 1024. An accepted mutation is atomically
visible but can disappear after a crash until a bucket barrier confirms its
causal prefix. The filesystem adapters opt in and issue barriers for fsync,
synchronous writes and synchronous mmap writeback; ordinary close does not
promise persistence. Clean unmount attempts a final checkpoint.

The server uses bounded staging and its existing crash-recoverable WAL. Switching
local destination volumes checkpoints preceding bucket writes. The Go SDK also
tracks receipts from reads/subscriptions and checkpoints preceding owners before
publishing an accepted reference on another owner. This does not introduce
distributed transactions or new replica-placement support.

A receipt is scoped to the issuing server incarnation. An unconfirmed receipt
from before restart yields `ESTALE` rather than a false durability confirmation;
affected mounts can need remount/recovery. RAM buckets retain their explicitly
volatile tier contract. See [the complete contract](../../docs/durability.md).

## Method and validation

- QEMU/KVM: 2 guest vCPUs, 512 MiB RAM, Linux 7.0.0-38-generic, virtio-net with
  QEMU user networking to a disposable host server. Host Ryzen 9 7900X3D with
  approximately 62 GiB RAM; background host work was not isolated.
- Normal writes: 8 MiB in 1 MiB application writes, then fsync.
- Dense mapping: all 256 pages of a 1 MiB mapping, msync(MS_SYNC), then fsync.
- Sparse mapping: 32 dirty 4 KiB pages spaced 32 KiB apart in a 1 MiB mapping.
  Rates count the 128 KiB dirty payload. Clean gaps remain untouched.
- Commit latency: 32 individual 4 KiB modifications followed by msync(MS_SYNC),
  then a final fsync. Rate counts commits, not MiB.
- Timings include page faults and data copying, exclude file setup and backend
  counter queries. Each workload validates contents; the host checks stored
  results and filesystem consistency after the guest unmounts.
- Volume fault tests exercise power loss at each grouped checkpoint step,
  including pages freed/reused only in volatile state and ambiguous flushes.
- Protocol tests cover barriers, subscription receipts, wrong-incarnation
  rejection and a two-node cross-owner reference dependency.
- C integration uses ASan/UBSan and Go's race detector. Mounted FUSE tests,
  kernel mmap/SQLite/coherence/failure-retry scenarios, and S3/admin regression
  tests pass. `go vet ./...` and final client/CLI race tests also pass.
- The kernel restart regression restores an earlier durable image of the
  fixture's closed volume, explicitly simulating loss of unconfirmed writes.
  The existing mount refreshes both the smaller size and old contents; fsync
  reports ESTALE for its lost receipt. This image-restore test complements the
  actual WAL fault simulation; it is not a physical power-cut measurement.

All mounts and storage fixtures are isolated from the production mount/buckets.
The SSD fixture was removed after the measurements; the host module was not
loaded or replaced. Timed improved runs preceded final restart-refresh and
capability-validation hardening, which leave successful write paths unchanged.

## Reproduction and artifacts

Build a baseline module using `git archive 14470de client/c kernel` in a disposable
directory. Run the current server harness with that module, then repeat without
`STQR_KERNEL_MODULE` for the new module:

```sh
TMPDIR=/path/to/disposable-directory-on-target-filesystem \
STQR_TEST_KERNEL=1 STQR_KERNEL_WRITE_BENCHMARK=1 \
STQR_KERNEL_IMAGE=/path/to/vmlinuz-7.0.0-38-generic \
STQR_KERNEL_MODULE=/path/to/baseline/kstqr.ko \
STQR_KERNEL_LOG=/tmp/kstqr-durability-baseline.log \
go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=4m
```

Functional validation omits `STQR_KERNEL_WRITE_BENCHMARK`, enables
`STQR_TEST_C=1 STQR_C_SANITIZE=1 STQR_TEST_FUSE=1`, sets `STQR_FUSE_BINARY` to
the freshly built FUSE executable, and uses `go test -race ./server` with the
relevant test selectors. Volume crash tests run with `go test -race ./volume`.

[Raw samples](results.json), [summary with ranges](summary.json),
[source manifest](source-manifest.json), [validation and VM logs](logs/).
Serial-log line endings and trailing whitespace are normalized only.
