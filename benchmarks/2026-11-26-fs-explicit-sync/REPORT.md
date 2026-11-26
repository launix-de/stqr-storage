# Filesystem durability only on explicit sync, 26 November 2026

The previous async transport change still forced physical persistence inside FS
publication and reference operations. This follow-up removes those internal
barriers. A real authenticated WebSocket audit records ordinary create/write/
read/rename/pin/unpin as accepted operations, **zero durable mutation requests and
zero fence requests**. Explicit Filesystem.Sync then fences the touched owners.
The recorded transcript is in `protocol-audit.log`.

The same change applies to native kernel CAS, intent/decision/retirement and
reference calls. Dependent operations join visibility results, not disk fences.
A hidden server-side durable Purge after accepted DELETE was also removed:
cleanup remains accepted and its final generation is included in the receipt.
Generic native/default reference semantics and S3 durable ACKs remain intact.

## Measured kernel workloads

Disposable Ubuntu 7.0.0-44 QEMU guests, isolated local server and memory-backed
backend. Same native harness and payload checks as the preceding report. Counts
are VolumeIO.Sync calls, not a direct count of hardware cache flushes. One volume
checkpoint currently contains several such calls. Background checkpoints and
bounded accepted-buffer pressure can persist data without a client fence.

| Workload | Previous async build | Explicit-sync build |
|---|---:|---:|
| 8 MiB sequential write + final fsync | 544 backend Sync calls | 36–40 |
| 32 scattered 4 KiB mmap writes + final msync | 1,936–1,952 | 12 |
| 128 small open/read/close operations | original audit: 1,024 | 0 in all three samples |

Raw measurements, including exact rates and per-repetition counts, are in
`results.json`. The short 32×4 KiB ordinary-write burst and its following explicit
fsync are separate records. Incidental backend checkpoint activity is included
in their counters; **no per-write fsync is requested by that workload**.

The memory-backed sequential write rate is still around tens of MiB/s. These
results establish reduced persistence amplification and correct burst semantics;
they do not establish GB/s or a fully profiled live Ubuntu boot. Read-cache and
small-range checksum costs from the earlier audit remain separate work.

## Reproduction and correctness

Use STQR_TEST_KERNEL=1 with a matching STQR_KERNEL_IMAGE and STQR_KERNEL_HEADERS:

```sh
STQR_KERNEL_WRITE_BENCHMARK=1 go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=9m
STQR_KERNEL_BENCHMARK=1 go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=9m
STQR_KERNEL_OVERLAY=1 go test ./server -run '^TestKSTQR(Block)?VirtualMachine$' -count=1 -v -timeout=9m
go test -race ./server -run 'TestFilesystemOrdinaryOperations|TestFilesystemReference|TestS3DurableACK' -count=1 -v
```

The suites retain mmap, read-only, overlay, reconnect, FLUSH/FUA and byte checks.
A crash-volume regression verifies that accepted DELETE plus physical cleanup
perform no Sync and may revert before a checkpoint, but survive a successful
explicit checkpoint without retaining a physical tombstone. Injected checkpoint
failure prevents a successful S3 PUT ACK. Windows shared-FS tests also pass.
