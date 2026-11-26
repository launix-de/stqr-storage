# Asynchronous connection fences: native write checks

Disposable QEMU guest, Ubuntu 7.0.0-44 kernel; isolated local test server and
memory-backed physical volume. These are protocol/format costs, not physical
network or disk throughput claims. Raw JSON samples are in `results.json`.

Before the external-edit burst, the first 8 MiB direct-write+fsync sample needed
576 backend Sync calls and reached 17.289 MiB/s. After the burst the two samples
needed 544 calls and reached 15.342 and 15.531 MiB/s. This demonstrates a modest
reduction in flush count, **not a bulk-throughput improvement**. Durable reference
and recovery-intent operations still dominate this workload.

The initial concurrent writeback implementation stalled on sparse mmap writes
sharing one inode head. The corrected implementation protects only that inode's
publication dependency; other inodes and object RPCs remain concurrent. Both
sparse mmap repetitions now complete, with byte checks retained. Distinct external
payload/index edits use a shared fence before head publication; repeated object
IDs retain their necessary dependency order.

Reproduce with `STQR_TEST_KERNEL=1`, `STQR_KERNEL_IMAGE` and
`STQR_KERNEL_HEADERS` pointing to a matching test kernel, then:

```sh
STQR_KERNEL_WRITE_BENCHMARK=1 go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=9m
STQR_KERNEL_OVERLAY=1 go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=9m
```

Server/C/Go regressions additionally prove read progress while the same socket's
physical sync is blocked, no success ACK per no-ACK write, error propagation at
the fence, finite receive cuts, parallel local-volume checkpoints and no implicit
checkpoint on disconnect. No historical object versions or format-aware server
operations were introduced.
