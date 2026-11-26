# Go filesystem and NBD parallel reads

The Go filesystem reader traverses only the indexed subtrees intersecting a
request, and scatters independent selected children into disjoint destination
slices. The NBD catalog similarly scatters selected object stripes. Both paths
allow at most 16 active object calls per logical read. Nested filesystem/lower
reads share that budget. A full budget executes the next child inline rather
than allocating waiting goroutines or blocking a worker on its own children.

Every started call joins before return, cancellation, inode-view validation, or
release of the NBD operation guard. The first storage error cancels siblings;
requests already completed remain harmless. No read sends a durability fence.
Custom stores retain serial access unless they explicitly advertise
`ConcurrentObjectRequests() bool`; native clients reuse their asynchronous
owner sockets and existing placement routing.

## Measurement

`BenchmarkNativeParallelRead` uses the actual HTTP/WebSocket server, native Go
client, placement lookup and a private RAM volume. Each read is 8 MiB across
1 MiB objects. Dataset construction explicitly syncs small setup batches before
measurement; these are test setup fences, not implicit production read/write
fences. The 1 ms profile adds context-aware independent object latency at the
client store boundary. The zero-delay profile measures native local loopback.

One paired run on a Ryzen 9 7900X3D, Linux amd64, Go default GOMAXPROCS=24:

| Adapter/profile | Serial MB/s | Parallel MB/s |
| --- | ---: | ---: |
| Filesystem, loopback | 207.04 | 460.68 |
| Filesystem, 1 ms/object | 168.01 | 337.63 |
| NBD, loopback | 195.79 | 437.08 |
| NBD, 1 ms/object | 177.56 | 373.91 |

Serial production sources were from `95d43b6`; the same benchmark and dataset
were used for both runs. These figures describe Go adapters and a RAM-backed
node under shared host load. They do not measure disk bandwidth, the C/kernel
client, or end-to-end PXE boot time.

Reproduce from the parallel-read branch:

```sh
go test ./server -run '^$' -bench '^BenchmarkNativeParallelRead' -benchtime=1s -count=3
```

The blocked-first-child tests verify overlap without relying on elapsed-time
speedups. Cancellation tests assert the active count is zero at return, and the
two-owner wire test rejects wrong-owner reads and extra connections. The
filesystem inode test changes the head during a selected read and verifies a
retry returns the current view.
