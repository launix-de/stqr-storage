# Low-overhead accounting and placement statistics

The change reduces work on existing aggregates before adding new measurements.
There is no new per-object, client-to-node or latency matrix, no additional disk
I/O, no statistics journal, and no protocol or volume-format change. Quota
admission, durability, eligibility and the actual handoff/reservation rules keep
their existing contracts.

## Accounting and hot paths

Transaction admission and publication previously materialized the same sorted
all-bucket accounting image six times. They now materialize the before/after
images once and derive owned totals from those exact images. Shared metadata
rounding is unchanged. Admission receives isolated temporary after slices, so
even a callback modifying them cannot corrupt the later observer publication.
All/Owned slices remain independent; later writes do not alter prior images.
Warmup and ownership-filter publications use the same helper.

This is a constant-factor reduction, not a new delta-accounting scheme: map
projection and exact shared-metadata distribution still scale with bucket count.
The placement reader uses binary search on the already sorted *local* quota
image. Externally supplied slices retain the existing general lookup. A warm
user traffic update first loads its existing counter and allocates only on a miss.

## Sampled rates

Node YAML accepts `placement_stats_half_life: 168h` (the default is one week).
It is a local startup setting; use the same value across nodes for comparable
placement preferences. Zero/omission selects the default; negative or malformed
durations are rejected. No new dashboard setting or administrative operation is
introduced.

Existing volume and range byte-rate samples use an elapsed-time exponential
average with `weight = 1 - exp(-ln(2) * elapsed / halfLife)`. Repeating the same
snapshot does not decay or initialize a rate. The first valid counter interval
seeds the average directly; a reboot or decreased volume counter starts a fresh
observation. Reconnected peers' old-boot snapshots are retained for diagnostics
but excluded from placement candidates until current-boot telemetry arrives.

Range updates retain their existing atomics and memory bounds; smoothing runs
when the planner samples a range, with at least one second between sample cuts.
There is one additional initialization flag per existing range meter. The
configuration lock is acquired once per target-map build, not per range or I/O.
The short-term capacity-growth forecast and watermarks are independent: a long
half-life cannot disable emergency capacity handling. Byte rates are advisory
storage-access/payload rates, not measured device saturation or client locality.

Range split/handoff history transfer, bottleneck-driven surplus replicas and
scan-free pressure planning require separate work. In particular the legacy
capacity-pressure costing path is not made scan-free by this change.

## Measurements

Baseline: unchanged master `c1989c0`. Linux amd64, Ryzen 9 7900X3D, Go's default
24-worker setting. Baseline and changed runs were sequential, each with
`-benchtime=300ms -count=3`; values below are medians. The baseline used the same
accepted-mutation/traffic harness and its original linear storage lookup.

| Path | Baseline | Changed | Allocation change |
| --- | ---: | ---: | --- |
| Accepted mutation, 1 bucket | 14.59 µs | 12.99 µs | 35,496 → 35,256 B; 96 → 89 allocations |
| Accepted mutation, 128 buckets | 145.25 µs | 74.54 µs | 133,320 → 116,712 B; 117 → 103 allocations |
| Accepted mutation, 1,024 buckets | 1,183.02 µs | 540.28 µs | 693,305 → 569,953 B; 125 → 110 allocations |
| Existing user traffic counter | 55.26 ns | 15.56 ns | 32 → 0 B; 2 → 0 allocations |
| Last-bucket lookup, 10,000 buckets | 8,774 ns | 29.20 ns | 0 allocations in both |

The accepted-mutation benchmark runs actual RAM-volume transactions with
admission, ownership accounting and an observer; setup is outside the timer.
It is not filesystem/network throughput and includes no device flush. Lookup
results describe the microbenchmark's last-entry case, not whole-planner speed.
The new scalar rate calculation measured 7.46 ns/op with no allocations; two
calculations are made per refreshed read/write meter, not per object request.

Reproduce the changed paths with:

```sh
go test ./volume ./server -run '^$' -bench '^BenchmarkStatistics' -benchtime=300ms -count=3
```

Regressions cover exact ownership rounding, callback isolation, accepted/durable
publication and rejected writes; sampling cadence and the first/duplicate/reset
intervals; duration configuration; lookup edge cases; and concurrent cold/warm
traffic counters. Existing volume and server suites exercise quota, recovery,
replication, retained copies, class transitions and capacity handoffs. The new
regressions also run with the race detector.
