# Capacity reserve recovery

These measurements compare `c7e66bc` with the reserve recovery change
(`a52c2f7`, applied to the current protocol in `d60ed94`). They measure capacity
handoff overhead, not a general storage-throughput ceiling.

The repeatable fixture is `TestCapacityReserveAvoidsRepeatedSmallHandoffs`:
two real local daemon instances, 8 MiB and 38 MiB data volumes, 256 distinct
SHA-256 object IDs, 128 KiB per object (32 MiB total). Accepted writes are followed
by `Sync` and byte-for-byte readback. Planning is checked after every write,
rather than relying on the scheduler ticker, so a faster host cannot evade the
capacity-recovery workload. Both versions use identical inputs.

| Version | Handoffs per run | Total seconds, three runs | Median |
| --- | ---: | --- | ---: |
| Before | 34 | 9.279 / 7.496 / 7.546 | 7.546 s |
| Reserve recovery | 3 | 1.349 / 1.289 / 1.188 | 1.289 s |

This fixture used about **91% fewer handoffs** and completed about **5.9 times
faster**. It is deliberately a small, metadata-heavy pressure test with cached
local files; these ratios do not predict network or large-volume throughput.

The production Ubuntu importer previously consumed 167.9 MiB of its tar archive
in 196.3 seconds (0.856 MiB/s). Its first subsequent 236-second sample consumed
177.9 MiB (about 0.75 MiB/s), while old release-bucket cleanup was still active.
Those windows cover different files and concurrent cleanup, so they do not
establish an importer speedup. Archive consumption is not durable publication.

That live check exposed a separate cleanup bottleneck: retry receipts were
committed one at a time while retaining the global operations mutex for up to
128 receipts. Cleanup now commits a page together, uses smaller object batches,
and yields outside the locks between pages. The importer is restarted only
after stale release cleanup completes; an additional observation is needed for
its end-to-end result.

Run the regression without touching configured production nodes:

```sh
go test -race ./server -run TestCapacityReserveAvoidsRepeatedSmallHandoffs -count=3 -v
```
