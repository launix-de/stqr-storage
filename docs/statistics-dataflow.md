# Measurement ownership and data flow

`Server.statistics` owns measured histories. Placement and the dashboard consume
views of those same records. `decay.Accumulator` owns integer interval exchange
and normalization; `decay.Series` derives configured horizons from that single
completed frame. Weighted point measurements use `decay.Mean` and the same decay
kernel. No planner file owns an accumulator, mean, delta baseline or sample clock.

| Measurement | Producer / maintained source | Single accumulation point | Consumers |
|---|---|---|---|
| Volume/node storage-access bytes | `volume.meteredIO`, `CollectStats` volume capture | `statisticsSource`, `statisticsIOCounters` | Dashboard and initial/periodic placement |
| Bucket/user payload bytes | `recordObjectTraffic`, bucket/account lifetime counters | Same IO counters and frame implementation | Bucket/user rows and summary cards |
| Range and per-reader demand | Actual object-operation instrumentation, keyed by placement range and bounded reader group | `statisticsBook.ranges`, ticked by telemetry publication | Native range snapshots and read-only placement demand views |
| Object operations, errors, flush counts and flush duration | Implemented `client.IOStats` counter families | Sparse activity counters using the same `statisticsCounter` | Shared telemetry, with no separate consumer filter |
| Positioned/view read and backend write operations | The **same** volume capture as its byte counters | Sparse backend activity counters | Shared telemetry; backend page operations are not mislabelled as object operations |
| CPU time / network counters when supplied | Node snapshot producer | Central counter records | Shared telemetry; unimplemented wire fields remain protocol placeholders |
| Positive allocation growth | Maintained volume allocation and quota-usage bucket/volume rows | Central gauge-delta counters | Capacity hysteresis and proportional range forecasts |
| Reader/peer path distance | Authenticated hints and measured peer RPC RTT | Central bounded path profiles / weighted mean | Read-only client/peer transfer-cost views |
| Successful handoff duration | Common actual execution after its coalescing gate | Central move-duration mean | Capacity forecasting horizon |

Frame publication uses actual elapsed seconds. Bursty pushes update the current
amount but cannot close a frame early. Querying the dashboard, running a PG worker
or repeatedly estimating placement never shifts a counter, samples a delta or
changes a mean. Optional zero-only activity families allocate no histories; when
the first event arrives, their actual previously observed zero interval preserves
the weekly average instead of seeding an invented nonzero prehistory.

Local and received peer observations share the ingestion implementation, scoped
by node/boot and ordered source snapshots. Peers need no new statistics schema;
each receiving node keeps one source record, not one filter per consumer. Stale
or mismatched incarnations cannot supply valid rates. Peer receipt timing may
differ from a source node's own frame timing; this is transport observation, not
a planner-owned statistics stream.

Raw cumulative totals remain the exact legacy protocol counters. Capacity,
quota backing, object counts, node state and resident memory are current facts or
gauges, not event rates; they retain their units and are never replaced by a
smoothed value for admission or ownership. `resourceSnapshot` uses the existing
shared builder to project authoritative RAM facts, without remeasuring CPU/RSS,
advancing telemetry sequence numbers or recording any time series.

The planner may retain decisions (hysteresis targets, pending rounds, trial scores,
random-seed sequence and retry deadlines). These are execution/optimization state,
not measurements. Physical growth includes internal storage work; the forecast
is advisory and pool-bounded. Actual reservation, capacity, quota, class, replica
and relocation checks remain authoritative.

Regression tests cover shared dashboard/placement frames, unchanged histories
under repeated planning, exact bytes and scaling, boot/reset/replay/stale handling,
allocation decreases, sparse zero history and weighted means. A source/AST check
rejects sampling/window operations in planner files and private measurement fields
on the range manager; its negative assertion is checked against a valid sampler
positive control. Bounded range/reader tests continue to prohibit per-object
histories and storage scans.
