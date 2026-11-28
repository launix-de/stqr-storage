# Placement: greedy initialization and bounded continuous improvements

## Scope of the first step

PR #20 uses the cached, constrained greedy target planner, existing durable
handoffs, disk replica recovery, capacity-weighted primary rotation and bounded
parallel client requests. The source is the pre-A* implementation that passed
all twelve CI jobs at `334f8ed`, rebased onto master `4308433`, plus independent
retained-copy accounting, streaming-retry and QEMU-fixture fixes.

The unbounded exact A* assignment search is excluded. Greedy retains valid
copies and enforces the supplied eligibility, distinct-node, capacity and quota
constraints, but does not promise minimum movement or finding every feasible
assignment. Unassigned work remains pending, not proved impossible.

The [priority audit](rebuild-priority-audit.md) describes the remaining durable
PG-wide NEXT-map, queue-completion and replica split/merge work. Other existing
limits, including RAM replication and forced recovery during interrupted replica
topology handoffs, are not solved by splitting this PR.

The full project/specification review of the earlier, larger PR is preserved at
[the reviewed commit](https://github.com/launix-de/stqr-storage/blob/e93e6bdf38f5b8b552b08910ca6a31396a79f0ec/docs/project-review-2026-11-25.md).
Its A* implementation findings concern that archived version, not this base.

## Implemented improvement loop

PG workers revisit placement on their existing 750-ms maintenance ticks, even
without a policy edit. Pending durable work and capacity requests come first.
The shared `rebuildPlacement` phase builds a constrained greedy seed and
improves it before returning the target assignment. Empty bucket maps and first
local object placements use this same phase with no migration cost; existing
objects and receipts retain their authoritative location. Required policy/replica work
precedes optional target differences. Existing per-PG handoff/ALL barriers and
live destination reservations remain authoritative.
Each source owner optimizes its currently authoritative PG ranges; this does
not replace the existing per-handoff rounds with a complete durable PG-wide map.

The optimizer keeps one valid incumbent. A trial replaces one location, swaps
primary roles, or changes two placements together. Randomized trials vary their
seed between cycles. Only strictly better weighted scores are accepted. Exact
128-bit counters validate node uniqueness, shared free space, private guarantee
backing and bucket quota, including sums that overflow uint64. There is no
exhaustive search frontier and no global-optimality claim.

The deadline is clamped to 100 ms, including optimizer initialization. Foreground
activity reduces it to 5 ms. Cancellation/deadline preserves the incumbent and
rolls back a trial that cannot be accepted in time. Small graphs may finish
earlier under the trial bound. Workspace admission uses a conservative 64-MiB
estimate; individual transformations touch at most 64 volumes. These are
cooperative computation bounds, not an OS/Go-GC hard-real-time guarantee.

The default equal-weight byte-equivalent criteria are:

- **Move:** newly copied payload and retry/abort metadata relative to CURRENT,
  crediting bytes already at a destination.
- **Future:** squared physical utilization plus four times predicted shortfall
  above 85% capacity over a 30-second horizon. Growth comes from the central
  positive-allocation measurements; a range receives its bucket/volume hash-width
  share. This measures physical allocation, including metadata and transfers,
  rather than inventing a per-range application-growth history. The pool reserve
  bounds this advisory forecast.
- **Traffic:** migration, client and replication byte-distance costs share one
  coefficient. Optional measured RTT/throughput profiles price candidate paths;
  measured per-volume read/write pressure and capacity-weighted primary
  concentration retain independent resource terms. These remain forecasts.
  [Reader/owner colocation](reader-owner-colocation.md) describes the common
  short/long half-life accumulators, bounded advisory protocol, local-volume
  ranking and configurable statistics windows.

All other PGs' occupancy and observed I/O remain fixed background. Retirement
of old copies is never credited as already free admission space. A normal
single-owner bucket that spans several local volumes can retain that exact
unpinned map as its seed. Its optional alternatives account for each retained
cohort part; a split does not become a mandatory move or consolidation.

The immutable map indexes bucket spans and primary counts, so PG workers do not
repeatedly scan unrelated ranges. Scoring reads maintained, immutable RAM
accounting images and per-range traffic counters only. No object, receipt or
replica census is allowed on a periodic tick, cache expiry, missing measurement,
invalid placement or policy change. There is no timed metadata-refresh window.
Unknown accounting stays unknown. Known bucket/volume physical totals include
metadata and receipts and change as storage operations commit. Whole-bucket
estimates retain these totals directly; after splitting, byte estimates are
apportioned by hash width normalized to the ranges held on each source volume.
Such child estimates are explicitly provisional: skew and compression changes
are not inferred from hash width. They never grant space or quota authority.
Only a selected concrete map difference enters exact inventory/reservation;
execution rechecks real capacity and quota before activation. Cost estimation,
greedy initialization and final map materialization are measured separately.

Actual range read/write bytes are collected for successful frontend operations,
including ranged/streamed reads and APPEND. They use the existing RangeStats wire
fields for concrete locations; unpinned cohorts retain their range counters
locally and their existing bucket/volume statistics on the wire. A Volume=0
RangeStats row is never emitted because existing decoders reject it.
Idle telemetry rows stop being pushed after 30 seconds; their cumulative
counters remain. At most 4,096 range rows are sampled per snapshot to preserve
existing wire limits; local optimization retains all active counters.
Removed/split ranges and retired source measurements are pruned.
Volume background rates are derived from successive same-boot snapshots, not
from lifetime totals or replayed snapshots. Clients still interpret FS/BD/S3
formats; the optimizer sees opaque objects and ranges.

## Contracts retained for further work

1. Construct range boundaries/layout requirements without assigning targets.
2. Build a valid initial assignment. With unchanged requirements, start from
   exactly the old assignment. Changed class/replica requirements cause only
   necessary initial changes. Unfulfilled requirements must stay visibly pending;
   retaining an under-replicated old map does not fulfill the desired policy.
3. Improve complete valid assignments within a time budget, for example 100 ms,
   and a separate memory budget. Search edges are transformations: role changes,
   replica moves or multi-placement exchanges. Keep the best valid incumbent.
4. Score transfer costs minus expected lifetime benefit, with modular soft
   criteria for storage and read/write load balance. Define a planning horizon
   and comparable units. Hard protection/admission constraints are never traded
   against a score. Count benefits relative to a fixed baseline to avoid
   repeatedly crediting a cycle. Negative edges mean the existing classical A*
   assumptions and lower bound cannot simply be reused.
5. At expiry, return the best valid assignment. Execute only the resulting
   CURRENT-to-target diff. Hypothetical search transformations cause no I/O;
   actual temporary copies still need capacity and an executable safe ordering.

Safe pruning, forced-decision propagation and equivalent-option reduction can
save work without promising polynomial exact optimization of NP-hard packing.
The general replica split/merge planner, full durable PG-wide NEXT map, RAM
replica restore, physical host/rack/zone enforcement and shared-controller/NIC
bottleneck modeling remain separate requirements. This patch retains their
existing support boundaries; it does not declare the entire spec implemented.

Validation includes deadline/cancellation controls, independently checked random
constraint graphs, exact large-integer limits, a paired 100→3 transfer-cost
fixture, capacity-vs-copy and network-vs-local choices, unpinned cohort retention,
and a real daemon improvement without a config change that preserves another PG
and the original retry receipt. `BenchmarkPlacementImprove` reports greedy plus
optimization wall time and allocations, with the optimization time separately.
