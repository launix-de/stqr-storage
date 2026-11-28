# Continuous reader/owner placement

Placement maintenance periodically revisits unchanged bucket policies using
bounded RAM statistics. Client formats, object identities, access grants and the
durable handoff protocol are unchanged. Advisory telemetry cannot authorize
storage, alter membership or relax a hard placement constraint.

## Shared short and long history

`internal/decay.Accumulator` stores two discounted amounts, with a timestamp and
half-life configuration. `Add` records events; `AddInterval` integrates a
cumulative-counter delta over an interval. `Rates` returns amounts per second.
`Mean` uses the same kernel for weighted measurements. Reads do not mutate the
history, so polling cadence does not accelerate decay. Replayed intervals are
ignored, invalid numeric observations are rejected, and idle rates decay.

Node YAML and `NodeConfigSet` accept:

```yaml
placement_statistics:
  short_half_life_seconds: 10
  long_half_life_seconds: 604800
```

Zero fields select these defaults. Negative, overflowing or inverted durations
are rejected. Changing the kernel forgets advisory history; cumulative wire
counters and ownership are unaffected. The shared implementation covers range
traffic, volume traffic, range/capacity growth and observed handoff duration.
Volume windows use local receipt time; remote wall clocks do not supply the
local decay epoch. Snapshot boot identity and sequence prevent replayed load.

## One price for network transfers

The network component of a placement score is:

```
one-time migration bytes * path distance
+ horizon * client bytes/second * client-to-primary distance
+ horizon * write bytes/second * primary-to-follower distances
```

Every term has the same Traffic coefficient. Client distance improvements can
pay for a peer migration at the same byte-distance break-even point. The
existing Move component separately charges storage copy/metadata work; it is
not a second network price. Local disk copies still incur storage work while
their intra-node network distance is zero. Existing destination bytes reduce
migration volume. Replication paths are directed; changing the primary updates
all primary-to-follower costs once.

Client and peer measurements share `client.PlacementDistance`:
`1 + RTT/1ms + 100MiB/s / measuredBytesPerSecond`. The bandwidth term is omitted
when unmeasured; invalid observations cost conservatively. This is an advisory
path estimate, not a physical-geography or device-bandwidth guarantee. Missing
paths are never assumed closer than observed alternatives. The current planning
horizon remains 30 seconds. Only better valid incumbents replace the current
solution, with the existing computation and memory budgets.

## Reader observations

The native extension is capability bit23, opcode `0x0123`. Existing message
layouts stay unchanged. `X-STQR-Reader` carries one random identity retained
across a bucket client's owner connections. Reports require a negotiated,
authorized bucket connection, matching bucket/reader identity and committed node
identities. Reports contain path measurements, never claimed traffic amounts or
object IDs. Actual frontend operations accumulate demand in range aggregates.

The Go SDK measures at most two learned candidate nodes per ten-second cycle.
It uses PING and a bounded 32KiB echo probe; temporary probe transports are
closed. Probing pauses after one minute without object activity and resumes on
subsequent activity. Measurements do not read objects. `SetPlacementMeasurements(false)`
disables automatic publication; `ReportPlacementHints` supplies manual
measurements to open owner sessions. A later manual sequence supersedes an older
in-flight automatic report. Legacy servers receive no unsupported probes.

The C SDK exposes `stqr_report_placement_hints`, preserves reader identity across
owner routing/reconnect and publishes to existing negotiated owner sessions.
It does not create background threads or initiate probes. Applications can
provide their own measurements. Clients without hints continue to operate;
missing reader data produces no invented proximity benefit.

Peer observers periodically measure PING on existing authenticated links. Probe
failure never declares a node absent, reduces replicas or changes ownership.

Profiles are scoped by bucket, authenticated account and reader identity. There
are at most 4,096 profiles, 16 paths per profile, eight reader aggregates per
range and 4,096 observed peer paths. Path evidence expires after five minutes.
Overflow reader traffic remains in ordinary aggregate load counters, but cannot
claim a locality improvement. Range state follows maintained placement entries;
there are no per-object history tables or periodic object/receipt scans.

## Existing local striping

Unrestricted `Volume=0` cohorts already distribute objects across eligible local
volumes. Existing objects and retry receipts select their original physical
volume. These semantics remain intact. The planner retains each cohort's
per-volume occupancy and I/O contributions instead of assuming one physical
disk or treating local disks as replicas of each other.

Initial bucket maps, first local object placement and periodic rebuilds all
call `rebuildPlacement`: the same constrained greedy seed followed by the same
bounded optimizer and cost function. Initial placement has zero retained bytes
and zero migration costs. The coordinator is only a seed preference for a new
bucket; forecast pressure can improve that seed. Healthy single-owner
coordinator maps retain `Volume=0` and their existing striping. Empty buckets
use the existing metadata commit, without a data handoff.

`selectVolumeProof` resolves existing objects and operation receipts first.
Only a proven absence enters initial local placement. The adapter compiles the
committed local cohort, class, live capacity and own guarantee backing into the
same resource problem; it contains no destination ranking. A unit of marginal
new work supplies a placement hint, not a compressed-size estimate or a quota
reservation. The unchanged mutation engine performs exact quota and physical
admission before publication. No object history is retained.

Both phases read volume background rates through the same snapshot helper.
Read and write bytes contribute to one shared volume-pressure term. Foreground
initial optimization is limited to 1ms; a single-copy problem examines each
eligible option once through the normal hard-check/score/rollback path. Greedy
construction and eligibility compilation remain bounded by the candidate pool.
Existing-object lookup returns before this work. There is no separate local
score formula and no new striping or splitting format.

The resource model balances measured byte pressure; it does not yet identify
shared controller saturation or infer device IOPS capability from mmap counters.
Those require additional generic resource measurements.

## Safety and verification

Hard class/mask, distinct-node, replica, quota, guarantee and temporary staging
constraints are checked by the planner and rechecked by the existing executor.
Optional telemetry is RAM-only. Restarting loses optimization history, never
committed ownership, content or retry receipts. Candidate exploration performs
no storage I/O; only a selected committed-map difference enters normal handoff
inventory and reservations.

Tests cover exact half-lives and cadence invariance, idle/replay/reboot behavior,
clock skew, bounded and authorized reader state, legacy negotiation and client
cancellation, equal transfer break-even, directional replication, rollback,
constraints, existing cohort preservation, local allocation and a real daemon
relocation driven by actual reads with unchanged policy and retained retry
result. C integration covers reporting and reconnect identity.
