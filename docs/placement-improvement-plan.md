# Placement: mergeable base and bounded improvements

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

## Follow-up: bounded search over valid placements

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
The follow-up must measure initialization separately from improvement time and
test deadline/cancellation, memory bounds, hard constraints, no-change stability,
infeasible capacity, lost replicas and copy-before-retire headroom.
