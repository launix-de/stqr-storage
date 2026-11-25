# Rebuild and I/O priority audit

This audit covers the placement-reconciliation draft based on the merged
PG-local rebuild implementation and its existing object,
migration, drain and deletion paths. The required order is:

1. Client reads and writes that can currently be served.
2. Mandatory migrations in the active rebuild round.
3. Resumable background work such as draining, deletion and reclamation.

The current base uses the constrained greedy target planner. The unbounded
exact A* extension is excluded; see the [bounded improvement plan and archived
project review](placement-improvement-plan.md) for the separate follow-up.

A request waiting for migration or an unavailable owner must not count as
runnable foreground work and thereby prevent its own dependency from running.
An atomic storage transaction already in progress must finish safely before
yielding. Priority must be checked again before the next transaction.

## Findings

### P1: Foreground priority is cooperative and incomplete

`server/io_priority.go` counts local object reads and queued mutations and
provides cooperative yielding and a bounded delay in `waitForUserIO`.
One WebSocket runs bounded parallel object calls, including streamed upload
commits; upload headers/chunks stay ordered. Immutable cached volume identities
prevent a locked volume from blocking selection of another volume. The focused
same-socket test covers both inline and streamed writes blocked on a real
volume lock while a read of another volume completes.

A foreground capacity request releases its locks before helping an already
bound handoff. The worker and helper share an execution gate; the helper lends
priority and the next gate holder rechecks durable completion. This is local
source-side inheritance, not yet a propagation guarantee for receiving peers.

Snapshot sending checks priority between objects and
receipt pages, but not at every storage boundary. `sendObject` hashes and
exports a whole object while holding a volume read handle. A large export can
therefore delay a write to another object on that volume. The receiving
`peerRangeObjectEnd` path now yields before taking `opMu`, but still restores
a whole object atomically. Replica mutation handlers and local COW captures
count local work as foreground work. Snapshot hash/export, receiver installation
and local codec/cleanup phases count rebuild I/O only while doing local work,
rather than across peer waits. Both source and target still need consistent
admission at every safe transaction boundary; waiting while holding a
volume handle would introduce a deadlock rather than solve priority.

### P1: Drain work is not a separate background scheduling class

`server/range_planner.go` selects node/volume drain handoffs together with
required policy/replica transitions. Drain handoffs share the local rebuild-I/O
counter; remote waits no longer hold it. A drain intent is
persistent and may already span many individual handoffs, but its remaining
work needs explicit classification and a resumable scheduling budget.

### P1: The requested round-completion protocol is absent

The PG implementation binds and completes one immutable handoff per PG round.
It has durable participant acknowledgements, but no separate peer-wide
"mandatory work finished; pause optional work" phase. There is no checkpoint
which suspends an optional drain/copy at an object boundary and carries it into
the next round on that signal. A prepared ownership transfer cannot simply be
forgotten: its exact plan and progress must survive suspension, and a fenced
transfer must resolve its authority obligations before being released.

### Resolved: NULL housekeeping precedes mandatory work

The NULL worker now selects pending handoffs, pressure relief and target-map
differences before memo collection, aborted-import collection and empty drain
completion. Housekeeping also requires idle admission. This fixes local
selection order; it does not establish a distributed queue-empty barrier.

### Partial: shared target-map solver

Each PG worker constructs range requirements separately from destination
assignment, solves the current source owner's ranges together, then advances
that cached map without rescoring all ranges after each successful handoff.
Equal class/mask eligibility pools are reused across buckets. Indexed range and
capacity heaps invalidate only crossed thresholds. New replica copies precede
removals, and primary roles can change without payload transfers. Temporary
volume allocation and final bucket quota growth are distinct costs.

This is not yet a single durable, complete PG-wide NEXT map: source owners
still negotiate individual handoffs. Infeasible whole ranges fall back to the
existing split/pressure paths, and replicated logical split/merge remains open.
Primary balancing currently weights ranges equally relative to node capacity;
maintained per-range byte/IO load measurements are still missing. Existing
Failure-Domain/EC/cache limitations are not solved by this solver. Its scheduling
bound does not cover legacy fallbacks or object/receipt metadata measurement.

## Behavior already covered

Bucket deletion is logically immediate after the catalog decision. Physical
reclamation runs in small pages with `TryLock`/`TryOpen`, checks foreground and
rebuild activity, and does not retain the global generation slot. Its committed
`DELETING` intent survives subsequent catalog generations and restarts; RAM
cursors can safely be reconstructed. Completion is probed separately before a
small final metadata commit. `TestDeletingBucketDoesNotHoldGenerationAndRespectsIOPriorities`
checks these properties, including progress into a subsequent generation.

Node drain intent also survives restart. Actual membership removal waits until
required copies have been handed off; that final safety condition must remain.
Waiting for the entire drain must not become a condition for completing each
preceding PG round.

## Required follow-up

Use one admission mechanism for local reads/writes, replica work, migration and
idle cleanup. Classify immutable round obligations separately from persistent
background intents. Yield at bounded, safe storage boundaries, checkpoint
optional operation progress, finish mandatory participant obligations, then
resume the still-pending intents in a subsequent round. Priority checks must
cover receiving nodes and peer queues as well as the source worker.

The four-node split and blocked-PG tests verify scope independence; they do
not establish strict foreground priority or the optional-work suspension
protocol. Those guarantees need dedicated integration tests before being
claimed.
