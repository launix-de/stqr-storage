# Rebuild and I/O priority audit

This audit covers the PG-local rebuild working branch and its existing object,
migration, drain and deletion paths. The required order is:

1. Client reads and writes that can currently be served.
2. Mandatory migrations in the active rebuild round.
3. Resumable background work such as draining, deletion and reclamation.

A request waiting for migration or an unavailable owner must not count as
runnable foreground work and thereby prevent its own dependency from running.
An atomic storage transaction already in progress must finish safely before
yielding. Priority must be checked again before the next transaction.

## Findings

### P1: Foreground priority is cooperative and incomplete

`server/io_priority.go` counts local object reads and queued mutations and
provides `waitForUserIO`. Snapshot sending checks this between objects and
receipt pages, but not at every storage boundary. `sendObject` hashes and
exports a whole object while holding a volume read handle. A large export can
therefore delay a write to another object on that volume. The receiving
`peerRangeObjectEnd` path takes `opMu` and restores a whole object without first
yielding to runnable user work. Replica mutation handlers take `opMu` directly
and are not included in the foreground counter. Both source and target need
consistent admission at safe transaction boundaries; waiting while holding a
volume handle would introduce a deadlock rather than solve priority.

### P1: Drain work is not a separate background scheduling class

`server/range_planner.go` selects node/volume drain handoffs together with
required policy/replica transitions. `rangeManager.move` marks every handoff as
priority-two work for its whole lifetime, including remote waits. Consequently
a blocked drain can suppress idle deletion indefinitely. A drain intent is
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

### P2: Housekeeping can run ahead of mandatory migration selection

The NULL worker calls memo collection, aborted-import collection and empty
drain completion before choosing a range plan. These operations are bounded,
but they do not all participate in the same idle admission check. They must
not be scheduled ahead of runnable mandatory PG migration or foreground I/O.

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
