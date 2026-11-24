# Disk replication

The PG's storage policy controls count/ALL replication and EVERYWHERE. The
placement map records one primary and the required full replicas on distinct
nodes. EVERYWHERE includes every member admitted by the PG's node mask,
including offline members. An unavailable required volume blocks dependent
writes; transport failure never changes placement or elects another writer.

## Write and recovery protocol

1. The primary serializes each object's mutations and validates the complete
   original request fingerprint, preconditions and CAS. It assigns a sequence
   and stages the resulting bytes without publishing it. The durable record
   binds bucket, object, original request ID/result, byte predicates, owner
   epoch and replica-set hash, including the range's policy version and runtime.
2. Required peers receive the original mutation in bounded chunks. Each checks
   actual byte/length predicates under its volume lock, prepares independently, and verifies the primary's
   fingerprint and exact result before returning READY. APPEND therefore has
   the same offset and bytes everywhere. Learners are not read sources.
3. After ALL-READY the primary flushes an irreversible commit decision. Followers
   publish object and original retry receipt atomically before replying to
   COMMIT. The primary publishes only after every required peer has replied.
4. The client receives success only after primary publication. Losing a reply
   or restarting a process does not execute the mutation again. The recovery
   worker replays decided commits. While the primary is alive, undecided prepares
   resume when the original request is retried. After a primary process restart,
   an orphaned prepare without a durable commit decision receives an explicit
   durable abort and its original failure is retained. Such a request was never
   acknowledged. Client cancellation does not abort or reduce the required set.

A definitive capacity/precondition rejection before ALL-READY takes the explicit
abort path. The primary persists the failure and frees private staging, then
retries abort delivery to all required peers through a durable outbox. Later
mutations can proceed after delivery; retrying the failed original request
returns its retained failure. A transport timeout follows the recovery path
above and does not itself authorize abort.

Data volume records distinguish prepared, decided, published and aborted writes.
An abort decision rejects delayed prepares and cannot replace a commit decision.
Prepared payload is copy-on-write: unchanged encoded extents stay referenced,
including individual compressed chunks. Small appends/range writes transfer
and allocate their delta. Metadata and staging consume physical capacity.

Reads continue from the old index during payload staging and peer waits. The
short local WAL/publication transaction takes the normal volume lock. A read
handle retains its consistent snapshot until closed. While a write is pending,
different copies may expose different locally committed contents; after ACK
every required copy contains that write or a later ordered write.

The SDK can select an existing committed replica for GET, GET_RANGE, STAT and
subscriptions when the primary transport is unavailable. This grants no write
authority. A replica read requires neither a primary call nor a failure timeout.

## Placement and policy changes

The dashboard PG editor accepts a copy count for HDD/SSD or EVERYWHERE within
the PG node selection. Changing compression or masks preserves the selected
count. EVERYWHERE encodes zero as the count and disables the count input. The
editor limits RAM to one copy in count mode until RAM replication is available.

Empty buckets can begin with their complete pinned replica set. If the required
set cannot yet be built, writes wait rather than acknowledging one copy. An
addition to existing data uses the durable range handoff journal: bulk COPY
while the old set accepts writes, a range fence for final CATCH_UP, then ACTIVATE.
The primary retains its data and original retry receipts. Existing replicas
learn the new map before the primary releases the fence.

Count reductions require the departing replica online, obtain durable scoped
proofs, activate the smaller set and reclaim only that retired copy.

PG node/volume mask changes and explicit volume/node drains also relocate existing
multi-copy sets within their storage strategy. The planner preserves valid copies.
A same-node volume change copies from the live primary to the new pinned volume,
fences the range for final catch-up, commits that location and only then reclaims
the retired volume's data and retry receipts. Intermediate committed pins remain
readable even while the PG's effective mask still describes other old locations.

Replacing a node first adds a full required copy, so the intermediate set can
contain N+1 copies. Every intermediate required copy participates in ALL writes.
Only after COPY/CATCH_UP/ACTIVATE does a separate online removal retire the old
copy. This requires spare disk space and, for a finite physical quota, enough
credits for that temporary extra copy. Insufficient capacity leaves the existing
set intact rather than reducing redundancy.

If the primary itself must move, the current live primary durably fences its
range before handing the role to an existing committed replica. This changes no
payload or retained retry response and transfers logical quota credits once;
physical credits stay on the nodes already storing the copies. Terminal failed
request outcomes are also transferred from the old primary before READY: a
follower which rejected PREPARE before storing it must retain the same failure
after becoming primary. Imported abort records are durable, reserve metadata
space and are reclaimed from retired volumes only after cutover. Neither node can
invent a new writer after a timeout. The regular handoff journal resumes the
same READY/decision after either participant restarts. An offline required old
copy prevents a new relocation plan; an already bound handoff waits for its exact
participants. Forced reconciliation after losing one of them remains unavailable.

An explicit EVERYWHERE node leave may retire the leaving copy after the remaining
copies are committed and all membership participants are online. Until the
ALL-member membership change commits, the old required count still applies and
writes wait. They resume under the smaller committed membership. A volume drain
alone does not remove a node from EVERYWHERE: it needs another allowed local
volume or an explicit policy/membership change.

 An offline
node is never removed because of a timeout. Codec-only changes of an otherwise
valid set freeze that bucket, rewrite every required copy while preserving
logical contents/receipts, and activate the new central PG version after ALL confirmation.

Pending data transactions prevent a handoff/control decision from stranding
their old binding. Logical growth reservations include invisible primary writes.
Logical accounting counts primaries once; physical accounting charges required
replicas, private metadata and staged allocations to their actual volumes.

## Compatibility and remaining work

Volume format 5 retains private replica records without object-content counters.
Readers accept formats 2/3/5; format 4 is explicitly rejected. Existing format-4
installations require a separate, verified migration before upgrade. The system manifest
retains replication protocol marker 2 (marker 1 remains readable) even after the
last replicated bucket is deleted, so older peers cannot join a cluster with incompatible range history.

Accepted client requests currently use the stronger fully durable ALL path for
replicated ranges. Private prepare/commit records never enter a volatile accepted
overlay. Single-copy accepted writes keep physical quota accounting up to date
before their checkpoint. Durability capability 8192 is separate from recovery
fence clearance (1024), physical quotas (2048) and replication (4096); the C and
Go clients share that assignment. Replica topology capability 16384 gates the
new role/volume handoffs; old nodes cannot join protocol-2 history even after
the last replicated bucket is deleted. Activating that history requires all
members to advertise the topology capability. Exclusive bucket-lease acquisition
waits for pending replica transactions before granting a new owner access.

RAM replica bootstrap/restore and all-copy runtime loss still require their
own boot-bound recovery implementation. Multi-copy RAM writes return 501.
Capacity/load-driven rebalancing of existing multi-copy groups, changing their
storage strategy, and forced promotion/reconciliation of a surviving data replica
remain separate work. Such unfinished transitions stay pending; the server does
not pick a seemingly newer payload as a new primary or silently discard a required offline copy.

Tests cover torn writes/sync failures in prepare, decision and publication;
nonblocking reads during staging; sparse and compressed copy-on-write; retained
request replies; replica add/remove; central codec changes; logical/physical
quotas; primary/follower volume drains under concurrent appends; disjoint node-mask
replacement with compression changes; and both participant restarts before/after
the handoff decision. An opt-in two-host SSH test also covers remote process
crash/restart and reads while the primary is offline. Run that test with `STQR_TEST_SSH`,
`STQR_TEST_LOCAL_IP`, `STQR_TEST_REMOTE_IP` and `STQR_TEST_BINARY` set, then
`go test ./server -run '^TestSSHReplicationAcceptance$' -v -count=1`. The binary
must be built from the same source. The fixture owns a unique temporary remote
directory and never stops an existing daemon or PM2 service.
