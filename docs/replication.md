# Disk replication

The PG's storage policy controls count/ALL replication and EVERYWHERE. The
placement map records one primary and the required full replicas on distinct
nodes. EVERYWHERE includes every member admitted by the PG's node mask,
including offline members. An unavailable required volume blocks dependent
writes; transport failure never changes placement or elects another writer.

## Write and recovery protocol

1. The primary serializes each object's mutations and validates the complete
   original request fingerprint, preconditions and CAS. It assigns a sequence
   and stages the resulting version without publishing it. The durable record
   binds bucket, object, original request ID/result, previous version, owner
   epoch and replica-set hash, including the range's policy version and runtime.
2. Required peers receive the original mutation in bounded chunks. Each checks
   the previous version, prepares independently, and verifies the primary's
   fingerprint and exact result before returning READY. APPEND therefore has
   the same offset/version everywhere. Learners are not read sources.
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
different copies may expose different locally committed versions; after ACK
every required copy contains that write or a later ordered version.

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
proofs, activate the smaller set and reclaim only that retired copy. An offline
node is never removed because of a timeout. Codec-only changes of an otherwise
valid set freeze that bucket, rewrite every required copy while preserving
versions/receipts, and activate the new central PG version after ALL confirmation.

Pending data transactions prevent a handoff/control decision from stranding
their old binding. Logical growth reservations include invisible primary writes.
Logical accounting counts primaries once; physical accounting charges required
replicas, private metadata and staged allocations to their actual volumes.

## Compatibility and remaining work

Volume format 4 adds private replica records to the durable object index. Readers
accept formats 2/3/4; the next durable write uses format 4. The system manifest
retains a replication protocol marker even after the last replicated bucket is
deleted, so older peers cannot join a cluster with incompatible range history.

Accepted client requests currently use the stronger fully durable ALL path for
replicated ranges. Private prepare/commit records never enter a volatile accepted
overlay. Single-copy accepted writes keep physical quota accounting up to date
before their checkpoint. Durability capability 8192 is separate from recovery
fence clearance (1024), physical quotas (2048) and replication (4096); the C and
Go clients share that assignment. Exclusive bucket-lease acquisition waits for
pending replica transactions before granting a new owner access.

RAM replica bootstrap/restore and all-copy runtime loss still require their
own boot-bound recovery implementation. Multi-copy RAM writes return 501.
Automatic relocation of existing multi-copy groups, replica-set replacement
across changed volume masks, and forced promotion/reconciliation of a surviving
data replica are not implemented by this change. Such unfinished transitions
remain pending; the server does not pick a raw highest-version object as a new
primary or silently discard a required offline copy.

Tests cover torn writes/sync failures in prepare, decision and publication;
nonblocking reads during staging; sparse and compressed copy-on-write; retained
request replies; replica add/remove; central codec changes; logical/physical
quotas; and an opt-in two-host SSH test with remote process crash/restart and
reads while the primary is offline. Run that test with `STQR_TEST_SSH`,
`STQR_TEST_LOCAL_IP`, `STQR_TEST_REMOTE_IP` and `STQR_TEST_BINARY` set, then
`go test ./server -run '^TestSSHReplicationAcceptance$' -v -count=1`. The binary
must be built from the same source. The fixture owns a unique temporary remote
directory and never stops an existing daemon or PM2 service.
