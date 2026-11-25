# Cluster cleanup and filesystem latency audit, 2026-11-26

This audit used bounded, read-only native API requests, process build identities,
and existing logs on the two running nodes. It did not restart nodes, establish
new bucket sessions, change bucket formats, or mutate production objects.
Measurements below describe the observed snapshots, not a controlled guest
benchmark or a deployed speedup.

## Observations and limits

Both nodes initially ran integration revision `ebe9ad5`. During observation,
another session restarted both at `abc67b7`, which already batches source cleanup
and avoids some single-inode filesystem redo. Counter differences crossing that
restart were discarded. The separately available asynchronous write-fence patch
reviewed here is `2b654bc`; its presence in a branch alone does not establish that
it was deployed.

| Observation | Local node | Other node |
| --- | ---: | ---: |
| Bucket catalog in the initial observations | 55 buckets, 32 deleting | 56 buckets, 33 deleting |
| Bucket usage measurement errors in those catalogs | 41 | 42 |
| One bucket-page request | 3,047.663 ms | 9,926.276 ms |
| One node-stats request | 44.123 ms | 85.073 ms |
| Sync calls over a later, same-incarnation 302.6-second interval | 468 | 64,148 |
| Object-count change over that interval | 0 | -40 |

The catalog observations occurred at different times. They are not an assertion
that both nodes had simultaneously divergent catalogs. Storage API read counters
in the same interval rose by about 164,988 MiB locally and 238,548 MiB remotely.
Those counters count `VolumeIO` reads, including page-cache hits; they do not
measure physical disk throughput. The snapshots showed no bucket object workload.
No live Linux guest RPC latency or kernel-client build identity was measured.

Logs repeatedly reported `another rebuild is active in this PG`, and the other
node sometimes reported that the PG coordinator was already processing the group.
The logs do not expose the exact persisted active-round/proposal combination.
The scheduler defect below is reproduced, but attributing a particular live
blocked round to that defect remains a hypothesis.

## Proven code defects and corrections

1. **Cleanup blocked unrelated deletion for an entire source sweep.**
   `rangeManager.cleanup` counted the whole sweep as active rebuild I/O. Bucket
   reclamation requires zero foreground/rebuild I/O, so its polling could not
   enter until the sweep ended. Scope rebuild priority and `opMu` to one bounded
   page. After releasing the page, explicitly offer one bounded bucket-reclaimer
   step: polling alone can miss every brief window between pages. The existing
   reclaimer rotates unfinished jobs and still checks foreground/rebuild priority.
   It now removes up to 32 objects per page instead of eight. Empty matching
   receipt batches do not open a write operation or flush unrelated accepted work.
   Range receipt scans use bounded pages of 128 IDs.
2. **Cold logical usage scanned object metadata.**
   Native STATFS and dashboard usage can trigger a full logical merge scan when
   the quota cache is cold. The existing dashboard probe context does not bound
   the complete synchronous scan; the measured 9.9-second response exceeded its
   shared three-second probe budget. Fully volume-pinned single-copy placements
   without clone peers can use maintained owned logical counters instead. The
   fast path verifies the placement stamp and preserves the merge fallback for
   unbound placements, replica sets and inherited CoW views. Usage probes read
   current counters so physical deletion progress is visible despite cached
   logical quota totals. This change does not make every fallback scan bounded.
3. **An unaccepted local proposal could hide accepted PG work.**
   The worker selected its persisted offer before the active accepted round.
   Prefer accepted local work, suppress competing local proposals while a foreign
   source is active, and reject offering a different round before the accepted
   round finishes. No timeout releases ownership or cancels accepted data work.
4. **CAS bypassed buffered mutation admission in the asynchronous patch.**
   `Handle.CAS` called the generic write-handle path instead of `Handle.mutate`.
   In `2b654bc` that generic path flushes accepted pages and waits for a running
   checkpoint. Route validated CAS through the common mutation path. On master
   this preserves CAS semantics; combined with the asynchronous patch it also
   permits buffered CAS during a physical fence. Comparison failures still fail,
   and the earlier fence cannot confirm a later CAS as durable.

The `abc67b7` integration already contains source batching. The additional cleanup
correction here concerns page-scoped priority and explicit progress for unfinished
bucket purges. The master-based change also carries batching because master did
not yet contain that integration-only optimization.

## Verification

The new regressions check accepted-round selection, maintained counters against an
independent full-scan control, transferred bytes and untouched neighboring ranges,
page-scoped priority, actual advancement of an unfinished bucket purge between
range pages, native STATFS, and accepted CAS without physical I/O.

Known-bad controls failed for the intended reasons:

- Offers-first selection stranded accepted work.
- Whole-sweep rebuild priority exposed no idle window before completion, including
  with the already-batched `abc67b7` cleanup implementation.
- Removing the explicit between-page reclaimer step left all 64 doomed objects
  present after the first range page; with the step, both jobs remove 32 objects.
- Restoring the logical scan made cold native STATFS perform 2,078 data-volume
  reads for 1,024 four-byte objects. The maintained-counter path performed zero
  in both master and asynchronous integration fixtures.
- Unmodified `2b654bc` CAS forced physical writes/syncs and could not complete
  behind a deliberately held sync. The corrected CAS completes while that sync
  remains held, and an explicit later checkpoint confirms its durability.

A 64-object range move including retained retry receipts used 20 source syncs in
an isolated integration run after batching. This count includes the move's source
phases, not only physical purging. It is a test result, not a live-cluster rate.
Race-enabled checks also cover deletion priority, finite quota/replica accounting,
CoW quota behavior, checkpoint failures, and accepted-write crash/reuse recovery.

The existing WAL/checkpoint durability ordering is preserved. Further reducing
barriers or allowing maintenance publication across a physical fence needs its
own recovery and concurrency proof. The remaining synchronous maintenance batches
can still retain `opMu` during their bounded physical transaction; this audit does
not claim those waits have been eliminated.
