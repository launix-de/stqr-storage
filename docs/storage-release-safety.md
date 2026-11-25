# Storage release safety investigation

This change starts from `c0a2f81` (the locally available `origin/master`), not
from the old `multinode` checkout. The earlier chain-audit probes ran against
`ac7067667e547438d5b83d6f20f879f4f977f51b`. Findings and passing tests from those
revisions must not be treated as interchangeable release evidence.

## Where the cyclic page came from

The earlier `TestStorageAuditCyclicTree` constructed the bad page itself:

```go
root := v.Header().ObjectRoot
raw := encodeNode(treeNode{
    kind: treeObjects, level: 1,
    entries: []treeEntry{{value: root}},
}, v.Identity().VolumeID, 1)
d.WriteAt(raw, int64(root))
```

Thus its only child points to the page being written. `encodeNode` calculates a
valid checksum for these deliberately invalid bytes. This bypasses the tree
mutation API, allocator, WAL and commit path. The companion empty-interior-root
probe also used `WriteAt` to inject its malformed root. Those probes asserted
that the old defects occurred; they were never successful-behavior release tests.

Outside tests, `encodeNode` is called only by `Format` in `volume/Volume.go`
(two initial leaf pages at distinct addresses) and by `transaction.putNode`
in `volume/tree.go`. All production callers of `putNode` are in that tree
implementation. The initial leaf pages have no child links.

No ordinary producer of the reported cycle was established. Reviewing
`transaction.insertAt`, `set`, `deleteAt`, `remove`, `metaPage`, `candidate`,
`claim`, `release` and `finalize` found no demonstrated path that generated that
self-link. Splitting reserves a separate metadata page in `busy` before using
it; a new root references the old root and the new sibling. Merging retires the
removed sibling and root collapse retires the former root. Retired ranges are
excluded from allocation within the transaction. Temporary free-tree siblings
and claim remainders have dedicated tracking while the parent is unpublished.
This review and the tests below are not a proof that every possible allocator
or recovery defect is absent.

The confirmed production defect was trusting CRC-valid topology during
recursive descent. The old `treeWalk` could recurse indefinitely, and insertion
could index an empty interior node. Each descent now checks that child and parent
have the same kind and that the child's level is exactly one lower. The level
limit therefore bounds recursion without an unbounded visited set. Empty
interior pages are rejected by decoding; deleting the last entry normalizes an
empty subtree to an empty leaf. `Scrub` checks the free tree as well as objects.
Bounded scans carry the validated child page into recursion, preserving the existing two-page seek regression instead of reading that leaf twice. The format and checksum representation do not change.

`TestStorageSafetyTreeCorruption` expects `ErrCorrupt`, never a panic or child
process crash. It injects empty interior nodes, self-links, two-page cycles,
wrong kinds and wrong levels in both trees and exercises reads, bounded scans,
mutation and free-space allocation. The split/merge/reuse test inserts and
removes 5,200 keys in randomized order twice, with commits and reopen. An
independent traversal checks unique page reachability and descending levels;
these are synthetic index keys, not a payload-integrity benchmark. Existing
volume crash/recovery tests provide the separate payload/WAL coverage.

## Client-controlled lock lifetime

There is no native client opcode that exposes `Volume.Open`, `Handle.Close`,
`RWMutex.Lock` or `Unlock`. Handles remain server-owned and are closed on local
success and error paths. Important boundaries reviewed:

| Entry path | Work performed while the volume handle is held | Client wait |
| --- | --- | --- |
| Native inline mutations | Local bounded payload and transaction | Response after handle close |
| Native streamed uploads | Commit reads a completed, verified local staging file | Header/chunk/CANCEL processing has no retained volume handle |
| Native GET and ranged GET | Copy/checksum into a bounded local file | WebSocket queue and socket writes follow handle close |
| Subscription initial snapshot | Local file/hash capture and registration barrier | Snapshot network delivery follows handle close |
| HTTP message RPC | Complete bounded request body is decoded before object dispatch | HTTP response follows local execution |
| S3 upload | HTTP body/signatures/checksums are staged before mutation | No body read under a volume handle |
| S3 conditional download | Local checked snapshot before HTTP output | HTTP writer receives the snapshot after close |
| S3/generic `Bucket.CopyTo` | Local snapshot before invoking the supplied writer | A blocked writer cannot pin the volume |
| Generic mutation `Reader` | Unknown reader is staged before object/admission/volume locks | A blocked reader cannot pin those locks |
| Replica receive and range transfer | Completed local spools; range sender captures local data before peer transmission | Peer network exchange occurs outside volume handles |
| COW read capture | Local spool under the read handle | Subsequent local/remote transfer uses the detached snapshot |

Known local mutation sources (`bytes.Reader`, `bytes.Buffer`, regular local
files and the private replica spool) do not require a duplicate staging copy.
Generic staging retains the existing RAM-vs-file spool distinction and its
per-operation size cap. External native stream staging already limits transfer
size and concurrent transfers. `Bucket.CopyTo` applies the native snapshot size
budget. Local checked copies and mutation attempts have a cooperative 30-second
server context; expiry is reported as retryable native status 503. WebSocket writes separately have a ten-second deadline.

The regression blocks a supplied upload reader or download writer and requires
an unrelated mutation to complete before releasing it. Existing stream,
subscription, replica and cancellation suites cover their respective boundaries.

A logical bucket maintenance/object-I/O lease is an authorization and ownership
fence, not a held volume mutex. Its persistence/release rules must not be replaced
with an unsafe timed mutex unlock. Similarly, a context cannot interrupt an
arbitrarily stalled OS/device operation or safely unlock a transaction in another
goroutine. The time budget is cooperative at I/O boundaries; this change does
not claim an absolute wall-clock upper bound under failing hardware.

## CAS permissions

External native CAS requires both `BucketRead` and `BucketWrite`, before
comparison and again in the mutation admission path. Separate permission checks
are intentional: the existing authorization function tests one permission at a
time, so passing a combined mask would mean "any", not "all". Tests cover a
read-only grant, write-only grant, both grants, matching comparison bytes and a
mismatch, through the bucket API and an authenticated native connection.
Private server frontend grants continue to implement the authorized S3 operation;
they are not exposed as native client credentials. Replication and private COW
primitives remain trusted server operations.

## Sparse BD reads and missing RAM placement

The old audit's sparse-tail failure is not reproduced on `c0a2f81`: Go QEMU/NBD
and Linux kernel BD already handle a range error by checking the short stripe's
length and supplying zeros for its implicit tail. The new real-server regression
checks populated prefixes, short tails, absent stripes, stripe boundaries and
the disk's final valid sector. It also calls the raw object RPC and requires
HTTP/native status 400 for the same out-of-object-range request. The storage
server's object-range semantics remain unchanged. Out-of-disk-range requests
still fail.

RAM object and bucket-inventory paths no longer index `patch.Entries[0]` when
placement is absent. They return 503. The regression uses an empty placement
image and covers inventory, GET and STAT.

## Temporary backend outage

The mounted native filesystem now retries transient transport failures with
backoff instead of returning them as permanent executable-page failures. GET,
ranged GET, STAT, CAS, redo mutations and statfs use this policy. Unknown mutation
outcomes retain the original request ID; CAS and redo never generate a fresh ID
inside the retry loop. Flush fence workers retry the same target marks. Permanent
semantic errors remain errors; a forced unmount explicitly cancels reconnects.

The disposable outage VM closes existing connections and the listener for two
seconds, checks a cold mmap fault waits and returns the right bytes, then loses
a successful CAS reply and verifies the exact replay applies the append once.
The fixture checks that both outages and the reply loss actually occurred.
This is a transport outage test against the same server incarnation, not a
physical power-loss test. The ordinary filesystem VM additionally exercises its
existing restart/checkpoint-image fixture. Unconfirmed durability marks must not
be silently accepted after a changed server incarnation.

The kernel BD client's exclusive-lease loss policy is separate: it must not
silently acquire a new writer lease after losing ownership. This change preserves
that fencing behavior; it does not claim transparent BD lease resurrection.

## Deferred work

[Issue #32](https://github.com/launix-de/stqr-storage/issues/32) tracks filesystem
object reclamation and equal-size PUT/CAS in-place reuse across every implemented
client. Its title and body are English. This safety change does not implement
that issue or change filesystem storage layout.

## Verification record

`GATES.md` is the current acceptance ledger. A passing failure-reproduction probe
from the earlier audit is not accepted as a regression pass. Targeted negative
controls are also run against an untouched copy of the starting revision;
verification limitations and any failing broad regression remain visible in the
ledger and final report.

Negative controls on an isolated, unchanged `c0a2f81` copy:

- Empty interior page: the new test fails because the old walk returns nil.
- Write-only CAS: the new test fails because a wrong guess yields 412 rather than 403.
- Missing RAM placement: the new test fails with the original index panic.
- Blocked reader/writer: both old paths prevent an unrelated mutation from completing.
- Outage VM with the old module: the cold mmap fault terminates the guest test with a bus error before the replay case; the fixture records only its first outage. The new module completes both outages and the replay case.
