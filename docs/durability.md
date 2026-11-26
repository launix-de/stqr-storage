# Accepted writes and explicit durability barriers

Native mutations keep their existing durable acknowledgement by default. With
HELLO capability `8192`, a client may explicitly request an **accepted** ACK:
the complete atomic mutation is visible to reads/CAS and subscriptions, but its
latest bytes and retry record may still be volatile. This is a weaker promise,
not a change to authentication, placement authority or object atomicity.

```go
info, err := conn.Put(ctx, objectID, payload, client.PutOptions{
    RequestID: client.NewRequestID(), Accepted: true,
})
if err != nil { return err }
// Other clients may already read this version. Make its observed causal prefix
// durable according to the bucket's storage tier before promising persistence:
return conn.Sync(ctx)
```

The SDK uses the durable path on servers without the capability, and for streamed
uploads above 1 MiB. The server may also give a stronger ACK when staging space
is unavailable or an individual mutation exceeds its staging budget. Durability
mode is excluded from the retry fingerprint: an identical-ID durable retry can
checkpoint an earlier accepted result without applying the mutation twice.

## Protocol

`CommitMark = boot:ID,sequence:u64` (24 bytes), scoped to the authenticated bucket
and the issuing node incarnation. Sequence zero is invalid. The node identity
and endpoint are learned from that connection, not embedded credentials.

| Code | Message | Body/result |
| --- | --- | --- |
| `0x0118` | OBJECT_ACCEPTED / request | `ObjectRef, opcode:u16, remaining_native_body`; inline PUT/PUT_RANGE/APPEND/TRUNC/DELETE/CAS only. Result is the original mutation result. |
| `0x0119` | BUCKET_SYNC / request | `bucket:ID,mark:CommitMark` → confirmed `CommitMark` |
| `0x011a` | DURABILITY_NOTICE / server push | `CommitMark`, sent before a subscription update which can expose accepted state |

ObjectRef is unchanged (80 bytes). The wrapper preserves the original request ID,
preconditions, CAS scope and payload checksums. Streams retain the durable native
commands. New requests require a bucket-bound WebSocket which negotiated the
capability. Response header TLV `48` carries an unconfirmed CommitMark on relevant
successful reads, snapshots and mutations; old decoders can skip this optional TLV.

SYNC captures a finite prefix and confirms it only after its affected local
volume generations are checkpointed. It may cover additional completed writes.
It does not wait for an endless stream of later writes or lock unrelated ranges
for the entire barrier. It confirms the bucket's tier: HDD/SSD require durable
storage; RAM still has its explicitly volatile lifetime/replica contract.

The initial implementation rejects marks from a different server boot with
`410` / Go `ErrGone` / C `-ESTALE`. It does not persist receipt histories across
incarnations or silently infer that an unconfirmed write survived. A mount with
such unconfirmed writes can therefore need remount/recovery even if a background
checkpoint happened to persist them. Confirmed marks are cleared on the clients.

## Concurrent connection fences (capability 524288)

`CONNECTION_SYNC` (`0x0122`, Request, empty body) returns `boot:ID,receive_cut:u64`.
The receiver captures prior admitted work before scheduling its goroutines.
Each storage message runs independently; replies may arrive out of order and are
matched by request ID. Later messages cannot indefinitely extend a fence.
There is one WebSocket writer and bounded framing buffers, not a storage worker
pool. Per-upload chunk dependencies still protect offset/hash validation.

Negotiated `OBJECT_ACCEPTED` FireAndForget writes have no success reply. Errors
retain their opcode/ID and ordinary error/redirect envelope, and precede any
covering fence reply. A fence checkpoints successful preceding work even when
another preceding write failed, then reports that failure. A failed checkpoint
never produces a success ACK. Disconnect before an ACK leaves an unknown outcome.

Each connection keeps a RAM-only map from actual local volumes to their touched
generations, including observed read/subscription dependencies. Volumes are
checkpointed concurrently; an already durable generation requires no further
flush. Bucket/volume switches never flush merely because the destination changed. Closing a
client/socket does not request Sync: its tracking is discarded. Applications may
omit Sync entirely. Necessary bounded-buffer checkpoints or clean volume
shutdown can still persist data without promising a client acknowledgement.

The client connects directly to placement owners and fences used owner sockets
concurrently; the server does not broadcast/proxy a connection fence. Existing
bucket-bound authentication remains unchanged. For old peers the SDK retains
legacy marks and conservative ordering.

## Causal references and multiple owners

Object IDs remain stable; the server does not interpret client formats. A format
must join independent payload/index writes, then join the visibility/results of prerequisites before publishing dependent
references or retiring recovery intent. Ordinary filesystem operations do not
request physical checkpoints.
There is no multi-object transaction. Overlapping block-device stripes retain
their necessary causal order; independent stripes pipeline on one owner socket.
Go exposes enqueue-only PutAsync/PutRangeAsync alongside result-returning methods.
The C client exposes completion callbacks and a request-ID demultiplexer; kernel
folio/writeback/async kiocb operations use event-driven completion, without holding
the WebSocket framing lock over network/storage waits.

## Storage implementation and bounds

Accepted mutations use a page overlay: at most 4 MiB of retained pages per volume
and a bounded per-operation staging overlay. A group temporarily reserves an
8 MiB contiguous WAL region, excluded from allocation and free in both its durable
and volatile allocator views. Fragmentation/space/budget limitations fall back to
the stronger native path. Existing volume formats and durable WAL anchors are used.

A checkpoint stages pages which were wholly free in the **durable** allocator
view directly, writes/syncs a complete redo image for shared or occupied pages,
syncs the commit anchor, applies redo, and publishes/syncs the superblock. Pages
freed/reused only in the volatile view must still use redo. The final group is
therefore recovered as the old checkpoint or the complete new group. Backend
write/flush ambiguity fences the volume rather than acknowledging it.

The server never periodically flushes accepted mutations. Its background cleanup
only checks RAM durability generations and forgets references already confirmed
by an explicit fence, a durable mutation, or a necessary bounded-buffer checkpoint.
Busy volumes are skipped without waiting for their locks. Clean volume close
flushes accepted state. Confirmed generation references are pruned, allowing
closed/replaced volumes to be collected. A Go client retains at most 1,024 pending
issuer receipts; exceeding that budget fails subsequent barriers rather than
silently forgetting an unconfirmed dependency. C clients retain fixed-size marks.

## Filesystem and S3 behavior

- The FUSE CLI opts filesystem writes into accepted ACKs after negotiation.
  `fsync`/`fdatasync` and synchronous-open writes call a real barrier. Ordinary
  close/FUSE FLUSH does not imply durability. CLI unmount attempts a final sync
  and returns an error if it fails. `--fsck` continues to use durable mutations.
- The kernel client uses accepted mutations, batches mmap writeback and confirms
  its pooled-connection marks for `fsync`, `msync(MS_SYNC)`, O_SYNC/O_DSYNC and the
  waiting phase of `syncfs`/`sync`. Clean unmount also attempts a checkpoint.
  Dirty mmap pages retain the existing retry/error behavior.
- Loss invalidates cached snapshots/pages. An accepted version may roll back to
  a lower durable version after restart, so authoritative inode refreshes must
  permit that decrease instead of retaining a vanished volatile size forever.
- S3 operations retain the durable default. Successful PUT/multipart publication
  does not inherit the filesystem's weaker write ACK setting.

See the tests in `volume/deferred_test.go` (power cuts including reused live
pages), `server/durability_test.go` (barriers, subscriptions and cross-owner
ordering), C integration tests and isolated kernel/FUSE tests.

Quota ownership accounting is cached independently of global metadata revisions.
Account edits, operation receipts and unchanged range-map envelopes do not
reconstruct volume ownership. Adding an empty bucket installs its new ownership
predicate without scanning existing records. A changed bucket with retained
objects, tombstones or retry receipts still triggers a recount; recovery-origin
and volume-cohort changes conservatively recount too. All predicate/cache changes
remain under the volume lock and do not change the on-disk format or ACK contract.

The [write-fence implementation](write-fences.md) separates readable publication
from durable acknowledgement and permits later bounded writes during physical
checkpoint I/O without holding the volume metadata lock.

## Filesystem adapters: explicit sync only

Native filesystem adapters use accepted results for all ordinary data, inode,
directory, recovery-intent, decision, deletion and reference changes. Ownership
reads must be fresh; that does not require durable mutation ACKs. Independent
object requests overlap, while actual CAS results and same-object visibility
keep their causal order. There are no automatic publication-phase fences.

Only explicit fsync/fdatasync/msync(MS_SYNC), O_SYNC/O_DSYNC, filesystem sync and
block FLUSH/FUA request persistence. Close discards connection tracking without a
fence. Necessary bounded-buffer checkpoints may still cause backend
Sync calls; those are distinct from a filesystem requesting durability.

CapabilityAcceptedReferences (1<<20) permits an optional trailing accepted Bool
on ObjectReference. Legacy/default reference requests remain durable; offline
maintenance does not accept the weaker mode. Accepted replies retain receipt
header48 and the exact local volume dependency until an explicit owner fence.

S3 PUT and multipart acknowledgements stay durable, including caller-supplied
publication options. Filesystem adapters scope their mode to their own calls;
they do not weaken the same raw Client used by a native or S3 caller.
