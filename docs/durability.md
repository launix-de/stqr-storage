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

## Causal references and multiple owners

A filesystem inode can reference separately addressed data and index objects.
Persisting only that inode could otherwise leave dangling references after a
crash. The Go SDK tracks receipts from reads and subscription pushes as well as
its own writes. Before publishing an accepted mutation on another owner, it
checkpoints the observed prefix on preceding owners. `Client.Sync` confirms all
remaining observed owners. An unavailable required issuer causes an error;
the implementation does not manufacture a durability proof through failover.

Within one node, each bucket uses at most one volatile destination volume at a
time. Switching destination checkpoints the preceding volume first, including
before a durable mutation on the new destination. Shared physical volumes can
checkpoint several buckets together. No multi-object transaction or atomic
multi-node commit is promised. Namespace intents retain their recovery semantics.
Overlay copy-up checkpoints a lower bucket before publishing source references
in its upper. The C/kernel client still has no cluster redirect routing; its
parallel connections all address the same configured node/bucket.

Upgrade all filesystem clients which publish such references before enabling
accepted filesystem writes alongside them. A custom native application which
builds cross-bucket references must explicitly checkpoint the referenced bucket
before durably publishing its reference.

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

A background worker attempts checkpoints once per second; capacity pressure can
checkpoint earlier. This is not a hard durability deadline. Clean volume close
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
