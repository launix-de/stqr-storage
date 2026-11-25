# Object subscriptions and filesystem caches

A bucket WebSocket can subscribe to an object using `SUBSCRIBE` (`0x0110`). The
server advertises `CapabilityObjectSubscriptions` (`512`) in HELLO. Commands use
the existing authenticated bucket connection; READ permission is required.
No second listener process or per-read communication through stored objects is
introduced.

Subscriptions are owner-local, transient RAM state. The placement map routes
SUBSCRIBE to the same primary that receives writes; ordinary GET may still read
a replica. Registration, ACK and UNSUBSCRIBE do not broadcast descriptors to the
cluster, persist them as objects, create snapshot spool files or force a disk
checkpoint. The server indexes watchers by object and session/subscription ID;
native clients index subscription IDs with hash tables. Native-sized (up to
1-MiB) COW read-through captures also use bounded RAM rather than a disk spool;
larger general-purpose COW preservation transfers retain their storage staging. LRU lists only choose
cache entries to evict. Iteration over an object's actual watchers is necessary
to deliver updates, and is not a global subscription search.

Initial snapshot buffers reserve RAM before allocation. The node bounds concurrent
snapshot payloads by its existing 1-GiB transfer staging budget and returns 507
when that temporary budget is exhausted. The Go buffered subscription API retains
its 16-MiB limit and receives streamed snapshots into RAM. Subscription routing
retains accepted-write durability marks without flushing them; subsequent
cross-owner writes or explicit SYNC must fence those dependencies.

The initial response contains a consistent snapshot, a subscription ID and a cursor. Snapshot copying and listener registration use the
same native volume read handle. Mutations publish their update under the write
handle which committed them, so there is no GET-to-registration gap. Disk ACKs
retain the native durability contract. A slow reader never holds up a mutation
waiting for notification delivery or an ACK.

```go
sub, err := conn.Subscribe(ctx, objectID, client.SubscribeOptions{})
if err != nil { return err }
defer sub.Close(ctx)

bytes, info, cursor, err := sub.Snapshot()
// Wait on sub.Changes(), then take another Snapshot(). Changes coalesce;
// Snapshot returns the latest complete contents rather than individual deltas.
```

`SubscribeOptions` also supports a byte window and an optional resume cursor.
The server currently sends complete replacement windows and DELETE events,
using the specified OBJECT_UPDATE wire format. Full subscriptions receive whole
object snapshots; a ranged subscription retains its original offset and length.
The SDK checks subscription identity and cursor continuity, rejects incomplete update sets
and validates payload hashes before publishing a snapshot.

The native snapshot transfer streams when necessary. The buffered Go subscription
API accepts snapshots up to 16 MiB, like the buffered object APIs. A filesystem
cache can fall back to an ordinary read when a snapshot cannot be cached.

## Bounded retention and resynchronization

The current implementation uses explicit fresh snapshots rather than promising
durable replay across socket loss or node restart. Reusing a subscription ID
with a resume cursor on a new registration returns a complete snapshot with a
strictly later cursor. Call `Close` before reusing an ID on a live client.

Unacknowledged publication is bounded by 16 events and 1 MiB per subscription,
and 64 MiB across a node. A single pushed replacement is limited to 1 MiB;
larger updates require resynchronization. There are at most 2,048 subscriptions
per socket and 16,384 per node. The SDK sends cumulative SUBSCRIPTION_ACK frames.
A retention overflow sends RESYNC_REQUIRED and releases that descriptor. A full
socket send queue closes the socket; it never drops a change while leaving its
snapshot apparently valid. Unexpected cursor gaps also invalidate the
SDK snapshot.

A range fence ends affected source subscriptions with RESYNC_REQUIRED before
final handoff readiness. New subscriptions cannot enter that fenced range.
This is the explicit resnapshot handoff variant: no live descriptor is silently
left behind or represented as transferred to the target. The next subscription
learns the committed owner through normal redirects and obtains its snapshot
there. Independent ranges remain subscribed. SHORT/transport loss invalidates
snapshots; reconnect/resume likewise uses a fresh authoritative snapshot.
Permission revocation ends affected subscribed sessions.

Notifications are asynchronous. A writer's ACK on one socket does not mean an
update has already arrived on another socket. This is not a linearizable
cross-client read cache or a multi-object transaction mechanism. Once a gap,
owner change, revocation or socket loss is observed, an invalid snapshot is
never used as a current cache entry.

## Lifetime and resource ownership

The SDK transport registry contains `weak.Pointer[Subscription]`, not owning
references to subscription objects. `runtime.AddCleanup` sends FF UNSUBSCRIBE
when an abandoned object is collected. Its cleanup argument contains the client
and ID, never the subscription itself. `Close(ctx)` immediately invalidates the
local snapshot and awaits the server's unsubscribe barrier. If its context
cannot complete that barrier, FF cleanup still releases the descriptor. Late
frames are ignored after local removal.

Filesystem caches use an LRU with an explicit byte budget and a hard limit of
1,024 entries. Eviction and close release subscriptions proactively; GC cleanup
is the fallback for abandoned handles. There are no immortal goroutines or
strong transport references keeping each cache entry alive.

Concurrent cold reads of the same object share one subscription request. Cache
backend requests wait for capacity on their own routed connection (at most eight
filesystem requests in flight), so a paused owner does not hold slots needed by
an independent owner. Closing the cache cancels its outstanding backend requests
and wakes waiting readers. Cache hits do not enter this transport queue.

## FUSE

`stqr-fuse` enables a subscription-backed cache by default, with 32 MiB per
bucket. Set `--metadata-cache-bytes=0` to disable it. Nodes which do not implement
the feature are read through the normal native API without retaining an
unsubscribed snapshot. Range payload reads continue to use native range methods;
the kernel page cache remains the main file-content cache.

Kernel entry, attribute and negative lookup timeouts remain zero: no blind TTL
is used to hide metadata changes. Readonly opens can keep page-cache data only
when the same live subscription ID and cursor cover both opens. A changed
subscription or cursor drops that cache. Updates and subscription
loss also invalidate live FUSE inodes through a bounded notification worker;
root/overlay namespace changes conservatively invalidate all live nodes.
Callbacks never block the WebSocket reader on a kernel notification.

Readonly close avoids a redundant remote Stat. Completed writes still have
synchronous native acknowledgements. FUSE negotiates requests up to 1 MiB to
reduce per-chunk protocol and commit overhead; buffered reads can use kernel
readahead. Writeback caching is not enabled.

Overlay readers reuse a decoded manifest only while its freshly obtained root
subscription ID/cursor and root bytes match. The manifest is immutable to readers;
namespace edits and fsck work on copies. The retained decoded manifest is limited
to a 1 MiB encoded body. Copied-up lower ranges reference immutable data/index object IDs.
Subscriptions do not rebase an overlay after maintenance rewrites its lower.

## C and kernel clients

The C library now implements the same subscription/snapshot/update/ACK commands
through an explicitly polled callback API; it creates no background userspace
thread. The standalone kernel adapter uses that API on a dedicated connection,
with a bounded metadata LRU, subscription-proved dentry reuse and asynchronous
invalidation of clean page-cache/mmap data. See [the kernel guide](kstqr.md) for
budgets, reconnect behavior, fallback boundaries and isolated VM tests. The C and
kernel adapters still do not follow cluster redirects; subscriptions do not add
multinode routing to those adapters.
