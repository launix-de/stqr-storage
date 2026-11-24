# Exclusive bucket maintenance

Production nodes implement durable exclusion for a bucket across ALL admitted
members. The bound user needs READ+DELETE for filesystem maintenance, READ+WRITE
for writable block access, or READ for readonly block access. A bucket must be
active, without a policy transition or in-flight handoff, and its other client
sockets must disconnect. All members must be reachable and support the protocol.
Normal data access in other buckets continues while a lease is held.

Ordinary native block exports exclude competing clients, but permit internal
range migration. If a write cannot be placed locally, a bounded capacity hint
immediately wakes the range worker. The server returns retryable status 503
while the normal durable handoff runs; the SDK waits and retries the same logical
request ID, learns the new owner and continues under the same lease token.
When no suitable cluster destination can admit the write, status 507 remains a
terminal capacity error. This also applies when HDD placement initially prefers
a small local disk over a larger remote SSD.

Filesystem maintenance and private clear inventories still pin placement.
The durable lease's opt-in `AllowHandoff` flag distinguishes native block clients
from these operations. Old persisted leases without that flag stay pinned.
Normal membership and bucket-policy changes still wait for lease release.

The smallest admitted node ID coordinates acquisition. Participants drain local
mutations under `opMu`, validate the account/grants, and persist a fence in the
admin volume before acknowledging. Each bucket journal retains its membership
epoch and sequence high-water mark after release. The coordinator generates a
fresh random token for every grant, independently of the request ID. Token,
account, epoch and sequence bind auxiliary connections to the active grant.

Native messages:

| Opcode | Request | Reply |
| --- | --- | --- |
| `0x0115` BUCKET_MAINTENANCE | ObjectRef | `mode:u8=1, token:ID` |
| `0x011c` BUCKET_BLOCK_MAINTENANCE | ObjectRef, readonly:Bool | same |
| `0x011d` BUCKET_MAINTENANCE_END | ObjectRef; original owner only | empty, after ALL release |
| `0x202a` BUCKET_MAINTENANCE_STATUS | empty; administrator | empty after member probes; no allocation or lease |

Transport-only test fixtures retain the legacy empty maintenance response.
Ordinary object operations and on-disk data formats are unchanged. The updated
SDK keeps the original socket pinned and uses normal routing for object I/O,
adding `X-STQR-Maintenance: token` to authenticated auxiliary upgrades. Passwords
and bucket grants remain necessary; the header does not replace authentication.
The SDK stops on original-socket loss, while passive participant reconnects can
reuse the still-valid grant. Scan requests do not change the grant token.

Loss of the original socket, or restart of its node, triggers orphan release.
Participants retain their fence on restart. A timeout or missing peer does not
expire a lease. Partial acquisition rolls back through the same fenced release
path, and a failed release remains retryable. Older prepares/releases cannot
resurrect a retired sequence or clear a newer grant. Normal membership changes
and changes to the leased bucket's configuration wait for release. Explicit node
exclusion retains its external fencing requirement and retires preceding-epoch
leases on the surviving nodes.

Peer messages `0x1088` and `0x1089` are accepted only on verified admitted-node
connections. `0x1088` carries bounded JSON with action 0=acquire, 1=participant
reserve, 2=release, 3=participant release, 4=probe and the lease record. Coordinator
requests have their own bounded worker queue so draining mutations can still
receive quota/control callbacks, independently of long delegated copies.
`0x1089` carries actor ID, username, operation ID, opcode and body in the native
binary codec; its allowlist contains only clear and clone. It does not widen the
node-local admin proxy's allowlist.

Clear acquires an exclusive bucket lease. Its durable receipt prevents a lost
response or later retry from clearing newly written data.

## Immediate bucket copies

`BUCKET_CLONE` (`0x2029`, administrator) takes source and destination bucket IDs.
Create a fresh destination first: even retained tombstones make a used bucket
unsuitable. The dashboard's **Kopieren** action creates it with the source storage
policy and then sends the native clone command. Grants are independent and must
be configured separately. Retrying uses the same operation ID.

Clone publishes symmetric per-bucket connections through an ALL-member catalog
change. Existing clients may stay connected; short write fences drain prior
mutations. No object inventory or payload copy is needed up front. The reply's
`objects=0` counts objects physically copied during this initial operation.
Applications needing a consistent running database/VM image must flush or freeze
their own writers around clone.

Each opaque object ID is independent. Reads prefer an own record, falling back
through clone connections only when no record has ever existed locally. A
retained tombstone stops fallback. Before a mutation, servers materialize the
old logical contents into still-dependent direct neighbors; they also preserve
absence before creating a new object. The actual PUT/ranged CAS then runs under
the normal volume lock. Late materializations never replace an existing record.
No content version, frozen base bucket or frontend-specific interpretation is
involved. Private capture/read/materialize operations are restricted to admitted
cluster peers; a destination user needs no source grant.

Identical storage policies may remain connected indefinitely. Different storage
policies, later policy changes and bucket deletion schedule bounded background
materialization, followed by durable edge removal. Deletion hides the bucket
immediately but retains its backing until neighbors are independent. Interrupted
materialization safely retries after restart. Sparse transfers carry only
allocated extents, including for multi-terabyte logical objects. Inherited
subscriptions do not write storage. Inventory, fsck and logical quotas include
inherited objects; a copy's fsck never sweeps its source.

Tests cover two-owner range I/O and Sync, readonly grants, passive restart with
auxiliary reauthentication, owner-socket loss, delayed reserve/release, clear and
clone replay, and single-node receipt restart recovery. The opt-in real-QEMU
fixture also publishes, collects, fetches, clones, clears and deletes an image
through a two-node cluster:

```sh
go test -race ./server -run 'Test(ClusterBlockMaintenance|ClusterMaintenanceRetains|DaemonBucketCopyReceipt)'
STQR_TEST_NETWORKBOOT=1 go test -race ./server -run '^TestClusterNetworkbootNBDWithQEMU$' -count=1
```
