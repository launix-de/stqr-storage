# Exclusive bucket maintenance

Production nodes implement durable exclusion for a bucket across ALL admitted
members. The bound user needs READ+DELETE for filesystem maintenance, READ+WRITE
for writable block access, or READ for readonly block access. A bucket must be
active, without a policy transition or in-flight handoff, and its other client
sockets must disconnect. All members must be reachable and support the protocol.
Normal data access in other buckets continues while a lease is held.

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

Clear and clone acquire source/destination leases in ID order. Clone preserves
native object addresses and payloads, using at most 1 MiB of payload memory per
iteration. Operation receipts are committed through the global system catalog
before the first data mutation and after completion. Interrupted operations
remain explicit partial results. Retrying a completed clear cannot delete later
writes, including after restart or submission through a different node.

Tests cover two-owner range I/O and Sync, readonly grants, passive restart with
auxiliary reauthentication, owner-socket loss, delayed reserve/release, clear and
clone replay, and single-node receipt restart recovery. The opt-in real-QEMU
fixture also publishes, collects, fetches, clones, clears and deletes an image
through a two-node cluster:

```sh
go test -race ./server -run 'Test(ClusterBlockMaintenance|ClusterMaintenanceRetains|DaemonBucketCopyReceipt)'
STQR_TEST_NETWORKBOOT=1 go test -race ./server -run '^TestClusterNetworkbootNBDWithQEMU$' -count=1
```
