# Daemon, volumes and cluster development

Build the daemon in the repository root with Go 1.24 or later:

```sh
go build -o stqr .
./stqr
```

There are no administrative CLI subcommands. `stqr` is the daemon; it serves the
embedded dashboard and the administrative protocol on HTTP/WebSocket, default
port 7419. The integrated [S3 endpoint](s3.md) uses `/s3/` on the same HTTP
server, including access-key management in the user dashboard. TLS is intentionally absent. For remote administration use an SSH
forward, for example `ssh -L 7419:127.0.0.1:7419 your-node`, then open
`http://localhost:7419`.

The first start creates `stqr.yaml` with mode 0600, stable node/cluster IDs, a
cluster token and `auth.bootstrap_admin` username/password. Logs do not print
secrets. The initial admin additionally has a salted bcrypt verifier in an
ordinary object in the reserved system bucket, held on `volumes/admin`. Every
login is checked against that object; plaintext bootstrap values are not an
authentication fallback. Missing configured volumes or damaged account data
cause startup to fail instead of silently creating a replacement identity.

The admin volume also holds the bucket catalog and immutable, versioned bucket
descriptions. Its fixed list object contains sorted 32-byte hashes of those
descriptions. New nodes and existing accounts-only nodes initialize the catalog
with the system bucket; `bucket_catalog_version: 1` records activation in the
local YAML. Subsequent startup validates the catalog and fails on missing or
corrupt data instead of resetting it. Cluster admission publishes this system
graph through ALL/EVERYWHERE and the common system placement group. A known peer
or `--connect` never bootstraps a second cluster.

## Startup parameters

Only explicitly supplied flags override the configuration. Overrides are saved.
Paths in the config are resolved relative to the config directory / data directory;
created volume records store absolute file/device paths.

| Parameter | Meaning |
| --- | --- |
| `--config stqr.yaml` | YAML configuration file |
| `--port 7419` | HTTP/WebSocket port, preserving the listen host |
| `--listen :7419` | Complete bind address; `--port` overrides its port if both are supplied |
| `--name name` | Local display name |
| `--advertise http://host:7419` | Published contact address |
| `--data-dir .` | Local data directory |
| `--volumes-dir volumes` | Directory for file volumes; also `VOLUMES_DIR` |
| `--system-volume-bytes 1048576` | Initial admin-file capacity, not a fixed maximum |
| `--ram-bytes 0` | Optional RAM-volume budget in bytes; 0 uses available system RAM |
| `--ramcache-bytes 0` | Reserved for the later read-cache implementation |
| `--allow-subnet CIDR,CIDR` | Allowed IPv4/IPv6 source networks; empty allows all |
| `--connect http://host:7419` | First-contact helper; durable discovered peers remain authoritative after restart |

By default only `127.0.0.0/8` and `::1/128` may connect, even if the listener binds
to all interfaces. Access checks use the actual peer address, never forwarded
headers. Set subnets explicitly for direct intranet access. Network changes take
effect immediately; listener, directory and RAM-budget changes require restart.
The dashboard marks saved settings awaiting restart. Node/cluster IDs are immutable;
bootstrap secrets are never returned to the browser.

## Volumes

The node/volume tree includes per-node add controls, physical allocation,
operating/guaranteed reserves, capacity available for admission, health and drain
state. File volumes support a slider or case-insensitive sizes such as `50M` /
`50m`; the dashboard converts to bytes. The volume library receives bytes only.
Unmounted/unclaimed raw devices, LVM aliases and unclaimed files are discovered by
`volume.ListBlockdevices`; the constructor rechecks and exclusively claims the
actual target before formatting/opening it. Existing files are not implicitly
resized by add. RAM volume sizes are limited by available system RAM (`MemAvailable`) and, if positive, the remaining RAM budget configured at process start.

The admin volume is a regular file that automatically doubles when a system-object
write needs more capacity. There is no fixed configured maximum; actual backend
space/I/O failures are reported. `system_volume_bytes` controls only initial size.
Raw devices are never truncated or implicitly extended.

**Details → Größe neu einlesen** executes `VOLUME_RESCAN` (0x201d), also exposed
as `Server.RescanVolume`. It re-queries the backend (Linux `BLKGETSIZE64` for raw
blockdevices) after an external enlargement such as `lvextend`. It neither grows
the LV nor rereads a partition table. `volume.Rescan` adopts the additional space
by committing additional ranges in the free-space B+-tree. Device shrinking is rejected. `volume.Grow` is an explicit
file/RAM operation. Only admin files grow automatically, starting at 1M by default.

**Vergrößern** uses `VOLUME_GROW_INFO` (0x201f) and `VOLUME_GROW` (0x2020).
File and RAM sliders span current capacity through current capacity plus free
space; RAM also respects the remaining configured budget. Both input and slider
use the same discrete size steps. The server rechecks limits before growth.
Blockdevices use their entire current physical capacity with no size selector.

**Aushängen & entfernen** persists DRAINING and an operation ID. The local
coordinator copies live data, sparse extents, versions, tombstones and retry
records to active compatible targets (HDD may use SSD; SSD never HDD). Without a
safe target it waits, keeps the source attached and retries when capacity becomes
available. Shutdown leaves the drain resumable. Only after destination commits
and config publication is the original device claim released. Files/devices are
not unlinked or wiped; they become discoverable again. The currently required
admin volume cannot be detached in this first stage.

Production Open uses the durable controller even for a single member. Cluster
range handoffs, telemetry and shutdown coordination are described below. Data
replica completion and subscription handoff remain separate work; no transport
heartbeat is treated as a quorum or timeout-based eviction.

## Original WebSocket protocol and queues

The dashboard logs in through `POST /api/session` using HTTP Basic and then uses
an HttpOnly, SameSite=Strict session cookie. The WebSocket requests `stqr.v1` and
passes the CSRF token in a second subprotocol. Origin, source subnet and current
account/session state are checked at upgrade; text WebSocket messages are not accepted.
The connection caches authenticated admin/cluster-peer flags. Account changes,
logout, cookie expiry and subnet changes invalidate affected socket capabilities
instead of performing account lookups on every message.

Administrative controls build binary buffers in `dashboard/protocol.js`, using
the same envelope, little-endian primitives and canonical varints as `client`.
There is no dashboard-only REST control channel. Volume add/remove/rescan and
configuration edits all pass through the original message dispatcher. HTTP
`/api/message` and JSON volume add/remove are compatibility entry points to those
same handlers. Requests preserve their operation IDs and volume operations store
persistent dedupe/outcome records in the admin volume.

Each node transport session has a bounded receive queue (16 messages), an answer
send queue (32 messages) and one replaceable **complete snapshot batch**. Reading,
dispatch and socket writing are separate loops. Requests get a correlated response;
fire-and-forget does not. Queue overflow closes an overloaded connection rather
than allocating unbounded pending work. Statistics use original `STATS_SUBSCRIBE`,
`STATS_SNAPSHOT`/StatsPart and `STATS_UNSUBSCRIBE`, immediately and every 3 seconds.
The Go collector is shared with the future generation-builder interface. Snapshot
parts carry SHA-256 and are fully verified/assembled by the browser before render.
The JavaScript sender also has a bounded queue and correlates request promises by ID.

Additional local administrative messages are defined in the architecture spec:
0x201a config get, 0x201b typed field patch, 0x201c volume discovery, 0x201d rescan, 0x201e available filesystem/RAM capacity, 0x201f growth bounds, and 0x2020 manual growth.
`BUCKET_INFO` (0x010c) accepts `name:Text` and returns the current `Policy` plus
`permissions:u32` over both HTTP and WebSocket. Rights come from the authenticated
account and bucket grants; locked/deleting buckets advertise read rights
only. Ordinary users cannot inspect the system bucket. Unknown names return 404,
missing grants return 403, malformed bodies return 400 and an unavailable/corrupt
catalog returns 503. Grant revocation invalidates affected WebSocket capabilities
before the control change is acknowledged. HTTP requests authenticate individually.
Bucket instances are loaded into RAM maps by ID/name at
startup. A successful catalog write updates the existing RAM instance only after
its description and replacement catalog are synced. Object access does not scan
the catalog on disk. The native client supplies its fixed bucket ID in the
`X-STQR-Bucket` upgrade header; missing buckets or grants fail before upgrade.
The socket stores the active Bucket pointer and effective rights. Data requests
use that capability without user/ACL lookups and cannot switch buckets, even
when the same account has access to both. Bucket-bound sockets expose object
operations only; the dashboard's unbound admin socket exposes administration.
Non-admin user sockets must select a bucket. Peer sockets authenticate once with
`node-<32-hex-ID>` and the cluster token and cache a separate peer flag; they may
use peer/statistics messages but cannot administer accounts/buckets. This grants
no membership or ownership: NODE_HELLO binds the transport identity; JOIN and
distributed control remain unimplemented. Targeted volume administration is a
separate allowlisted peer RPC and revalidates the initiating admin at the target.
Catalog corruption is checked on restart, without rereading all descriptions
for every request.

Bucket administration implements the specification's `BUCKET_CREATE` (0x2003),
`BUCKET_SET_POLICY` (0x2004), quota/rename/lock (0x2005–0x2007), deletion (0x2008)
and user grant/revoke (0x2009–0x200a). `Grants` is the persisted per-bucket user
sub-array of stable account IDs and permission bits; credentials remain solely in
the account objects. These operations require ADMIN and a nonzero request ID.
Their receipts commit with the changed description, so a lost reply can be
recovered without repeating the operation. The system bucket is protected.

`BUCKET_LIST` (0x2022) is the dashboard integration point. Its body is
`after_name:Text,limit:u32` (1..1000; empty name starts at the beginning). The
result is one `Text` containing `{buckets:[BucketDescription...],next:"..."}`.
Pass `next` as the next `after_name`; empty `next` ends the list. Deleted buckets
and private retry receipts are omitted, and pages respect the message budget.
The dashboard follows pagination and renders the bucket descriptions, policies and grants.

Deletion publishes a blocked bucket before a resumable worker tombstones its
local data. Config changes of nonempty disk buckets copy/recompress data into
the requested local layout before making it effective. Both return 202 while
running; `ADMIN_OPERATION_STATUS` reports their durable completion. Workers
resume after restart. New writes wait with 503 during a policy transition;
reads continue from the current placement. Old unreferenced physical copies
remain for later compaction/cleanup. A volume drain persists replacement bucket
locations before detaching the source.

Native GET/GET_RANGE/STAT, PUT/PUT_RANGE, APPEND, TRUNC, DELETE and CAS now dispatch
to methods on `Bucket`. They enforce authorization, configured codec and logical
absolute quota. HDD buckets may use SSD; SSD buckets never fall back to HDD, and
user data never falls back to the admin volume. Atomicity, disk flushes and
request-ID deduplication come from the volume engine. The client connection
remains bound to URL, username, password and bucket ID. Streaming uploads are
staged and checksummed before publication (four transfers/1 GiB per connection);
downloads spool a consistent snapshot (at most 1 GiB per request) before streaming, keeping network stalls
outside volume locks. A learned single-node RAM runtime is retained across client
reconnects; requests from a lost runtime return 410.

Multinode replication, guaranteed-capacity reservations, finite physical bucket
quotas and nonempty RAM/disk conversions remain unsupported and return 501.
There is no fabricated redundancy/quota success or implicit user bucket creation.

The working tree remains uncommitted until the requested UI/volume-management review.

The size slider uses discrete decade steps within K/M/G/T/P units: 1M..9M,
10M..90M, 100M..900M, then 1G, and so on. Slider positions are fixed indices into
these steps; dragging never rescales the maximum. Text input updates the selected
step immediately (including an exact custom size); dragging writes a K/M/G/T/P
literal back to the field. The final endpoint is the exact page-aligned free-space
limit, fetched with VOLUME_CAPACITY_GET. File capacity uses Statfs.Bavail (df's
available space), RAM uses MemAvailable. New file/RAM sizes are revalidated by the
server when adding. Existing files and blockdevices show their full fixed size
with no slider. Blockdevice add always uses the actual backend capacity.

## User management

The dashboard uses binary `USER_LIST` (0x2021), `USER_CREATE` (0x200b),
`USER_SET_PASSWORD` (0x200d), and `USER_DELETE` (0x200e) on the existing queued
WebSocket connection. Listing returns `Vec(User)` (ID, name, role, disabled),
never password hashes. Creation uses the specification's `User,password:Text`;
the dashboard creates USER accounts by default, with an Administrator checkbox
for creating ADMIN accounts. Role dropdowns in the list send USER_UPDATE
(0x200c) immediately. The current user cannot delete, demote or disable their own account; their
inline role selector is disabled regardless of other administrators.
Role changes preserve the password and revoke existing sessions/capabilities;
self role changes are rejected server-side. A USER dashboard login reports missing admin
rights distinctly from an incorrect password. Self-deletion is rejected by the
server as well as disabled in the UI. Password changes invalidate existing
credentials/sessions, including the administrator's own session when applicable.
Account updates and retry receipts commit atomically to the admin volume; only
bcrypt verifiers and keyed request digests are stored, never plaintext passwords.

Browser sessions are process-local. After a WebSocket disconnect the dashboard
checks HTTP session validity: expired/revoked sessions return to login, while a
new session cookie refreshes the CSRF binding. Temporary network failures keep
reconnecting; callbacks from an old connection cannot mark a new one offline.

Sidebar badges show volume, bucket and user counts. Bucket/user counts load
immediately after login and refresh after local changes.

The Buckets view lists policies and user grants. It supports creation, settings,
quota changes, rename, lock/unlock, deletion, and editing/revoking per-user rights.
Clicking a bucket name or row opens its settings directly. Per-user permissions
and write protection live in that same settings form, rather than separate table
actions. Write protection is a checkbox. The access table has one row per user,
checkboxes for read/write/append/delete, a plus action to add users, and a remove
action. These access/write-protection changes save immediately; failed requests
restore the displayed state and retain their request ID for retry. Write
protection blocks mutations while keeping objects readable.
The Buckets view has global summary cards for physical capacity, bucket count,
read throughput and write throughput. Capacity/used/free and throughput reuse
exactly the same pushed node snapshot and time window as the Volumes view,
including system-volume overhead; no additional polling or I/O scan is needed.
Bucket count/state includes the complete list, unaffected by the search filter.
Per-bucket quota bars remain separate logical-accounting values.

Finite logical quotas show a live used/limit bar in the bucket list, refreshed
every three seconds while the view is open. BUCKET_LIST includes optional live
`used_bytes` values from the same logical accounting as quota admission; these
are not persisted in bucket descriptions. Usage errors show an unavailable value
rather than an empty bar.
All actions use the existing queued binary messages. Quota-only edits use
BUCKET_SET_QUOTA to avoid an unnecessary policy migration. Pending policy/deletion
work refreshes every three seconds until complete. System-bucket mutations are
disabled in the UI and rejected server-side.

After creating a user with “Gleichnamigen Bucket anlegen” checked, the same bucket
form opens with that name and user selected. Confirmation first creates the
bucket, then grants all four object permissions to the selected user. Each step
retains its request ID across retries. If the grant fails after creation, the
form explicitly reports the partial result and retries only the grant; it never
creates a second bucket. Closing that form leaves the created bucket intact.

Quota inputs use exact uint64 byte counts, including zero. Unsafe JSON integer
tokens in bucket pages are preserved before parsing so policy versions and quotas
are not rounded by JavaScript. More than one node replica, guaranteed reservations,
and finite physical quotas remain unavailable in the form until server support.


Bucket forms provide a discrete two-way quota slider and Maximum action.
BUCKET_QUOTA_CAPACITY (0x2023) returns the current physical free/reserved capacity,
the logical maximum and calculated typical-object-size profiles. There is no
editable metadata percentage. The 1M (Blockdevices) profile is the default for
new buckets. Capacity currently covers the single local node; peer aggregation
is an explicit extension point. Existing usage is included in an edited bucket's
absolute maximum. Exact logical values and slider endpoints stay BigInt values.
# Offline filesystem collection

The bucket-bound WebSocket supports BUCKET_MAINTENANCE (0x0115) and BUCKET_SCAN
(0x0116) for `stqr-fuse --fsck`. Maintenance requires read/delete grants, an active
bucket without pending policy transition, and no other connection to that bucket.
It blocks new bucket connections, other HTTP object accesses, reference-creating
writes and bucket configuration mutations until the owner's request loop ends.
The owner can read, scan, use CAS for namespace recovery (with its normal write
grant), and delete. It must abort on transport loss and start a
new complete traversal; maintenance ownership does not survive reconnect/restart.
These restrictions apply only to the offline maintenance session. Ordinary FUSE
reads do not persist synchronization records. The scan is hash-ordered and merges
versions/tombstones across local copies before returning live records. Tombstones
can advance a cursor without returning live entries. The daemon does not decode
filesystem formats or determine reachability; the client performs mark-and-sweep.

## First multinode preparation: rebuild race measurements

`client/rebuild.go` implements the specified REBUILD_OFFER (0x1004) and
STATS_FRAGMENT (0x1007) payload codecs with round/identity checks, bounded fragment
sizes and counts, and strict decoding. These offer/fragment primitives remain
separate from the implemented single-primary scoped handoff path (0x1050–0x105f).
The general race-election algorithm is deferred pending stress measurements.

`server/rebuild_race_test.go` is a deterministic event-scheduler baseline. It
uses those codecs and real serialized collector snapshot layouts, models 2/3/5/9/32
nodes, delayed/crossing offers, fragmentation, duplicates and pause/reconnect, and
records message/byte counts and seed-reproducible potential binding conflicts.
A candidate with every valid snapshot is merely a *potential* binding in this
model; it never acquires production authority. No grants, abort rule, membership
eviction, persistent binding or chosen coordinator algorithm is implemented.

Run it with `go test ./server -run '^TestRebuild' -v`. The initial seven scenarios
use 200 seeds each. In the five-node `crossing_offers` baseline, 10/200 seeds
allowed more than one potential binding; seeds are 4,21,57,62,83,114,132,151,153,161.
This exposes the need for a defined binding boundary and for communicating that
boundary before a later competing offer can gain authority. It is not evidence
for any particular final election protocol. The explicit two-node regression
constructs the same ambiguity without randomness.

Ticks are virtual scheduler time, not measured network milliseconds or cluster
convergence latency. `BenchmarkRebuildOfferBaseline` measures the CPU/allocation
cost of this simulation, not the throughput of real peers. Real socket/network,
persistent decision, process-crash and multi-host stress tests remain necessary
before choosing and accepting the coordinator algorithm. Join, CURRENT/NEXT,
range routing, ownership fencing and ALL replication remain unimplemented.

## Placement groups

`PlacementGroup.go` implements immutable compiled effective masks, stable IDs,
all/blacklist/whitelist filters, default-all semantics and independent desired
masks. Unknown group/node and empty whitelist fail closed. Offline identities are
kept without probing device availability; a newly attached volume is implicitly
eligible for all/blacklist, and needs explicit inclusion for whitelist. Bucket
object selection and quota preview use the same effective mask. Changing an
ordinary native policy preserves its existing PG reference.

The system bucket stores a reference to its metadata PG and retains EVERYWHERE.
Each node's metadata volume is selected by the PG whitelist through the same
resolver as any other bucket. Internal account/peer/PG objects and catalog opening
use that resolver, with one committed local metadata store in the current engine.
The exclusively claimed root volume is a bootstrap anchor for loading the PG
object itself, rather than an unconditional bucket-ID/physical-volume override.
Existing system descriptions are migrated once without replacing user accounts
or payloads. Missing/corrupt established PG metadata is an error, never a reset.

Cross-node desired/effective PG changes use the durable global control path;
range handoffs evacuate placements before the desired mask becomes effective.
Native PG list/set/delete and bucket-PG messages are available. A dedicated
PG editor in the dashboard remains pending.

## Multinode acceptance gate

The regular daemon now connects/reconnects configured/discovered authenticated
contacts. `--connect` is only an initial helper; the `peers` system object is the
authority for discovered addresses and the YAML peer list is a derived cache.
Peers broadcast their directories after connecting and relay set-union updates;
this never admits members, changes ownership or revives excluded membership.
`TestPeerDirectoryReconnectsWithoutOriginalInitiator` stops three nodes, erases
the returning nodes' YAML peer caches, and reconnects two nodes while the old
initiator remains stopped. This proves discovery/telemetry recovery, not complete
storage availability or committed membership recovery.

A prepared candidate has `join_pending: true`, no new bootstrap administrator and
no normal browser/client authentication. HELLO advertises the PendingNode bit;
NODE_HELLO_ACK has epoch/generation/revision zero and no invented singleton
membership. Its statistics likewise report no committed placement. It is visible
as JOINING and may prepare
local volumes through an authenticated active peer. The candidate verifies the
actor against that peer's live account authority over the reverse direction of
the same socket (0x1037); it does not use copied/stale accounts. Durable membership,
EVERYWHERE account/policy replication and activation still remain to implement.

`startPeerObservation` subscribes to LOCAL stats every three seconds, validates
fragments/digest/source/boot/sequence and relays only complete snapshots to CLUSTER
subscribers. Disconnect retains the last snapshot; a replacement connection cannot
be overwritten by the old observer. Both Go and JS assemble interleaved sources
independently with bounded buffers. WebSocket telemetry coalesces one complete
pending batch per node, preserving fair service to other nodes. The dashboard
retains per-node samples, sums physical capacity and computes throughput from
each node's own deltas; old samples are visibly stale, never membership removals.

`NODE_ADMIN_REQUEST` (0x2024) targets a node ID using the original opcode/body and
operation ID. `PEER_ADMIN_REQUEST` (0x1036) delegates the actor identity on the
authenticated peer link. An active target looks up the current admin account; a pending candidate verifies
it with the referring active peer. The target owns its local config and retry receipts. Proxy recursion and bucket/user/object
commands are rejected. The browser tree has add/grow/drain controls for each
node; drain still uses the local evacuation implementation until the rebuild
controller replaces it with committed cross-node placement.

Checks implemented for this path:

- Native peer statistics pushes, dashboard subscription, disconnect retention
  and new BootID after target reopen (`TestPeerStatsPushDashboardAndReconnect`).
- Remote volume add/grow, duplicate retry, target-local receipt and current role
  validation (`TestNodeAdminTargetsVolumesAndPreservesReceipts`).
- Opt-in Chrome dashboard check: two node rows, add a file on the remote node,
  grow it there, and assert the source did not acquire that volume
  (`TestDashboardPeerVolumesBrowser`, `STQR_TEST_BROWSER_MODULE` selects Playwright).
  This test now uses ordinary configuration/advertisement and automatic discovery;
  it neither manually dials/attaches observers nor copies account images.

These checks do not establish extension, crash-safe rebuild or cluster SHORT.

The preserved single-node baseline is commit `71c7d09` on `origin/master`.
The current implementation work is on branch `multinode`. Do not announce the
cluster as ready for user testing until all of the following work together:

- Statistics heartbeats and an all-node dashboard, including stale/paused nodes.
- Administrators can discover/add/grow/drain volumes on any node. A node owns
  its volume configuration locally; the aggregated list is administrative data.
- Persistent rebuild rounds, unique coordinator binding, CURRENT/NEXT planning,
  copy/catch-up/fencing/cutover/cleanup, recovery, and queued `needrebuild` runs.
  Rebuild binding is per affected current range groups: disjoint scopes may run
  concurrently, including on overlapping node sets. Global membership/admin
  decisions retain their separate ALL barrier and decision chain.
- Required: one bucket/client stores more data than fits on one node, using
  learned range routing. Data replication is optional for this milestone.
- SHORT/update shutdown preserves placement/data; unaffected nodes keep serving.
  Only dependent operations wait for the paused owner; replica reads are preferred
  when replication is present. No timeout-based membership removal.
- SIGTERM, SIGINT, PM2 stop/restart and normal Linux shutdown always use SHORT;
  they never initiate node or volume drain. Only an explicit authenticated admin
  command/dashboard action requests DRAIN. Previously requested drains remain
  resumable after restart.
- DRAIN shutdown migrates a departing node empty before removal.
- Volume drain is scheduled in the next rebuild and releases the volume only
  after its needed contents have moved and ownership is safe.
- Real multi-process/multi-host checks cover extension, rolling restarts, node
  and volume drain, concurrent object mutations and coordinator interruption.

Peer identity/telemetry transport alone is not membership admission or a working
rebuild. Model scheduler results are not real cluster availability measurements.
The user's existing local volumes and accounts must survive upgrades; use
separate fixtures for destructive/failure tests.

## Durable daemon control and range handoff

Production `Open` activates the global controller, immutable system checkpoints
and separate node-local `control.stqr`/`ranges.stqr` journals. Bootstrap commits
its own system state; discovered candidates join through the ALL participant
barrier. User/bucket admin changes from any member prepare an isolated metadata
draft and publish only after commit. Local volume configuration remains local.
The explicit transport-only test helper does not describe production admission.

The complete accounts/PG/bucket/map graph is validated before READY. The system
head precedes APPLIED; replay reinstalls that head into fresh runtime memory.
A requester is durable before its first remote reservation. Unprepared recovery
cancels all bound participants; prepared recovery finishes the same decision.
Proofs remain available after release and voluntary membership departure.

Single-primary ranges have independent durable source/target transitions.
Disjoint scopes may proceed without an unrelated offline member. A plan claims
its complete old parent, splits only the selected path, bulk copies data, fences
source mutations, catches up canonical versions and retry receipts, then commits
ownership. Old physical records are purged only after target commit. Cleanup
never invents a higher-version tombstone. Global admin checkpoints collect
committed scoped archives under ALL reservations before capturing the map.

The automatic planner uses class, PG masks, active physical capacity and local
node/volume drain intent. It prefers cheaper, including empty, halves and reacts
to physical admission pressure for concentrated object hashes. Physical import
reservations prevent ordinary writes from consuming another handoff's capacity;
reopening reconstructs them from durable plans and canonical target indexes.
Transfer chunks are at most 1 MiB. Temporary payload files and paginated index
walks bound memory. Per-object copy memos live in a private indexed volume,
not an unbounded RAM map or permanently appended per-object journal records;
completed memos are reclaimed in batches.

Declined offers are durably cancelled on both participants before releasing
source intent; delayed RESERVE cannot revive them. A fresh attempt ID permits
replanning the same unchanged parent. Aborted staged copies/receipts and transfer
memos are reclaimed in bounded pages while protecting newer owners/active scopes.
Bucket quota/policy claims bind durable per-bucket mutation fences. Global quota
shrinks sum fresh authoritative usage from every reserved participant; abort and
release clear the fence without blocking unrelated buckets or reads.
Desired class/PG changes advance the current primaries' own ranges through
scoped handoffs; reads resolve each range against its effective/desired policy
version. The global policy is activated only after all ranges reach the desired
version. Suitable existing local targets are preferred, and their canonical
objects use atomic codec rewrites rather than purge/reimport. Pure codec changes
run under the global metadata barrier without unnecessary ownership movement.

A committed bucket DELETE intent also drives idempotent cleanup on every member;
the follow-up global checkpoint marks deletion complete only after all local
object and retry indexes are empty. Tombstoned catalog identity survives restart.

The SDK learns validated partial maps, authenticates destinations in the same
cluster, and maintains independent routed connections. A paused owner does not
block known independent ranges. RAM requests bind a range runtime before their
first mutation; a sole RAM copy lost on restart commits an empty new runtime
before serving again, and old-runtime retries are rejected.

SIGTERM/SIGINT and native NODE_SHUTDOWN mode SHORT persist PAUSED, stop new local
mutations, flush and send GOAWAY. Placement and membership remain. An explicit
NODE_DRAIN evacuates ordinary ranges and requests membership removal through a
persisted surviving coordinator. The leaver retains its bound metadata anchor
only for barrier recovery after its active system PG placement is removed.
It stops after ALL release/request completion; `drain_complete` prevents an
unattended restart from silently creating/rejoining a cluster. An explicit volume
drain releases its device claim only when no map/active handoff references it
and both object and retry indexes are empty. Files are never unlinked by detach.
The dashboard offers native delegated volume management, node maintenance and
node drain; no separate administration CLI is introduced.

Integration evidence uses disposable local daemons and real HTTP/WebSocket
connections, separate from deployment or performance claims:

- `TestDaemonControlBootstrapAndAutomaticJoin`: admission, administration from
  both members, reverse restart, waiting SHORT reads, exact persisted requester
  recovery, bulk-copy writes, final catch-up, APPEND retry identity and cutover.
- `TestDaemonAutomaticExtension`: 6 MiB through one client on two 4 MiB data
  volumes, automatic distribution, volume evacuation/detach, node departure and
  restart after APPLIED while ALL release is deliberately withheld.
- `TestDaemonExtensionWithConcentratedObjectHashes`: capacity handoff despite all
  new hashes sharing the same initial half; authoritative usage excludes copies.
- `TestDaemonCodecPolicyConvertsEveryOwner` and
  `TestDaemonStorageClassPolicyMovesToEligibleOwner`: distributed physical codec
  changes, combined HDD->SSD/raw->lz4 handoff, unchanged versions and restart.
- `TestDaemonBucketDeletionReclaimsEveryNode`: replicated DELETE intent, physical
  object/receipt cleanup on both members, map removal and restart persistence.
- `TestDaemonNativeShortShutdownCommand` and the process signal tests: native
  command/operation status, GOAWAY, unchanged ownership and persistent data.
- `TestDaemonRAMRuntimeResetFencesOldRetries`: lost single RAM copy, durable
  runtime advancement, fresh access and rejected old retries.
- `TestDaemonRAMNodeDrainPreservesLiveRuntime`: live RAM evacuation, unchanged
  content/version/runtime through the existing client, and loss of the last RAM
  copy after its new owner restarts.
- `TestDaemonRAMHandoffCrashRecovery`: process loss before sealing, after target
  READY, after source COMMIT and after target COMMIT; sealed copies survive a
  source restart, unsealed shadows never become replicas, and lost committed
  RAM is never restored from an obsolete source.
- `TestDaemonClusterShutdownAndReverseRestart`,
  `TestDaemonClusterShutdownWaitsForEveryReadyAfterCrash` and
  `TestDaemonClusterShutdownMissedStopWaitsForReconnect`: ALL-READY before STOP,
  persistent mutation fences, reverse restart, missed STOP redelivery and
  idempotent old certificates. The dashboard uses NODE_SHUTDOWN mode 2.

Native clients process GOAWAY before retrying pending requests. A departed
primary invalidates only its old routing segments; independent/newer segments
remain. SHORT retains placement. A broken cached connection refreshes routing
through another known member without declaring the original owner failed.

Native object inventory and the S3 adapter's inventory merge bounded pages from
the bucket's current owners. Per-owner map stamps reject a half-applied handoff;
tombstones advance cursors without leaking deleted entries or skipping remote
objects. A required offline owner prevents a successful partial list, while
unrelated object reads remain available. Delegation revalidates the current user
and ACL on the target and cannot access the system namespace. The merge retains
at most one result page plus one candidate page, independently of node count.
`TestDaemonInventoryMergesTombstonesAndWaitsOnlyForOwners` covers paginated live
objects on both nodes, deletions, SHORT waiting and reconnection.

Multi-node S3 requests use the native bucket SDK, including adaptive maps,
streaming, retry identities and distributed inventory. A private node frontend
connection binds the bucket, original HTTP grant, current account revision and
a random live-operation ID. The target verifies that operation with its admitted
issuer over the authenticated peer channel. A cluster token and catalog IDs
alone cannot invent an ordinary user session. These connections cannot log into
the dashboard or HTTP administration API. They are closed with the HTTP request;
source shutdown cancels them, and DRAIN waits for active frontend operations
before withdrawing their issuer. A departed issuer invalidates its sessions.
`TestDaemonS3ExtendsAndReadsThroughEitherNode` stores six MiB on two four-MiB data
volumes, reads through the other endpoint, lists both owners, checks HTTP 507 on
physical exhaustion and exercises write-only/delete-only HTTP grants.
`TestFrontendCredentialsStayPrivateAndBucketBound` covers forged operation IDs,
wrong credentials/bucket, administrative HTTP denial and account revocation.

Streaming mutation completion returns the same adaptive redirect headers as an
inline mutation. Epoch/runtime bindings are checked again at the final storage
access; a staged upload cannot silently enter a new RAM era. Streaming writes
into an inherited RAM range report that range's runtime, not the target's boot.
`TestStreamingPutFinishesWithMapRedirectAfterHandoff` pauses a two-MiB upload,
commits handoff, then verifies the 301 patch and identical-ID retry at the target.

Finite logical quotas now use durable per-node growth credits. Credit transfers
flush the donor debit before transmission, then flush the recipient credit before
acknowledgement. They have no timeout-based refund. Range handoffs carry their
sealed live-byte total into both READY records and transfer that used budget at
COMMIT. ALL checkpoints exclude unfinished grants/handoffs and retain per-node
quota/range journal watermarks; changing limits creates a fresh quota epoch.
Existing local credits can be spent while another node is SHORT-offline.
The quota peer operations are 0x1060–0x1063; object-client Map/Policy bytes retain
their existing schema. Physical exhaustion still independently returns 507.
`TestQuotaTransferCrashWindowsConserveCredits` reopens both journals at DEBIT,
RECEIVE and ACK boundaries. `TestDaemonFiniteQuotaAcrossOwnersAndRestart` covers
parallel writers, hard exhaustion, a global checkpoint, reverse restart, remote
credit reuse and an unrelated offline owner. `TestDaemonQuotaMovesLiveBytesAndChangesLimits`
covers populated-range handoff, global shrink validation and quota-epoch ABA.

PG administration and desired-mask activation have native operations 0x2030–0x2033
and matching JavaScript wrappers. `TestDaemonPlacementMaskTransitionsKeepOldAndNewReadable`
covers HDD and RAM, restart during transition, retirement of old storage, safe
unassignment/deletion and create-request retries without resurrection. A PG
editor in the dashboard has not been built yet.

The complete cluster milestone is still pending. Guaranteed physical capacity,
finite physical bucket quotas and recovery of a pending global control decision during forceful loss remain. The broader
acceptance gate is not closed by the focused two-host test alone.
Data replication is optional for the requested milestone and is not advertised
as implemented. A transport-only heartbeat or two dashboard rows is insufficient
evidence for a working storage cluster. No race-election algorithm is inferred
from these tests; that remains subject to the requested stress measurements.


## Opt-in two-host acceptance

`TestSSHClusterAcceptance` runs a temporary local node and a temporary daemon on
an explicitly supplied SSH host. It copies the candidate into a unique `/tmp`
directory, uses new identities and volumes, and never restarts or reformats the
existing PM2 deployment. All admin mutations use native binary messages.

```sh
go build -race -o /tmp/stqr-cluster-candidate .
STQR_TEST_SSH=stqr \
STQR_TEST_LOCAL_IP=192.168.178.108 \
STQR_TEST_REMOTE_IP=192.168.178.34 \
STQR_TEST_BINARY=/tmp/stqr-cluster-candidate \
go test -race ./server -run '^TestSSHClusterAcceptance$' -v -timeout 180s
```

The test has passed over these two real hosts: six MiB in two four-MiB data
volumes, remote volume creation, cross-host reads with unchanged versions,
finite quota measured from both owners, production SIGTERM/SHORT, full CLUSTER
STOP with the former joiner starting first, remote volume evacuation/detachment,
node departure and continued use of the original client connection. All test
processes and temporary remote data are cleaned up. This is correctness evidence,
not a throughput measurement or a claim of implemented data replication.

Local logical usage is counted once per committed placement image, then adjusted
under the same storage-commit boundary as mutations. Repeated operations do not
charge twice. This uses one scalar per bucket, not an object-sized RAM index.
Warm administrative usage reads use 0x1064 with current owner-map stamps; they do
not stream every object record on each dashboard refresh. Physical reservation
promises continue to use separate admission proofs, never these measurements.

The volume engine accepts a bounded `GuaranteedAllowance` independently of
`InFlightAllowance`. Both are charged exactly once against actual physical net
growth; another bucket's reserve remains protected. `UseGuaranteed` is a trusted
whole-reserve option and must not be used to share guarantees between buckets.
`TestGuaranteedAllowanceProtectsOtherBucketsAndImports` and
`TestCombinedImportAndGuaranteeAllowanceChargesOnce` verify isolation, physical
counter sums, failed writes and request replay. Full bucket guarantee allocation
is still unsupported; this engine primitive alone does not promise capacity.


## Explicit exclusion recovery

Native FORCE_DISCONNECT (0x2013) binds a canonical victim list, the expected
membership epoch and explicit data-loss/external-fencing confirmations. Every
survivor must participate; transport loss alone never removes a member. The
lowest surviving ID coordinates and 0x1065 delegates from other admins. Capability
64 identifies range recovery; capability 128 gates the extended global recovery path. Existing complete operation receipts remain valid
after the epoch advances and after restart.

An ALL-survivor promise pauses ordinary scoped transitions. 0x1067 collects
bounded participant pages and durably resolves interrupted offers. A surviving
source retains uncommitted data; a sealed orphan target can be promoted through
the new immutable system graph. A known COMMIT is retained, never rolled back to
an obsolete source. If both participants survive, the exact target COMMIT is
completed through 0x1066 before quota checkpointing. An unfinishable committed
participant claim is retired without erasing its historical proof. Cleanup
rechecks authority immediately before each physical deletion. Unsealed fragments
are not recovered as acknowledged data.

Finite quotas are reinitialized from the planned ownership image, including
promoted sealed bytes and excluding newly LOST stale copies. Old-epoch stranded
grants cannot spend against the new allocation or block later administration.
LOST absence returns 410; full PUT or explicit DELETE can establish a new known
state. Partial operations cannot invent missing prior bytes. Historical PREPARE
returns its saved READY without sweeping later repairs.

Tests cover bulk interruption, READY, source/target COMMIT, delayed old requests,
restart, stale object and retry-record removal, lost quota acknowledgements,
three-node delegation and a source-only COMMIT between surviving participants.
The last case also checks a separate bounded admin queue: otherwise a delegated
coordinator and its caller could wait for callbacks on the same dispatch worker.
The optional NODE_HELLO_ACK boot identity binds reverse peer traffic to the
actual process while leaving canonical membership digest bytes unchanged.

A retained, restarted RAM source can finish its exact offer from a sealed live
remote target under the exclusion fence, retaining the original runtime and
counting the recovered bytes in its fresh quota allocation. The corresponding
three-node restart test passes repeatedly.

Global control recovery inspects durable COMMIT witnesses (0x1068) before a new
exclusion is constructed. 0x1070 replays that exact proposal and ALL-READY
certificate, including administration, shutdown, joins, cooperative departures
and a previously committed exclusion. Bound joining candidates are inspected;
a COMMIT visible only there is retained. Admin demotion by the parent takes
effect before the new exclusion. Extended claims retain the original observed
epoch and operation ID through membership recovery. Applied parent slots are
archived durably when a later FORCE supersedes their unfinished release, so old
operation receipts remain answerable after restart.

Every survivor durably records the construction barrier (0x1069) before range
recovery can mutate private staging. Cancellation before that barrier restores
the suspended control slot and requester. Pending retries after local APPLY
resume the original exclusion instead of creating another membership change.
Tests cover these transitions and both living/lost earlier coordinators.
Still pending: superseding an uncommitted earlier FORCE construction or a bound
uncommitted join; these paths reject or wait without discarding sealed state.
No real deployment has been force-disconnected for these tests.

The dashboard seeds offline member rows from the committed system image. It
retains departed/excluded reports for inspection but excludes their capacity
from current cluster totals. `NODE_CONFIG_GET` adds `member_nodes`,
`excluded_nodes` and decimal-string `membership_epoch`; no browser u64 rounding
is allowed. Statistics bit 9 (`GLOBAL_CONTROL`) exposes a pending control round
through a small immutable activity snapshot, without taking the ledger lock
under the node configuration lock.

The dashboard's explicit exclusion dialog uses native 0x2013, requires both
loss and external-fencing confirmations, retains its operation ID/observed epoch
on ambiguous outcomes, and polls the original operation while it is running.


## Integrated multipart and copy routing

The merged S3 implementation uses `openS3` for ordinary HTTP operations,
CopyObject/UploadPartCopy sources and background multipart recovery/expiration.
Every path routes through bucket objects when there are multiple members;
private frontend credentials keep the original account/permission and are
released at the end of the operation. A temporarily unreachable credential
issuer returns 503 instead of a false 401 during SHORT/reconnect.

`TestDaemonS3MultipartCopyAndExpiryAfterMigration` moves an open multipart upload
to the other node, completes it through the former owner, copies its content,
recovers an interrupted publication after frontend restart, and expires staged
parts on the remote owner. Payloads, successful completion and reclaimed remote
physical bytes are checked. `TestFrontendIssuerOfflineRemainsRetryable` checks
that a missing issuer preserves 503 while invalid credentials remain rejected.

`TestDaemonUpgradeFromDiscoveryOnlyCluster` opens a prior discovery-only setup
with existing acknowledged data, restarts the join-pending node first, and checks
shared committed membership, unchanged object bytes/versions and local volume
ownership. The existing deployment was backed up before its matching upgrade.

## PM2 shutdown behavior

A supervisor must preserve a successful administrative shutdown. For PM2 use
`stop_exit_codes: [0]` and a suitable graceful `kill_timeout` (the test uses
30000 ms). Otherwise PM2 immediately resurrects a drained node or a cluster STOP.
An explicit `pm2 restart stqr` still starts the process again; the terminating
SIGINT/SIGTERM takes SHORT and retains placement/data. Nonzero failures retain
PM2's ordinary restart policy. Save the configured entry with `pm2 save`.

`TestSSHPM2ClusterAcceptance` runs the same two-host correctness checks using a
unique temporary PM2 entry, including full STOP/restart and final node DRAIN.
Enable it with `STQR_TEST_PM2=1` alongside the SSH test variables. Cleanup deletes
only the test entry; the test never saves PM2's list or changes the existing
service. This additional acceptance test passed on the supplied SSH host.

## Rescuing data volumes from failed hardware

The replacement has a fresh node ID. Permanently stop or isolate the old process
before choosing **Node ersetzen** in the dashboard. This executes
`NODE_RECOVERY_BEGIN` (0x2014): survivors remove the old member, preserve its disk
range provenance and hold unavailable ranges with 503. This path does not accept
loss. Other ranges remain available and a new replacement can join normally.
The remaining cluster must retain its committed system state.

Attach the old disks to a surviving/new node and choose **Gerettete Volumes**.
Vormerken sends `VOLUME_RECOVERY_STAGE` (0x2015) with a name and existing absolute
path. No creation or formatting occurs. Select the full set and take it over
with `VOLUME_RECOVERY_COMMIT` (0x2016). Unpinned ranges can span multiple drives;
all cohort members need matching durable ownership witnesses. Missing media,
missing provenance or an unresolved source-migration fence are rejected with
409. Policy or quota violations prevent activation. `VOLUME_RECOVERY_RELEASE`
(0x2017) releases an unactivated staged drive without rebinding it.

All current nodes approve the new range owner; only afterwards are the header
owner and local provenance changed atomically and the volume activated. Payload
is not copied. Versions, tombstones and retry receipts survive. A committed
interrupted takeover resumes on restart. Admin operations use stable request IDs;
ambiguous outcomes can be checked with OPERATION_STATUS. Staging and release
receipts remain local, while recovery decisions are in the replicated system
manifest. NODE_ADMIN can target another node for all three volume operations.

Imported drives remain restricted to their imported scopes, including after
restart. Unrelated stale objects cannot override live data or become migration
input. Pending ranges stay unavailable until their complete media cohort is
recovered. Old LOST semantics remain LOST after recovery. A later cooperative
drain migrates valid scopes before discarding proven obsolete copies.

This requires format-3 provenance to have been written before failure. Existing
format-2 volumes are upgraded in place when the new daemon writes; a previously
failed format-2 node with no provenance still needs its original recovery state.
Upgrading is one-way: older binaries reject format 3. Back up volumes and node
configuration before upgrading.
