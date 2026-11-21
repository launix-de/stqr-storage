# Single-node daemon and volume administration

Build the daemon in the repository root with Go 1.24 or later:

```sh
go build -o stqr .
./stqr
```

There are no administrative CLI subcommands. `stqr` is the daemon; it serves the
embedded dashboard and the administrative protocol on HTTP/WebSocket, default
port 7419. TLS is intentionally absent. For remote administration use an SSH
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
corrupt data instead of resetting it. Future EVERYWHERE replication and join
remain explicit TODOs; a known peer or `--connect` never bootstraps a second cluster.

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
| `--connect http://host:7419` | Parsed join intent; currently fails explicitly because multinode is not implemented |

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

Single-node placement generation changes are trivial local commits. TODOs identify
the later generation builder, peer stats, authority fencing, replica completion,
subscription handoff and shutdown coordination. There is no simulated quorum,
multinode ACK or failure-based eviction.

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
no membership or ownership: NODE_HELLO/JOIN and distributed control remain 501.
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
