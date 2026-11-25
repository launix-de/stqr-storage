# STQR

**An easy to configure storage management system. Start with one disk. Grow on your own terms.**

### Physical view: start small, add capacity

```text
Nodes
└── Node 1
    └── Volume 1 · 8 TB
```

```text
Nodes
├── Node 1
│   ├── Volume 1 ·  8 TB
│   ├── Volume 2 ·  8 TB
│   ├── Volume 3 · 16 TB
│   └── Volume 4 · 16 TB
└── Node 2
    ├── Volume 1 ·  8 TB
    ├── Volume 2 ·  8 TB
    ├── Volume 3 · 16 TB
    └── Volume 4 · 16 TB
```

### Logical view: buckets and their clients

```text
Buckets                         Connected clients
├── videos ────────────────────► FUSE
├── database1 ─────────────────► memcp
├── database2 ─────────────────► memcp
├── webserver-scripts ─────────► FUSE
├── webserver1 ────────────────► FUSE + overlay
├── webserver2 ────────────────► FUSE + overlay
└── vm-disks ──────────────────► QEMU
```

Volumes provide physical capacity; buckets give applications a logical namespace
independent of disk and node boundaries. FUSE, native clients and S3 expose that
storage in different ways. Named virtual disks connect to QEMU through its standard
NBD driver. See [implementation status](#implementation-status) for current capabilities.

STQR is built around a simple idea: adding storage should not require redesigning
how your applications access their data. Start the daemon, select a disk, create
a bucket and user, and mount it. The architecture is designed to carry that same
storage concept from one node and one disk to a petabyte-scale cluster, adding
drives and nodes as your needs grow.

Use it for an expanding media library, shared application files or application
storage. Access data through a Linux FUSE mount, the native Go API or an
S3-compatible endpoint built into the daemon. Add volumes and nodes to extend
capacity while applications keep their bucket and object addresses. The name
is a nod to *store* and *SPQR*.

## Why STQR exists

A full disk often becomes an infrastructure project. Depending on the setup,
expanding a conventional Linux filesystem can involve moving data, changing
partitions, extending logical volumes, unmounting filesystems or scheduling
downtime. Some setups support online growth, but someone still has to plan and
manage the storage underneath.

Adding a second server introduces another question: how do both machines see the
same files? Databases already provide shared access to relational data. For files,
the step from a local disk to shared, expandable storage is often much larger.

A system such as Ceph addresses these needs, but adopting a full storage cluster
also introduces deployment and management complexity. That is a substantial step
for a NAS or web application that has simply outgrown its first disk.

STQR aims to close this gap with one continuous storage model. Begin with the
hardware you have, add capacity when you need it, and keep the application-facing
interface as the installation grows. A three-node deployment is not a prerequisite
for getting started.

## Simple to start, room to grow

- **A short path to a mounted filesystem.** Start the daemon, select a disk in the
  dashboard, create a bucket and user, then mount the bucket with `stqr-fuse`.
- **No three-node starting requirement.** One machine and one disk are a valid
  starting point. Add disks or nodes as capacity needs grow, and select disk
  replication independently through a placement group.
- **Automatic initial setup.** The daemon generates its configuration, identity
  and bootstrap credentials on first start. There is no configuration file to
  assemble before you can log in and set up storage.
- **Placement under your control.** Choose storage classes and placement groups
  for buckets. Capacity-driven range migration distributes data across eligible
  volumes; volume and node drains move data before hardware is removed.
- **Several ways to use the same storage.** Mount files with FUSE, use native
  object operations, connect an S3 client to the daemon's HTTP endpoint, or
  expose virtual disks through NBD.
  Each frontend has its own data format; use a dedicated bucket for each.

The zero-config starting experience uses generated defaults and dashboard setup;
advanced configuration remains available as your requirements grow.

## What would you put on it?

| Use case | What STQR is designed to provide |
| --- | --- |
| **Growing media collections and NAS** | A mounted namespace backed by storage that can grow across disks and nodes |
| **VM-host clusters** | Named sparse virtual disks through standard NBD, with PG-controlled disk replication and explicit flush barriers |
| **Database clusters, including memcp** | Native object access, partial updates, append and CAS, with application-level coordination kept in the database |
| **S3 object storage** | An integrated S3-compatible endpoint for existing SDKs, upload tools and applications |
| **PHP and web-server clusters** | A shared view of application scripts, with optional private writable overlays for configuration and local files |
| **Diskless machines and user homes** | Ubuntu/Debian image preparation, a PXE boot gateway and PAM-controlled mounts of per-user home buckets |

Choose FUSE for existing file-based applications, S3 for applications using an
S3 SDK, or the native API when your application manages its own object layout.
Use NBD for virtual block devices. These access paths use the same bucket
administration and storage backend, from a local daemon to a cluster.

## A concrete use case: shared PHP code, private changes

Several PHP workers or hosts can mount the same code bucket readonly. Each can
add a private upper bucket for configuration and writable application files.
The upper can live on a local STQR server while the shared lower lives on a
remote server. Each application instance gets the same base scripts and its own
writable view, without copying the entire code tree.

```text
                     shared code bucket
                       /           \
                 readonly         readonly
                    |                 |
             private upper A   private upper B
                    |                 |
               PHP mount A       PHP mount B
```

The overlay stores deletion markers in directory metadata. A permissions-only
change keeps a reference to the original content; a partial write stores the
changed ranges and references the remaining lower data. This avoids copying a
whole large file just to change a few bytes.

A whitelist such as `conf.json,files/` restricts writes to that file and
directory tree. It is persisted in the upper bucket so a remount retains the
same policy. Reads can still reach the rest of the shared code. This is a
filesystem policy: users with direct writable SDK credentials can access the
bucket outside FUSE.

The integration tests exercise multiple mounts and private uppers, PHP OPcache,
readonly mmap, local `flock`/`LOCK_EX`, permissions, remounts and cleanup.

## Implementation status

STQR implements storage across multiple nodes, an administration dashboard,
native Go and C clients, Linux and Windows filesystem clients, an integrated S3
endpoint, NBD virtual disks and networkboot tools.
The cluster distributes object ranges across primary owners, routes clients to
those owners and migrates ranges as capacity and placement requirements change.

| Component | Available functionality |
| --- | --- |
| Cluster | Membership and shared administration, client routing, capacity-driven range splits and migration, recovery of interrupted handoffs, volume/node drain and adoption of rescued data volumes |
| Object API | `Get`, `Put`, ranged access, atomic `Append`, `Trunc`, `Delete`, `Stat`, whole-object and ranged CAS, streaming transfers and retry deduplication |
| Storage | File and Linux raw-device volumes, volatile RAM volumes, checksums, crash recovery, grow/rescan, compaction/scrub and raw/gzip/LZ4/xz storage |
| Placement | Central PG storage/compression/replication policies, node/volume filters, capacity distribution and migration for policy changes |
| Dashboard | Users, grants, S3 keys, quotas/guarantees, PG editing, node/volume management and live statistics; searchable bucket/user lists with endless scrolling |
| Go client | Authenticated, bucket-bound connections, direct routing to range owners, reconnects and request IDs for retry deduplication |
| C client | Native static/shared library, object operations, subscriptions and Sync; direct-node connections without cluster redirect following |
| S3 | GET/HEAD/PUT/DELETE, ranges/conditions, listing, multipart, copy, bulk delete, checksums, SigV4, presigned URLs and private/public-read object ACLs |
| Linux FUSE | Read/write and readonly mounts, permissions, sparse files, rename, symlinks, hardlinks, xattrs, FIFO/socket inodes, live capacity reporting and offline `--fsck` |
| Linux kernel client | Standalone `kstqr` mounts sharing the normal native filesystem format, subscriptions, mmap/writeback and live capacity reporting; see [kernel documentation](kernel/kstqr/README.md) |
| Windows client | WinFsp drive letters, setup GUI, encrypted saved profiles, per-user login startup and Explorer change notifications; see [windows/](windows/README.md) |
| Native overlays | Partial-content copy-up, metadata overrides, whiteouts and persisted write whitelists |
| Virtual disks | Named sparse block devices, QEMU-compatible NBD exports, ranged I/O, FLUSH/FUA and TRIM; no QEMU rebuild required |
| Networkboot | Template installation/editing, boot-artifact publication, PXE gateway, per-machine writable sessions and PAM home activation |

Placement groups share a storage policy across any number of buckets. A bucket
keeps its own name, access grants and quotas, while its PG controls storage class,
compression, replica count and eligible nodes/volumes. Node-specific volume
filters can allow all volumes, blacklist selected volumes or whitelist selected
volumes. Adding a volume can also add it to chosen PG lists. Policy changes
remain pending where required nodes are offline; their existing placement is
preserved. See [central PG policies](docs/server.md#central-placement-group-storage-policies).

Disk replication uses **one primary and required full replicas per range**.
PG settings select a fixed replica count or EVERYWHERE within the PG's node
mask. Writes require ALL copies; a missing required node never silently reduces
durability. Prepared writes stay invisible, retries retain their original result,
and reads can use a committed replica while the primary is offline. Independent
buckets remain available. Online replica additions, count reductions and codec
changes preserve object bytes and retry receipts. Logical quotas count data once;
physical quotas include every required copy and staging allocation. PG mask
changes and drains can relocate existing disk replica sets, including a fenced
handoff of the primary role. Replacing a node temporarily adds a required copy
before retiring the old one, so spare physical capacity is needed.

RAM replica restore, load-driven balancing of existing multi-copy groups and
storage-strategy changes for those groups remain pending; multi-copy RAM writes
return 501. No automatic writer failover is performed. Read-cache tiers and
erasure coding remain on the roadmap. Local daemon, fault-injection and opt-in
SSH tests cover replication and recovery;
large deployments still need field validation and reproducible benchmarks.
The replication storage records require volume format 4. Formats 2 and 3 remain
readable and upgrade on the next write; older binaries must not reopen an
upgraded volume. See [replication notes](docs/replication.md).

The native store offers atomic operations per object. Applications and frontends
build their own coordination above that interface; STQR does not provide a
multi-object database transaction engine. Persistent volumes use a packed format
with B+-tree metadata and redo recovery. Native mutations and S3 use durable
acknowledgements by default. Linux FUSE/kernel clients and the NBD exporter can
use accepted writes whose persistence is deferred until a barrier: filesystem
`fsync`/`fdatasync` and NBD FLUSH/FUA confirm the required durable prefix. Ordinary
file close is not a persistence barrier. RAM volumes remain volatile even after
Sync. See [durability](docs/durability.md) for the client and restart contract.
APIs and on-disk formats may change as development continues.

## Getting started

Build with **Go 1.24 or later**. FUSE additionally requires Linux, `/dev/fuse`
and a suitable fusermount helper. The Go FUSE adapter talks directly to the
kernel; no C libfuse linkage is required.

```sh
git clone https://github.com/launix-de/stqr-storage.git
cd stqr-storage
go build -o stqr .
go build -o stqr-fuse ./cmd/stqr-fuse
./stqr
```

Open <http://localhost:7419>. On first start, the daemon creates `stqr.yaml`;
the initial credentials are under `auth.bootstrap_admin`. Keep this file private.
Use the dashboard to add a data volume, create a bucket and grant a user access.
The reserved admin volume stores accounts and bucket descriptions separately
from your application data.

To add a fresh installation to an existing cluster, prepare any data volumes and
choose **Join cluster** in the dashboard before creating application buckets.
Enter a cluster node URI and administrator credentials; the node adopts the
cluster accounts and policies while keeping its prepared volumes. The
[join guide](docs/server.md#join-a-cluster-from-the-dashboard) describes eligibility
and network requirements.

By default, access is restricted to loopback clients. The daemon currently
serves plain HTTP/WebSocket, including Basic authentication. Use a protected
network, an SSH tunnel or a TLS reverse proxy for remote access. S3 can be exposed
through an Apache or nginx TLS endpoint using the same HTTP server. The
[server guide](docs/server.md) describes configuration and access controls.

### Grow storage volumes

In **Nodes/Volumes**, choose **Vergrößern** on the desired storage volume. The
dialog shows its owning node, backing path, current capacity and available space.
File and RAM volumes support exact unit input, a discrete slider, quick steps
and a before/after preview. The target must be 4-KiB aligned and fit within the
current capacity plus available disk space or RAM budget. Limits can be refreshed
and are checked again when executing the request.

For raw storage devices, extend the device externally first, then use the dialog
to adopt its full new capacity. This does not resize an LV or partition itself.
Growth preserves volume identity and stored objects, also when administered
through another cluster node. Buckets remain object namespaces.

### Mount a bucket

Use the bucket's **32-character hexadecimal ID**, not its display name. Replace
the example values below with a bucket and credentials created in the dashboard.

```sh
mkdir -p ./mnt
./stqr-fuse --uri http://localhost:7419 \
  --username alice --password 'PASSWORD' \
  --bucket BUCKET_ID --init ./mnt
```

`--init` creates a missing filesystem root and preserves an existing one. The
bucket must already exist. The mount process runs in the foreground; unmount
from another terminal with `fusermount3 -u ./mnt` (or `fusermount -u ./mnt`).

There are three mount modes:

| Options | Mode |
| --- | --- |
| `--bucket ID` | Normal read/write filesystem |
| `--readonly-bucket ID` | Readonly filesystem |
| Both | Native overlay: readonly lower plus writable upper |

For an overlay, first populate a normal filesystem bucket as the lower and
create a separate empty upper bucket. Then mount with:

```sh
./stqr-fuse --uri http://localhost:7419 \
  --username alice --password 'PASSWORD' \
  --readonly-bucket LOWER_ID --bucket UPPER_ID \
  --whitelist=conf.json,fop-files/ --init ./mnt
```

An initialized overlay upper must be mounted with its matching lower. Separate
`--readonly-uri`, `--readonly-username` and `--readonly-password` options select
another server or account for the lower. `--allow-other` permits other local
users, such as PHP workers, where the host's FUSE configuration allows it;
normal Unix permissions still apply. See `./stqr-fuse -h` and the
[client and FUSE guide](docs/client-api.md) for details.

For automation, `--password-stdin` reads the password from standard input instead
of a command-line argument. `df` reports the mounted bucket's usage and available
eligible cluster capacity, capped by its quota; an overlay reports its upper.
These figures are advisory snapshots, described in [capacity reporting](docs/space.md).

Linux also has a standalone [kernel client](docs/kstqr.md) with no resident FUSE
process. It shares normal filesystem buckets with FUSE and supports native page
cache/mmap writeback. Its C transport connects to the configured node and does
not follow cluster redirects. The [Windows client](windows/README.md) mounts the
same normal filesystem format through WinFsp. Its setup dialog can save an
encrypted per-user profile and reconnect a drive at login; subscription updates
notify Explorer about remote changes. Frontend-specific limits are in those guides.

### Connect an S3 application

Create a dedicated S3 bucket and grant a user access. In the dashboard's user
administration, generate an S3 access key and secret for that user. Existing
bucket grants control read, write and delete permissions; keys can be revoked
without creating a separate S3 account system.

Configure your SDK or tool with:

| Setting | Value |
| --- | --- |
| Endpoint | `http://localhost:7419/s3` |
| Addressing | Path style |
| Bucket | The bucket's display name |
| Region | `eu-west-1` |
| Credentials | The user's S3 access key and secret |

The endpoint runs inside the daemon. It supports ordinary SDK workflows,
including multipart uploads, server-side copy and presigned URLs. S3 metadata
and content are stored together in native objects; replacing or deleting an
object reclaims its previous allocation without FUSE fsck.

Objects default to private. The standard `public-read` ACL permits anonymous
GET/HEAD, including ranges and conditional reads, through a permanent object URL.
Signed ACL requests can publish or revoke that access without changing the
payload. Public access does not grant listing or mutation rights.

Buckets are managed through STQR administration. General IAM/bucket policies,
object versioning, nonempty tags, general lifecycle policies and S3 server-side
encryption headers are outside the supported subset. The [S3 guide](docs/s3.md)
documents the exact operations, upload cleanup and reverse-proxy configuration. Preserve the signed
host, path and query when putting the endpoint behind TLS.

### Use virtual disks and networkboot

`stqr-nbd` exposes named virtual disks from one bucket over a private Unix NBD
socket. QEMU uses its existing NBD driver; the guest can create an ordinary
filesystem on each exported disk. Missing stripes read as zeros, and complete
discarded stripes return their payload allocation to STQR. Devices can grow
sparsely through the library or `stqr-nbd resize`, including through a running
exporter's private control socket. Reconnect NBD clients to discover the new size.
The dashboard's **Buckets → Disks** view lists and grows devices, including those
served by an exporter on another machine through its existing cluster lease.

```sh
make -C qemu-plugin-nbd
qemu-plugin-nbd/bin/stqr-nbd --bucket vm-disks --size 34359738368 create system
qemu-plugin-nbd/bin/stqr-nbd --bucket vm-disks --socket "$PWD/vm-disks.sock" serve
```

Create the bucket first. These commands read the local `stqr.yaml`; `--config`
can select a private credentials file for a dedicated user. QEMU receives the
socket and export name rather than STQR credentials. Writable exports require
READ+WRITE grants and an exclusive cluster-wide block lease. FLUSH/FUA confirms
persistence. See the [NBD guide](qemu-plugin-nbd/README.md) for QEMU configuration
and exporter limits.

The [networkboot tools](networkboot/README.md) use these disks to install and edit
Ubuntu/Debian templates, publish boot artifacts and serve diskless machines through
a PXE gateway. Each boot session has a private writable layer. A separate PAM
module activates a user's home bucket at login and tracks its sessions. Unix
accounts, STQR grants and PAM configuration are explicit deployment steps; the
tools do not provision them automatically.

### Use the Go API

A connection belongs to exactly one bucket. Its constructor takes the node URL,
username, password and bucket ID; subsequent operations need only the object ID.
Object addresses are native 32-byte values. Applications choose those addresses;
they need not be hashes of the payload.

```go
conn, err := client.Dial(ctx, nodeURL, username, password, bucketID)
if err != nil {
    return err
}
defer conn.Close()

_, err = conn.Put(ctx, objectID, []byte("hello"), client.PutOptions{
    RequestID: client.NewRequestID(),
})
```

Import `github.com/launix-de/stqr-storage/client`. Preserve the request ID and
operation arguments when retrying an unknown outcome. Use streaming methods for
large objects; the convenience buffered read is limited to 16 MiB. More examples
and the retry contract are in the [client guide](docs/client-api.md).

Object subscriptions provide versioned snapshots and change notifications for
bounded filesystem caches and native applications. Reconnects or gaps require
fresh snapshots; notifications do not create a distributed POSIX lock or a
multi-object transaction. See [subscriptions](docs/object-subscriptions.md).

The [native C library](client/c/README.md) offers object operations, subscriptions,
capacity queries and Sync as static/shared libraries. Build it with
`make -C client/c`. It uses authenticated bucket-bound connections directly,
without a Go helper; callers handle reconnects and retry IDs, and cluster redirect
following is not implemented in this client.

## Planning your deployment

For shared scripts and overlays, use a stable base and give each application
instance its own writable bucket. For native applications, choose the object API
when you want to manage indexing and coordination yourself. Keep filesystem,
S3 and application-native data in separate buckets: they share a storage engine,
not an interchangeable on-disk namespace.

In a cluster, distinguish capacity from availability. Missing required copies
block writes to their ranges; the Go client can read a committed replica while
the primary is offline. This does not promote that replica to writer. Independent
ranges can continue through their own connections. Some shared administration
changes require all participating members. A normal shutdown preserves placement
for restart; use drain to evacuate a volume or node before removing it. RAM
volumes deliberately lose their contents on restart.

After permanently fencing a failed node, existing data volumes can be staged and
adopted on a replacement without copying their payload. Recovery validates the
complete required media cohort and durable ownership evidence before activation;
unrecovered ranges stay unavailable. The [recovery guide](docs/server.md#rescuing-data-volumes-from-failed-hardware)
describes the dashboard workflow and its boundaries.

Offline filesystem collection, block exports and bucket clear/clone use
cluster-wide exclusion to keep competing clients out of the affected bucket.
Other buckets remain usable. Lease acquisition requires all admitted members;
a network timeout alone does not release ownership. See
[bucket maintenance](docs/bucket-maintenance.md) for the modes and recovery rules.

The current FUSE implementation has these operational constraints:

- **Space reclamation is offline.** Unlinked files and superseded chunks can
  leave unreachable objects. Close every mount using the writable bucket, then
  run the same connection options with `--fsck` instead of mounting. For an
  overlay, supply both bucket options. Use dedicated filesystem buckets: fsck
  treats objects outside the filesystem graph as garbage.
- **An overlay's referenced base content must remain stable.** Changing or
  removing a lower inode used by copy-up can produce `ESTALE`. Upper fsck
  removes redundant whiteouts after lower maintenance, but does not rebase
  content or reconnect replaced lower directories. Lower fsck does not track
  offline uppers' dependencies; it can collect data an upper still needs.
  Keep the lower quiescent during upper reconciliation.
- **Multi-mount has explicit coordination limits.** Object subscriptions invalidate
  the native cache. Distributed POSIX locks and writable mmap coherence remain
  separate concerns. Normal Linux filesystem buckets support symlinks and
  hardlinks; native overlays have additional limits described in the
  [filesystem guide](docs/filesystem-posix.md).
- **Native overlays target modest writable namespaces.** Namespace changes
  currently rebuild a sparse manifest, bounded to 64 MiB. This path needs more
  work for very large or intensely modified directory trees.

## How the architecture supports growth

The distributed design separates an object's stable address from its physical
location. Clients route directly using a versioned range map. The planner splits
ranges and migrates them to eligible volumes as capacity and bucket policies
change. Handoffs copy data, catch up writes and commit ownership before reclaiming
the old copy; interrupted transitions have a durable recovery path. Adding
hardware is a storage operation while applications keep their object addresses
and access interface.

That mechanism is the foundation for a broader vision: continuous growth from
one disk to a petabyte-scale installation, with workload-specific placement
instead of a fixed cluster layout. HDD, SSD and RAM storage are available today.
Disk replication and NBD virtual disks use the same placement machinery.
Read-cache tiers, erasure coding and RAM replica restore remain future work.
Replica reads serve locally committed data during an in-progress mutation,
keeping hot objects readable without waiting for the entire write to finish.
Applications that need stronger coordination can provide it above the object
interface; acknowledged writes retain
their intended durability contract.

The [architecture specification](STQR_Architekturspezifikation.md), written in
German, describes these mechanisms, their trade-offs and the remaining open
questions. Comparative performance claims await reproducible benchmarks.

## Development and testing

Pull requests and master pushes run CI for Go vet/race checks, localhost clusters,
crash recovery, dashboard browser acceptance, C sanitizers, FUSE, NBD, networkboot
and native Windows callbacks/setup. The server suite is discovered automatically
and split into disjoint groups. Kernel-module VMs, physical PXE boots, actual
Windows drive mounts and external-host acceptance remain separate checks.

Persistent format changes require a published Git release/checkpoint tag of the
previous compatible state. The format CI builds every tagged version, uses its
server and frontends to generate data, then verifies the same persisted files
with the tested version and again after restart. The corpus covers native objects,
filesystems, block devices and S3. See [CI and format checkpoints](docs/ci.md)
for the workflow, coverage and mandatory tag rules.

```sh
go test ./...
go test -race ./...
go vet ./...
```

Tests cover the protocol, authorization, retries, volume recovery, filesystem
namespace operations and overlay cleanup. Cluster integration tests use separate
daemons and temporary volumes to exercise admission, capacity expansion, routed
reads and writes, migration, drain and restart recovery. S3 tests cover SDK
compatibility, authentication, checksums, multipart recovery and reclamation.
Real kernel mounts are opt-in:

```sh
go build -race -o stqr-fuse ./cmd/stqr-fuse
STQR_TEST_FUSE=1 STQR_FUSE_BINARY="$PWD/stqr-fuse" go test -race ./... -count=1
```

These tests require a host that permits FUSE mounts. The PHP integration also
needs PHP CLI with OPcache and Python 3. Fixtures use temporary volumes and buckets.
Bug reports with a reproducer, recovery tests and measured workload results are
welcome; larger changes should discuss the relevant storage or protocol contract.

Further reading: [server and administration](docs/server.md),
[client API and FUSE](docs/client-api.md), [kernel mounts](docs/kstqr.md),
[Windows drives](windows/README.md), [C API](client/c/README.md),
[S3 access](docs/s3.md), [virtual disks](qemu-plugin-nbd/README.md),
[networkboot](networkboot/README.md), [replication](docs/replication.md),
[durability](docs/durability.md), [volume format](docs/volume-format.md),
[CI](docs/ci.md),
and the [architecture specification](STQR_Architekturspezifikation.md).

## License

STQR is licensed under the [GNU General Public License v3.0](LICENSE.md)
(`GPL-3.0-only`).
