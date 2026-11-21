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

The bucket catalog and individual bucket configuration objects are **specified,
not implemented** in this stage. The admin volume currently holds account and
local administrative-operation objects. Future EVERYWHERE replication and join
are explicit TODOs; a known peer or `--connect` never bootstraps a second cluster.

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
with a crash-safe bank rewrite; shrinking is rejected. `volume.Grow` is an explicit
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
account/session state are checked; text WebSocket messages are not accepted.

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
Unsupported bucket/object/peer commands return 501, never a fabricated success.

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
