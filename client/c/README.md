# Native C client

The C library connects directly to STQR over an authenticated, bucket-bound
WebSocket. It does not embed Go, call a helper daemon, or implement its operations
through another language's SDK. URL, username, password, bucket ID and format are required
by `stqr_dial`. The server checks the grant at upgrade; it cannot be changed on an
established connection.

`stqr_bucket_space(client, &space)` returns live bucket usage and remaining
eligible cluster space, capped by a finite quota. The kernel adapter uses it
for `statfs`/`df`; see [capacity reporting](../../docs/space.md).

Build with a C11 compiler, make and OpenSSL development headers:

```sh
make c-client
make -C client/c check
STQR_TEST_C=1 STQR_C_SANITIZE=1 go test ./server -run '^TestCClient$' -count=1
```

The outputs are `libstqr.a`, `libstqr.so` and the public header `include/stqr.h`.
Link with `-lstqr -lcrypto`. The test command uses a temporary server and volume;
it never connects to the developer's running daemon. AddressSanitizer and UBSan
are enabled by `STQR_C_SANITIZE=1`.

```c
#include <stqr.h>

int save_settings(const char *url, const char *user, const char *password,
                  stqr_id bucket, stqr_object_id object)
{
    stqr_client *conn = NULL;
    stqr_options options = {0};
    stqr_info info;
    int err = stqr_dial(&conn, url, user, password, bucket, "fs", 30000);
    if (err) return err;
    err = stqr_request_id(&options.request_id);
    if (!err) err = stqr_put(conn, object, "hello", 5, &options, &info);
    /* On -EINPROGRESS, preserve options and the arguments for a later retry. */
    stqr_close(conn);
    return err;
}
```

## Contract

- Object addresses are 32 raw bytes; bucket/request IDs are 16 raw bytes. Hex
  parsing helpers are provided. The library never hashes an object ID again.
- The API includes get/get-range, stat, put/put-range, append, truncate, delete
  and range/object CAS. Append returns the committed starting offset.
- PUT and CAS address the caller's existing object ID. Repeated updates do not
  introduce client-generated replacement objects; storage range reuse and
  payload release are performed by the server. The C library has no filesystem
  namespace, open-file reference tracker, snapshot manager or filesystem garbage
  collector. Those operations belong to the Go filesystem or kernel adapter.
- `stqr_object_pin`/`unpin` and `stqr_object_claim`/`release_claim` coordinate
  object lifetimes when `STQR_CAP_OBJECT_REFERENCES` is advertised. Pins and
  claims use caller-owned tokens, survive reconnect/restart/handoff, and must be
  explicitly released. Claim conflict returns `-EBUSY`; unsupported servers
  return `-EOPNOTSUPP`. These READ-authorized coordination operations do not grant
  WRITE or DELETE permissions. Dependency pins have separate
  `stqr_object_pin_dependency`/`unpin_dependency` APIs and must survive ordinary
  open-handle recovery while an overlay or snapshot may still depend on them.
- Return values are zero on success or negative errno. `stqr_status` exposes the
  last HTTP/STQR status, including 412 for an unmet precondition. There is no
  shared global error state. Dial failures leave the output pointer NULL.
- Every mutation requires caller-owned options with a nonzero request ID. Generate
  one per logical mutation. Reuse the ID and all arguments for a retry, including
  after reconnect. A potentially sent mutation interrupted by transport loss
  returns `-EINPROGRESS`; the library does not silently report failure or generate
  another ID. `stqr_reconnect` reauthenticates; it never replays operations itself.
- `has_length` distinguishes an omitted length precondition from zero. `absent`
  is create-only. Optional `has_compare`, `compare_offset`, `compare` and
  `compare_size` guard a mutation with actual bytes checked under the same volume
  lock. Range CAS compares its addressed bytes; whole-object CAS supplies all
  previous bytes. Native objects have no content-version field.
- Data returned by get belongs to the caller and is freed with `stqr_free`.
  Failed get calls clear the data pointer and length. Empty objects may return
  a NULL data pointer with length zero.
- Calls on a connection, including close/reconnect, must be externally serialized.
  Different connections can run concurrently. Credentials remain private to each
  connection and are cleared before freeing. Close accepts NULL; a freed pointer
  must never be passed to close again.
- Buffered payloads are limited to 16 MiB per call. Payloads above 1 MiB use the
  protocol's chunk transfer with SHA-256, ordering and length verification.
  Large objects can be accessed using ranged reads/writes; this is not an object
  size restriction. Callback-based, unbounded streaming is not yet exposed.
- URLs accept `http://host[:port]` or `ws://host[:port]/api/ws`, including bracketed
  IPv6. DNS resolution is provided by the POSIX adapter. Range-owner redirects
  are described below. TLS and compression extensions are not implemented.
- The timeout bounds socket connection and individual blocking socket I/O;
  DNS resolution uses the system resolver. There is no asynchronous cancellation
  or background heartbeat/reconnect thread.
- Runtime IDs are retained across reconnect; an observed RAM-runtime change is
  reported as `-ESTALE`. Reads and mutations follow validated owner redirects as described below.

## Split write fences

`stqr_fence_capture` captures a client's preceding accepted-write marks while
holding that client's normal external serialization. `stqr_fence_wait` uses
independent, cached control connections, so the original client can continue
object I/O. Apply results with `stqr_fence_apply` under the original client lock;
close the fence with `stqr_fence_close`. A fence can be reused after completion.

For several clients, use `stqr_fence_reset` followed by serialized
`stqr_fence_add` calls. Same-owner, same-bucket, same-boot marks are merged to
the maximum; distinct boots remain separate proof targets.
After capture, distinct `stqr_fence_wait_one` targets may run in parallel; join
all of them before applying, resetting or closing the fence. Confirmations are
bucket-scoped and never erase later, unconfirmed writes. The existing `stqr_sync`
API keeps its serialized-call contract. No new wire messages or storage format
are introduced.

## Migrating explicit format arguments

`stqr_dial` takes the format between the bucket ID and timeout. This changes the
C source and binary calling interface. Rebuild and relink C consumers against the
matching header and library, and deploy those rebuilt consumers together with the
library. Rebuild kernel modules using the matching client core as well. Upgrade
all server members before relying on persistent format enforcement.

## Kernel reuse

`src/core.c` implements the canonical little-endian wire codec, WebSocket upgrade,
framing, object RPCs and chunk validation without OS networking or allocator calls.
`src/port.h` defines the socket, allocation, randomness and hashing boundary.
`src/posix.c` supplies the userspace implementation. A kernel adapter can compile
that same core with kernel socket/crypto/memory functions; OpenSSL and libc are
not dependencies of the core. Neither environment requires a resident client
helper process.

The standalone Linux adapter is now implemented in [kernel/kstqr](../../kernel/kstqr/README.md).
See the [kernel client guide](../../docs/kstqr.md) for mounting and VM tests.

## Object subscriptions

`stqr_capabilities()` reports HELLO capabilities. On nodes advertising
`STQR_CAP_SUBSCRIPTIONS`, `stqr_subscribe()` returns a consistent full or ranged
snapshot and its subscription cursor. Snapshots use the same validated chunk transfer
as GET. `stqr_set_update_handler()` installs a callback receiving borrowed update
bytes; `stqr_poll()` processes updates when ordinary RPCs are idle. RPC reply waits
also dispatch incoming updates. The caller must serialize poll, RPCs and close on
one connection, and callbacks must not reenter that connection. Different
connections remain independent; the library starts no background thread.

The codec checks lengths, enums, multipart fields and SHA-256 before dispatch,
and cumulatively ACKs received updates. Consumers maintaining snapshots must check
the subscription ID, cursor continuity and supported change
shape before applying an event. A gap or RESYNC_REQUIRED requires a fresh snapshot.
The callback receives NULL on connection loss/close, invalidating all snapshots
covered by that connection. Reconnect does not retain live server registrations;
subscribe again, optionally supplying the old cursor for a fresh resnapshot.
`stqr_unsubscribe()` is a barrier, and closing a connection releases all remaining
subscriptions. Subscribe failures which leave an indeterminate registration close
the connection, so no hidden server descriptor remains.

The kernel adapter uses a dedicated subscription connection with an 8 MiB bounded
LRU and explicitly unsubscribes evicted entries. The Go SDK's GC/AddCleanup lifetime
strategy is unchanged; the C/kernel implementations use explicit ownership.

The framing path sends a header and its masked payload together in one socket
send, reducing small-RPC latency without changing framing, hashes or ACK semantics.

## Explicit durability

Default mutations keep durable ACKs. Set `stqr_options.accepted=1` to request a
weaker visible ACK where `STQR_CAP_DURABILITY` is supported. `stqr_sync()` confirms
all observed pending writes on the connection. `stqr_pending_mark()` and
`stqr_sync_through()` support a shared barrier across a mount's connections;
`stqr_note_durable()` propagates only a real successful response for that same
bucket. Receipt observation also covers reads and subscription notices. Marks
survive reconnect. Known acknowledged prefixes remain separate from unknown
in-flight mutation outcomes, which retain `-ESTALE`. Close alone is not a sync. See [the complete contract](../../docs/durability.md).

Sync covers all retained sessions and server incarnations. Reads, ordinary
writes, reconnect and close never request persistence. The scalar pending-mark
getter performs no I/O and returns `-EOPNOTSUPP` if multiple boot IDs are needed;
use a captured fence or explicit connection sync for that set. Sync-through and
durable notifications remain bound to the mark's known owner and bucket.

Each owner retains up to 32 detached, boot-qualified ACK prefixes in RAM, plus
its current connection prefix.
Repeated disconnects coalesce marks only within the same boot. A new boot's read
or write reply does not erase an older prefix or turn it into an unknown outcome.
Explicit sync requests `SyncThrough` for every old prefix and a connection fence
for new work. A new connection's ACK cannot certify another socket's old work,
even within the same boot. A failed old-prefix proof is returned to the caller
and retained for a later explicit attempt; it is never guessed successful. The
bounded-prefix limit fails closed rather than dropping an uncertified mark.

## Cluster routing

Reads and mutations, including chunked payloads above 1 MiB, follow authoritative
range-owner redirects. The client validates the
bucket/range and primary node in the returned map patch, flushes accepted writes
before switching owners and retries with the same request ID. Up to 32
authenticated endpoint sessions are retained per logical bucket connection;
switching owners preserves existing subscriptions. Poll visits every live session,
and unsubscribe uses the session which owns that subscription. A transport loss,
explicit reconnect or bounded-pool eviction invalidates subscription coverage;
ordinary routing does not. Eviction confirms outstanding writes before closing a
session. There is no automatic alternate-address failover. A rejected chunked
mutation replays its complete payload with the original logical request ID; a
transport failure with an uncertain outcome is never replayed automatically.

The unversioned object schema uses WebSocket subprotocol `stqr.v2` and HELLO
version 2; v1 native object messages are rejected. PG-local rebuild coordination
is internal to nodes and does not require another native client wire version.

Temporary `503` replies are retried on the existing connection, with the same
request ID, body and any chunked payload, for at most the connection's configured timeout. Backoff is
interruptible and does not consume the redirect budget. Transport failures with
an uncertain mutation outcome still return `-EINPROGRESS`; callers must preserve
the original request ID when reconnecting. Permanent errors are returned directly.

`stqr_object_lease(client, readonly)` acquires the server's exclusive opaque-object
bucket lease on a fresh dedicated connection. Owner redirects carry the returned
lease authorization and retain the original owning session. `stqr_close` releases
it; explicit reconnect returns `-ESTALE` because it cannot safely reacquire a lost
lease. The kernel block-device adapter uses this API; the server still only sees
ordinary object operations and does not interpret device metadata.

Response durability headers use a 16-byte server boot ID and 64-bit sequence.
Tag 48 identifies an accepted bucket prefix; optional tag 49 confirms that all
local volume generations captured for that prefix are already durable. This
optional confirmation is omitted when a volume is busy, so unrelated replies
never wait for that volume's storage work. Older
clients ignore tag 49. The C client clears only covered marks from the same boot;
confirmation cannot clear uncertainty from an unknown in-flight mutation. Legacy peers retain their boot-qualified marks across reconnect and
confirm them through `SyncThrough` rather than a replacement connection fence.

A local failure before an entire request frame reaches the transport (for example,
allocation failure or a kernel caller already under fatal cancellation) does not
invalidate other requests or their accepted durability prefixes. The port may
return the internal `SQ_SEND_NOT_SUBMITTED` result only when it proves that no
byte was submitted; the API reports `EINTR` for that cancellation. Ordinary send
errors, including `EINTR` after transport dispatch, remain uncertain for mutations.
This proof applies to one frame: failure of a later upload chunk cannot undo an
already submitted request header or earlier chunks. No cancellation initiates a
sync, and only explicit caller sync confirms persistence.
