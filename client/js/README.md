# Native JavaScript bucket SDK

`index.mjs` is a dependency-free ESM client for browsers and Node 22+ (or a runtime
providing WHATWG fetch, WebSocket, Blob and AbortSignal). The server serves it at
`/assets/sdk/index.mjs`. It uses the canonical native binary protocol. Application
formats belong entirely to callers; the SDK and tunnel never interpret them.

## Same-origin gateway

```js
import {Client} from '/assets/sdk/index.mjs';
const session = await (await fetch('/api/session')).json();
const bucket = await Client.connect({
  endpoint: location.origin,
  bucket: 'my-bucket', // name, name:literal-name, or canonical hexadecimal ID
  format: 'my-application', // required, opaque and case sensitive
  routing: 'gateway', // default; no plaintext dashboard login password
  csrf: session.csrf_token,
});
try {
  const {data, info} = await bucket.get(objectID);
  console.log(info.length); // BigInt
} finally {
  await bucket.close();
}
```

The HttpOnly session and CSRF token authenticate the seed. The client learns each
object's committed owner and opens `/api/bucket-tunnel?node=<node-id>` on that same
gateway for remote owners. Only committed membership supplies upstream addresses;
arbitrary destination URLs are never accepted. The relay forwards unchanged native
binary WebSocket frames with native framing limits and TCP backpressure. It costs
another network hop and connection/bandwidth resources on the gateway.

Scoped delegation credentials stay **on the server**. They bind the original
account incarnation, live originating request, bucket and format; the owner
verifies the issuer's live grant over its authenticated peer channel. JavaScript
receives neither a delegated credential nor the user's plaintext password. The
original session and effective grant are rechecked each second: logout, expiry,
account disablement and grant removal terminate the tunnel. Ordinary seed sockets
use the existing capability-revocation policy and reject further operations even
if their TCP socket remains open. Close the SDK when logging out.

## Direct node connections

```js
const bucket = await Client.connect({
  endpoint: 'http://seed.example:7419', bucket: 'documents', format: 'my-format',
  routing: 'direct',
  credentials: async () => ({username, password}),
});
```

Direct mode authenticates each node independently and validates HELLO node/cluster
identities against learned routing information. The credentials callback obtains
credentials on demand; the SDK writes none to browser storage. Explicit
`username`/`password` options support native-style automation and retain those
strings in process memory until close. The dashboard uses cookie-only gateway
mode instead. Gateway mode also supports an explicit credentials callback.

A page's own response headers cannot grant it CORS access to other nodes. Set the
origin policy **on each addressed node**, in YAML:

```yaml
browser_origins:
  - http://console.example:8080
  - http://10.42.0.0/16
  - http://[::1]:8080
```

Exact origins include scheme and port. CIDR entries accept literal-IP page origins
in that subnet, with the specified scheme and any port; hostnames are not resolved
against CIDRs. The default is same-origin only. `allow_subnets` separately limits
the caller's IP. Allowed browser connection endpoints return matching CORS and
preflight headers; that does not confer authentication or administrative rights.
WebSocket origin checks use the same policy. Native selectors travel in encoded
WebSocket subprotocol offers because browser WebSockets cannot set HTTP headers;
credentials never appear in URL query parameters.

The embedding page must permit owner nodes in CSP `connect-src`. CORS cannot
override mixed-content blocking, HTTPS-to-HTTP restrictions or browser Private /
Local Network Access prompts. The daemon's committed addresses remain native HTTP;
use the same-origin tunnel when browsers cannot reach those addresses directly.

## Native API parity

All operations are Promises and may complete out of order. Await dependent
operations or fence them explicitly. IDs are canonical hexadecimal native addresses:
64 digits for objects, 32 for buckets/request IDs. The SDK never rehashes caller
object IDs. Lengths and offsets use BigInt; unsafe Number integers are rejected.

| Go / C native functionality | JavaScript API |
|---|---|
| Dial / dial_bucket; canonical bucket ID | `Client.connect(options)`, `client.bucket` |
| Get / GetRange / Stat | `get(id) -> {data,info}`, `getRange(id,offset,length,options)`, `stat(id)` |
| GetTo / GetRangeTo / conditional reads | `getTo(id,sink,options)`, `getRangeTo(id,offset,length,sink,options)`, `getRangeToMatching(id,offset,length,condition,sink,options)` |
| Put / PutRange / Append | `put(id,bytes,options)`, `putRange(id,offset,bytes,options)`, `append(id,bytes,options) -> {info,startOffset}` |
| PutFrom / PutRangeFrom / AppendFrom | `putFrom(id,source,length,options)`, `putRangeFrom(id,offset,source,length,options)`, `appendFrom(id,source,length,options)` |
| Trunc / Delete / CAS | `truncate` / `trunc`, `delete`, `cas(id,offset,expected,replacement,options)` |
| No-success-ACK async writes | `putAsync`, `putRangeAsync`; `sync()` reports late failures |
| Accepted ACK / commit marks / durability | `{accepted:true}`, `pendingMark`, `syncThrough`, `noteDurable`, `sync` |
| C split / combined fences | `captureFence`, exported `Fence.add`, `waitOne`, `wait`, `apply`, `reset`, `close` |
| Native inventory / maintenance inventory | `scanObjects(after,limit,options) -> {objects:[{id,info}],next}`, `{maintenance:true}` |
| BucketAccess / Space | `bucketAccess`, `space` |
| Object lease / maintenance / release | `beginObjectLease(readonly,options)`, `beginMaintenance`, `endMaintenance` |
| Pins / retirement claims / dependency pins | `pinObject`, `unpinObject`, `claimObjectRetirement`, `releaseObjectRetirement`, `pinObjectDependency`, `unpinObjectDependency` |
| Offline reference cleanup / roots | `resetAbandonedObjectReferences`, `reclaimBucketMetadata`, `canReclaimDependencies`, `readObjectDependencies`, `objectReferencesSupported` |
| Subscriptions / snapshot / unsubscribe | `subscribe(id,options)`, `subscription.snapshot()`, async iteration, `subscription.close()` |
| Reconnect / close | `reconnect`, `close` |
| C completion/readable/update/poll callbacks | Browser event loop, Promises and async iteration; no manual polling |
| C allocation/free and ID helpers | Garbage collection, exported `newID`, `hex`, `unhex` |

Options include `signal`; mutations also accept `requestId`, `expectedLength`,
`absent`, `compare`, `compareOffset`, and `accepted`. CAS has `scope:'range'`
(equal-size byte ranges, at most 1 MiB) or `'object'` (whole-object replacement,
offset zero, at most 1 MiB). Atomic read conditions are `{offset,bytes,length?}`;
pass them through `getRange(...,{condition})` or the conditional sink method.
Reference methods take an object ID and caller-owned 128-bit token retained until
release. Graph interpretation remains caller code.

Requests default to a 30-second deadline (`requestTimeout`). Automatic transport
and 503 retries preserve logical request identities. `StatusError` retains native
status, headers and request ID. `UnknownOutcomeError` carries the request ID when
cancellation/network loss cannot prove a mutation absent; retry identical input
with that ID rather than inventing a new append. A lost lease coordinator is never
silently reacquired. Acquire/release use a dedicated pinned coordinator and drain
the client's own auxiliary connections; unrelated clients still block exclusivity.
Cancellation sends native CANCEL. If acquisition has an unknown outcome, its
coordinator is closed so a granted lease cannot remain hidden behind a lost reply;
create a new Client after losing that coordinator.

Buffered reads are limited to 16 MiB. Sink methods consume native frames with an
incremental SHA-256 and await the sink; delivered bytes are tentative until the
method resolves, because final checksum/network failure may follow earlier chunks.
WebSocket implementations can still buffer incoming frames while a sink is slow.
Uploads need a digest before transmission. Blob/File sources are hashed/replayed
in 1-MiB slices; iterable inputs are staged into immutable Blobs first. Staging
consumes runtime-managed storage proportional to payload size, without requiring
a single contiguous JavaScript buffer. Retries reuse the staged input.

Subscriptions maintain replacement snapshots and a bounded update queue, ACKing
complete update cursors. Deletes, resync requests, unsupported deltas, source loss
and moves explicitly invalidate the snapshot; callers must resubscribe. These
cursors describe subscription progress, never object versions. `close()` is
idempotent, releases local leases by socket close and distributed leases by native
release, and should be awaited. It is not a durability barrier.

## Verification

```sh
node --test client/js/*.test.mjs
go test ./server -run '^TestJSClient' -count=1 -v -timeout=3m
STQR_TEST_BROWSER_MODULE=/path/to/playwright go test ./server -run '^TestJSClientBrowser$' -count=1 -v
```

Real-server tests cover single-node and two-node direct/gateway operations,
stream framing, references, subscriptions, read conditions, accepted/no-ACK
writes, durability fences, inventory and lease release/reacquisition. Chrome
tests actual cookie/CSRF tunnel use, direct cross-origin owner connections and
logout. Negative cases include origin policy, selectors, unknown targets,
forged/wrong-bucket/wrong-format/released grants, disabled users and revoked ACLs.
