Coding Rules
============

The server API is http+websocket and has besides the authorization mechanism three classes of protocol operations:
 - client operations (connect to bucket, get/put/cas/append/trunc/delete, subscriptions and so on)
 - admin operations (manage volumes, manage buckets, manage users, settings, monitor subscriptions)
 - peer operations (rebalancing protocol, adding and removing nodes)
depending on the type of user (peer, admin, client), only certain operations are allowed.
Client connections are pinned to a bucket, the server precaches security capabilities based on the ACL per connection. Cached permissions must be invalidated when the corresponding account, session or ACL is revoked or changed.

There is no format logic allowed inside the server code. The server only knows buckets and objects. The only exception: S3 is builtin into the server. (but builds upon the bucket objects infrastructure so it is a stqr client inside the server)

The client library must be available in multiple languages (go, c, rust, js) and offers all client operations on buckets.
Libraries on top of that may implement the formats (blockdevices bd, filesystem fs, s3) using the client library.

The clients are urged to learn the placement maps of their mounted bucket and contact the cluster nodes themselves.
The server supports this behaviour by sending 3xx responses to tell them chunks of the bucket's placement map so the client can retry to query the owning node of the requested object itself.
For some reasons (e.g. CORS or simple implementations), this behaviour can be deactivated in the current websocket session. In this case, the server will tunnel the request to the correct node instead of returning 3xx.

Rebuilding the placement map must balance multiple aspects in order to improve throughput and efficiency of the cluster:
 - always fullfilling hard constraints like storage class (ram, hdd, ssd), minimum replica amount, volume space constraints, bucket quotas
 - as minimal as possible cluster-movements of data (keep data on the volume where it was in the last generation whenever possible)
 - making enough free space on almost-full or full volumes to work efficiently and avoid stalls/delays
 - improving throughput and reducing network delays of the cluster by considering statistics (reads/writes per user, bucket, volume, placement range), especially if bottlenecks are detected (full disks, read/write bandwith of volumes and network connections)
 - smart placement of the replicas: balance writes (evenly-spread owners), short path to reader clients (client's ping and per-range access statistics)
 - more-than-required replicas for frequent reads (only triggered by bottleneck detection) with creation and cleanup hysteresis built on statistics
 - prefer placing data near their connected clients (the client or server can measure the ping and per-node-throughput between each single client-to-clusternode and provide those hints to the cluster so the cluster can aggregate (e.g. average) those statistics per user or bucket)
 - efficient coordination: do not perform full-scans on volumes or buckets, keep aggregated statistics iteratively in ram, use sliding windows like: currentframe=currentframe+current_readwrite_amount during ops, aggframe=aggframe*(1-a)+a*currentframe per generation where a = 1 - exp(-ln(2) * time_since_last_frame / half_life), using the same time unit for both values and a configurable half-life (default: 1 week)

Formats must follow the following rules:
 - formats are always built upon bucket objects. No extension of the server protocol required.
 - one format string per format -> a bucket is pinned on a format by first use (additional version information might be encoded inside the bucket data, e.g. in a root/metadata object)
 - backwards compatibility with older versions of the format but with the goal to migrate to newer format either by a central client-side migration routine on client connect or per objekt during writes - depending on the situation)
 - keep object ids as stable as possible to avoid big data movements on simple write operations (prefer range-put and cas over multiple version copies)
 - distribute object ids over the full numeric range (no id+1, rather use hashes) to support striping of the data
 - prefer splitting large objects into multiple ones <= 1MB (there is no server-side limit and exceptions like big metadata records or no-split up to 4MB are allowed)
 - formats must be self-contained and usable even after bucket rename/cow-copies (no hashing of the bucket name into object IDs e.g.)
 - keep the number of hops (number of successive reads over linked objects) as minimal as possible to avoid delays (e.g. instead of metadata -> chunkmap -> chunk, use a flexible-length part of the metadata at the end to store the chunkmap)
 - provide a dashboard plugin for each format so the user can examine and manipulate
 - when the format is established, benchmark it on the server and improve the server's handling of your format by working on the generic bucket objects handling (e.g. introducing more parallelity, microtune the implementation of the operators) - no format-specific messages
 - do not sync every write unless its API contract guarantees persistence (like in S3 PUT); bundle writes when the contract allows it. Unsynced writes may become durable through a later checkpoint, but must not be assumed to persist without one.

Further invariants for coding at the server's mechanisms:

Avoid full scans: normal requests and placement decisions must never full-scan buckets or volumes; recovery, bootstrap, scub and explicit administration might have limited, continuable scans.

Atomic operations (cas, put, append) must not expose visible intermediate-states to any client.

Confirmed persistence (e.g. sync) guarantees durability of the confirmed items.

Cancellations, crashes and reboots must leave an automatically recoverable state, assuming the backing store honors successful persistence barriers.

Only free after no more uses are proven. This refers to handles, running requests, COW-connections between buckets and so on.

Placement changes object location, but never content or access rules. Writes and reads during relocations must yield either the correct result and outcome or return an error code that allows the client to retry (e.g. on another location or at a later point in time).

Node Outages are not counted as valid data. An unreachable owner is not an empty object. Missing information must not be filled with incorrect data.

Timeout does not mean "not executed". Certain writes may succeed even if the confirmation does not reach the connection peer. Non-idempotent commands like appends or increments must have a safeguard against repeated application, e.g. a request id or a CAS guard. Holding a lease alone does not prevent duplicate application.
