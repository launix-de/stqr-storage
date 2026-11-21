# Bucket-bound object connection

The network constructor always receives URL, username, password and a bucket ID:

```go
conn, err := client.Dial(ctx, nodeURL, username, password, bucketID)
if err != nil {
    return err
}
defer conn.Close()
info, err := conn.Put(ctx, objectID, payload, client.PutOptions{
    RequestID: client.NewRequestID(),
})
```

`Client` exposes only object operations inside that bucket: Get/GetRange,
Put/PutRange, Append, Trunc, Delete, Stat, CAS, the necessary streaming variants
and Close. Object IDs are native `[32]byte` addresses. No per-call bucket argument,
Bucket(name), global multi-bucket handle, key-address alias API, administration
or cluster metrics are offered. Another bucket requires another connection.
The shared wire codec/statistics types remain in this package to avoid server /
client import cycles; they do not add administrative methods to the connection.

FUSE uses the already bound Get/CAS subset:

```go
filesystem, err := client.OpenFilesystem(ctx, conn, client.FSOptions{
    Create: true,
})
```

The in-memory filesystem backend is an explicitly volatile development backend,
not an unauthenticated network connection. Real network connections always require
all constructor arguments.

The transport uses HTTP Basic for the `stqr.v1` WebSocket upgrade, HELLO on each
new session, bounded send/pending queues and correlated responses. Data transfers
above 1 MiB use 1-MiB chunks and SHA-256. Streaming uses temporary files and bounded
buffers instead of buffering a complete large object; temporary files are removed
on success, cancellation and invalid transfers. `Get` convenience allocations are
limited to 16 MiB; use `GetTo` or ranged reads for larger payloads. This is not a
logical object-size limit. Inputs must remain unchanged until a call returns;
blocking reader/writer implementations are responsible for their own I/O cancellation.

On transport loss, operations reconnect and reuse their request ID. Cancellation
of a potentially sent mutation returns `UnknownOutcomeError` with that ID; retry
it unchanged. `StatusError` preserves server status/follow headers and maps 404,
412, 507 and 501 to ErrNotFound, ErrPrecondition, ErrFull and ErrNotImplemented.
No blind HTTP redirect forwards credentials to another host. Segmented routing,
learned ownership/RAM runtime IDs and replica routing are explicit multinode TODOs.

This server milestone implements volume administration first. Bucket management
is still specified only; native object handlers return 501. The client wire path
is tested against a protocol peer, including streaming, cancellation, request
multiplexing and a committed mutation whose connection fails before the reply.
The daemon integration checks successful normal-user HELLO, explicit 501 for
object commands and 403 for unauthorized administrative messages.
