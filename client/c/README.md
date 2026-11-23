# Native C client

The C library connects directly to STQR over an authenticated, bucket-bound
WebSocket. It does not embed Go, call a helper daemon, or implement its operations
through another language's SDK. URL, username, password and bucket ID are required
by `stqr_dial`. The server checks the grant at upgrade; it cannot be changed on an
established connection.

Build with a C11 compiler, make and OpenSSL development headers:

```sh
make -C client/c
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
    int err = stqr_dial(&conn, url, user, password, bucket, 30000);
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
- Return values are zero on success or negative errno. `stqr_status` exposes the
  last HTTP/STQR status, including 412 for an unmet precondition. There is no
  shared global error state. Dial failures leave the output pointer NULL.
- Every mutation requires caller-owned options with a nonzero request ID. Generate
  one per logical mutation. Reuse the ID and all arguments for a retry, including
  after reconnect. A potentially sent mutation interrupted by transport loss
  returns `-EINPROGRESS`; the library does not silently report failure or generate
  another ID. `stqr_reconnect` reauthenticates; it never replays operations itself.
- `has_version` and `has_length` distinguish an omitted precondition from zero.
  `absent` is create-only and cannot be combined with an expected version.
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
  IPv6. DNS resolution is provided by the POSIX adapter. No redirects, TLS,
  compression extensions or implicit credential forwarding are performed.
- The timeout bounds socket connection and individual blocking socket I/O;
  DNS resolution uses the system resolver. There is no asynchronous cancellation
  or background heartbeat/reconnect thread.
- Runtime IDs are retained across reconnect; an observed RAM-runtime change is
  reported as `-ESTALE`. Multinode routing/redirect following is not implemented.

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
