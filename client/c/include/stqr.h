/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef STQR_H
#define STQR_H
#ifdef __KERNEL__
#include <linux/types.h>
#include <linux/stddef.h>
typedef u8 stqr_u8;
typedef u16 stqr_u16;
typedef u32 stqr_u32;
typedef u64 stqr_u64;
#else
#include <stddef.h>
#include <stdint.h>
typedef uint8_t stqr_u8;
typedef uint16_t stqr_u16;
typedef uint32_t stqr_u32;
typedef uint64_t stqr_u64;
#endif
#ifdef __cplusplus
extern "C" {
#endif
#define STQR_BUFFER_LIMIT (16U * 1024U * 1024U)
typedef struct { stqr_u8 bytes[16]; } stqr_id;
typedef struct { stqr_u8 bytes[32]; } stqr_object_id;
typedef struct stqr_client stqr_client;
typedef struct { stqr_id subscription; stqr_u64 cursor; } stqr_proof;
typedef struct {
    stqr_id request_id;
    stqr_u64 length;
    int has_length, absent;
 stqr_u64 compare_offset;
 const void *compare; size_t compare_size;
 int has_compare;
    int accepted; /* opt-in visible ACK; stqr_sync confirms persistence */
} stqr_options;
typedef struct {
    stqr_u64 length;
 stqr_proof proof; /* local cache proof; not on wire */
    stqr_id runtime;
    stqr_u32 flags;
    stqr_u16 layout;
    stqr_u8 compression;
} stqr_info;
/* Return 0 or negative errno. status() preserves the last HTTP/STQR status.
 * Calls on one connection must be serialized; close must not race a call.
 * Owner redirects reuse bounded per-endpoint sessions, retaining subscriptions.
 * sync covers all sessions; pending_mark confirms other sessions before returning
 * one remaining node-local mark. Explicit reconnect invalidates all subscriptions.
 * Dial always takes URL, username, password and bucket. No global credentials.
 * URLs are http:// or ws://; encrypted transport is not currently supported.
 */
int stqr_dial(stqr_client **out, const char *url, const char *username,
              const char *password, stqr_id bucket, unsigned timeout_ms);
void stqr_close(stqr_client *client);
int stqr_reconnect(stqr_client *client);
int stqr_status(const stqr_client *client);
/* A failed, possibly sent mutation returns -EINPROGRESS. Reconnect and retry
 * with identical arguments and the same caller-owned request_id. No blind retry.
 */
int stqr_request_id(stqr_id *out);
int stqr_parse_id(const char *hex, stqr_id *out);
int stqr_parse_object_id(const char *hex, stqr_object_id *out);
void stqr_free(void *data);
int stqr_get(stqr_client *, stqr_object_id, void **data, size_t *length, stqr_info *);
int stqr_get_range(stqr_client *, stqr_object_id, stqr_u64 offset,
                   stqr_u64 length, void **data, size_t *actual, stqr_info *);
int stqr_stat(stqr_client *, stqr_object_id, stqr_info *);
/* Advisory cluster capacity for this bucket. Unlimited buckets report eligible
 * free cluster storage; finite quotas cap free space. No space is reserved. */
typedef struct {
    stqr_u64 used, total, free, available, quota;
    int quota_limited;
} stqr_space;
int stqr_bucket_space(stqr_client *, stqr_space *);
int stqr_put(stqr_client *, stqr_object_id, const void *, size_t,
             const stqr_options *, stqr_info *);
int stqr_put_range(stqr_client *, stqr_object_id, stqr_u64, const void *, size_t,
                   const stqr_options *, stqr_info *);
int stqr_append(stqr_client *, stqr_object_id, const void *, size_t,
                const stqr_options *, stqr_info *, stqr_u64 *start);
int stqr_trunc(stqr_client *, stqr_object_id, stqr_u64,
               const stqr_options *, stqr_info *);
int stqr_delete(stqr_client *, stqr_object_id, const stqr_options *);
/* scope: 0 compares equal-sized byte ranges; 1 replaces the whole object.
 * Object replacement compares all old bytes, or uses absent=1. CAS <= 1 MiB.
 */
int stqr_cas(stqr_client *, stqr_object_id, unsigned scope, stqr_u64 offset,
             const void *expected, size_t expected_len,
             const void *replacement, size_t replacement_len,
             const stqr_options *, stqr_info *);
#define STQR_CAP_DURABILITY 8192U
typedef struct { stqr_id boot; stqr_u64 sequence; } stqr_commit_mark;
/* Mark observation includes reads and subscription updates, preserving causal
 * dependencies. Close is not a durability barrier. Marks survive reconnect;
 * loss of an unconfirmed server incarnation makes sync return -ESTALE. */
int stqr_pending_mark(stqr_client *, stqr_commit_mark *);
int stqr_sync_through(stqr_client *, const stqr_commit_mark *, stqr_commit_mark *);
int stqr_sync(stqr_client *);
/* Split barrier: capture/apply while serializing the original client; wait uses
 * independent control sockets and may run concurrently with that client's I/O.
 * A fence may be reused after wait/apply; do not capture/close while it waits.
 * Successful confirmations never erase later, still-unconfirmed writes.
 * This confirms storage writes; it does not create versions or transactions. */
typedef struct stqr_fence stqr_fence;
int stqr_fence_capture(stqr_client *, stqr_fence **);
/* Combine captures from multiple serialized clients. Same-owner marks are
 * merged to their maximum. After capture, distinct targets may wait in parallel.
 * Only call apply/close/reset after every target has completed. */
int stqr_fence_reset(stqr_fence **);
int stqr_fence_add(stqr_client *, stqr_fence *);
unsigned stqr_fence_count(const stqr_fence *);
int stqr_fence_wait_one(stqr_fence *, unsigned target);
int stqr_fence_wait(stqr_fence *);
void stqr_fence_apply(stqr_client *, const stqr_fence *);
void stqr_fence_close(stqr_fence *);

/* Propagate a real successful SYNC response across connections to this bucket. */
void stqr_note_durable(stqr_client *, const stqr_commit_mark *);
/* Subscriptions use the same wire protocol as the Go SDK. Calls, polling and
 * callbacks are serialized by the caller. Callback data is borrowed until return;
 * callbacks must not call this connection. NULL update means connection loss.
 * There is no background userspace thread; poll from your event loop (or use a
 * dedicated connection). Unsubscribe is a barrier. Resume returns a fresh snapshot.
 */
#define STQR_CAP_SUBSCRIPTIONS 512U
#define STQR_UPDATE_REPLACE 0
#define STQR_UPDATE_DELETE 4
#define STQR_UPDATE_RESYNC 5
typedef struct {
    stqr_id subscription;
    stqr_u64 cursor, offset, length;
    stqr_u32 part, parts;
    unsigned change;
    const void *data;
    size_t size;
} stqr_update;
typedef void (*stqr_update_handler)(void *, const stqr_update *);
void stqr_set_update_handler(stqr_client *, stqr_update_handler, void *);
stqr_u64 stqr_capabilities(const stqr_client *);
int stqr_subscribe(stqr_client *, stqr_object_id, stqr_id subscription,
                   int ranged, stqr_u64 offset, stqr_u64 length,
                   const stqr_u64 *resume, void **data, size_t *actual,
                   stqr_info *, stqr_u64 *cursor);
int stqr_unsubscribe(stqr_client *, stqr_id subscription);
/* 0: no event before timeout, 1: one event processed, negative errno on loss.
 * Once a frame starts, the normal socket timeout applies to its remainder.
 */
int stqr_poll(stqr_client *, unsigned timeout_ms);
#ifdef __cplusplus
}
#endif
#endif
