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
typedef struct {
    stqr_id request_id;
    stqr_u64 version, length;
    int has_version, has_length, absent;
} stqr_options;
typedef struct {
    stqr_u64 version, length;
    stqr_id runtime;
    stqr_u32 flags;
    stqr_u16 layout;
    stqr_u8 compression;
} stqr_info;
/* Return 0 or negative errno. status() preserves the last HTTP/STQR status.
 * Calls on one connection must be serialized; close must not race a call.
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
 * Object replacement requires an expected version or absent=1. CAS <= 1 MiB.
 */
int stqr_cas(stqr_client *, stqr_object_id, unsigned scope, stqr_u64 offset,
             const void *expected, size_t expected_len,
             const void *replacement, size_t replacement_len,
             const stqr_options *, stqr_info *);
#ifdef __cplusplus
}
#endif
#endif
