/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef STQR_PORT_H
#define STQR_PORT_H
#include "stqr.h"
#ifdef __KERNEL__
#include <linux/string.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#else
#include <string.h>
#include <errno.h>
#include <stdio.h>
#endif
/* The core has no OS socket, allocator, crypto or threading dependencies.
 * Userspace and kernel supply these operations. I/O returns exact bytes or errno.
 */
/* Reserved internal result: sq_send rejected the complete frame before any
 * byte could reach the transport. Only a port that proves this may return it.
 * Ordinary errno (including EINTR) can follow partial delivery and is ambiguous.
 * Never return this from recv, or after a transport worker has been dispatched. */
#define SQ_SEND_NOT_SUBMITTED (-4096)
struct stqr_socket;
void *sq_alloc(size_t n);
void sq_free(void *p);
void sq_secret_free(void *p, size_t n);
int sq_random(void *p, size_t n);
int sq_hash(unsigned bits, const void *p, size_t n, stqr_u8 *out);
int sq_connect(struct stqr_socket **out, const char *host, unsigned port, unsigned ms);
int sq_send(struct stqr_socket *, const void *, size_t);
int sq_recv(struct stqr_socket *, void *, size_t);
/* Wait for readable bytes or disconnect: 0 timeout, 1 ready, negative errno. */
int sq_wait(struct stqr_socket *, unsigned ms);
/* Monotonic retry budget and interruptible backoff; neither touches framing. */
stqr_u64 sq_now_ms(void);
int sq_pause(unsigned ms);
void sq_disconnect(struct stqr_socket *);
void sq_set_readable(struct stqr_socket *, void (*)(void *), void *);
#endif
