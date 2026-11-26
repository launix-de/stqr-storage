/* SPDX-License-Identifier: GPL-3.0-only */
#define _POSIX_C_SOURCE 200809L
#include "port.h"
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <poll.h>
#include <time.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
struct stqr_socket { int fd; };
void *sq_alloc(size_t n) { return calloc(1, n ? n : 1); }
void sq_free(void *p) { free(p); }
void sq_secret_free(void *p, size_t n) { if (p) { OPENSSL_cleanse(p, n); free(p); } }
int sq_random(void *p, size_t n) { return n <= 0x7fffffff && RAND_bytes(p, (int)n) == 1 ? 0 : -EIO; }
int sq_hash(unsigned bits, const void *p, size_t n, stqr_u8 *out) {
    unsigned len = 0;
    return EVP_Digest(p, n, out, &len, bits == 160 ? EVP_sha1() : EVP_sha256(), NULL) == 1 ? 0 : -EIO;
}
int sq_connect(struct stqr_socket **out, const char *host, unsigned port, unsigned ms) {
    struct addrinfo hints = {0}, *list, *a;
    struct timeval tv;
    char service[8];
    int fd = -1, err = -ECONNREFUSED;
    *out = NULL;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    snprintf(service, sizeof(service), "%u", port);
    if (getaddrinfo(host, service, &hints, &list)) return -EHOSTUNREACH;
    for (a = list; a; a = a->ai_next) {
        struct pollfd pollfd;
        int soerr = 0;
        socklen_t slen = sizeof(soerr);
        fd = socket(a->ai_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) { err = -errno; continue; }
        if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) { err = -errno; close(fd); fd = -1; continue; }
        if (connect(fd, a->ai_addr, a->ai_addrlen) < 0 && errno != EINPROGRESS) { err = -errno; close(fd); fd = -1; continue; }
        pollfd.fd = fd; pollfd.events = POLLOUT; pollfd.revents = 0;
        if (poll(&pollfd, 1, (int)ms) <= 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) || soerr) {
            err = soerr ? -soerr : -ETIMEDOUT; close(fd); fd = -1; continue;
        }
        if (fcntl(fd, F_SETFL, 0) < 0) { err = -errno; close(fd); fd = -1; continue; }
        /* A notification ACK followed by a small RPC must not wait for
         * delayed TCP ACKs. WebSocket frames are already sent contiguously. */
        { int on=1; if (setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&on,sizeof(on))) {
            err=-errno;close(fd);fd=-1;continue;
        } }
        tv.tv_sec = ms / 1000; tv.tv_usec = (ms % 1000) * 1000;
        if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) || setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv))) {
            err = -errno; close(fd); fd = -1; continue;
        }
        break;
    }
    freeaddrinfo(list);
    if (fd < 0) return err;
    *out = sq_alloc(sizeof(**out));
    if (!*out) { close(fd); return -ENOMEM; }
    (*out)->fd = fd;
    return 0;
}
int sq_send(struct stqr_socket *s, const void *p, size_t n) {
    const char *b = p;
    while (n) {
        ssize_t k = send(s->fd, b, n, MSG_NOSIGNAL);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) return k ? -errno : -EPIPE;
        b += k; n -= (size_t)k;
    }
    return 0;
}
int sq_recv(struct stqr_socket *s, void *p, size_t n) {
    char *b = p;
    while (n) {
        ssize_t k = recv(s->fd, b, n, 0);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) return k ? -errno : -ECONNRESET;
        b += k; n -= (size_t)k;
    }
    return 0;
}
void sq_disconnect(struct stqr_socket *s) { if (s) { close(s->fd); free(s); } }

int sq_wait(struct stqr_socket *s, unsigned ms) {
    struct pollfd p = {.fd = s->fd, .events = POLLIN};
    int n;
    if (ms > 3600000) return -EINVAL;
    do { n = poll(&p, 1, (int)ms); } while (n < 0 && errno == EINTR);
    return n < 0 ? -errno : n ? 1 : 0;
}

stqr_u64 sq_now_ms(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (stqr_u64)t.tv_sec*1000+t.tv_nsec/1000000; }
int sq_pause(unsigned ms) { struct timespec t = {ms/1000, (long)(ms%1000)*1000000}; return nanosleep(&t, NULL) ? -errno : 0; }

/* Userspace owns its event loop; stqr_poll performs the readiness wait. */
void sq_set_readable(struct stqr_socket *s, void (*ready)(void *), void *arg) { (void)s; (void)ready; (void)arg; }
