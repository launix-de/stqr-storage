// SPDX-License-Identifier: GPL-3.0-only
#include "port.h"
#include <crypto/hash.h>
#include <linux/in.h>
#include <linux/in6.h>
#include <linux/inet.h>
#include <linux/net.h>
#include <linux/random.h>
#include <linux/slab.h>
#include <linux/vmalloc.h>
#include <net/net_namespace.h>
#include <net/sock.h>
struct stqr_socket {
	struct socket *sock;
};
void *sq_alloc(size_t n) { return kvzalloc(n ? n : 1, GFP_NOFS); }
void sq_free(void *p) { kvfree(p); }
void sq_secret_free(void *p, size_t n)
{
	if (p) {
		memzero_explicit(p, n);
		kvfree(p);
	}
}
int sq_random(void *p, size_t n)
{
	get_random_bytes(p, n);
	return 0;
}
int sq_hash(unsigned bits, const void *p, size_t n, stqr_u8 *out)
{
	struct crypto_shash *tfm;
	struct shash_desc *desc;
	int err;
	tfm = crypto_alloc_shash(bits == 160 ? "sha1" : "sha256", 0, 0);
	if (IS_ERR(tfm))
		return PTR_ERR(tfm);
	desc = kmalloc(sizeof(*desc) + crypto_shash_descsize(tfm), GFP_NOFS);
	if (!desc) {
		crypto_free_shash(tfm);
		return -ENOMEM;
	}
	desc->tfm = tfm;
	err = crypto_shash_digest(desc, p, n, out);
	kfree_sensitive(desc);
	crypto_free_shash(tfm);
	return err;
}
int sq_connect(struct stqr_socket **out, const char *host, unsigned port, unsigned ms)
{
	struct sockaddr_storage address = {0};
	struct sockaddr_in *v4 = (struct sockaddr_in *)&address;
	struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)&address;
	struct stqr_socket *s;
	int family, len, err;
	*out = NULL;
	if (in4_pton(host, -1, (u8 *)&v4->sin_addr, -1, NULL)) {
		family = AF_INET;
		len = sizeof(*v4);
		v4->sin_family = family;
		v4->sin_port = htons(port);
	} else {
		/* A failed IPv4 parse may have touched the overlapping storage. */
		memset(&address, 0, sizeof(address));
		if (!in6_pton(host, -1, (u8 *)&v6->sin6_addr, -1, NULL))
			return -EINVAL; /* Resolve hostnames in the mount helper. */
		family = AF_INET6;
		len = sizeof(*v6);
		v6->sin6_family = family;
		v6->sin6_port = htons(port);
	}
	s = kzalloc(sizeof(*s), GFP_NOFS);
	if (!s)
		return -ENOMEM;
	err = sock_create_kern(&init_net, family, SOCK_STREAM, IPPROTO_TCP, &s->sock);
	if (err) {
		kfree(s);
		return err;
	}
	s->sock->sk->sk_rcvtimeo = msecs_to_jiffies(ms);
	s->sock->sk->sk_sndtimeo = msecs_to_jiffies(ms);
	sk_set_memalloc(s->sock->sk);
	err = kernel_connect(s->sock, (struct sockaddr_unsized *)&address, len, 0);
	if (err) {
		sock_release(s->sock);
		kfree(s);
		return err;
	}
	*out = s;
	return 0;
}
int sq_send(struct stqr_socket *s, const void *p, size_t n)
{
	struct msghdr msg = {.msg_flags = MSG_NOSIGNAL};
	struct kvec vec;
	while (n) {
		int k;
		vec.iov_base = (void *)p;
		vec.iov_len = n;
		k = kernel_sendmsg(s->sock, &msg, &vec, 1, n);
		if (k <= 0)
			return k ? k : -EPIPE;
		p = (const char *)p + k;
		n -= k;
	}
	return 0;
}
int sq_recv(struct stqr_socket *s, void *p, size_t n)
{
	struct msghdr msg = {0};
	struct kvec vec;
	while (n) {
		int k;
		vec.iov_base = p;
		vec.iov_len = n;
		k = kernel_recvmsg(s->sock, &msg, &vec, 1, n, 0);
		if (k <= 0)
			return k ? k : -ECONNRESET;
		p = (char *)p + k;
		n -= k;
	}
	return 0;
}
void sq_disconnect(struct stqr_socket *s)
{
	if (s) {
		kernel_sock_shutdown(s->sock, SHUT_RDWR);
		sock_release(s->sock);
		kfree(s);
	}
}
