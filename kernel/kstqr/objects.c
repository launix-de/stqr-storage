// SPDX-License-Identifier: GPL-3.0-only
/* Included after core.c: reuse its checked canonical codec, not another ABI. */
#include "kstqr.h"
#include <linux/ktime.h>
#include <linux/hashtable.h>
#include <linux/delay.h>
#include <linux/sched/signal.h>
#include <linux/completion.h>
#include <linux/overflow.h>
#include <linux/sort.h>
#include <linux/xattr.h>
static bool valid_name(const char *);
static int ks_overlay_node_cas(struct ks_fs *, const struct ks_snapshot *, const struct ks_node *);
static int ks_overlay_prune_orphan(struct ks_fs *, stqr_object_id);
static int delete_orphan_object(struct ks_fs *, stqr_object_id);
const stqr_object_id ks_root_id = {{0x3f, 0xda, 0xbc, 0x50, 0xba, 0xc2, 0xa0, 0xe8,
				    0x3a, 0x0a, 0x4b, 0x32, 0x46, 0xca, 0x79, 0xbe,
				    0x13, 0x7a, 0x00, 0x19, 0x40, 0x39, 0x44, 0x2a,
				    0x8d, 0x27, 0x51, 0x1e, 0x3b, 0x14, 0x21, 0xbe}};
struct segment {
	u64 length, offset;
	bool lower;
	stqr_object_id id;
};
struct segments {
	struct segment *p;
	size_t n, cap;
};
struct member {
	stqr_object_id id;
	struct buf before, after, writes;
	bool writes_legacy;
};
static bool eqid(stqr_object_id a, stqr_object_id b)
{
	return !memcmp(a.bytes, b.bytes, 32);
}
static bool nullid(stqr_object_id id)
{
	return zero(id.bytes, 32);
}
/* A single owner connection can carry many requests. Only encoding/framing
 * holds rpc->lock; reply callbacks complete independent callers and folios. */
static void ks_rpc_receive(struct work_struct *work)
{
	struct ks_rpc *rpc = container_of(work, struct ks_rpc, receive);
	int result;
	mutex_lock(&rpc->lock);
	do {
		result = rpc->client ? stqr_poll(rpc->client, 0) : 0;
	} while (result > 0);
    /* Socket loss also removes subscription coverage. Reconnect from this
     * existing event worker so an idle mmap can regain coverage without a new
     * application read and without introducing another socket or polling loop. */
    if(result<0 && rpc->fs && !READ_ONCE(rpc->fs->stopping))
    {
        rpc->reconnect_epoch++;
        ks_reconnect_wait(rpc->fs,rpc->client);
    }
	mutex_unlock(&rpc->lock);
}
static void ks_rpc_readable(void *arg)
{
	struct ks_rpc *rpc = arg;
	ks_queue_fence_work(&rpc->receive);
}
void ks_rpc_start(struct ks_rpc *rpc,struct ks_fs *fs)
{
    rpc->fs=fs;
	stqr_set_accepted_references(rpc->client,1);
    INIT_WORK(&rpc->receive, ks_rpc_receive);
	rpc->receive_started = true;
	stqr_set_readable_handler(rpc->client, ks_rpc_readable, rpc);
	ks_rpc_readable(rpc);
}
void ks_rpc_stop(struct ks_rpc *rpc)
{
	if (!rpc->receive_started)
		return;
	mutex_lock(&rpc->lock);
	stqr_set_readable_handler(rpc->client, NULL, NULL);
	mutex_unlock(&rpc->lock);
	cancel_work_sync(&rpc->receive);
	rpc->receive_started = false;
}
struct ks_async_result {
	struct completion done;
	int error, status;
	void *data;
	size_t length;
	stqr_info info;
};
static void ks_async_completed(void *arg, int error, int status, void *data,
			       size_t length, const stqr_info *info)
{
	struct ks_async_result *result = arg;
	result->error = error;
	result->status = status;
	result->data = data;
	result->length = length;
	if (info)
		result->info = *info;
	complete(&result->done);
}
struct ks_rpc *ks_rpc_lock(struct ks_fs *f, stqr_object_id id)
{
	unsigned slot = (id.bytes[0] ^ id.bytes[7] ^ id.bytes[19] ^ id.bytes[31]) % KS_RPC_SLOTS;
	struct ks_rpc *rpc = &f->rpc[slot];
	mutex_lock(&rpc->lock);
	return rpc;
}
/* Mounted storage has hard reconnect semantics: a transient node outage is
 * not a missing file or corrupt executable page. Per-attempt socket deadlines
 * still apply, but only explicit cancellation or a semantic server error ends
 * the operation. Mutation callers retain their original request identity. */
bool ks_transport_retryable(stqr_client *client, int err)
{
	switch (err) {
	case -ENOTCONN:
	case -ECONNRESET:
	case -ECONNREFUSED:
	case -EPIPE:
	case -ETIMEDOUT:
	case -EHOSTUNREACH:
	case -ENETUNREACH:
	case -ENETDOWN:
	case -EINPROGRESS:
		return true;
	case -EAGAIN:
		return stqr_status(client) == 0 || stqr_status(client) == 503;
	default:
		return false;
	}
}
int ks_reconnect_wait(struct ks_fs *f, stqr_client *client)
{
	unsigned pause = 20;
	for (;;) {
		int err;
		if (READ_ONCE(f->stopping))
			return -ESHUTDOWN;
		if (fatal_signal_pending(current))
			return -EINTR;
		err = stqr_reconnect(client);
        if(!err) {
            /* A foreground request may win the reconnect race against the
             * receiver. Restore coverage in either path while framing is held. */
            if (client == f->rpc[0].client)
                ks_cache_reconnected(f);
            return 0;
        }
		if (!ks_transport_retryable(client, err))
			return err;
		msleep(pause);
		pause = min(pause * 2, 1000U);
	}
}
/* Software contract: a completed 503 rejects just this request, not the
 * transport. Retry it with the same identity after yielding outside framing.
 * A lost shared connection may fail many callers: only the first caller for
 * that incarnation reconnects. Later completions must never tear down its
 * replacement and abort the next burst of otherwise independent requests.
 * Caller holds rpc->lock on entry and return; epoch is captured before submit.
 */
int ks_rpc_retry(struct ks_fs *f, struct ks_rpc *rpc, u64 epoch, int status)
{
    if (READ_ONCE(f->stopping))
        return -ESHUTDOWN;
    if (fatal_signal_pending(current))
        return -EINTR;
    /* Synchronous SDK entry can reject an already closed socket before
     * replacing its previous status. Such a stale503 is transport loss. */
    if (status == 503 && rpc->client && rpc->client->socket) {
        mutex_unlock(&rpc->lock);
        msleep(50);
        mutex_lock(&rpc->lock);
        return 0;
    }
    if (epoch != rpc->reconnect_epoch)
        return 0;
    rpc->reconnect_epoch++;
    return ks_reconnect_wait(f, rpc->client);
}
/* STATFS is read-only but can wait for cluster capacity information. A mounted
 * filesystem already has its normal receiver: only submit holds framing. Keep
 * the stack output/completion alive with the same uninterruptible join as GET;
 * a caller's fatal signal must never run receive/broken() for shared requests. */
int ks_bucket_space(struct ks_fs *f, stqr_space *space)
{
	struct ks_rpc *rpc = &f->rpc[0];
	int err;
	mutex_lock(&rpc->lock);
	for (;;) {
		struct ks_async_result result = {0};
		u64 epoch = rpc->reconnect_epoch;
		int status = 0;
		init_completion(&result.done);
		err = stqr_bucket_space_async(rpc->client, space, ks_async_completed, &result);
		mutex_unlock(&rpc->lock);
		if (!err) {
			wait_for_completion(&result.done);
			err = result.error;
			status = result.status;
		}
		sq_free(result.data);
		mutex_lock(&rpc->lock);
		if (!(err == -EAGAIN ? (status == 0 || status == 503) :
					ks_transport_retryable(NULL, err)))
			break;
		err = ks_rpc_retry(f, rpc, epoch, status);
		if (err)
			break;
	}
	mutex_unlock(&rpc->lock);
	return err;
}

int ks_object_reference(struct ks_fs *f, stqr_object_id id, stqr_id token, unsigned action)
{
	struct ks_rpc *rpc = ks_rpc_lock(f, id);
	int (*operation)(stqr_client *, stqr_object_id, stqr_id);
	int err;
	switch (action) {
	case 0: operation = stqr_object_pin; break;
	case 1: operation = stqr_object_unpin; break;
	case 2: operation = stqr_object_claim; break;
	case 3: operation = stqr_object_release_claim; break;
	case 4: operation = stqr_object_pin_dependency; break;
	case 5: operation = stqr_object_unpin_dependency; break;
	default: mutex_unlock(&rpc->lock); return -EINVAL;
	}
	bool auth_retry = false;
	for (;;) {
        u64 epoch = rpc->reconnect_epoch;
        int status = 0;
		if(rpc->receive_started) {
            struct ks_async_result result={0};init_completion(&result.done);
            err=stqr_object_reference_async(rpc->client,id,token,action,ks_async_completed,&result);
            mutex_unlock(&rpc->lock);
            if(!err){wait_for_completion(&result.done);err=result.error;status=result.status;}
            sq_free(result.data);mutex_lock(&rpc->lock);
        }else { err = operation(rpc->client, id, token); status = stqr_status(rpc->client); }
        /* Reference status 429 reports global/account table capacity. Expose ENFILE
         * to VFS for this operation only. Remap only the SDK's server-status
         * errno: a stale status after transport loss must not suppress hard
         * reconnection. Status 503 outage waits and 412 preconditions retain their
         * existing retry handling. */
        if (err == -EREMOTEIO && status == 429)
            err = -ENFILE;
		if (err == -EACCES && !auth_retry)
			auth_retry = true;
		else if (!(err == -EAGAIN ? (status == 0 || status == 503) : ks_transport_retryable(NULL, err)))
			break;
		/* Tokens are idempotent across reconnect; only an explicit kernel
         * sync promises their persistence across server restart. */
		err = ks_rpc_retry(f, rpc, epoch, status);
		if (err)
			break;
	}
	mutex_unlock(&rpc->lock);
	return err;
}
static int get_object_locked(struct ks_fs *f, struct ks_rpc *rpc, stqr_object_id id,
			     void **p, size_t *n, stqr_info *info)
{
	int err;
	for (;;) {
        u64 epoch = rpc->reconnect_epoch;
		err = stqr_get(rpc->client, id, p, n, info);
		if (!ks_transport_retryable(rpc->client, err))
			break;
		sq_free(*p);
		*p = NULL;
		*n = 0;
		err = ks_rpc_retry(f, rpc, epoch, stqr_status(rpc->client));
		if (err)
			break;
	}
	return err;
}
static int get_object(struct ks_fs *f, stqr_object_id id, void **p, size_t *n, stqr_info *info)
{
	struct ks_rpc *rpc;
	int err;
    u64 epoch;
	atomic64_inc(&f->rpc_gets);
	for (;;) {
		struct ks_async_result result = {0};
		init_completion(&result.done);
		rpc = ks_rpc_lock(f, id);
        epoch = rpc->reconnect_epoch;
		if (!rpc->receive_started) {
			err = get_object_locked(f, rpc, id, p, n, info);
			mutex_unlock(&rpc->lock);
			return err;
		}
		err = stqr_get_async(rpc->client, id, ks_async_completed, &result);
		mutex_unlock(&rpc->lock);
		if (!err) {
			wait_for_completion(&result.done);
			err = result.error;
			if (!err) {
				*p = result.data; *n = result.length; *info = result.info;
				return 0;
			}
			sq_free(result.data);
		}
		if (!(err==-EAGAIN ? (result.status==0||result.status==503) :
              ks_transport_retryable(NULL,err)))return err;
		mutex_lock(&rpc->lock);
		err = ks_rpc_retry(f, rpc, epoch, result.status);
		mutex_unlock(&rpc->lock);
		if (err)
			return err;
	}
}
/* Filesystem software contract: inode/directory, intent, decision and retirement
 * CAS all use accepted results. A completion confirms comparison and visibility,
 * not physical persistence. Dependent phases await those results without disk
 * fences; only explicit VFS sync/O_SYNC requests and unmount checkpoint owners.
 * A normal read/write/open/close makes no server-crash durability promise. */
static int cas_object(struct ks_fs *f, stqr_object_id id, const void *p, size_t n,
		      const void *expected, size_t expected_size, bool absent)
{
	struct ks_rpc *rpc;
	stqr_options o = {0};
	stqr_info info;
	int err;
	if (f->readonly)
		return -EROFS;
	o.accepted = 1;
	o.absent = absent;
	err = stqr_request_id(&o.request_id);
	if (err)
		return err;
	rpc = ks_rpc_lock(f, id);
	ks_cache_forget(f, id);
	atomic64_inc(&f->rpc_cas);
	bool auth_retry = false, replayed = false;
    int status=0;
	for (;;) {
        u64 epoch = rpc->reconnect_epoch;
        status = 0;
		if (rpc->receive_started) {
            struct ks_async_result result = {0};
            init_completion(&result.done);
            err = stqr_cas_async(rpc->client, id, 1, 0, expected, expected_size,
                                 p, n, &o, ks_async_completed, &result);
            mutex_unlock(&rpc->lock);
            if (!err) { wait_for_completion(&result.done); err = result.error; info = result.info; status=result.status; }
            sq_free(result.data);
            mutex_lock(&rpc->lock);
        } else {
            err = stqr_cas(rpc->client, id, 1, 0, expected, expected_size, p, n, &o, &info);
            status=stqr_status(rpc->client);
        }
		if (replayed && f->references && err == -EAGAIN && status == 412) {
			void *observed = NULL;
			size_t size = 0;
			int check = get_object_locked(f, rpc, id, &observed, &size, &info);
			/* Modern peers need no native CAS history. Resolve an uncertain
			 * retry only by its exact after-image, never by replaying an entire
			 * filesystem append against a newer inode snapshot. */
			err = !check && size == n && !memcmp(observed, p, n) ? 0 : -EIO;
			sq_free(observed);
			break;
		}
		if (err == -EACCES && !auth_retry)
			auth_retry = true;
		else if (!(err==-EAGAIN ? (status==0||status==503) : ks_transport_retryable(NULL,err)))
			break;
		// Retain the same request identity throughout a temporary outage.
		err = ks_rpc_retry(f, rpc, epoch, status);
		if (err)
			break;
		if (status != 503)
            replayed = true;
	}
	ks_cache_forget(f, id);
	mutex_unlock(&rpc->lock);
	return err;
}
/* One deletion attempt; callers retain its status across the framing unlock,
 * just as they retain its request identity across recovery. */
static int ks_delete_locked(struct ks_rpc *rpc, stqr_object_id id,
                            const stqr_options *opt, int *status)
{
    struct ks_async_result result = {0};
    stqr_options accepted = *opt;
    int err;
    accepted.accepted = 1;
    opt = &accepted;
    if (status)
        *status = 0;
    if (!rpc->receive_started) {
        err = stqr_delete(rpc->client, id, opt);
        if (status)
            *status = stqr_status(rpc->client);
        return err;
    }
    init_completion(&result.done);
    err = stqr_delete_async(rpc->client, id, opt, ks_async_completed, &result);
    mutex_unlock(&rpc->lock);
    if (!err) {
        wait_for_completion(&result.done);
        err = result.error;
        if (status)
            *status = result.status;
    }
    sq_free(result.data);
    mutex_lock(&rpc->lock);
    return err;
}
static int immutable(struct ks_fs *f, const void *p, size_t n, stqr_object_id *id)
{
	if (n > SQ_CHUNK)
		return -E2BIG;
	sq_random(id->bytes, 32);
	return cas_object(f, *id, p, n, NULL, 0, true);
}
#include "redo.c"
static int decode_node_format(const void *p, size_t n, struct ks_node *node, bool cow_six)
{
	struct buf d = {.p = (u8 *)p, .n = n};
	const u8 *v;
	size_t length;
	unsigned version, layout;
	memset(node, 0, sizeof(*node));
	if (n < 6 || n > SQ_CHUNK || memcmp(p, "STFS", 4))
		return -EUCLEAN;
	take(&d, 4);
	version = readnum(&d, 2);
	if (version < 1 || version > 7)
		return -EOPNOTSUPP;
	node->kind = readnum(&d, 1);
	node->mode = readnum(&d, 4);
	node->uid = readnum(&d, 4);
	node->gid = readnum(&d, 4);
	node->mtime = readnum(&d, 8);
	node->size = readnum(&d, 8);
	layout = readnum(&d, 1);
	if (layout == 0) {
		node->inline_data = readblob(&d, &length);
		if (length != node->size || length > 4096)
			return -EUCLEAN;
	} else if (layout == 1) {
		node->depth = readnum(&d, 1);
		v = take(&d, 32);
		if (v)
			memcpy(node->root.bytes, v, 32);
		if (!node->depth || node->depth > 16 || nullid(node->root) || !node->size)
			return -EUCLEAN;
	} else if (layout == 2 && (version == 7 || version == 6 && cow_six)) {
		node->lower = true;
		node->depth = readnum(&d, 1);
		v = take(&d, 32);
		if (v)
			memcpy(node->root.bytes, v, 32);
		if (!node->depth || node->depth > 16 || nullid(node->root) || !node->size ||
		    node->kind != 1)
			return -EUCLEAN;
	} else
		return -EUCLEAN;
	if (version == 2) {
		node->removed = boolean(&d);
		if (!node->removed)
			return -EUCLEAN;
	}
	if (version >= 3) {
		node->removed = boolean(&d);
		node->write_path = readblob(&d, &node->write_path_len);
		if (d.err || node->write_path_len > 4096 ||
		    (node->write_path_len && memchr(node->write_path, 0, node->write_path_len)))
			return -EUCLEAN;
	}
	if (version >= 4) {
		node->atime = readnum(&d, 8);
		node->ctime = readnum(&d, 8);
	} else
		node->atime = node->ctime = node->mtime;
	node->links = 1;
	if (version >= 5) {
		size_t start = d.pos, previous_length = 0;
		const u8 *previous = NULL;
		unsigned count, at;
		node->links = readnum(&d, 4);
		start = d.pos;
		count = readnum(&d, 4);
		if (count > 128)
			return -EUCLEAN;
		for (at = 0; at < count; at++) {
			char name[256];
			const u8 *key;
			size_t key_length, value_length;
			int cmp;
			key = readblob(&d, &key_length);
			if (d.err || !key_length || key_length > 255 || memchr(key, 0, key_length))
				return -EUCLEAN;
			memcpy(name, key, key_length);
			name[key_length] = 0;
			if (!valid_name(name) || !strchr(name, '.') || name[0] == '.' ||
			    name[key_length - 1] == '.')
				return -EUCLEAN;
			cmp =
			    previous ? memcmp(previous, key, min(previous_length, key_length)) : -1;
			if (cmp > 0 || (!cmp && previous_length >= key_length))
				return -EUCLEAN;
			readblob(&d, &value_length);
			if (d.err || value_length > 65536 || d.pos - start > 262144)
				return -EUCLEAN;
			previous = key;
			previous_length = key_length;
		}
		node->xattrs = d.p + start;
		node->xattrs_size = d.pos - start;
	}
	if (version >= 5 && node->kind == 2 && !node->removed && !node->links)
		return -EUCLEAN;
	if (version == 6 && !cow_six) {
		node->mutable = readnum(&d, 8) != 0;
		if (!node->mutable)
			return -EUCLEAN;
	}
	if (version >= 7) {
		const u8 *digest;
		node->mutable = boolean(&d);
		digest = take(&d, 32);
		if (digest)
			memcpy(node->digest, digest, 32);
		if (boolean(&d)) {
			digest = take(&d, 32);
			if (digest)
				memcpy(node->lower_owner.bytes, digest, 32);
			digest = take(&d, 32);
			if (digest)
				memcpy(node->lower_digest, digest, 32);
			if (nullid(node->lower_owner))
				return -EUCLEAN;
		}
	}

	if (done(&d) || node->kind < 1 || node->kind > 5 || (node->kind > 2 && version < 5) ||
	    (node->removed && node->kind != 2) || (node->mode & ~07777U) || node->size > S64_MAX ||
	    node->atime > S64_MAX || node->mtime > S64_MAX || node->ctime > S64_MAX)
		return -EUCLEAN;
	if (node->kind == 3 && (node->depth || !node->size || node->size > 4095 ||
				memchr(node->inline_data, 0, node->size)))
		return -EUCLEAN;
	if ((node->kind == 4 || node->kind == 5) && node->size)
		return -EUCLEAN;
	return 0;
}
static int decode_node(const void *p, size_t n, struct ks_node *node)
{
	int err = decode_node_format(p, n, node, false);
	if (err && n >= 6 && !memcmp(p, "STFS\6\0", 6))
		return decode_node_format(p, n, node, true);
	return err;
}

static int lower_fingerprint(const struct ks_node *n, u8 digest[32])
{
	struct buf b = {0};
	int err;
	put(&b, "STQR readonly lower data v1\0", sizeof("STQR readonly lower data v1\0") - 1);
	number(&b, n->mtime, 8);
	number(&b, n->size, 8);
	put(&b, n->root.bytes, 32);
	number(&b, n->depth, 1);
	blob(&b, n->inline_data, n->depth ? 0 : n->size);
	err = b.err ? b.err : sq_hash(256, b.p, b.n, digest);
	drop(&b);
	return err;
}
static int check_lower_anchor(struct ks_fs *f, const struct ks_node *n)
{
	struct ks_snapshot lower = {0};
	u8 digest[32];
	int err;
	if (nullid(n->lower_owner))
		return 0;
	if (!f->lower)
		return -EUCLEAN;
	err = ks_plain_snapshot(f->lower, n->lower_owner, &lower);
	if (!err)
		err = lower_fingerprint(&lower.node, digest);
	if (!err && memcmp(digest, n->lower_digest, 32))
		err = -ESTALE;
	ks_snapshot_free(&lower);
	return err;
}
static void encode_node(struct buf *b, const struct ks_node *node)
{
	put(b, "STFS", 4);
	number(b, 7, 2);
	number(b, node->kind, 1);
	number(b, node->mode, 4);
	number(b, node->uid, 4);
	number(b, node->gid, 4);
	number(b, node->mtime, 8);
	number(b, node->size, 8);
	number(b, node->lower ? 2 : node->depth ? 1 : 0, 1);
	if (node->depth) {
		number(b, node->depth, 1);
		put(b, node->root.bytes, 32);
	} else
		blob(b, node->inline_data, node->size);
	number(b, node->removed, 1);
	blob(b, node->write_path, node->write_path_len);
	number(b, node->atime, 8);
	number(b, node->ctime, 8);
	number(b, node->removed ? 0 : node->links, 4);
	if (node->xattrs)
		put(b, node->xattrs, node->xattrs_size);
	else
		number(b, 0, 4);
	number(b, node->mutable, 1);
	put(b, node->digest, 32);
	number(b, !nullid(node->lower_owner), 1);
	if (!nullid(node->lower_owner)) {
		put(b, node->lower_owner.bytes, 32);
		put(b, node->lower_digest, 32);
	}
}
void ks_snapshot_free(struct ks_snapshot *s)
{
	kfree(s->directory_proof);
	sq_free(s->namespace_head);
	sq_free(s->raw);
	sq_free(s->view);
	memset(s, 0, sizeof(*s));
}
static bool marker(const void *raw, size_t n, stqr_object_id *id)
{
	if (n != 38 || memcmp(raw, "STFL\1\0", 6))
		return false;
	memcpy(id->bytes, (const u8 *)raw + 6, 32);
	return !nullid(*id);
}

static int validate_tx_image(stqr_object_id id, const void *raw, size_t len, struct ks_node *node)
{
	struct buf d = {.p = (u8 *)raw, .n = len};
	const u8 *image;
	size_t size;
	if (len < 4 || memcmp(raw, "STOR", 4))
		return decode_node(raw, len, node);
	if (!eqid(id, ks_root_id))
		return -EUCLEAN;
	take(&d, 4);
	if (readnum(&d, 2) != 1)
		return -EOPNOTSUPP;
	take(&d, 16);
	image = readblob(&d, &size);
	return d.err ? d.err : done(&d) ? -EUCLEAN : decode_node(image, size, node);
}
static int tx_decode(struct buf *raw, stqr_object_id txid, struct member **out, unsigned *count,
		     unsigned *state)
{
	struct buf d = {.p = raw->p, .n = raw->n};
	struct member *m;
	unsigned i;
	const u8 *p;
	size_t n;
	if (raw->n < 11 || raw->n > (128U << 20) ||
	    (memcmp(raw->p, "STFT", 4) || raw->p[5] || raw->p[4] < 1 || raw->p[4] > 3))
		return -EUCLEAN;
	take(&d, 6);
	*state = readnum(&d, 1);
	*count = readnum(&d, 4);
	if (*state > 2 || !*count || *count > 1024 || *count > (raw->n - 11) / 42)
		return -EUCLEAN;
	m = sq_alloc(*count * sizeof(*m));
	if (!m)
		return -ENOMEM;
	for (i = 0; i < *count; i++) {
		struct ks_node check;
		p = take(&d, 32);
		if (p)
			memcpy(m[i].id.bytes, p, 32);
		if (raw->p[4] < 3)
			readnum(&d, 8);
		p = readblob(&d, &n);
		m[i].before.p = (u8 *)p;
		m[i].before.n = n;
		if (d.err || validate_tx_image(m[i].id, p, n, &check))
			goto bad;
		p = readblob(&d, &n);
		m[i].after.p = (u8 *)p;
		m[i].after.n = n;
		m[i].writes_legacy = raw->p[4] < 3;
		if (raw->p[4] >= 2 && skip_edits(&d, &m[i].writes, raw->p[4] < 3))
			goto bad;
		if (d.err || validate_tx_image(m[i].id, p, n, &check) || nullid(m[i].id) ||
		    eqid(txid, m[i].id) || (i && memcmp(m[i - 1].id.bytes, m[i].id.bytes, 32) >= 0))
			goto bad;
	}
	if (done(&d))
		goto bad;
	*out = m;
	return 0;
bad:
	sq_free(m);
	return -EUCLEAN;
}
/* Iterative helping: no recursive descriptor chain on the kernel stack. */
static int help_tx_legacy(struct ks_fs *f, stqr_object_id first)
{
	stqr_object_id *stack = sq_alloc(128 * sizeof(*stack));
	unsigned depth = 0, rounds = 0;
	int err = 0, first_result = -EAGAIN;
	if (!stack)
		return -ENOMEM;
	stack[0] = first;
	while (rounds++ < 8192) {
		struct buf raw = {0}, journal = {0};
		stqr_info info;
		struct member *m = NULL;
		unsigned count = 0, state, i;
		int decision = 1;
		u8 mark[38] = "STFL\1\0";
		stqr_object_id id = stack[depth];
		memcpy(mark + 6, id.bytes, 32);
		err = intent_get(f, id, &raw, &journal, &info);
		if (err) {
			drop(&raw);
			drop(&journal);
			break;
		}
		if (raw.n == 7 && !memcmp(raw.p, "STFD\1\0", 6)) {
			unsigned completed = raw.p[6];
			drop(&raw);
			drop(&journal);
			if (!depth) {
				first_result = completed == 1 ? 0 : -EAGAIN;
				break;
			}
			// A completed dependency is not the decision of our own intent.
			depth--;
			continue;
		}
		err = tx_decode(&raw, id, &m, &count, &state);
		if (err) {
			drop(&raw);
			drop(&journal);
			break;
		}
		for (i = 0; i < count; i++) {
			struct buf head = {0};
			stqr_info ci;
			stqr_object_id other;
			err = get_object(f, m[i].id, (void **)&head.p, &head.n, &ci);
			if (err) {
				drop(&head);
				break;
			}
			if (state) {
				if (head.n == sizeof(mark) && !memcmp(head.p, mark, sizeof(mark))) {
					struct buf *image = state == 1 ? &m[i].after : &m[i].before;
					if (state == 1 && m[i].writes.n) {
						u64 index = 0;
						unsigned j;
						for (j = 0; j < i; j++) {
							struct buf scan = m[j].writes;
							if (scan.n)
								index += readnum(&scan, 4);
						}
						err = apply_edits(f, id, &m[i].writes,
								  m[i].writes_legacy, &index);
						if (err) {
							struct buf observed = {0};
							stqr_info oi;
							int check = get_object(f, m[i].id,
									       (void **)&observed.p,
									       &observed.n, &oi);
							if (!check && (observed.n != sizeof(mark) ||
								       memcmp(observed.p, mark,
									      sizeof(mark))))
								err = 0;
							drop(&observed);
							drop(&head);
							break;
						}
					}
					err = cas_object(f, m[i].id, image->p, image->n, head.p,
							 head.n, false);
					if (err == -EAGAIN) {
						err = 0;
						decision = 0;
						drop(&head);
						break;
					}
				}
			} else if (head.n != sizeof(mark) || memcmp(head.p, mark, sizeof(mark))) {
				if (marker(head.p, head.n, &other)) {
					if (depth == 127)
						err = -ELOOP;
					else {
						stack[++depth] = other;
						decision = -1;
					}
					drop(&head);
					break;
				}
				if (head.n != m[i].before.n ||
				    memcmp(head.p, m[i].before.p, head.n)) {
					decision = 2;
					drop(&head);
					break;
				}
				err = cas_object(f, m[i].id, mark, sizeof(mark), head.p, head.n,
						 false);
				if (err == -EAGAIN) {
					err = 0;
					decision = 0;
					drop(&head);
					break;
				}
			}
			drop(&head);
			if (err)
				break;
		}
		if (!err && state && decision > 0) {
			intent_retire(f, id, &journal, &info, state);
			if (!depth)
				first_result = state == 1 ? 0 : -EAGAIN;
			else
				depth--;
		} else if (!err && !state && decision > 0) {
			{
				struct buf next = {0};
				put(&next, journal.p, journal.n);
				if (!next.err) {
					next.p[6] = decision;
					err = cas_object(f, id, next.p, next.n, journal.p,
							 journal.n, false);
				} else
					err = next.err;
				drop(&next);
			}
			if (err == -EAGAIN)
				err = 0;
		}
		sq_free(m);
		drop(&raw);
		drop(&journal);
		if (err || (state && decision > 0 && eqid(id, first)))
			break;
		cond_resched();
	}
	sq_free(stack);
	return err ? err : first_result;
}
#include "reference_cleanup.c"
#include "reference_redo.c"
static int help_tx(struct ks_fs *f, stqr_object_id first)
{
	return f->references ? help_tx_pinned(f, first, NULL) : help_tx_legacy(f, first);
}
static int ks_overlay_snapshot_cached(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
static int ks_plain_snapshot_cached(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *s)
{
	int err;
	memset(s, 0, sizeof(*s));
	s->id = id;
	err = ks_cache_peek(f, id, &s->raw, &s->raw_len, &s->info);
	if (!err)
		err = decode_node(s->raw, s->raw_len, &s->node);
	if (err)
		ks_snapshot_free(s);
	else {
		s->node.identity = id;
		sq_hash(256, s->raw, s->raw_len, s->node.head_digest);
	}
	return err;
}
int ks_snapshot_cached(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *s)
{
	return f->lower ? ks_overlay_snapshot_cached(f, id, s)
			: ks_plain_snapshot_cached(f, id, s);
}
/* Open must validate the post-PIN inode at its owner, but a fresh read of
 * identical metadata does not invalidate uninterrupted subscription coverage.
 * In particular, throwing that coverage away on every exec reloads otherwise
 * unchanged shared-library pages. Compare exact bytes; a changed/missing head
 * takes the ordinary cache-miss path and retains existing snapshot/guard checks.
 */
int ks_validate_open_cache(struct ks_fs *f, stqr_object_id id)
{
	void *cached = NULL, *fresh = NULL;
	size_t cached_size = 0, fresh_size = 0;
	stqr_info info;
	int err = ks_cache_peek(f, id, &cached, &cached_size, &info);
	if (err == -ENODATA || err == -ENOENT)
		return 0;
	if (err)
		return err;
	err = get_object(f, id, &fresh, &fresh_size, &info);
	if (err == -ENOENT) {
		ks_cache_forget(f, id);
		err = 0;
	} else if (!err && (fresh_size != cached_size || memcmp(cached, fresh, fresh_size))) {
		ks_cache_forget(f, id);
	}
	sq_free(cached);
	sq_free(fresh);
	return err;
}
int ks_snapshot(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *s)
{
	return f->lower ? ks_overlay_snapshot(f, id, s) : ks_plain_snapshot(f, id, s);
}
int ks_plain_snapshot(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *s)
{
	unsigned attempt;
	int err;
	memset(s, 0, sizeof(*s));
	s->id = id;
	for (attempt = 0; attempt < 128; attempt++) {
		stqr_object_id txid;
		err = ks_cache_get(f, id, &s->raw, &s->raw_len, &s->info);
		if (err == -ENODATA)
			err = get_object(f, id, &s->raw, &s->raw_len, &s->info);
		if (err)
			return err;
		if (!marker(s->raw, s->raw_len, &txid)) {
			err = validate_tx_image(id, s->raw, s->raw_len, &s->node);
			if (err)
				ks_snapshot_free(s);
			else {
				s->node.identity = id;
				sq_hash(256, s->raw, s->raw_len, s->node.head_digest);
			}
			return err;
		}
		ks_cache_forget(f, id);
		if (f->readonly) {
			struct buf raw = {0}, journal = {0}, view = {0};
			stqr_info info;
			struct member *m = NULL;
			unsigned n, state, i;
			struct buf selected = {0};
			u8 guard_digest[32];
			bool legacy = false;
			err = intent_get(f, txid, &raw, &journal, &info);
			if (err == -ENOENT || (!err && raw.n == 7 && !memcmp(raw.p, "STFD\1\0", 6))) {
				drop(&raw);
				drop(&journal);
				sq_free(s->raw);
				s->raw = NULL;
				continue;
			}
			if (!err)
				err = tx_decode(&raw, txid, &m, &n, &state);
			if (!err) {
				err = -EUCLEAN;
				for (i = 0; i < n; i++)
					if (eqid(m[i].id, id)) {
						struct buf *image =
						    state == 1 ? &m[i].after : &m[i].before;
						put(&selected, image->p, image->n);
						legacy = m[i].writes_legacy;
						if (state == 1 && m[i].writes.n)
							put(&view, m[i].writes.p, m[i].writes.n);
						err = selected.err ? selected.err : view.err;
						break;
					}
			}
			sq_hash(256, journal.p, journal.n, guard_digest);
			sq_free(m);
			drop(&raw);
			drop(&journal);
			sq_free(s->raw);
			s->raw = selected.p;
			s->raw_len = selected.n;
			if (!err)
				err = validate_tx_image(id, s->raw, s->raw_len, &s->node);
			if (!err) {
				s->view = view.p;
				s->node.view = view.p;
				s->node.view_size = view.n;
				s->node.view_legacy = legacy;
				s->node.identity = id;
				sq_hash(256, s->raw, s->raw_len, s->node.head_digest);
				s->node.guard = txid;
				memcpy(s->node.guard_digest, guard_digest, 32);
				/* guard is checked by actual journal bytes below */
			} else {
				drop(&view);
				ks_snapshot_free(s);
			}
			return err;
		}
		sq_free(s->raw);
		s->raw = NULL;
		err = help_tx(f, txid);
		if (err && err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
static int segment_add(struct segments *s, struct segment segment)
{
	if (!segment.length)
		return 0;
	if (s->n && nullid(segment.id) && nullid(s->p[s->n - 1].id)) {
		s->p[s->n - 1].length += segment.length;
		return 0;
	}
	if (s->n == s->cap) {
		size_t cap = s->cap ? s->cap * 2 : 16;
		void *p;
		if (cap > 1048576)
			return -E2BIG;
		p = sq_alloc(cap * sizeof(*s->p));
		if (!p)
			return -ENOMEM;
		if (s->n)
			memcpy(p, s->p, s->n * sizeof(*s->p));
		sq_free(s->p);
		s->p = p;
		s->cap = cap;
	}
	s->p[s->n++] = segment;
	return 0;
}
static int index_segments_range(struct ks_fs *f, const struct ks_node *view, stqr_object_id id,
			        u64 length, unsigned depth, u64 start, u64 wanted,
			        struct segments *out)
{
	struct buf b = {0};
	stqr_info info;
	unsigned version, count, i;
	u64 total = 0;
	int err;
	if (start > length || wanted > length - start)
		return -EUCLEAN;
	err = view ? get_view_object(f, view, id, (void **)&b.p, &b.n, &info)
		   : get_object(f, id, (void **)&b.p, &b.n, &info);
	if (err)
		return err;
	if (b.n < 10 || b.n > SQ_CHUNK || memcmp(b.p, "STIX", 4)) {
		err = -EUCLEAN;
		goto out;
	}
	take(&b, 4);
	version = readnum(&b, 2);
	count = readnum(&b, 4);
	if ((version != 1 && version != 2 && version != 3) || !count || count > 1024) {
		err = -EUCLEAN;
		goto out;
	}
	for (i = 0; i < count; i++) {
		struct segment s = {0};
		const u8 *p;
		bool lower = false;
		s.length = readnum(&b, 8);
		p = take(&b, 32);
		if (p)
			memcpy(s.id.bytes, p, 32);
		s.offset = readnum(&b, version == 1 ? 4 : 8);
		if (version == 3)
			lower = boolean(&b);
		else if (version == 2 && readnum(&b, 8)) {
			/* Decode old local indexes without reviving historical object
			 * versions. A nonzero legacy lower reference needs explicit
			 * materialization; never reinterpret it as local data. */
			err = -EOPNOTSUPP;
			goto out;
		}
		if (b.err || !s.length || total > length || s.length > length - total ||
		    s.offset > S64_MAX || s.length > S64_MAX - s.offset ||
		    (depth > 1 && (s.offset || lower)) || (nullid(s.id) && (s.offset || lower)) ||
		    (depth == 1 && !nullid(s.id) && s.offset + s.length > SQ_CHUNK)) {
			err = -EUCLEAN;
			goto out;
		}
		s.lower = lower;
		total += s.length;
		/* Validate every entry of this visited node, but do not fetch unrelated
		 * descendants. A small read must not walk the entire file's index. */
		if (start < total && start + wanted > total - s.length) {
			u64 skip = start > total - s.length ? start - (total - s.length) : 0;
			u64 take = min(total, start + wanted) - (total - s.length) - skip;
			if (depth > 1 && !nullid(s.id)) {
				err = index_segments_range(f, view, s.id, s.length, depth - 1,
						   skip, take, out);
			} else {
				if (!nullid(s.id))
					s.offset += skip;
				s.length = take;
				err = segment_add(out, s);
			}
		}
		if (err)
			goto out;
	}
	err = done(&b);
	if (!err && total != length)
		err = -EUCLEAN;
out:
	drop(&b);
	return err;
}
static int index_segments(struct ks_fs *f, stqr_object_id id, u64 length, unsigned depth,
			  struct segments *out)
{
	return index_segments_range(f, NULL, id, length, depth, 0, length, out);
}
/* A page-cache window is at most one MiB. Limit metadata callers too: many
 * tiny extents must not allocate an unbounded set of callbacks/buffers. Each
 * batch shares the existing routed connection and submits before waiting. */
#define KS_RANGE_BATCH 32
struct ks_range_read {
	struct ks_fs *store;
	struct ks_rpc *rpc;
	stqr_object_id id;
	u64 offset, epoch;
	void *destination;
	size_t length;
	struct ks_async_result result;
	bool pending, finished, sparse;
};
static int range_read_submit(struct ks_range_read *job)
{
	struct ks_rpc *rpc = ks_rpc_lock(job->store, job->id);
	int err;
	job->rpc = rpc;
	job->epoch = rpc->reconnect_epoch;
	job->pending = false;
	memset(&job->result, 0, sizeof(job->result));
	init_completion(&job->result.done);
	if (rpc->receive_started) {
		err = stqr_get_range_async(rpc->client, job->id, job->offset, job->length,
					   ks_async_completed, &job->result);
		job->pending = !err;
		if (err)
			job->result.error = err;
	} else {
		/* Bootstrap reads before the receive worker starts retain the ordinary
		 * synchronous wrapper. This is visibility I/O, never a durability fence. */
		err = stqr_get_range(rpc->client, job->id, job->offset, job->length,
				     &job->result.data, &job->result.length, &job->result.info);
		job->result.error = err;
		job->result.status = stqr_status(rpc->client);
	}
	mutex_unlock(&rpc->lock);
	return err;
}
static void range_read_drain(struct ks_range_read *jobs, unsigned count)
{
	for (unsigned i = 0; i < count; i++) {
		if (jobs[i].pending) {
			wait_for_completion(&jobs[i].result.done);
			jobs[i].pending = false;
		}
	}
}
static void range_read_free(struct ks_range_read *jobs, unsigned count)
{
	range_read_drain(jobs, count);
	for (unsigned i = 0; i < count; i++) {
		sq_free(jobs[i].result.data);
		jobs[i].result.data = NULL;
	}
}
static bool range_read_retryable(const struct ks_async_result *result)
{
	return result->error == -EAGAIN ? result->status == 0 || result->status == 503 :
		ks_transport_retryable(NULL, result->error);
}
static bool range_read_sparse_error(const struct ks_range_read *job)
{
	/* Qualify by errno as well as status: a stale 416 after socket loss must
	 * not turn a transport failure into a successful zero-filled read. */
	return job->sparse && job->result.error == -EREMOTEIO && job->result.status == 416;
}
static int range_read_finish(struct ks_range_read *jobs, unsigned count)
{
	int err = 0;
	for (;;) {
		struct ks_rpc *retried[KS_RANGE_BATCH];
		unsigned retried_count = 0, unfinished = 0;
		/* Drain the entire wave before reconnecting or releasing any callback
		 * context. A lost socket can complete several independent requests. */
		range_read_drain(jobs, count);
		for (unsigned i = 0; i < count; i++) {
			struct ks_range_read *job = &jobs[i];
			if (job->finished)
				continue;
			if (range_read_sparse_error(job)) {
				void *raw = NULL;
				size_t size = 0;
				stqr_info info;
				/* Mutable FS pages may end before their logical extent: the Go
				 * format defines the remainder as zeros. A full GET proves the
				 * object exists; a missing/corrupt payload is never a hole. */
				err = get_object(job->store, job->id, &raw, &size, &info);
				if (err) {
					sq_free(raw);
					goto out;
				}
				if (size != info.length) {
					sq_free(raw);
					err = -EUCLEAN;
					goto out;
				}
				memset(job->destination, 0, job->length);
				if (job->offset < size)
					memcpy(job->destination, (u8 *)raw + job->offset,
					       min_t(u64, job->length, size - job->offset));
				sq_free(raw);
				sq_free(job->result.data);
				job->result.data = NULL;
				job->finished = true;
				continue;
			}
			if (job->result.error && !range_read_retryable(&job->result)) {
				err = job->result.error;
				goto out;
			}
			if (!job->result.error && (job->result.length > job->length ||
			    (job->result.length != job->length && (!job->sparse ||
			     job->result.info.length != job->offset + job->result.length)))) {
				err = -EUCLEAN;
				goto out;
			}
		}
		for (unsigned i = 0; i < count; i++) {
			struct ks_range_read *job = &jobs[i];
			if (job->finished)
				continue;
			if (!job->result.error) {
				if (job->result.length < job->length)
					memset(job->destination, 0, job->length);
				if (job->result.length)
					memcpy(job->destination, job->result.data, job->result.length);
				sq_free(job->result.data);
				job->result.data = NULL;
				job->finished = true;
				continue;
			}
			unfinished++;
		}
		if (!unfinished) {
			err = 0;
			break;
		}
		for (unsigned i = 0; i < count; i++) {
			struct ks_range_read *job = &jobs[i];
			bool retry_owner = true;
			if (job->finished)
				continue;
			for (unsigned j = 0; j < retried_count; j++)
				if (retried[j] == job->rpc)
					retry_owner = false;
			if (retry_owner) {
				mutex_lock(&job->rpc->lock);
				err = ks_rpc_retry(job->store, job->rpc, job->epoch, job->result.status);
				mutex_unlock(&job->rpc->lock);
				if (err)
					goto out;
				retried[retried_count++] = job->rpc;
			}
			sq_free(job->result.data);
			job->result.data = NULL;
			/* Retry submissions still form a wave; a delayed first reply never
			 * prevents the other rejected ranges from being resubmitted. */
			err = range_read_submit(job);
			if (err && !range_read_retryable(&job->result) && !range_read_sparse_error(job))
				goto out;
		}
	}
out:
	range_read_free(jobs, count);
	return err;
}
static int index_read(struct ks_fs *f, const struct ks_node *view, stqr_object_id id, u64 length,
		      unsigned depth, void *dst, size_t count, u64 offset)
{
	struct segments s = {0};
	struct ks_range_read *jobs = NULL;
	unsigned pending = 0;
	size_t pending_bytes = 0;
	int err = index_segments_range(f, view, id, length, depth, offset, count, &s);
	offset = 0;
	if (err)
		goto out;
	jobs = sq_alloc(KS_RANGE_BATCH * sizeof(*jobs));
	if (!jobs) {
		err = -ENOMEM;
		goto out;
	}
	for (size_t i = 0; i < s.n && count; i++) {
		size_t n;
		struct segment *p = &s.p[i];
		if (offset >= p->length) {
			offset -= p->length;
			continue;
		}
		n = min_t(u64, count, p->length - offset);
		if (nullid(p->id)) {
			memset(dst, 0, n);
		} else if (!p->lower && view && view->view) {
			/* Prepared/guarded views may overlay sparse bytes in the logical
			 * object. Preserve that resolution rather than issuing raw ranges. */
			void *data = NULL;
			size_t size;
			stqr_info info;
			err = get_view_object(f, view, p->id, &data, &size, &info);
			if (!err) {
				u64 at = p->offset + offset;
				memset(dst, 0, n);
				if (at < size)
					memcpy(dst, (u8 *)data + at, min_t(u64, n, size - at));
			}
			sq_free(data);
			if (err)
				goto out;
		} else {
			struct ks_fs *store = p->lower ? f->lower : f;
			struct ks_range_read *job;
			if (!store) {
				err = -EUCLEAN;
				goto out;
			}
			if (pending && (pending == KS_RANGE_BATCH || pending_bytes + n > SQ_CHUNK)) {
				err = range_read_finish(jobs, pending);
				pending = 0;
				pending_bytes = 0;
				if (err)
					goto out;
				memset(jobs, 0, KS_RANGE_BATCH * sizeof(*jobs));
			}
			job = &jobs[pending++];
			job->store = store;
			job->id = p->id;
			job->offset = p->offset + offset;
			job->destination = dst;
			job->length = n;
			job->sparse = view && view->mutable;
			pending_bytes += n;
			atomic64_inc(&f->rpc_ranges);
			err = range_read_submit(job);
			if (err && !range_read_retryable(&job->result) && !range_read_sparse_error(job))
				goto out;
			err = 0;
		}
		dst = (u8 *)dst + n;
		count -= n;
		offset = 0;
	}
	if (count)
		err = -EUCLEAN;
	if (!err && pending) {
		err = range_read_finish(jobs, pending);
		pending = 0;
	}
out:
	/* Includes semantic/submission/guard errors: every submitted completion is
	 * observed before its context or destination can disappear. */
	if (pending)
		range_read_free(jobs, pending);
	sq_free(jobs);
	sq_free(s.p);
	return err;
}

static int read_node(struct ks_fs *f, const struct ks_node *n, void *dst, size_t count, u64 offset)
{
	if (n->lower) {
		struct ks_node local = *n;
		if (!f->lower)
			return -EUCLEAN;
		local.lower = false;
		memset(&local.lower_owner, 0, sizeof(local.lower_owner));
		memset(&local.identity, 0, sizeof(local.identity));
		return ks_read(f->lower, &local, dst, count, offset);
	}

	if (offset > n->size || count > n->size - offset)
		return -EAGAIN;
	if (!count)
		return 0;
	if (!n->depth) {
		memcpy(dst, n->inline_data + offset, count);
		return 0;
	}
	return index_read(f, n, n->root, n->size, n->depth, dst, count, offset);
}
int ks_read(struct ks_fs *f, const struct ks_node *node, void *dst, size_t count, u64 offset)
{
	/* Inherited inode identities and their read guards belong to the lower
	 * bucket. Switch stores before both the payload read and guard checks. */
	if (node->inherited) {
		struct ks_node local = *node;
		if (!f->lower)
			return -EUCLEAN;
		local.inherited = false;
		return ks_read(f->lower, &local, dst, count, offset);
	}
	const struct ks_node *n = node;
	struct ks_snapshot fresh = {0};
	unsigned attempt;
	int err = 0;
	for (attempt = 0; attempt < 128; attempt++) {
		stqr_info observed;
		int readerr = check_lower_anchor(f, n);
		if (!readerr)
			readerr = read_node(f, n, dst, count, offset);
		if (!readerr)
			readerr = check_lower_anchor(f, n);
		if (!n->depth || nullid(n->identity)) {
			err = readerr;
			break;
		}
		{
			void *raw = NULL;
			size_t len = 0;
			u8 digest[32];
			err = get_object(f, nullid(n->guard) ? n->identity : n->guard, &raw, &len,
					 &observed);
			if (!err)
				err = sq_hash(256, raw, len, digest);
			sq_free(raw);
			if (!err &&
			    !memcmp(digest, nullid(n->guard) ? n->head_digest : n->guard_digest,
				    32)) {
				err = readerr;
				break;
			}
		}
		if (err && err != -ENOENT)
			break;
		ks_cache_forget(f, node->identity);
		ks_snapshot_free(&fresh);
		err = ks_snapshot(f, node->identity, &fresh);
		if (err)
			break;
		if (fresh.node.size != node->size) {
			err = -EAGAIN;
			break;
		}
		n = &fresh.node;
	}
	if (attempt == 128)
		err = -EAGAIN;
	ks_snapshot_free(&fresh);
	return err;
}
static int build_segments(struct ks_fs *f, struct ks_node *node, struct segments *s)
{
	node->lower = false;
	bool inherited = false;
	for (size_t j = 0; j < s->n; j++)
		inherited |= s->p[j].lower;
	if (!inherited)
		memset(&node->lower_owner, 0, sizeof(node->lower_owner));
	unsigned depth = 0;
	int err = 0;
	u64 length = 0;
	size_t i;
	for (i = 0; i < s->n; i++) {
		if (s->p[i].length > S64_MAX - length)
			return -EFBIG;
		length += s->p[i].length;
	}
	if (!length) {
		node->lower = false;
		node->size = 0;
		node->depth = 0;
		node->inline_data = NULL;
		memset(&node->root, 0, 32);
		return 0;
	}
	do {
		struct segments next = {0};
		for (i = 0; i < s->n; i += 1024) {
			struct buf b = {0};
			struct segment part = {0};
			size_t j, count = min_t(size_t, 1024, s->n - i);
			bool modern = false;
			for (size_t k = i; k < i + count; k++)
				modern |= s->p[k].lower;
			put(&b, "STIX", 4);
			number(&b, modern ? 3 : 1, 2);
			number(&b, count, 4);
			for (j = i; j < i + count; j++) {
				number(&b, s->p[j].length, 8);
				put(&b, s->p[j].id.bytes, 32);
				number(&b, s->p[j].offset, modern ? 8 : 4);
				if (modern)
					number(&b, s->p[j].lower, 1);
				part.length += s->p[j].length;
			}
			err = b.err ? b.err
				    : stable_id(node->identity, 1, depth + 1, i / 1024, &part.id);
			if (!err)
				err = stage_block(f, node, part.id, 0, b.p, b.n, true, false);
			drop(&b);
			if (!err)
				err = segment_add(&next, part);
			if (err) {
				sq_free(next.p);
				return err;
			}
		}
		sq_free(s->p);
		*s = next;
		depth++;
		if (depth > 16)
			return -EFBIG;
	} while (s->n > 1);
	node->size = length;
	node->depth = depth;
	node->root = s->p[0].id;
	node->inline_data = NULL;
	return 0;
}
static int blob_bytes(struct ks_fs *f, struct ks_node *node, const void *data, size_t n)
{
	node->lower = false;
	struct segments s = {0};
	size_t pos, unit = node->kind == 2 ? 4096 : SQ_CHUNK;
	int err = 0;
	if (n <= 4096) {
		node->mutable = true;
		node->size = n;
		node->depth = 0;
		node->inline_data = data;
		memset(&node->root, 0, 32);
		return 0;
	}
	for (pos = 0; pos < n; pos += unit) {
		struct segment part = {.length = min_t(size_t, n - pos, unit)};
		err = stable_id(node->identity, 0, 0, pos / unit, &part.id);
		if (!err)
			err = stage_block(f, node, part.id, 0, (const u8 *)data + pos, part.length,
					  true, false);
		if (!err)
			err = segment_add(&s, part);
		if (err)
			break;
	}
	if (!err)
		err = build_segments(f, node, &s);
	sq_free(s.p);
	return err;
}
static int slice_segments(struct segments *out, struct segments *in, u64 start, u64 end)
{
	size_t i;
	u64 pos = 0;
	for (i = 0; i < in->n && pos < end; i++) {
		struct segment part = in->p[i];
		u64 next = pos + part.length;
		int err;
		if (next > start && pos < end) {
			u64 a = max(start, pos) - pos, b = min(end, next) - pos;
			part.length = b - a;
			if (!nullid(part.id))
				part.offset += a;
			err = segment_add(out, part);
			if (err)
				return err;
		}
		pos = next;
	}
	return 0;
}
static int member_set(struct member *, struct ks_snapshot *, const struct ks_node *);
static void member_free(struct member *);
static int transact(struct ks_fs *, struct member *, unsigned);
static int node_cas(struct ks_fs *f, struct ks_snapshot *old, struct ks_node *next)
{
	if (f->lower && old->node.kind == 2)
		return ks_overlay_node_cas(f, old, next);

	struct buf b = {0};
	int err;
	if (next->edits || f->references) {
		struct member m = {0};
		err = member_set(&m, old, next);
		if (!err)
			err = transact(f, &m, 1);
		member_free(&m);
		edits_free(next->edits);
		next->edits = NULL;
		return err;
	}
	encode_node(&b, next);
	err = b.err ? b.err : cas_object(f, old->id, b.p, b.n, old->raw, old->raw_len, false);
	drop(&b);
	return err;
}
#include "mutable.c"
#include "orphans.c"
#include "anchors.c"
static bool snapshot_changed(struct ks_fs *f, const struct ks_snapshot *old)
{
	struct buf raw = {0};
	stqr_info info;
	bool changed = false;
	if (!nullid(old->id) && !get_object(f, old->id, (void **)&raw.p, &raw.n, &info))
		changed = raw.n != old->raw_len || memcmp(raw.p, old->raw, raw.n);
	drop(&raw);
	return changed;
}

int ks_write(struct ks_fs *f, stqr_object_id id, u64 *position, const void *data, size_t count,
	     bool append)
{
	unsigned attempt;
	int err;
	if (f->readonly)
		return -EROFS;
	if (f->lower) {
		err = ks_overlay_copyup(f, id);
		if (err)
			return err;
	}

	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot old;
		struct ks_node next;
		struct segments before = {0}, after = {0};
		u64 off, end, phase = ktime_get_ns();
		u8 *small = NULL;
		err = ks_plain_snapshot(f, id, &old);
		atomic64_add(ktime_get_ns() - phase, &f->write_snapshot_ns);
		phase = ktime_get_ns();
		if (err)
			return err;
		next = old.node;
		off = append ? next.size : *position;
		if (next.kind != 1 || off > S64_MAX || count > S64_MAX - off) {
			ks_snapshot_free(&old);
			return -EFBIG;
		}
		end = off + count;
		if (max(end, next.size) <= 4096) {
			small = sq_alloc(4096);
			if (!small) {
				ks_snapshot_free(&old);
				return -ENOMEM;
			}
			err = ks_read(f, &old.node, small, old.node.size, 0);
			if (!err) {
				memcpy(small + off, data, count);
				err = blob_bytes(f, &next, small, max(end, next.size));
			}
		} else {
			err = prepare_mutable_write(f, &old, &next, off, data, count);
		}

		next.mtime = next.ctime = ktime_get_real_ns();
		atomic64_add(ktime_get_ns() - phase, &f->write_prepare_ns);
		phase = ktime_get_ns();
		if (!err)
			err = node_cas(f, &old, &next);
		atomic64_add(ktime_get_ns() - phase, &f->write_publish_ns);
		if ((err == -EAGAIN || err == -ENOENT) && snapshot_changed(f, &old))
			err = -EAGAIN;
		ks_snapshot_free(&old);
		edits_free(next.edits);
		next.edits = NULL;
		sq_free(small);
		sq_free(before.p);
		sq_free(after.p);
		if (err != -EAGAIN) {
			if (!err)
				*position = end;
			return err;
		}
		atomic64_inc(&f->write_retries);
		cond_resched();
	}
	return -EAGAIN;
}
static int ks_plain_setattr(struct ks_fs *f, stqr_object_id id, struct iattr *attr)
{
	unsigned attempt;
	int err;
	if (f->readonly)
		return -EROFS;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot old;
		struct ks_node next;
		struct segments in = {0}, out = {0};
		u8 *small = NULL;
		err = ks_snapshot(f, id, &old);
		if (err)
			return err;
		next = old.node;
		if (attr->ia_valid & ATTR_SIZE) {
			u64 size = attr->ia_size;
			if (next.kind != 1 || attr->ia_size < 0) {
				ks_snapshot_free(&old);
				return -EINVAL;
			}
			if (size <= 4096) {
				small = sq_alloc(4096);
				if (!small) {
					ks_snapshot_free(&old);
					return -ENOMEM;
				}
				err = ks_read(f, &old.node, small, min(size, next.size), 0);
				if (!err)
					err = blob_bytes(f, &next, small, size);
				if (!err) {
					struct segments none = {0};
					err = retire_pages(f, &old.node, &next, &none);
				}
			} else {
				err = prepare_mutable_resize(f, &old, &next, size);
			}

			next.mtime = ktime_get_real_ns();
		}
		if (attr->ia_valid & ATTR_MODE)
			next.mode = attr->ia_mode & 07777;
		if (attr->ia_valid & ATTR_UID)
			next.uid = from_kuid(&init_user_ns, attr->ia_uid);
		if (attr->ia_valid & ATTR_GID)
			next.gid = from_kgid(&init_user_ns, attr->ia_gid);
		if (attr->ia_valid & ATTR_ATIME) {
			if (!timespec64_valid_strict(&attr->ia_atime))
				err = -EINVAL;
			else
				next.atime = timespec64_to_ns(&attr->ia_atime);
		}
		if (attr->ia_valid & ATTR_MTIME) {
			if (!timespec64_valid_strict(&attr->ia_mtime))
				err = -EINVAL;
			else
				next.mtime = timespec64_to_ns(&attr->ia_mtime);
		}
		next.ctime = ktime_get_real_ns();
		if (!err)
			err = node_cas(f, &old, &next);
		if ((err == -EAGAIN || err == -ENOENT) && snapshot_changed(f, &old))
			err = -EAGAIN;
		ks_snapshot_free(&old);
		edits_free(next.edits);
		next.edits = NULL;
		sq_free(small);
		sq_free(in.p);
		sq_free(out.p);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
static bool valid_name(const char *s)
{
	size_t n = strlen(s), i = 0;
	if (!n || n > 255 || !strcmp(s, ".") || !strcmp(s, "..") || strchr(s, '/'))
		return false;
	while (i < n) {
		unsigned c = (u8)s[i++], need, value, minimum;
		if (c < 128)
			continue;
		if (c >= 0xc2 && c <= 0xdf) {
			need = 1;
			value = c & 31;
			minimum = 128;
		} else if (c >= 0xe0 && c <= 0xef) {
			need = 2;
			value = c & 15;
			minimum = 2048;
		} else if (c >= 0xf0 && c <= 0xf4) {
			need = 3;
			value = c & 7;
			minimum = 65536;
		} else
			return false;
		if (need > n - i)
			return false;
		while (need--) {
			c = (u8)s[i++];
			if ((c & 192) != 128)
				return false;
			value = (value << 6) | (c & 63);
		}
		if (value < minimum || value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff))
			return false;
	}
	return true;
}
int ks_directory(struct ks_fs *f, const struct ks_node *node, struct ks_entry **out, size_t *count)
{
	struct buf b = {0};
	struct ks_entry *entries = NULL;
	u64 total;
	unsigned i;
	int err;
	*out = NULL;
	*count = 0;
	if (node->kind != 2)
		return -ENOTDIR;
	if (node->removed)
		return -ENOENT;
	if (node->size > KS_DIR_LIMIT || node->size < 14)
		return -E2BIG;
	b.p = sq_alloc(node->size);
	b.n = node->size;
	if (!b.p)
		return -ENOMEM;
	err = ks_read(f, node, b.p, b.n, 0);
	if (err)
		goto out;
	if (memcmp(b.p, "STDR\1\0", 6)) {
		err = -EUCLEAN;
		goto out;
	}
	take(&b, 6);
	total = readnum(&b, 8);
	if (total > (b.n - 14) / 34 || total > 1048576) {
		err = -EUCLEAN;
		goto out;
	}
	entries = sq_alloc((total + 1) * sizeof(*entries));
	if (!entries) {
		err = -ENOMEM;
		goto out;
	}
	for (i = 0; i < total; i++) {
		const u8 *p;
		size_t n;
		p = readblob(&b, &n);
		if (b.err || !n || n > 255 || memchr(p, 0, n)) {
			err = -EUCLEAN;
			goto out;
		}
		memcpy(entries[i].name, p, n);
		entries[i].name[n] = 0;
		p = take(&b, 32);
		if (p)
			memcpy(entries[i].id.bytes, p, 32);
		if (b.err || nullid(entries[i].id) || !valid_name(entries[i].name) ||
		    (i && strcmp(entries[i - 1].name, entries[i].name) >= 0)) {
			err = -EUCLEAN;
			goto out;
		}
	}
	err = done(&b);
	if (!err) {
		*out = entries;
		*count = total;
		entries = NULL;
	}
out:
	sq_free(entries);
	drop(&b);
	return err;
}
#include "directory_lookup.h"
static int entry_cmp(const void *a, const void *b)
{
	return strcmp(((const struct ks_entry *)a)->name, ((const struct ks_entry *)b)->name);
}
static int directory_image(struct ks_fs *f, struct ks_node *node, struct ks_entry *e, size_t n,
			   struct buf *content)
{
	size_t i;
	node->links = 2;
	for (i = 0; i < n; i++) {
		struct ks_snapshot child;
		int err = ks_snapshot(f, e[i].id, &child);
		if (err)
			return err;
		if (child.node.kind == 2)
			node->links++;
		ks_snapshot_free(&child);
	}
	sort(e, n, sizeof(*e), entry_cmp, NULL);
	put(content, "STDR\1\0", 6);
	number(content, n, 8);
	for (i = 0; i < n; i++) {
		blob(content, e[i].name, strlen(e[i].name));
		put(content, e[i].id.bytes, 32);
	}
	if (content->err)
		return content->err;
	node->mtime = node->ctime = ktime_get_real_ns();
	{
		struct ks_node before = *node;
		struct segments keep = {0};
		int e = blob_bytes(f, node, content->p, content->n);
		size_t at;
		if (!e && node->depth) {
			for (at = 0; at < content->n; at += 4096) {
				struct segment ref = {.length =
							  min_t(size_t, 4096, content->n - at)};
				e = stable_id(node->identity, 0, 0, at / 4096, &ref.id);
				if (!e)
					e = segment_add(&keep, ref);
				if (e)
					break;
			}
		}
		if (!e)
			e = retire_pages(f, &before, node, &keep);
		sq_free(keep.p);
		return e;
	}
}
int ks_initialize(struct ks_fs *f, u32 uid, u32 gid)
{
	struct ks_node node = {.kind = 2, .mode = 0755, .uid = uid, .gid = gid, .links = 2};
	struct buf content = {0}, raw = {0};
	int err;
	node.atime = node.mtime = node.ctime = ktime_get_real_ns();
	put(&content, "STDR\1\0", 6);
	number(&content, 0, 8);
	node.size = content.n;
	node.inline_data = content.p;
	encode_node(&raw, &node);
	err = raw.err ? raw.err : cas_object(f, ks_root_id, raw.p, raw.n, NULL, 0, true);
	drop(&content);
	drop(&raw);
	return err;
}
int ks_create(struct ks_fs *f, stqr_object_id parent, const char *name, unsigned mode, u32 uid,
	      u32 gid, bool dir, stqr_object_id *created)
{
	return ks_newnode(f, parent, name, mode, uid, gid, dir ? 2 : 1, NULL, created);
}
int ks_newnode(struct ks_fs *f, stqr_object_id parent, const char *name, unsigned mode, u32 uid,
	       u32 gid, unsigned kind, const char *target, stqr_object_id *created)
{
	if (f->lower)
		return ks_overlay_newnode(f, parent, name, mode, uid, gid, kind, target, created);

	unsigned attempt;
	int err;
	bool made = false;
	if (f->readonly)
		return -EROFS;
	if (kind < 1 || kind > 5 || (kind == 3 && (!target || !*target || strlen(target) > 4095)))
		return -EINVAL;
	if (!valid_name(name))
		return -EINVAL;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot old;
		struct ks_entry *entries = NULL;
		size_t n;
		struct buf content = {0};
		struct ks_node next;
		err = ks_snapshot(f, parent, &old);
		if (err)
			return err;
		err = ks_directory(f, &old.node, &entries, &n);
		next = old.node;
		if (!err && find_sorted_entry(entries, n, name) >= 0)
			err = -EEXIST;
		if (!err && !made) {
			struct ks_node child = {.kind = kind,
						.mode = mode & 07777,
						.uid = uid,
						.gid = gid,
						.links = kind == 2 ? 2 : 1};
			struct buf raw = {0}, child_content = {0};
			child.mtime = child.atime = child.ctime = ktime_get_real_ns();
			if (kind == 2) {
				put(&child_content, "STDR\1\0", 6);
				number(&child_content, 0, 8);
				child.inline_data = child_content.p;
				child.size = child_content.n;
			}
			if (kind == 3) {
				child.inline_data = (const u8 *)target;
				child.size = strlen(target);
			}
			encode_node(&raw, &child);
			err = raw.err ? raw.err : immutable(f, raw.p, raw.n, created);
			drop(&raw);
			drop(&child_content);
			made = !err;
		}
		if (!err) {
			entries[n].id = *created;
			strscpy(entries[n++].name, name, 256);
			err = directory_image(f, &next, entries, n, &content);
			if (!err)
				err = node_cas(f, &old, &next);
		}
		edits_free(next.edits);
		next.edits = NULL;
		if ((err == -EAGAIN || err == -ENOENT) && snapshot_changed(f, &old))
			err = -EAGAIN;
		ks_snapshot_free(&old);
		sq_free(entries);
		drop(&content);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
static void member_free(struct member *m)
{
	drop(&m->before);
	drop(&m->after);
	drop(&m->writes);
}
static int member_set(struct member *m, struct ks_snapshot *s, const struct ks_node *after)
{
	m->id = s->id;
	put(&m->before, s->raw, s->raw_len);
	encode_node(&m->after, after);
	encode_edits(&m->writes, after);
	edits_free(after->edits);
	((struct ks_node *)after)->edits = NULL;
	return m->before.err ? m->before.err : m->after.err;
}
static int member_cmp(const void *a, const void *b)
{
	return memcmp(((const struct member *)a)->id.bytes, ((const struct member *)b)->id.bytes,
		      32);
}
static int transact(struct ks_fs *f, struct member *m, unsigned count)
{
	struct buf b = {0};
	unsigned i;
	stqr_object_id id;
	stqr_id origin = {{0}};
	int err;
	if (!count || count > 1024)
		return -E2BIG;
	sort(m, count, sizeof(*m), member_cmp, NULL);
	put(&b, "STFT\3\0", 6);
	number(&b, 0, 1);
	number(&b, count, 4);
	for (i = 0; i < count; i++) {
		if (i && eqid(m[i - 1].id, m[i].id)) {
			drop(&b);
			return -EINVAL;
		}
		put(&b, m[i].id.bytes, 32);
		blob(&b, m[i].before.p, m[i].before.n);
		blob(&b, m[i].after.p, m[i].after.n);
		if (m[i].writes.n)
			put(&b, m[i].writes.p, m[i].writes.n);
		else
			number(&b, 0, 4);
	}
	if (f->references && !b.err) {
		err = sq_random(id.bytes, 32);
		if (!err)
			err = stqr_request_id(&origin);
		if (!err)
			err = ks_object_reference(f, id, origin, 0);
		if (!err)
			err = intent_store_at(f, &b, id);
	} else
		err = b.err ? b.err : intent_store(f, &b, &id);
	drop(&b);
	if (!err)
		err = f->references ? help_tx_pinned(f, id, &origin) : help_tx(f, id);
	if (!zero(origin.bytes, 16)) {
		int released = ks_retire_pin(f, id, origin);
		if (!err)
			err = released;
	}
	return err;
}
int ks_remove(struct ks_fs *f, stqr_object_id parent, const char *name, bool dir)
{
	if (f->lower)
		return ks_overlay_remove(f, parent, name, dir);

	unsigned attempt;
	int err;
	if (f->readonly)
		return -EROFS;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot old, child = {0};
		struct ks_entry *entries = NULL, *children = NULL;
		size_t n, nc;
		long at;
		struct buf content = {0};
		struct ks_node next;
		struct member *members = NULL;
		unsigned member_count = 2;
		err = ks_snapshot(f, parent, &old);
		if (err)
			return err;
		next = old.node;
		err = ks_directory(f, &old.node, &entries, &n);
		at = err ? -1 : find_sorted_entry(entries, n, name);
		if (!err && at < 0)
			err = -ENOENT;
		if (!err)
			err = ks_snapshot(f, entries[at].id, &child);
		if (!err && (child.node.kind == 2) != dir)
			err = dir ? -ENOTDIR : -EISDIR;
		if (!err && dir) {
			err = ks_directory(f, &child.node, &children, &nc);
			if (!err && nc)
				err = -ENOTEMPTY;
		}
		if (!err) {
			memmove(entries + at, entries + at + 1, (n - at - 1) * sizeof(*entries));
			n--;
			err = directory_image(f, &next, entries, n, &content);
		}
		if (!err) {
			members = sq_alloc(3 * sizeof(*members));
			if (!members)
				err = -ENOMEM;
			if (!err)
				err = member_set(&members[0], &old, &next);
			next = child.node;
			if (dir)
				next.removed = true;
			else if (!next.links)
				err = -EAGAIN;
			else {
				next.links--;
				next.ctime = ktime_get_real_ns();
			}
			if (!err)
				err = member_set(&members[1], &child, &next);
			if (!err && (next.removed || !next.links)) {
				bool queued;
				err = orphan_member(f, child.id, true, &members[2], &queued);
				if (!err && queued)
					member_count++;
			}
			if (!err)
				err = transact(f, members, member_count);
		}

		if (members) {
			member_free(&members[0]);
			member_free(&members[1]);
			member_free(&members[2]);
			sq_free(members);
		}
		edits_free(next.edits);
		next.edits = NULL;
		if ((err == -EAGAIN || err == -ENOENT) && snapshot_changed(f, &old))
			err = -EAGAIN;
		ks_snapshot_free(&old);
		ks_snapshot_free(&child);
		sq_free(entries);
		sq_free(children);
		drop(&content);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
/* VFS supplies a referenced ancestry path. Verify every edge against the
 * exact snapshots enrolled in the transaction, protecting against remote moves.
 */
static int guard_path(struct ks_fs *f, stqr_object_id target, stqr_object_id moving,
		      struct member *members, unsigned *count, const stqr_object_id *path,
		      size_t length)
{
	size_t at;
	if (!length || !eqid(path[0], target) || !eqid(path[length - 1], ks_root_id))
		return -ESTALE;
	for (at = 0; at < length; at++) {
		struct ks_snapshot snapshot = {0};
		struct ks_node node;
		unsigned i;
		int err = 0;
		if (eqid(path[at], moving))
			return -EINVAL;
		for (i = 0; i < *count; i++)
			if (eqid(members[i].id, path[at]))
				break;
		if (i == *count) {
			if (*count == 1024)
				return -E2BIG;
			err = ks_snapshot(f, path[at], &snapshot);
			if (!err) {
				node = snapshot.node;
				if (node.kind != 2 || node.removed)
					err = -EAGAIN;
				else
					err = member_set(&members[(*count)++], &snapshot, &node);
			}
		} else
			err = decode_node(members[i].before.p, members[i].before.n, &node);
		if (!err && at) {
			struct ks_entry *entries = NULL;
			size_t n, j;
			err = ks_directory(f, &node, &entries, &n);
			if (!err) {
				for (j = 0; j < n; j++)
					if (eqid(entries[j].id, path[at - 1]))
						break;
				if (j == n)
					err = -EAGAIN;
			}
			sq_free(entries);
		}
		ks_snapshot_free(&snapshot);
		if (err)
			return err;
	}
	return 0;
}
int ks_rename(struct ks_fs *f, stqr_object_id from, const char *name, stqr_object_id to,
	      const char *newname, unsigned flags, const stqr_object_id *from_path,
	      size_t from_length, const stqr_object_id *to_path, size_t to_length)
{
	if (f->lower)
		return ks_overlay_rename(f, from, name, to, newname, flags);

	unsigned attempt;
	int err;
	if (f->readonly)
		return -EROFS;
	if (flags & ~(RENAME_NOREPLACE | RENAME_EXCHANGE) ||
	    flags == (RENAME_NOREPLACE | RENAME_EXCHANGE))
		return -EINVAL;
	if (eqid(from, to) && !strcmp(name, newname))
		return 0;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot *snapshots = sq_alloc(4 * sizeof(*snapshots));
		struct ks_snapshot *a, *b, *source, *dest;
		struct ks_entry *ae = NULL, *be = NULL, *children = NULL;
		struct buf ac = {0}, bc = {0};
		struct ks_node an = {0}, bn = {0};
		struct member *m = sq_alloc(1024 * sizeof(*m));
		unsigned count = 0, i;
		size_t na = 0, nb = 0, nc;
		long ai = -1, bi = -1;
		bool same = eqid(from, to), noop = false;
		if (!m || !snapshots) {
			sq_free(m);
			sq_free(snapshots);
			return -ENOMEM;
		}
		a = snapshots;
		b = snapshots + 1;
		source = snapshots + 2;
		dest = snapshots + 3;
		err = ks_snapshot(f, from, a);
		if (!err)
			err = ks_directory(f, &a->node, &ae, &na);
		if (!err && !same) {
			err = ks_snapshot(f, to, b);
			if (!err)
				err = ks_directory(f, &b->node, &be, &nb);
		}
		if (same) {
			be = ae;
			nb = na;
		}
		if (!err) {
			ai = find_sorted_entry(ae, na, name);
			bi = find_sorted_entry(be, nb, newname);
			if (ai < 0)
				err = -ENOENT;
			else if ((flags & RENAME_EXCHANGE) && bi < 0)
				err = -ENOENT;
			else if ((flags & RENAME_NOREPLACE) && bi >= 0)
				err = -EEXIST;
		}
		if (!err)
			err = ks_snapshot(f, ae[ai].id, source);
		if (!err && bi >= 0)
			err = ks_snapshot(f, be[bi].id, dest);
		if (!err && bi >= 0 && eqid(source->id, dest->id))
			noop = true;
		if (!err && !noop && !(flags & RENAME_EXCHANGE) && bi >= 0) {
			if ((source->node.kind == 2) != (dest->node.kind == 2))
				err = source->node.kind == 2 ? -ENOTDIR : -EISDIR;
			else if (dest->node.kind == 2) {
				err = ks_directory(f, &dest->node, &children, &nc);
				if (!err && nc)
					err = -ENOTEMPTY;
			}
		}
		if (!err && !noop) {
			an = a->node;
			bn = same ? a->node : b->node;
			if (flags & RENAME_EXCHANGE) {
				stqr_object_id tmp = ae[ai].id;
				ae[ai].id = be[bi].id;
				be[bi].id = tmp;
			} else if (same) {
				stqr_object_id sid = ae[ai].id;
				if (bi >= 0) {
					memmove(ae + bi, ae + bi + 1, (na - bi - 1) * sizeof(*ae));
					na--;
					if (bi < ai)
						ai--;
				}
				ae[ai].id = sid;
				strscpy(ae[ai].name, newname, 256);
			} else {
				stqr_object_id sid = ae[ai].id;
				memmove(ae + ai, ae + ai + 1, (na - ai - 1) * sizeof(*ae));
				na--;
				if (bi < 0)
					bi = nb++;
				be[bi].id = sid;
				strscpy(be[bi].name, newname, 256);
			}
			err = directory_image(f, &an, ae, na, &ac);
			if (!err)
				err = member_set(&m[count++], a, &an);
			if (!err && !same) {
				err = directory_image(f, &bn, be, nb, &bc);
				if (!err)
					err = member_set(&m[count++], b, &bn);
			}
			if (!err && !(flags & RENAME_EXCHANGE) && dest->raw) {
				struct ks_node removed = dest->node;
				if (removed.kind == 2)
					removed.removed = true;
				else if (!removed.links)
					err = -EAGAIN;
				else
					removed.links--;
				removed.ctime = ktime_get_real_ns();
				if (!err)
					err = member_set(&m[count++], dest, &removed);
				if (!err && (removed.removed || !removed.links)) {
					bool queued;
					err = orphan_member(f, dest->id, true, &m[count], &queued);
					if (!err && queued)
						count++;
				}
			}
			if (!err && !same && source->node.kind == 2)
				err = guard_path(f, to, source->id, m, &count, to_path, to_length);
			if (!err && !same && (flags & RENAME_EXCHANGE) && dest->node.kind == 2)
				err = guard_path(f, from, dest->id, m, &count, from_path,
						 from_length);
			if (!err)
				err = transact(f, m, count);
		}
		edits_free(an.edits);
		edits_free(bn.edits);
		for (i = 0; i < count; i++)
			member_free(&m[i]);
		sq_free(m);
		ks_snapshot_free(a);
		ks_snapshot_free(b);
		ks_snapshot_free(source);
		ks_snapshot_free(dest);
		sq_free(snapshots);
		sq_free(ae);
		if (!same)
			sq_free(be);
		sq_free(children);
		drop(&ac);
		drop(&bc);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}

int ks_link(struct ks_fs *f, stqr_object_id parent, const char *name, stqr_object_id target)
{
	if (f->lower)
		return -EOPNOTSUPP; /* same restriction as the Go overlay API */

	unsigned attempt;
	if (f->readonly)
		return -EROFS;
	if (!valid_name(name))
		return -EINVAL;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot p = {0}, child = {0};
		struct ks_entry *entries = NULL;
		struct member *m = sq_alloc(2 * sizeof(*m));
		struct buf content = {0};
		struct ks_node next = {0}, linked;
		size_t n;
		int err;
		if (!m)
			return -ENOMEM;
		err = ks_snapshot(f, parent, &p);
		if (!err)
			err = ks_directory(f, &p.node, &entries, &n);
		if (!err && find_sorted_entry(entries, n, name) >= 0)
			err = -EEXIST;
		if (!err)
			err = ks_snapshot(f, target, &child);
		if (!err && child.node.kind == 2)
			err = -EPERM;
		if (!err && !child.node.links)
			err = -ENOENT;
		if (!err && child.node.links == U32_MAX)
			err = -EMLINK;
		if (!err) {
			next = p.node;
			linked = child.node;
			linked.links++;
			linked.ctime = ktime_get_real_ns();
			entries[n].id = target;
			strscpy(entries[n++].name, name, 256);
			err = directory_image(f, &next, entries, n, &content);
			if (!err)
				err = member_set(m, &p, &next);
			if (!err)
				err = member_set(m + 1, &child, &linked);
			if (!err)
				err = transact(f, m, 2);
		}
		edits_free(next.edits);
		next.edits = NULL;
		member_free(m);
		member_free(m + 1);
		sq_free(m);
		sq_free(entries);
		drop(&content);
		if ((err == -EAGAIN || err == -ENOENT) && snapshot_changed(f, &p))
			err = -EAGAIN;
		ks_snapshot_free(&p);
		ks_snapshot_free(&child);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
static int xattr_name(const char *name)
{
	size_t length = strlen(name);
	if (!valid_name(name) || !strchr(name, '.') || name[0] == '.' || name[length - 1] == '.')
		return -EINVAL;
	if (strncmp(name, "user.", 5) && strncmp(name, "trusted.", 8) &&
	    strncmp(name, "security.", 9))
		return -EOPNOTSUPP;
	return 0;
}
int ks_getxattr(struct ks_fs *f, stqr_object_id id, const char *name, void *buffer, size_t size)
{
	struct ks_snapshot s;
	struct buf d = {0};
	unsigned n, at;
	int err = xattr_name(name);
	if (err)
		return err;
	err = ks_snapshot(f, id, &s);
	if (err)
		return err;
	d.p = (u8 *)s.node.xattrs;
	d.n = s.node.xattrs_size;
	if (!d.p) {
		err = -ENODATA;
		goto out;
	}
	n = readnum(&d, 4);
	err = -ENODATA;
	for (at = 0; at < n; at++) {
		const u8 *key, *value;
		size_t kn, vn;
		key = readblob(&d, &kn);
		value = readblob(&d, &vn);
		if (kn == strlen(name) && !memcmp(key, name, kn)) {
			if (buffer && size < vn)
				err = -ERANGE;
			else {
				if (buffer && vn)
					memcpy(buffer, value, vn);
				err = vn;
			}
			break;
		}
	}
out:
	ks_snapshot_free(&s);
	return err;
}
int ks_listxattr(struct ks_fs *f, stqr_object_id id, char *buffer, size_t size, bool trusted)
{
	struct ks_snapshot s;
	struct buf d = {0};
	unsigned n, at;
	size_t total = 0;
	int err;
	err = ks_snapshot(f, id, &s);
	if (err)
		return err;
	d.p = (u8 *)s.node.xattrs;
	d.n = s.node.xattrs_size;
	if (!d.p) {
		ks_snapshot_free(&s);
		return 0;
	}
	n = readnum(&d, 4);
	for (at = 0; at < n; at++) {
		const u8 *key;
		size_t kn, vn;
		key = readblob(&d, &kn);
		readblob(&d, &vn);
		if (!trusted && kn >= 8 && !memcmp(key, "trusted.", 8))
			continue;
		if (buffer && total + kn + 1 > size) {
			err = -ERANGE;
			goto out;
		}
		if (buffer) {
			memcpy(buffer + total, key, kn);
			buffer[total + kn] = 0;
		}
		total += kn + 1;
	}
	err = total;
out:
	ks_snapshot_free(&s);
	return err;
}
static int ks_plain_setxattr(struct ks_fs *f, stqr_object_id id, const char *name,
			     const void *value, size_t size, int flags)
{
	unsigned attempt;
	int err = xattr_name(name);
	if (err)
		return err;
	if (f->readonly)
		return -EROFS;
	if (size > 65536)
		return -E2BIG;
	if (flags & ~(XATTR_CREATE | XATTR_REPLACE) || flags == (XATTR_CREATE | XATTR_REPLACE))
		return -EINVAL;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot s;
		struct ks_node next;
		struct buf old = {0}, attrs = {0};
		unsigned count = 0, at;
		bool exists = false, inserted = false;
		err = ks_snapshot(f, id, &s);
		if (err)
			return err;
		if (s.node.kind != 1 && s.node.kind != 2) {
			ks_snapshot_free(&s);
			return -EPERM;
		}
		old.p = (u8 *)s.node.xattrs;
		old.n = s.node.xattrs_size;
		if (old.p)
			count = readnum(&old, 4);
		for (at = 0; at < count; at++) {
			const u8 *key;
			size_t kn, vn;
			key = readblob(&old, &kn);
			readblob(&old, &vn);
			if (kn == strlen(name) && !memcmp(key, name, kn))
				exists = true;
		}
		if (exists && (flags & XATTR_CREATE))
			err = -EEXIST;
		if (!exists && (!value || (flags & XATTR_REPLACE)))
			err = -ENODATA;
		if (!err) {
			unsigned next_count = count + (exists ? (value ? 0 : -1) : 1);
			if (next_count > 128)
				err = -ENOSPC;
			number(&attrs, next_count, 4);
			old.pos = old.p ? 4 : 0;
			for (at = 0; !err && at < count; at++) {
				const u8 *key, *ovalue;
				size_t kn, vn;
				char key_name[256];
				int cmp;
				key = readblob(&old, &kn);
				ovalue = readblob(&old, &vn);
				memcpy(key_name, key, kn);
				key_name[kn] = 0;
				cmp = strcmp(name, key_name);
				if (!inserted && cmp <= 0) {
					if (value) {
						blob(&attrs, name, strlen(name));
						blob(&attrs, value, size);
					}
					inserted = true;
				}
				if (cmp) {
					blob(&attrs, key, kn);
					blob(&attrs, ovalue, vn);
				}
			}
			if (!inserted && value) {
				blob(&attrs, name, strlen(name));
				blob(&attrs, value, size);
			}
			if (attrs.err)
				err = attrs.err;
			else if (attrs.n > 262144)
				err = -ENOSPC;
			if (!err) {
				next = s.node;
				next.xattrs = attrs.p;
				next.xattrs_size = attrs.n;
				next.ctime = ktime_get_real_ns();
				err = node_cas(f, &s, &next);
			}
		}
		drop(&attrs);
		ks_snapshot_free(&s);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}

struct ks_fence_work {
 struct work_struct work;
 struct completion done;
 stqr_fence *fence;
 struct ks_fs *fs;
 unsigned target;
 int error;
};
static void ks_fence_worker(struct work_struct *work)
{
 struct ks_fence_work *job = container_of(work, struct ks_fence_work, work);
 unsigned saved = memalloc_nofs_save();
 for (;;) {
  job->error = stqr_fence_wait_one(job->fence, job->target);
  if (!ks_transport_retryable(NULL, job->error)) break;
  if (READ_ONCE(job->fs->stopping)) { job->error = -ESHUTDOWN; break; }
  msleep(100);
 }
 memalloc_nofs_restore(saved);
 complete(&job->done);
}
int ks_checkpoint(struct ks_fs *f)
{
 struct ks_fence_work *jobs = NULL;
 unsigned count = 0;
 int err;
 mutex_lock(&f->sync_lock);
 /* Modern fences travel over the original owner sockets. Completion receives
  * all previously admitted work; no cloned connection or lock-held wait. */
 if (f->rpc[0].receive_started &&
     (stqr_capabilities(f->rpc[0].client) & STQR_CAP_CONNECTION_SYNC)) {
  struct ks_async_result result = {0};
  init_completion(&result.done);
  mutex_lock(&f->rpc[0].lock);
  err = stqr_connection_sync_async(f->rpc[0].client, ks_async_completed, &result);
  mutex_unlock(&f->rpc[0].lock);
  if (!err) { wait_for_completion(&result.done); err = result.error; }
  sq_free(result.data);
  mutex_unlock(&f->sync_lock);
  return err;
 }
 err = stqr_fence_reset(&f->fence);
 // Capture local framing state only; merge marks from all slots and the
 // subscription reader so each owner receives one barrier, not five.
 for (unsigned n = 0; n < KS_RPC_SLOTS && !err; n++) {
  mutex_lock(&f->rpc[n].lock);
  if (f->rpc[n].client) err = stqr_fence_add(f->rpc[n].client, f->fence);
  mutex_unlock(&f->rpc[n].lock);
 }
 if (!err) err = ks_cache_fence_add(f, f->fence);
 if (err) goto done;
 count = stqr_fence_count(f->fence);
 if (!count) goto done;
 jobs = kcalloc(count, sizeof(*jobs), GFP_NOFS);
 if (!jobs) { err = -ENOMEM; goto done; }
 for (unsigned n = 0; n < count; n++) {
  jobs[n].fence = f->fence;
  jobs[n].fs = f;
  jobs[n].target = n;
  INIT_WORK(&jobs[n].work, ks_fence_worker);
  init_completion(&jobs[n].done);
  ks_queue_fence_work(&jobs[n].work);
 }
 for (unsigned n = 0; n < count; n++) {
  wait_for_completion(&jobs[n].done);
  flush_work(&jobs[n].work);
  if (!err) err = jobs[n].error;
 }
 for (unsigned n = 0; n < KS_RPC_SLOTS; n++) {
  mutex_lock(&f->rpc[n].lock);
  stqr_fence_apply(f->rpc[n].client, f->fence);
  mutex_unlock(&f->rpc[n].lock);
 }
 ks_cache_fence_apply(f, f->fence);
done:
 kfree(jobs);
 mutex_unlock(&f->sync_lock);
 return err;
}

int ks_setattr(struct ks_fs *f, stqr_object_id id, struct iattr *a)
{
	return f->lower ? ks_overlay_setattr(f, id, a) : ks_plain_setattr(f, id, a);
}

int ks_setxattr(struct ks_fs *f, stqr_object_id id, const char *n, const void *v, size_t z,
		int flags)
{
	return f->lower ? ks_overlay_setxattr(f, id, n, v, z, flags)
			: ks_plain_setxattr(f, id, n, v, z, flags);
}
