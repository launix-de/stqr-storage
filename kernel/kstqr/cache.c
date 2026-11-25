// SPDX-License-Identifier: GPL-3.0-only
#include "kstqr.h"
#include "port.h"
#include <linux/delay.h>
#include <linux/hashtable.h>
#include <linux/kthread.h>
#include <linux/slab.h>
// Match the current server per-session subscription ceiling. Payload and entry
// descriptors both consume metadata_cache_bytes; tiny inode sets must not thrash
// merely because they exceed the old unrelated 512-entry cap.
#define KS_CACHE_ENTRIES 2048
struct cached_inode {
	struct hlist_node hash;
	struct list_head lru;
	stqr_object_id id;
	stqr_id subscription;
	stqr_info info;
	u64 cursor;
	void *raw;
	size_t size;
	bool valid, ready, early;
};
struct ks_cache {
	struct ks_fs *fs;
	struct mutex lock, wire;
	DECLARE_HASHTABLE(objects, 8);
	struct list_head lru;
	stqr_client *client;
	struct task_struct *reader;
	size_t bytes;
	unsigned count;
	bool available;
};
static u32 object_hash(stqr_object_id id)
{
	return (u32)id.bytes[0] | (u32)id.bytes[9] << 8 | (u32)id.bytes[18] << 16 |
	       (u32)id.bytes[31] << 24;
}
static struct cached_inode *find(struct ks_cache *c, stqr_object_id id)
{
	struct cached_inode *e;
	hash_for_each_possible(c->objects, e, hash,
			       object_hash(id)) if (!memcmp(e->id.bytes, id.bytes, 32)) return e;
	return NULL;
}
static void discard_data(struct ks_cache *c, struct cached_inode *e)
{
	c->bytes -= e->size;
	sq_free(e->raw);
	e->raw = NULL;
	e->size = 0;
	e->valid = false;
}
/* Runs under wire serialization, and takes only the data lock. Never waits for
 * a folio or calls the native RPC pool while processing a notification. */
static void update(void *arg, const stqr_update *u)
{
	struct ks_cache *c = arg;
	struct cached_inode *e;
	mutex_lock(&c->lock);
	if (!u) {
		list_for_each_entry(e, &c->lru, lru) discard_data(c, e);
		c->available = false;
		ks_queue_invalidation(c->fs, NULL);
		goto out;
	}
	list_for_each_entry(e, &c->lru,
			    lru) if (!memcmp(e->subscription.bytes, u->subscription.bytes, 16))
	{
		void *raw = NULL;
		if (!e->ready) {
			e->early = true;
			goto out;
		}
		if (u->cursor && u->cursor <= e->cursor)
			goto out;
		if (e->valid && u->cursor == e->cursor + 1 && u->parts == 1 && !u->part &&
		    u->change == STQR_UPDATE_REPLACE && !u->offset && u->size == u->length &&
		    u->size <= (1U << 20) && u->size <= c->fs->cache_budget - (c->bytes - e->size))
			raw = kmemdup(u->data, u->size, GFP_NOFS);
		discard_data(c, e);
		if (raw) {
			e->raw = raw;
			e->size = u->size;
			e->info.length = u->length;
			e->cursor = u->cursor;
			e->valid = true;
			c->bytes += e->size;
		}
		ks_queue_invalidation(c->fs, &e->id);
		break;
	}
out:
	mutex_unlock(&c->lock);
}
/* Caller holds wire; remove the entry before waiting for the unsubscribe
 * barrier, because that wait itself may dispatch a final update. */
static void remove_entry(struct ks_cache *c, struct cached_inode *e)
{
	stqr_id sid = e->subscription;
	stqr_object_id id = e->id;
	mutex_lock(&c->lock);
	discard_data(c, e);
	hash_del(&e->hash);
	list_del(&e->lru);
	c->count--;
	c->bytes -= sizeof(*e);
	mutex_unlock(&c->lock);
	kfree(e);
	ks_queue_invalidation(c->fs, &id);
	stqr_unsubscribe(c->client, sid);
}
void ks_cache_forget(struct ks_fs *f, stqr_object_id id)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e;
	if (!c)
		return;
	mutex_lock(&c->wire);
	mutex_lock(&c->lock);
	e = find(c, id);
	mutex_unlock(&c->lock);
	if (e)
		remove_entry(c, e);
	mutex_unlock(&c->wire);
}
static int copy_entry(struct ks_cache *c, struct cached_inode *e, void **p, size_t *n,
		      stqr_info *info)
{
	if (!c->available || !e || !e->valid)
		return -ENODATA;
	*p = kmemdup(e->raw, e->size, GFP_NOFS);
	if (!*p)
		return -ENOMEM;
	*n = e->size;
	*info = e->info;
	info->proof.subscription = e->subscription;
	info->proof.cursor = e->cursor;
	list_move_tail(&e->lru, &c->lru);
	return 0;
}
/* Certify only the bytes actually observed by the caller. Taking a newer stamp
 * after an update would incorrectly certify an old directory snapshot. */
bool ks_cache_stamp(struct ks_fs *f, stqr_object_id id, const stqr_info *observed,
		    struct ks_cache_stamp *out)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e;
	bool valid = false;
	if (!c)
		return false;
	mutex_lock(&c->lock);
	e = find(c, id);
	if (c->available && e && e->valid && e->ready &&
	    (e->cursor == observed->proof.cursor &&
	     !memcmp(e->subscription.bytes, observed->proof.subscription.bytes, 16))) {
		out->subscription = e->subscription;
		out->cursor = e->cursor;
		list_move_tail(&e->lru, &c->lru);
		valid = true;
	}
	mutex_unlock(&c->lock);
	return valid;
}
bool ks_cache_stamp_current(struct ks_fs *f, stqr_object_id id, const struct ks_cache_stamp *stamp)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e;
	bool valid = false;
	if (!c)
		return false;
	mutex_lock(&c->lock);
	e = find(c, id);
	valid = c->available && e && e->valid && e->ready && e->cursor == stamp->cursor &&
		!memcmp(e->subscription.bytes, stamp->subscription.bytes, 16);
	if (valid)
		list_move_tail(&e->lru, &c->lru);
	mutex_unlock(&c->lock);
	return valid;
}
int ks_cache_peek(struct ks_fs *f, stqr_object_id id, void **p, size_t *n, stqr_info *info)
{
	struct ks_cache *c = f->cache;
	int err;
	if (!c)
		return -ENODATA;
	mutex_lock(&c->lock);
	err = copy_entry(c, find(c, id), p, n, info);
	mutex_unlock(&c->lock);
	return err;
}
bool ks_cache_proof(struct ks_fs *f, stqr_object_id id, stqr_proof *proof)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e;
	bool found = false;
	if (!c)
		return false;
	mutex_lock(&c->lock);
	e = find(c, id);
	if (c->available && e && e->valid && e->size >= 4 && !memcmp(e->raw, "STFS", 4)) {
		proof->subscription = e->subscription;
		proof->cursor = e->cursor;
		found = true;
	}
	mutex_unlock(&c->lock);
	return found;
}
int ks_cache_get(struct ks_fs *f, stqr_object_id id, void **p, size_t *n, stqr_info *info)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e;
	void *raw = NULL;
	size_t size = 0;
	u64 cursor;
	stqr_info meta;
	int err;
	if (!c)
		return -ENODATA;
	mutex_lock(&c->lock);
	err = copy_entry(c, find(c, id), p, n, info);
	mutex_unlock(&c->lock);
	if (err != -ENODATA) {
		if (!err)
			atomic64_inc(&f->cache_hits);
		return err;
	}
	atomic64_inc(&f->cache_misses);
	mutex_lock(&c->wire);
	mutex_lock(&c->lock);
	e = find(c, id);
	err = copy_entry(c, e, p, n, info);
	mutex_unlock(&c->lock);
	if (err != -ENODATA)
		goto out;
	if (!c->available) {
		err = -ENODATA;
		goto out;
	}
	if (e)
		remove_entry(c, e);
	if (c->fs->cache_budget < sizeof(*e)) {
		err = -ENODATA;
		goto out;
	}
	for (;;) {
		mutex_lock(&c->lock);
		e = (c->count >= KS_CACHE_ENTRIES || sizeof(*e) > c->fs->cache_budget - c->bytes)
			? list_first_entry(&c->lru, struct cached_inode, lru)
			: NULL;
		mutex_unlock(&c->lock);
		if (!e)
			break;
		remove_entry(c, e);
	}
	e = kzalloc(sizeof(*e), GFP_NOFS);
	if (!e) {
		err = -ENOMEM;
		goto out;
	}
	e->id = id;
	err = stqr_request_id(&e->subscription);
	if (err) {
		kfree(e);
		goto out;
	}
	mutex_lock(&c->lock);
	list_add_tail(&e->lru, &c->lru);
	hash_add(c->objects, &e->hash, object_hash(id));
	c->count++;
	c->bytes += sizeof(*e);
	mutex_unlock(&c->lock);
	err = stqr_subscribe(c->client, id, e->subscription, 0, 0, 0, NULL, &raw, &size, &meta,
			     &cursor);
	if (!err && size <= (1U << 20)) {
		mutex_lock(&c->lock);
		if (!e->early && c->available && size <= c->fs->cache_budget - c->bytes) {
			e->raw = raw;
			e->size = size;
			e->info = meta;
			e->cursor = cursor;
			e->valid = e->ready = true;
			c->bytes += size;
			raw = NULL;
			err = copy_entry(c, e, p, n, info);
		} else
			err = -ENODATA;
		mutex_unlock(&c->lock);
	} else if (!err)
		err = -ENODATA;
	sq_free(raw);
	if (err) {
		remove_entry(c, e);
		/* Capability/budget/transport limitations fall back to native reads. */
		if (err != -ENOMEM && err != -ENOENT)
			err = -ENODATA;
	}
out:
	mutex_unlock(&c->wire);
	return err;
}
static int reader(void *arg)
{
	struct ks_cache *c = arg;
	while (!kthread_should_stop()) {
		int n = 0;
		unsigned budget = 128;
		if (mutex_trylock(&c->wire)) {
			if (!c->available) {
				/* Reauthentication restores capabilities after disconnect;
				 * all old snapshots remain invalid and resubscribe on demand. */
				if (!stqr_reconnect(c->client)) {
					mutex_lock(&c->lock);
					c->available = !!(stqr_capabilities(c->client) &
							  STQR_CAP_SUBSCRIPTIONS);
					mutex_unlock(&c->lock);
					/* Pages faulted via fallback while disconnected have no
					 * coverage. Drop them again once subscriptions can resume.
					 */
					ks_queue_invalidation(c->fs, NULL);
				}
			}
			while (c->available && budget-- && !kthread_should_stop() &&
			       (n = stqr_poll(c->client, 0)) > 0)
				cond_resched();
			mutex_unlock(&c->wire);
		}
		if (n <= 0)
			msleep_interruptible(READ_ONCE(c->available) ? 1 : 250);
	}
	return 0;
}
int ks_cache_start(struct ks_fs *f, const char *user, const char *password, unsigned timeout)
{
	struct ks_cache *c;
	int err;
	if (!f->cache_budget || !(stqr_capabilities(f->rpc[0].client) & STQR_CAP_SUBSCRIPTIONS))
		return 0;
	c = kzalloc(sizeof(*c), GFP_KERNEL);
	if (!c)
		return -ENOMEM;
	c->fs = f;
	mutex_init(&c->lock);
	mutex_init(&c->wire);
	hash_init(c->objects);
	INIT_LIST_HEAD(&c->lru);
	err = stqr_dial(&c->client, f->url, user, password, f->bucket, timeout);
	if (err) {
		kfree(c);
		return err;
	}
	c->available = true;
	stqr_set_update_handler(c->client, update, c);
	f->cache = c;
	c->reader = kthread_run(reader, c, "kstqr-watch");
	if (IS_ERR(c->reader)) {
		err = PTR_ERR(c->reader);
		c->reader = NULL;
		ks_cache_stop(f);
		return err;
	}
	return 0;
}
void ks_cache_stop(struct ks_fs *f)
{
	struct ks_cache *c = f->cache;
	struct cached_inode *e, *next;
	if (!c)
		return;
	if (c->reader)
		kthread_stop(c->reader);
	WRITE_ONCE(f->stopping, true);
	cancel_work_sync(&f->invalidation_work);
	stqr_set_update_handler(c->client, NULL, NULL);
	stqr_close(c->client);
	list_for_each_entry_safe(e, next, &c->lru, lru)
	{
		sq_free(e->raw);
		kfree(e);
	}
	f->cache = NULL;
	kfree(c);
}

int ks_cache_mark(struct ks_fs *f, stqr_commit_mark *m)
{
	struct ks_cache *c = f->cache;
	int err;
	memset(m, 0, sizeof(*m));
	if (!c)
		return 0;
	mutex_lock(&c->wire);
	err = stqr_pending_mark(c->client, m);
	mutex_unlock(&c->wire);
	return err;
}
void ks_cache_note_durable(struct ks_fs *f, const stqr_commit_mark *m)
{
	struct ks_cache *c = f->cache;
	if (!c)
		return;
	mutex_lock(&c->wire);
	stqr_note_durable(c->client, m);
	mutex_unlock(&c->wire);
}

int ks_cache_sync(struct ks_fs *f)
{
	struct ks_cache *c = f->cache;
	int err = 0;
	if (!c)
		return 0;
	mutex_lock(&c->wire);
	err = stqr_sync(c->client);
	mutex_unlock(&c->wire);
	return err;
}
