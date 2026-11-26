// SPDX-License-Identifier: GPL-3.0-only
/* Same STFS/STOL orphan queue as client/filesystem_open.go. */
static int orphan_root(stqr_object_id *id)
{
	static const char domain[] = "STQR filesystem orphans v1";
	return sq_hash(256, domain, sizeof(domain) - 1, id->bytes);
}
static int orphan_snapshot(struct ks_fs *f, bool create, struct ks_snapshot *s,
			   struct buf *body)
{
	stqr_object_id root;
	int err = orphan_root(&root);
	if (err)
		return err;
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		ks_cache_forget(f, root);
		err = ks_plain_snapshot(f, root, s);
		if (err == -ENOENT && create) {
			struct ks_node node = {.kind = 1, .links = 1, .mode = 0600};
			struct buf raw = {0}, empty = {0};
			put(&empty, "STOL\1\0", 6);
			number(&empty, 0, 4);
			node.inline_data = empty.p;
			node.size = empty.n;
			encode_node(&raw, &node);
			err = raw.err ? raw.err : cas_object(f, root, raw.p, raw.n, NULL, 0, true);
			drop(&raw);
			drop(&empty);
			if (!err || err == -EAGAIN)
				continue;
		}
		if (err)
			return err;
		if (s->node.kind != 1 || s->node.size < 10 || s->node.size > KS_DIR_LIMIT)
			return -EUCLEAN;
		body->p = sq_alloc(s->node.size);
		if (!body->p)
			return -ENOMEM;
		body->n = body->cap = s->node.size;
		err = ks_read(f, &s->node, body->p, body->n, 0);
		if (err == -EAGAIN) {
			ks_snapshot_free(s);
			drop(body);
			continue;
		}
		if (err)
			return err;
		if (memcmp(take(body, 4), "STOL", 4) || readnum(body, 2) != 1)
			return -EUCLEAN;
		u64 count = readnum(body, 4);
		if (body->err || count * 32 != body->n - body->pos)
			return -EUCLEAN;
		for (size_t i = 0; i < count; i++) {
			const u8 *id = body->p + 10 + 32 * i;
			if (zero(id, 32) || !memcmp(id, root.bytes, 32) ||
			    (i && memcmp(id - 32, id, 32) >= 0))
				return -EUCLEAN;
		}
		body->pos = 0;
		return 0;
	}
	return -EAGAIN;
}
static int orphan_member(struct ks_fs *f, stqr_object_id target, bool add,
			 struct member *member, bool *changed)
{
	struct ks_snapshot *old = sq_alloc(sizeof(*old));
	struct buf body = {0}, next_body = {0};
	struct segments keep = {0};
	struct ks_node next;
	u32 count;
	size_t at;
	int err;
	*changed = false;
	if (!f->references) {
		sq_free(old);
		return 0;
	}
	if (!old)
		return -ENOMEM;
	err = orphan_snapshot(f, add, old, &body);
	if (err == -ENOENT && !add) {
		err = 0;
		goto out;
	}
	if (err)
		goto out;
	count = (body.n - 10) / 32;
	for (at = 0; at < count && memcmp(body.p + 10 + at * 32, target.bytes, 32) < 0; at++)
		;
	bool found = at < count && !memcmp(body.p + 10 + at * 32, target.bytes, 32);
	if (found == add) {
		err = add ? member_set(member, old, &old->node) : 0;
		*changed = add && !err;
		goto out;
	}
	put(&next_body, "STOL\1\0", 6);
	number(&next_body, add ? count + 1 : count - 1, 4);
	put(&next_body, body.p + 10, at * 32);
	if (add) {
		put(&next_body, target.bytes, 32);
		put(&next_body, body.p + 10 + at * 32, (count - at) * 32);
	} else
		put(&next_body, body.p + 10 + (at + 1) * 32, (count - at - 1) * 32);
	err = next_body.err ? next_body.err : next_body.n > KS_DIR_LIMIT ? -E2BIG : 0;
	next = old->node;
	if (!err)
		err = blob_bytes(f, &next, next_body.p, next_body.n);
	if (!err)
		err = sq_hash(256, next_body.p, next_body.n, next.digest);
	next.mtime = next.ctime = ktime_get_real_ns();
	if (!err && next.depth)
		for (size_t pos = 0; pos < next_body.n && !err; pos += SQ_CHUNK) {
			struct segment part = {.length = min_t(size_t, SQ_CHUNK, next_body.n - pos)};
			err = stable_id(next.identity, 0, 0, pos / SQ_CHUNK, &part.id);
			if (!err)
				err = segment_add(&keep, part);
		}
	if (!err)
		err = retire_pages(f, &old->node, &next, &keep);
	if (!err)
		err = member_set(member, old, &next);
	edits_free(next.edits);
	if (!err)
		*changed = true;
out:
	ks_snapshot_free(old);
	sq_free(old);
	sq_free(keep.p);
	drop(&body);
	drop(&next_body);
	return err;
}
static int orphan_remove(struct ks_fs *f, stqr_object_id target)
{
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct member m = {0};
		bool changed;
		int err = orphan_member(f, target, false, &m, &changed);
		if (!err && changed)
			err = transact(f, &m, 1);
		member_free(&m);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
static int delete_orphan_object(struct ks_fs *f, stqr_object_id id)
{
	struct ks_rpc *rpc = ks_rpc_lock(f, id);
	stqr_options opt = {0};
	int err = stqr_request_id(&opt.request_id);
	ks_cache_forget(f, id);
	while (!err) {
		err = ks_delete_locked(rpc, id, &opt);
		if (!ks_transport_retryable(rpc->client, err))
			break;
		err = ks_reconnect_wait(f, rpc->client);
	}
	mutex_unlock(&rpc->lock);
	ks_cache_forget(f, id);
	return err == -ENOENT ? 0 : err;
}
static int collect_orphan(struct ks_fs *f, stqr_object_id id)
{
	struct ks_snapshot *s = sq_alloc(sizeof(*s));
	struct segments data = {0}, indexes = {0};
	stqr_id token;
	u64 position = 0;
	int err;
	if (!s)
		return -ENOMEM;
	err = stqr_request_id(&token);
	if (!err)
		err = ks_object_reference(f, id, token, 2);
	if (err)
		goto out;
	ks_cache_forget(f, id);
	err = ks_plain_snapshot(f, id, s);
	if (err == -ENOENT) {
		err = f->lower ? ks_overlay_prune_orphan(f, id) : 0;
		if (!err)
			err = orphan_remove(f, id);
		goto release;
	}
	if (err)
		goto release;
	if (!s->node.removed && s->node.links) {
		err = orphan_remove(f, id);
		goto release;
	}
	/* O_PATH and init_special_inode file operations do not enter our
	 * open/release hooks. Keep symlink/FIFO/socket metadata for offline
	 * recovery rather than claiming their local handle lifetime is known. */
	if (s->node.kind != 1 && s->node.kind != 2) {
		err = 0;
		goto release;
	}
	if (s->node.depth && !s->node.lower) {
		err = index_segments(f, s->node.root, s->node.size, s->node.depth, &data);
		if (!err)
			err = collect_index_ids(f, s->node.root, s->node.depth, &indexes);
	}
	if (err)
		goto release;
	/* Destroy the head first. A failed later DELETE leaves graph garbage for
	 * offline fsck, never a head pointing at prematurely deleted data. */
	err = delete_orphan_object(f, id);
	for (size_t i = 0; !err && i < data.n; i++) {
		struct segment *part = &data.p[i];
		/* Legacy/random/shared references do not prove private ownership.
		 * Only canonical blocks derived from this inode may be freed online. */
		if (owns_data_block(&s->node, part, position))
			err = delete_orphan_object(f, part->id);
		position += part->length;
	}
	for (size_t i = 0; !err && i < indexes.n; i++)
		if (index_kept(&s->node, data.n, indexes.p[i].id))
			err = delete_orphan_object(f, indexes.p[i].id);
	if (!err)
		err = orphan_remove(f, id);
release:
	{
		int released = ks_object_reference(f, id, token, 3);
		if (!err)
			err = released;
	}
out:
	ks_snapshot_free(s);
	sq_free(s);
	sq_free(data.p);
	sq_free(indexes.p);
	return err;
}
int ks_collect_orphans(struct ks_fs *f, bool *pending)
{
	struct ks_snapshot *s = sq_alloc(sizeof(*s));
	struct buf body = {0};
	int err;
	*pending = false;
	if (!s)
		return -ENOMEM;
	if (f->readonly || !f->references) {
		err = 0;
		goto out;
	}
	err = orphan_snapshot(f, false, s, &body);
	if (err == -ENOENT) {
		err = 0;
		goto out;
	}
	if (err)
		goto out;
	size_t count = (body.n - 10) / 32;
	size_t start = count ? f->orphan_cursor % count : 0;
	size_t batch = min_t(size_t, count, 64);
	*pending = batch < count;
	for (size_t index = 0; index < batch; index++) {
		stqr_object_id id;
		size_t at = 10 + ((start + index) % count) * 32;
		memcpy(id.bytes, body.p + at, 32);
		err = collect_orphan(f, id);
		if (err == -EBUSY || err == -EAGAIN) {
			*pending = true;
			err = 0;
		}
		if (err)
			break;
		cond_resched();
	}
	if (count)
		f->orphan_cursor = (start + batch) % count;
out:
	ks_snapshot_free(s);
	sq_free(s);
	drop(&body);
	return err;
}
