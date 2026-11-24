// SPDX-License-Identifier: GPL-3.0-only
/* Shared wire contract: client/filesystem_redo.go (STFT v2 / STFJ / STFD). */
struct ks_edit {
	struct ks_edit *next;
	stqr_object_id id;
	u64 offset;
	bool absent, full, remove;
	struct buf data;
};
static void edits_free(struct ks_edit *w)
{
	while (w) {
		struct ks_edit *next = w->next;
		drop(&w->data);
		sq_free(w);
		w = next;
	}
}
static int object_stat(struct ks_fs *f, stqr_object_id id, stqr_info *info)
{
	struct ks_rpc *rpc = ks_rpc_lock(f, id);
	int err = stqr_stat(rpc->client, id, info);
	if (err == -ENOTCONN || err == -ECONNRESET || err == -EPIPE || err == -ETIMEDOUT ||
	    (err == -EAGAIN && !stqr_status(rpc->client))) {
		err = stqr_reconnect(rpc->client);
		if (!err)
			err = stqr_stat(rpc->client, id, info);
	}
	mutex_unlock(&rpc->lock);
	return err;
}
static int stable_id(stqr_object_id owner, unsigned kind, unsigned level, u64 slot,
		     stqr_object_id *out)
{
	struct buf b = {0};
	int err;
	put(&b, "STQR mutable filesystem block v1\0", 33);
	put(&b, owner.bytes, 32);
	number(&b, kind, 1);
	number(&b, level, 1);
	number(&b, slot, 8);
	err = b.err ? b.err : sq_hash(256, b.p, b.n, out->bytes);
	drop(&b);
	return err;
}
static int stage_block(struct ks_fs *f, struct ks_node *node, stqr_object_id id, u64 off,
		       const void *data, size_t n, bool full, bool remove)
{
	stqr_info info = {0};
	struct ks_edit *w;
	struct buf prior = {0};
	int err = object_stat(f, id, &info);
	bool absent = err == -ENOENT;
	if (err && !absent)
		return err;
	if (remove && absent)
		return 0;
	if (full && !absent) {
		err = get_object(f, id, (void **)&prior.p, &prior.n, &info);
		if (err)
			return err;
		if (prior.n == n && !memcmp(prior.p, data, n)) {
			drop(&prior);
			return 0;
		}
		if (prior.n == n) {
			size_t first = 0, last = n;
			while (first < last && prior.p[first] == ((const u8 *)data)[first])
				first++;
			while (last > first && prior.p[last - 1] == ((const u8 *)data)[last - 1])
				last--;
			off = first;
			data = (const u8 *)data + first;
			n = last - first;
			full = false;
		}
	}
	w = sq_alloc(sizeof(*w));
	if (!w) {
		drop(&prior);
		return -ENOMEM;
	}
	w->id = id;
	w->offset = off;
	w->absent = absent;
	w->full = full;
	w->remove = remove;
	put(&w->data, data, n);
	drop(&prior);
	if (w->data.err) {
		err = w->data.err;
		edits_free(w);
		return err;
	}
	if (!node->edits)
		node->edits = w;
	else {
		struct ks_edit *tail = node->edits;
		while (tail->next)
			tail = tail->next;
		tail->next = w;
	}
	node->mutable = true;
	return 0;
}
static void encode_edits(struct buf *b, const struct ks_node *node)
{
	struct ks_edit *w;
	unsigned count = 0;
	for (w = node->edits; w; w = w->next)
		count++;
	number(b, count, 4);
	for (w = node->edits; w; w = w->next) {
		put(b, w->id.bytes, 32);
		number(b, w->absent, 1);
		number(b, w->full, 1);
		number(b, w->remove, 1);
		number(b, w->offset, 8);
		blob(b, w->data.p, w->data.n);
		blob(b, NULL, 0);
	}
}
static int edit_id_compare(const void *a, const void *b)
{
	return memcmp(a, b, 32);
}
static int skip_edits(struct buf *d, struct buf *out, bool legacy)
{
	size_t start = d->pos;
	unsigned count = readnum(d, 4), i;
	stqr_object_id *ids;
	if (count > (1U << 20) || count > (d->n - d->pos) / 44)
		return -EUCLEAN;
	ids = count ? sq_alloc(count * sizeof(*ids)) : NULL;
	if (count && !ids)
		return -ENOMEM;
	for (i = 0; i < count; i++) {
		const u8 *id = take(d, 32);
		u64 off;
		if (legacy)
			readnum(d, 8);
		bool absent = boolean(d), full = boolean(d), remove = boolean(d);
		size_t n;
		off = readnum(d, 8);
		readblob(d, &n);
		if (!legacy) {
			size_t ignored;
			readblob(d, &ignored);
		}
		if (d->err || !id || zero(id, 32) || off > SQ_CHUNK || n > SQ_CHUNK - off ||
		    (full && off) || (remove && (absent || full || off || n))) {
			sq_free(ids);
			return -EUCLEAN;
		}
		memcpy(ids[i].bytes, id, 32);
	}
	if (count > 1)
		sort(ids, count, sizeof(*ids), edit_id_compare, NULL);
	for (i = 1; i < count; i++)
		if (eqid(ids[i - 1], ids[i])) {
			sq_free(ids);
			return -EUCLEAN;
		}
	sq_free(ids);
	out->p = d->p + start;
	out->n = d->pos - start;
	return 0;
}
static int redo_request(stqr_object_id tx, u64 index, stqr_id *rid)
{
	struct buf b = {0};
	u8 hash[32];
	int err;
	put(&b, "STQR filesystem REDO request v1\0", 32);
	put(&b, tx.bytes, 32);
	number(&b, index, 8);
	err = b.err ? b.err : sq_hash(256, b.p, b.n, hash);
	if (!err)
		memcpy(rid->bytes, hash, 16);
	drop(&b);
	return err;
}
static int apply_edits(struct ks_fs *f, stqr_object_id tx, const struct buf *encoded, u64 *index)
{
	struct buf d = *encoded;
	unsigned count = readnum(&d, 4), i;
	int err = 0;
	for (i = 0; i < count; i++) {
		stqr_object_id id;
		stqr_options opt = {0};
		stqr_info info;
		struct ks_rpc *rpc;
		const u8 *p = take(&d, 32), *data;
		size_t n;
		bool full, remove;
		u64 off;
		if (!p)
			return -EUCLEAN;
		memcpy(id.bytes, p, 32);
		opt.absent = boolean(&d);
		full = boolean(&d);
		remove = boolean(&d);
		off = readnum(&d, 8);
		data = readblob(&d, &n);
		{
			size_t ignored;
			readblob(&d, &ignored);
		}
		if (d.err)
			return -EUCLEAN;
		err = redo_request(tx, (*index)++, &opt.request_id);
		if (err)
			return err;
		rpc = ks_rpc_lock(f, id);
		ks_cache_forget(f, id);
		if (remove)
			err = stqr_delete(rpc->client, id, &opt);
		else if (full)
			err = stqr_put(rpc->client, id, data, n, &opt, &info);
		else
			err = stqr_put_range(rpc->client, id, off, data, n, &opt, &info);
		if (err == -EINPROGRESS || err == -EACCES || err == -ENOTCONN ||
		    err == -ECONNRESET || err == -EPIPE || err == -ETIMEDOUT) {
			if (!stqr_reconnect(rpc->client)) {
				if (remove)
					err = stqr_delete(rpc->client, id, &opt);
				else if (full)
					err = stqr_put(rpc->client, id, data, n, &opt, &info);
				else
					err = stqr_put_range(rpc->client, id, off, data, n, &opt,
							     &info);
			}
			if (err == -EINPROGRESS)
				err = -EIO;
		}
		mutex_unlock(&rpc->lock);
		ks_cache_forget(f, id);
		if (remove && err == -ENOENT)
			err = 0;
		if (err)
			return err;
	}
	return done(&d);
}
static int intent_get(struct ks_fs *f, stqr_object_id id, struct buf *raw, struct buf *head,
		      stqr_info *info)
{
	struct buf d;
	u64 length, count, i;
	int err = get_object(f, id, (void **)&head->p, &head->n, info);
	if (err)
		return err;
	if (head->n < 4 || memcmp(head->p, "STFJ", 4)) {
		put(raw, head->p, head->n);
		return raw->err;
	}
	d = *head;
	take(&d, 4);
	if (readnum(&d, 2) != 1)
		return -EUCLEAN;
	take(&d, 1);
	length = readnum(&d, 4);
	count = readnum(&d, 4);
	if (d.err || length <= 65536 || length > (128U << 20) ||
	    count != (length + SQ_CHUNK - 1) / SQ_CHUNK || count * 32 != d.n - d.pos)
		return -EUCLEAN;
	for (i = 0; i < count; i++) {
		struct buf part = {0};
		stqr_info pi;
		stqr_object_id oid;
		const u8 *p = take(&d, 32);
		if (!p)
			return -EUCLEAN;
		memcpy(oid.bytes, p, 32);
		if (eqid(oid, id) || nullid(oid))
			return -EUCLEAN;
		err = get_object(f, oid, (void **)&part.p, &part.n, &pi);
		if (err || part.n != min_t(u64, SQ_CHUNK, length - raw->n)) {
			struct buf finished = {0};
			stqr_info fi;
			int check = get_object(f, id, (void **)&finished.p, &finished.n, &fi);
			drop(&part);
			if (!check && finished.n == 7 && !memcmp(finished.p, "STFD\1\0", 6) &&
			    (finished.p[6] == 1 || finished.p[6] == 2)) {
				drop(raw);
				drop(head);
				*head = finished;
				*info = fi;
				put(raw, head->p, head->n);
				return raw->err;
			}
			drop(&finished);
			return err ? err : -EUCLEAN;
		}
		put(raw, part.p, part.n);
		drop(&part);
		if (raw->err)
			return raw->err;
	}
	if (raw->n < 7 || head->p[6] > 2)
		return -EUCLEAN;
	raw->p[6] = head->p[6];
	return 0;
}
static int intent_store(struct ks_fs *f, struct buf *raw, stqr_object_id *id)
{
	struct buf head = {0};
	size_t at;
	int err = 0;
	if (raw->n > (128U << 20))
		return -E2BIG;
	if (raw->n <= 65536)
		return immutable(f, raw->p, raw->n, id);
	put(&head, "STFJ\1\0", 6);
	number(&head, 0, 1);
	number(&head, raw->n, 4);
	number(&head, (raw->n + SQ_CHUNK - 1) / SQ_CHUNK, 4);
	for (at = 0; at < raw->n; at += SQ_CHUNK) {
		stqr_object_id part;
		err = immutable(f, raw->p + at, min_t(size_t, SQ_CHUNK, raw->n - at), &part);
		if (err)
			break;
		put(&head, part.bytes, 32);
	}
	if (!err)
		err = head.err ? head.err : immutable(f, head.p, head.n, id);
	drop(&head);
	return err;
}
static void intent_retire(struct ks_fs *f, stqr_object_id id, const struct buf *head,
			  stqr_info *info, unsigned state)
{
	u8 terminal[7] = "STFD\1\0";
	struct buf d;
	unsigned count, i;
	int err;
	if (head->n < 7 ||
	    (memcmp(head->p, "STFJ", 4) && !(head->p[4] >= 2 && !memcmp(head->p, "STFT", 4))))
		return;
	terminal[6] = state;
	err = cas_object(f, id, terminal, sizeof(terminal), head->p, head->n, false);
	if (err)
		return;
	if (memcmp(head->p, "STFJ", 4))
		return;
	d = *head;
	take(&d, 11);
	count = readnum(&d, 4);
	for (i = 0; i < count; i++) {
		stqr_object_id part;
		stqr_info pi;
		stqr_options opt = {0};
		struct ks_rpc *rpc;
		const u8 *p = take(&d, 32);
		if (!p)
			return;
		memcpy(part.bytes, p, 32);
		if (object_stat(f, part, &pi))
			continue;
		if (stqr_request_id(&opt.request_id))
			continue;
		rpc = ks_rpc_lock(f, part);
		stqr_delete(rpc->client, part, &opt);
		mutex_unlock(&rpc->lock);
	}
}
static int get_view_object(struct ks_fs *f, const struct ks_node *node, stqr_object_id id,
			   void **out, size_t *length, stqr_info *info)
{
	struct buf d = {.p = (u8 *)node->view, .n = node->view_size};
	unsigned count, i;
	int err;
	*out = NULL;
	*length = 0;
	if (!node->view)
		return get_object(f, id, out, length, info);
	count = readnum(&d, 4);
	for (i = 0; i < count; i++) {
		const u8 *key = take(&d, 32), *data;
		u64 off;
		bool absent = boolean(&d), full = boolean(&d), remove = boolean(&d);
		size_t n;
		u8 *body;
		off = readnum(&d, 8);
		data = readblob(&d, &n);
		{
			size_t ignored;
			readblob(&d, &ignored);
		}
		if (d.err)
			return -EUCLEAN;
		if (memcmp(key, id.bytes, 32))
			continue;
		if (remove)
			return -ENOENT;
		if (full) {
			body = sq_alloc(n);
			if (!body)
				return -ENOMEM;
			memcpy(body, data, n);
			*out = body;
			*length = n;
			memset(info, 0, sizeof(*info));
			info->length = n;
			return 0;
		}
		err = get_object(f, id, out, length, info);
		if (absent && err == -ENOENT) {
			err = 0;
			*out = NULL;
			*length = 0;
		}
		if (err)
			return err;
		if (off + n > *length) {
			body = sq_alloc(off + n);
			if (!body) {
				sq_free(*out);
				*out = NULL;
				return -ENOMEM;
			}
			memcpy(body, *out, *length);
			sq_free(*out);
			*out = body;
			*length = off + n;
		}
		memcpy((u8 *)*out + off, data, n);
		info->length = *length;
		return 0;
	}
	return get_object(f, id, out, length, info);
}
