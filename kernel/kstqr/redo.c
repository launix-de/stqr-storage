// SPDX-License-Identifier: GPL-3.0-only
/* Shared wire contract: client/filesystem_redo.go (STFT v3 / STFJ / STFD). */
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
    int err;
    for (;;) {
        u64 epoch = rpc->reconnect_epoch;
        int status = 0;
        if (rpc->receive_started) {
            struct ks_async_result result = {0};
            init_completion(&result.done);
            err = stqr_stat_async(rpc->client, id, ks_async_completed, &result);
            mutex_unlock(&rpc->lock);
            if (!err) {
                wait_for_completion(&result.done);
                err = result.error;
                *info = result.info;
                status = result.status;
            }
            sq_free(result.data);
            mutex_lock(&rpc->lock);
        } else {
            err = stqr_stat(rpc->client, id, info);
            status = stqr_status(rpc->client);
        }
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
/* Only independent object edits form a no-ACK burst. Repeated edits of one
 * object retain their explicit data dependency even though the wire transport
 * is free to execute independent messages in any order. */
struct ks_edit_key { struct hlist_node hash; stqr_object_id id; };
static bool edits_independent(const struct buf *encoded,bool legacy)
{
 DECLARE_HASHTABLE(keys,6);
 struct buf d=*encoded;
 unsigned count=readnum(&d,4),i;
 struct ks_edit_key *entries;
 bool independent=true;
 if(d.err||count>d.n/44)return false;
 entries=kcalloc(count,sizeof(*entries),GFP_NOFS);
 if(!entries)return false;
 hash_init(keys);
 for(i=0;i<count;i++) {
  const u8 *id=take(&d,32);struct ks_edit_key *prior;size_t ignored;u32 key;
  if(!id){independent=false;break;}
  memcpy(entries[i].id.bytes,id,32);memcpy(&key,id,sizeof(key));
  hash_for_each_possible(keys,prior,hash,key)if(eqid(prior->id,entries[i].id)){independent=false;break;}
  if(!independent)break;
  hash_add(keys,&entries[i].hash,key);
  if(legacy)readnum(&d,8);
  boolean(&d);boolean(&d);boolean(&d);readnum(&d,8);readblob(&d,&ignored);
  if(!legacy)readblob(&d,&ignored);
  if(d.err){independent=false;break;}
 }
 kfree(entries);return independent&&!done(&d);
}
struct ks_edit_completion {
 struct ks_async_result result;
 stqr_object_id id;
 const void *data;
 size_t size;
 u64 offset;
 bool full, submitted;
};
static int apply_edits(struct ks_fs *f, stqr_object_id tx, const struct buf *encoded, bool legacy,
		       u64 *index)
{
	struct buf d = *encoded;
	unsigned count = readnum(&d, 4), i;
    bool independent=edits_independent(encoded,legacy);
	int err = 0;
    struct ks_edit_completion *results = independent ? kcalloc(count,sizeof(*results),GFP_NOFS) : NULL;
	for (i = 0; i < count; i++) {
		stqr_object_id id;
		stqr_options opt = {.accepted=1};
		stqr_info info;
		struct ks_rpc *rpc;
		const u8 *p = take(&d, 32), *data;
		size_t n;
		bool full, remove;
		u64 off;
		if (!p) {err=-EUCLEAN;goto complete;}
		memcpy(id.bytes, p, 32);
		if (legacy)
			readnum(&d, 8); /* obsolete counter: decode only */
		opt.absent = boolean(&d);
		full = boolean(&d);
		remove = boolean(&d);
		off = readnum(&d, 8);
		data = readblob(&d, &n);
		if (!legacy) {
			size_t ignored;
			readblob(&d, &ignored);
		}
		if (d.err) {err=-EUCLEAN;goto complete;}
		err = redo_request(tx, (*index)++, &opt.request_id);
		if (err)goto complete;
		rpc = ks_rpc_lock(f, id);
		ks_cache_forget(f, id);
        /* Accepted replies establish visibility and expose operation errors;
         * physical durability is requested only by the caller's sync. */
        if(results && !remove &&
           (stqr_capabilities(rpc->client)&STQR_CAP_CONNECTION_SYNC)) {
            struct ks_edit_completion *job=&results[i];
            init_completion(&job->result.done);job->id=id;job->data=data;job->size=n;
            job->offset=off;job->full=full;
            if(opt.absent&&full)
                err=stqr_cas_async(rpc->client,id,1,0,NULL,0,data,n,&opt,ks_async_completed,&job->result);
            else if(full)
                err=stqr_put_result_async(rpc->client,id,data,n,&opt,ks_async_completed,&job->result);
            else
                err=stqr_put_range_result_async(rpc->client,id,off,data,n,&opt,ks_async_completed,&job->result);
            mutex_unlock(&rpc->lock);
            if(err)goto complete;
            job->submitted=true;continue;
        }
		bool auth_retry = false;
		for (;;) {
            u64 epoch = rpc->reconnect_epoch;
            int status = 0;
			if (remove)
				err = ks_delete_locked(rpc, id, &opt, &status);
			else if (full) {
				err = stqr_put(rpc->client, id, data, n, &opt, &info);
                status = stqr_status(rpc->client);
            } else {
				err = stqr_put_range(rpc->client, id, off, data, n, &opt, &info);
                status = stqr_status(rpc->client);
            }
			if (err == -EACCES && !auth_retry)
				auth_retry = true;
			else if (!(err == -EAGAIN ? (status == 0 || status == 503) :
                       ks_transport_retryable(NULL, err)))
				break;
			err = ks_rpc_retry(f, rpc, epoch, status);
			if (err)
				break;
		}
		if (err == -EAGAIN && f->references && !remove) {
			void *observed = NULL;
			size_t actual = 0;
			int check = get_object_locked(f, rpc, id, &observed, &actual, &info);
			/* The exclusive descriptor claim keeps all heads marked. A
			 * repeated create-only write may recognize its prior after-image
			 * without retaining server PUT/CAS history. */
			err = -EIO; /* A decided REDO failure must not restart a whole append. */
			if (!check && ((full && actual == n && !memcmp(observed, data, n)) ||
			    (!full && off <= actual && n <= actual - off &&
			     !memcmp((u8 *)observed + off, data, n))))
				err = 0;
			sq_free(observed);
		}
		mutex_unlock(&rpc->lock);
		ks_cache_forget(f, id);
		if (remove && err == -ENOENT)
			err = 0;
		if (err)goto complete;
	}
    err=done(&d);
complete:
    if(results)for(unsigned j=0;j<count;j++)if(results[j].submitted) {
        struct ks_edit_completion *job=&results[j];int result;
        wait_for_completion(&job->result.done);result=job->result.error;
        if(result==-EAGAIN && job->result.status==412 && f->references) {
            void *observed=NULL;size_t size=0;stqr_info info;
            int check=get_object(f,job->id,&observed,&size,&info);
            result=-EIO;
            if(!check && ((job->full&&size==job->size&&!memcmp(observed,job->data,size)) ||
               (!job->full&&job->offset<=size&&job->size<=size-job->offset&&
                !memcmp((u8 *)observed+job->offset,job->data,job->size))))result=0;
            sq_free(observed);
        }
        sq_free(job->result.data);if(!err)err=result;
    }
    kfree(results);
    return err;
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
static int intent_store_at(struct ks_fs *f, struct buf *raw, stqr_object_id id)
{
	struct buf head = {0};
	size_t at;
	int err = 0;
	if (raw->n > (128U << 20))
		return -E2BIG;
	if (raw->n <= 65536)
		return cas_object(f, id, raw->p, raw->n, NULL, 0, true);
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
		err = head.err ? head.err : cas_object(f, id, head.p, head.n, NULL, 0, true);
	drop(&head);
	return err;
}
static int intent_store(struct ks_fs *f, struct buf *raw, stqr_object_id *id)
{
	int err = sq_random(id->bytes, 32);
	return err ? err : intent_store_at(f, raw, *id);
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
		ks_delete_locked(rpc, part, &opt, NULL);
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
		if (node->view_legacy)
			readnum(&d, 8); /* obsolete counter: decode only */
		bool absent = boolean(&d), full = boolean(&d), remove = boolean(&d);
		size_t n;
		u8 *body;
		off = readnum(&d, 8);
		data = readblob(&d, &n);
		if (!node->view_legacy) {
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
