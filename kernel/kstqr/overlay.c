// SPDX-License-Identifier: GPL-3.0-only
/* Included after objects.c. STOR/STOM and lazy STFS/STIX references are the
 * native Go/FUSE wire format, not a second overlay representation. */
#include <linux/dcache.h>
#include <linux/refcount.h>
static int ov_lower_reference_attr(struct ks_node *, stqr_id, struct buf *);
struct ov_dir {
	stqr_object_id id, lower;
	bool opaque, metadata;
	struct ks_node node;
	struct buf image;
	struct ks_entry *entries;
	size_t count;
};
struct ks_overlay_state {
	refcount_t refs;
	stqr_info info;
	struct buf root_head;
	char **policy;
	unsigned policies;
	struct ov_dir **dirs;
	size_t count;
	struct member orphan_member;
	stqr_object_id orphan_id;
	bool orphan, orphan_has_inode;
};
static void ov_put(struct ks_overlay_state *s)
{
	if (!s || !refcount_dec_and_test(&s->refs))
		return;
	member_free(&s->orphan_member);
	for (size_t i = 0; i < s->count; i++) {
		drop(&s->dirs[i]->image);
		sq_free(s->dirs[i]->entries);
		sq_free(s->dirs[i]);
	}
	if (s->policy)
		for (unsigned i = 0; i < s->policies; i++)
			sq_free(s->policy[i]);
	drop(&s->root_head);
	sq_free(s->policy);
	sq_free(s->dirs);
	sq_free(s);
}
void ks_overlay_stop(struct ks_fs *f)
{
	ov_put(f->overlay_state);
	f->overlay_state = NULL;
}
static struct ov_dir *ov_find(struct ks_overlay_state *s, stqr_object_id id)
{
	for (size_t i = 0; i < s->count; i++)
		if (eqid(s->dirs[i]->id, id))
			return s->dirs[i];
	return NULL;
}
static int ov_node(struct ov_dir *d, const struct ks_node *node)
{
	struct ks_node n = *node;
	struct buf image = {0};
	int err;
	n.size = n.depth = 0;
	n.inline_data = NULL;
	memset(&n.root, 0, 32);
	n.lower = false;
	n.write_path = NULL;
	n.write_path_len = 0;
	n.inherited = false;
	encode_node(&image, &n);
	err = image.err ? image.err : decode_node(image.p, image.n, &n);
	if (err) {
		drop(&image);
		return err;
	}
	drop(&d->image);
	d->image = image;
	d->node = n;
	return 0;
}
static int ov_add_dir(struct ks_overlay_state *s, struct ov_dir *d)
{
	struct ov_dir **next = sq_alloc((s->count + 1) * sizeof(*next));
	if (!next)
		return -ENOMEM;
	if (s->count)
		memcpy(next, s->dirs, s->count * sizeof(*next));
	next[s->count++] = d;
	sq_free(s->dirs);
	s->dirs = next;
	return 0;
}
static bool ov_space(const u8 *p, size_t n)
{
	if (!n)
		return false;
	u32 c = p[0];
	unsigned bytes = 1;
	if (c >= 0xc2 && c <= 0xdf) {
		c &= 31;
		bytes = 2;
	} else if (c >= 0xe0 && c <= 0xef) {
		c &= 15;
		bytes = 3;
	} else if (c >= 0xf0 && c <= 0xf4) {
		c &= 7;
		bytes = 4;
	}
	if (bytes > n)
		return false;
	for (unsigned i = 1; i < bytes; i++)
		c = (c << 6) | (p[i] & 63);
	return c == 32 || (c >= 9 && c <= 13) || c == 0x85 || c == 0xa0 || c == 0x1680 ||
	       (c >= 0x2000 && c <= 0x200a) || c == 0x2028 || c == 0x2029 || c == 0x202f ||
	       c == 0x205f || c == 0x3000;
}
static bool ov_path_valid(const char *p, bool rule)
{
	size_t n = strlen(p), start = 0;
	if (!n || n > 4096 || p[0] == '/')
		return false;
	if (rule) {
		size_t last = n - 1;
		while (last && ((u8)p[last] & 0xc0) == 0x80)
			last--;
		if (ov_space((u8 *)p, n) || ov_space((u8 *)p + last, n - last))
			return false;
	}
	if (rule && p[n - 1] == '/')
		n--;
	for (size_t i = 0; i <= n; i++)
		if (i == n || p[i] == '/') {
			char name[256];
			size_t z = i - start;
			if (!z || z > 255)
				return false;
			memcpy(name, p + start, z);
			name[z] = 0;
			if (!valid_name(name))
				return false;
			start = i + 1;
		}
	return true;
}
static int ov_string(struct buf *b, char **out, size_t max)
{
	size_t n;
	const u8 *p = readblob(b, &n);
	if (b->err || n > max || memchr(p, 0, n))
		return -EUCLEAN;
	*out = sq_alloc(n + 1);
	if (!*out)
		return -ENOMEM;
	memcpy(*out, p, n);
	return 0;
}
static int ov_decode(const void *raw, size_t length, struct ks_overlay_state **out)
{
	struct buf b = {.p = (u8 *)raw, .n = length};
	struct ks_overlay_state *s = sq_alloc(sizeof(*s));
	int err = 0;
	stqr_object_id previous = {{0}};
	if (!s)
		return -ENOMEM;
	refcount_set(&s->refs, 1);
	if (length < 14 || memcmp(take(&b, 4), "STOM", 4) || readnum(&b, 2) != 1) {
		err = -EUCLEAN;
		goto fail;
	}
	s->policies = readnum(&b, 4);
	if (b.err || s->policies > 4096 || s->policies > length) {
		err = -EUCLEAN;
		goto fail;
	}
	s->policy = sq_alloc(s->policies * sizeof(char *));
	if (!s->policy) {
		err = -ENOMEM;
		goto fail;
	}
	for (unsigned i = 0; i < s->policies; i++) {
		err = ov_string(&b, &s->policy[i], 4096);
		if (err)
			goto fail;
		if (!ov_path_valid(s->policy[i], true) ||
		    (i && strcmp(s->policy[i - 1], s->policy[i]) >= 0)) {
			err = -EUCLEAN;
			goto fail;
		}
	}
	unsigned count = readnum(&b, 4);
	if (b.err || !count || count > length / 70) {
		err = -EUCLEAN;
		goto fail;
	}
	for (unsigned i = 0; i < count; i++) {
		struct ov_dir *d = sq_alloc(sizeof(*d));
		const u8 *p;
		size_t n;
		if (!d) {
			err = -ENOMEM;
			goto fail;
		}
		err = ov_add_dir(s, d);
		if (err) {
			sq_free(d);
			goto fail;
		}
		p = take(&b, 32);
		if (p)
			memcpy(d->id.bytes, p, 32);
		p = take(&b, 32);
		if (p)
			memcpy(d->lower.bytes, p, 32);
		if (b.err || nullid(d->id) || (i && memcmp(previous.bytes, d->id.bytes, 32) >= 0)) {
			err = -EUCLEAN;
			goto fail;
		}
		previous = d->id;
		d->opaque = boolean(&b);
		d->metadata = boolean(&b);
		p = readblob(&b, &n);
		if (b.err) {
			err = b.err;
			goto fail;
		}
		put(&d->image, p, n);
		err = d->image.err ? d->image.err : decode_node(d->image.p, d->image.n, &d->node);
		if (err || d->node.kind != 2 || !nullid(d->node.root) || d->node.lower ||
		    d->opaque != nullid(d->lower)) {
			err = -EUCLEAN;
			goto fail;
		}
		d->count = readnum(&b, 4);
		if (b.err || d->count > (b.n - b.pos) / 34) {
			err = -EUCLEAN;
			goto fail;
		}
		d->entries = sq_alloc((d->count + 1) * sizeof(*d->entries));
		if (!d->entries) {
			err = -ENOMEM;
			goto fail;
		}
		for (size_t j = 0; j < d->count; j++) {
			p = readblob(&b, &n);
			if (b.err || !n || n > 255 || memchr(p, 0, n)) {
				err = -EUCLEAN;
				goto fail;
			}
			memcpy(d->entries[j].name, p, n);
			if (!valid_name(d->entries[j].name) ||
			    (j && strcmp(d->entries[j - 1].name, d->entries[j].name) >= 0)) {
				err = -EUCLEAN;
				goto fail;
			}
			p = take(&b, 32);
			if (p)
				memcpy(d->entries[j].id.bytes, p, 32);
		}
	}
	if (done(&b) || !ov_find(s, ks_root_id) || ov_find(s, ks_root_id)->node.removed) {
		err = -EUCLEAN;
		goto fail;
	}
	*out = s;
	return 0;
fail:
	ov_put(s);
	return err;
}
static int ov_dir_cmp(const void *a, const void *b)
{
	const struct ov_dir *const *x = a, *const *y = b;
	return memcmp((*x)->id.bytes, (*y)->id.bytes, 32);
}
static int ov_encode(struct ks_overlay_state *s, struct buf *b)
{
	sort(s->dirs, s->count, sizeof(*s->dirs), ov_dir_cmp, NULL);
	put(b, "STOM", 4);
	number(b, 1, 2);
	number(b, s->policies, 4);
	for (unsigned i = 0; i < s->policies; i++)
		blob(b, s->policy[i], strlen(s->policy[i]));
	number(b, s->count, 4);
	for (size_t i = 0; i < s->count; i++) {
		struct ov_dir *d = s->dirs[i];
		sort(d->entries, d->count, sizeof(*d->entries), entry_cmp, NULL);
		put(b, d->id.bytes, 32);
		put(b, d->lower.bytes, 32);
		number(b, d->opaque, 1);
		number(b, d->metadata, 1);
		blob(b, d->image.p, d->image.n);
		number(b, d->count, 4);
		for (size_t j = 0; j < d->count; j++) {
			blob(b, d->entries[j].name, strlen(d->entries[j].name));
			put(b, d->entries[j].id.bytes, 32);
		}
	}
	return b->err;
}
static int ov_state_once(struct ks_fs *f, struct ks_overlay_state **out)
{
	struct ks_snapshot root = {0};
	struct buf raw = {0}, d;
	stqr_info info;
	struct ks_node content;
	const u8 *p;
	size_t n;
	void *data = NULL;
	struct ks_overlay_state *s = NULL;
	int err;
	// Resolve the same STFL/STFT intent as ordinary inode readers. A read-only
	// mount observes committed redo virtually; a writer can finish the intent.
	err = ks_plain_snapshot(f, ks_root_id, &root);
	if (err)
		return err;
	raw.p = root.raw;
	raw.n = root.raw_len;
	info = root.info;
	mutex_lock(&f->overlay_cache_lock);
	s = f->overlay_state;
	if (s && s->root_head.n == raw.n && !memcmp(s->root_head.p, raw.p, raw.n) &&
	    !memcmp(s->info.runtime.bytes, info.runtime.bytes, 16)) {
		refcount_inc(&s->refs);
		mutex_unlock(&f->overlay_cache_lock);
		ks_snapshot_free(&root);
		*out = s;
		return 0;
	}
	mutex_unlock(&f->overlay_cache_lock);
	s = NULL;
	d = raw;
	d.pos = 0;
	if (raw.n < 23 || memcmp(take(&d, 4), "STOR", 4) || readnum(&d, 2) != 1) {
		err = -EOPNOTSUPP;
		goto done;
	}
	p = take(&d, 16);
	if (!p || memcmp(p, f->lower->bucket.bytes, 16)) {
		err = -EXDEV;
		goto done;
	}
	p = readblob(&d, &n);
	err = d.err ? d.err : decode_node(p, n, &content);
	if (err || done(&d) || content.kind != 1 || content.lower || content.size > KS_DIR_LIMIT) {
		err = -EUCLEAN;
		goto done;
	}
	data = sq_alloc(content.size);
	if (!data) {
		err = -ENOMEM;
		goto done;
	}
	content = root.node;
	err = read_node(f, &content, data, content.size, 0);
	{
		struct buf observed = {0};
		stqr_info oi;
		u8 digest[32];
		int guard = get_object(f, nullid(content.guard) ? ks_root_id : content.guard,
				       (void **)&observed.p, &observed.n, &oi);
		if (!guard)
			guard = sq_hash(256, observed.p, observed.n, digest);
		drop(&observed);
		if (!guard &&
		    memcmp(digest,
			   nullid(content.guard) ? content.head_digest : content.guard_digest,
			   32)) {
			ks_cache_forget(f, ks_root_id);
			err = -EAGAIN;
		} else if (guard)
			err = guard;
	}
	if (!err && !zero(content.digest, 32)) {
		u8 digest[32];
		err = sq_hash(256, data, content.size, digest);
		if (!err && memcmp(digest, content.digest, 32))
			err = -EUCLEAN;
	}
	if (err)
		goto done;
	err = ov_decode(data, content.size, &s);

	if (err)
		goto done;
	s->info = info;
	put(&s->root_head, raw.p, raw.n);
	if (s->root_head.err) {
		err = s->root_head.err;
		ov_put(s);
		goto done;
	}
	mutex_lock(&f->overlay_cache_lock);
	struct ks_overlay_state *old = f->overlay_state;
	f->overlay_state = s;
	refcount_inc(&s->refs);
	mutex_unlock(&f->overlay_cache_lock);
	ov_put(old);
	*out = s;
done:
	sq_free(data);
	ks_snapshot_free(&root);
	return err;
}
static int ov_state(struct ks_fs *f, struct ks_overlay_state **out)
{
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		int err = ov_state_once(f, out);
		if (err != -EAGAIN)
			return err;
		cond_resched();
	}
	return -EAGAIN;
}
static int ov_edit(struct ks_fs *f, struct ks_overlay_state **out)
{
	struct ks_overlay_state *base, *s = NULL;
	int err = ov_state(f, &base);
	if (err)
		return err;
	s = sq_alloc(sizeof(*s));
	if (!s) {
		ov_put(base);
		return -ENOMEM;
	}
	refcount_set(&s->refs, 1);
	s->info = base->info;
	put(&s->root_head, base->root_head.p, base->root_head.n);
	if (s->root_head.err) {
		err = s->root_head.err;
		goto done;
	}
	s->policies = base->policies;
	s->policy = sq_alloc(s->policies * sizeof(char *));
	if (!s->policy) {
		err = -ENOMEM;
		goto done;
	}
	for (unsigned i = 0; i < s->policies; i++) {
		size_t n = strlen(base->policy[i]) + 1;
		s->policy[i] = sq_alloc(n);
		if (!s->policy[i]) {
			err = -ENOMEM;
			goto done;
		}
		memcpy(s->policy[i], base->policy[i], n);
	}
	for (size_t i = 0; i < base->count; i++) {
		struct ov_dir *d = sq_alloc(sizeof(*d)), *a = base->dirs[i];
		if (!d) {
			err = -ENOMEM;
			goto done;
		}
		err = ov_add_dir(s, d);
		if (err) {
			sq_free(d);
			goto done;
		}
		d->id = a->id;
		d->lower = a->lower;
		d->opaque = a->opaque;
		d->metadata = a->metadata;
		err = ov_node(d, &a->node);
		if (err)
			goto done;
		d->count = a->count;
		d->entries = sq_alloc((d->count + 1) * sizeof(*d->entries));
		if (!d->entries) {
			err = -ENOMEM;
			goto done;
		}
		memcpy(d->entries, a->entries, d->count * sizeof(*d->entries));
	}
	*out = s;
	s = NULL;
done:
	ov_put(base);
	ov_put(s);
	return err;
}
static int ov_publish(struct ks_fs *f, struct ks_overlay_state *s)
{
	struct buf body = {0}, node = {0}, root = {0};
	struct ks_node content = {.kind = 1, .links = 1};
	int err = 0;
	if (s->root_head.n) {
		struct buf d = s->root_head;
		const u8 *raw;
		size_t len;
		take(&d, 22);
		raw = readblob(&d, &len);
		err = d.err ? d.err : decode_node(raw, len, &content);
	}
	content.identity = ks_root_id;
	content.kind = 2;
	if (!err)
		err = ov_encode(s, &body);
	if (!err && body.n > KS_DIR_LIMIT)
		err = -E2BIG;
	if (!err)
		err = blob_bytes(f, &content, body.p, body.n);
	if (!err) {
		content.kind = 1;
		err = sq_hash(256, body.p, body.n, content.digest);
		if (err)
			goto done;
		encode_node(&node, &content);
		put(&root, "STOR", 4);
		number(&root, 1, 2);
		put(&root, f->lower->bucket.bytes, 16);
		blob(&root, node.p, node.n);
		if (root.err)
			err = root.err;
		else if ((content.edits || s->orphan || f->references) && s->root_head.n) {
			struct member *m = sq_alloc(3 * sizeof(*m));
			unsigned count = 1;
			if (!m) {
				err = -ENOMEM;
				goto done;
			}
			m[0].id = ks_root_id;
			put(&m[0].before, s->root_head.p, s->root_head.n);
			put(&m[0].after, root.p, root.n);
			encode_edits(&m[0].writes, &content);
			if (s->orphan_has_inode) {
				m[count] = s->orphan_member;
				memset(&s->orphan_member, 0, sizeof(s->orphan_member));
				count++;
			}
			if (s->orphan) {
				bool queued;
				err = orphan_member(f, s->orphan_id, true, &m[count], &queued);
				if (!err && queued)
					count++;
			}
			if (!err) {
				err = transact(f, m, count);
			}
			for (unsigned i = 0; i < 3; i++)
				member_free(&m[i]);
			sq_free(m);
		} else {
			err = cas_object(f, ks_root_id, root.p, root.n, s->root_head.p,
					 s->root_head.n, !s->root_head.n);
		}
	}
done:
	edits_free(content.edits);
	drop(&body);
	drop(&node);
	drop(&root);
	return err;
}
static bool ov_upper_ref(struct ks_overlay_state *s, stqr_object_id id)
{
	for (size_t i = 0; i < s->count; i++)
		for (size_t j = 0; j < s->dirs[i]->count; j++)
			if (!nullid(id) && eqid(s->dirs[i]->entries[j].id, id))
				return true;
	return false;
}
static int ov_entry(struct ov_dir *d, const char *name, stqr_object_id id)
{
	long at = find_entry(d->entries, d->count, name);
	if (at < 0) {
		struct ks_entry *p = sq_alloc((d->count + 1) * sizeof(*p));
		if (!p)
			return -ENOMEM;
		if (d->count)
			memcpy(p, d->entries, d->count * sizeof(*p));
		sq_free(d->entries);
		d->entries = p;
		at = d->count++;
	}
	strscpy(d->entries[at].name, name, 256);
	d->entries[at].id = id;
	return 0;
}
static int ov_entries_info(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id id,
			   struct ks_entry **out, size_t *count, stqr_info *lower_info)
{
	struct ov_dir *d = ov_find(s, id);
	struct ks_snapshot lower;
	struct ks_entry *base = NULL, *next;
	size_t n = 0, total = 0;
	int err = 0;
	if (d && d->node.removed)
		return -ENOENT;
	if (!d || !d->opaque) {
		err = ks_plain_snapshot(f->lower, d ? d->lower : id, &lower);
		if (err)
			return err;
		if (lower_info)
			*lower_info = lower.info;
		err = ks_directory(f->lower, &lower.node, &base, &n);
		ks_snapshot_free(&lower);
		if (err)
			return err;
	}
	next = sq_alloc((n + (d ? d->count : 0) + 1) * sizeof(*next));
	if (!next) {
		sq_free(base);
		return -ENOMEM;
	}
	for (size_t i = 0; i < n; i++) {
		if (d && find_entry(d->entries, d->count, base[i].name) >= 0)
			continue;
		if (ov_upper_ref(s, base[i].id)) {
			err = -EOPNOTSUPP;
			goto fail;
		}
		next[total++] = base[i];
	}
	if (d)
		for (size_t i = 0; i < d->count; i++)
			if (!nullid(d->entries[i].id))
				next[total++] = d->entries[i];
	sort(next, total, sizeof(*next), entry_cmp, NULL);
	sq_free(base);
	*out = next;
	*count = total;
	return 0;
fail:
	sq_free(base);
	sq_free(next);
	return err;
}
static int ov_entries(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id id,
		      struct ks_entry **out, size_t *count)
{
	return ov_entries_info(f, s, id, out, count, NULL);
}

/* Point lookups must not materialize, merge and sort a complete virtual
 * directory. Upper entries (including whiteouts) win; otherwise consult exactly
 * the lower directory represented by this overlay-state snapshot. */
/* Certify a single name binding without materializing a virtual directory. */
static int ov_child_proven(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id parent,
			   const char *name, stqr_object_id *out, struct ks_directory_proof *proof)
{
	struct ov_dir *d = ov_find(s, parent);
	struct ks_snapshot lower = {0};
	struct ks_entry *entries = NULL;
	stqr_info lower_info = {0};
	size_t count = 0;
	bool consulted_lower = false;
	int err = 0;
	long at;
	if (proof)
		memset(proof, 0, sizeof(*proof));
	if (d) {
		if (d->node.removed)
			return -ENOENT;
		at = find_entry(d->entries, d->count, name);
		if (at >= 0) {
			if (nullid(d->entries[at].id))
				err = -ENOENT;
			else
				*out = d->entries[at].id;
			goto done;
		}
		if (d->opaque) {
			err = -ENOENT;
			goto done;
		}
	}
	consulted_lower = true;
	err = ks_plain_snapshot(f->lower, d ? d->lower : parent, &lower);
	if (!err) {
		lower_info = lower.info;
		err = ks_directory(f->lower, &lower.node, &entries, &count);
	}
	ks_snapshot_free(&lower);
	if (err)
		goto done;
	at = find_entry(entries, count, name);
	if (at < 0)
		err = -ENOENT;
	else if (ov_upper_ref(s, entries[at].id))
		err = -EOPNOTSUPP;
	else
		*out = entries[at].id;
done:
	sq_free(entries);
	if (proof && (!err || err == -ENOENT)) {
		proof->has_lower = consulted_lower;
		proof->lower_id = d ? d->lower : parent;
		proof->valid = ks_cache_stamp(f, ks_root_id, &s->info, &proof->root) &&
			       (!consulted_lower || ks_cache_stamp(f->lower, proof->lower_id,
								   &lower_info, &proof->lower));
	}
	return err;
}
static int ov_child(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id parent,
		    const char *name, stqr_object_id *out)
{
	return ov_child_proven(f, s, parent, name, out, NULL);
}
int ks_overlay_lookup(struct ks_fs *f, stqr_object_id parent, const char *name, stqr_object_id *out,
		      struct ks_directory_proof *proof)
{
	struct ks_overlay_state *s;
	int err = ov_state(f, &s);
	if (err)
		return err;
	err = ov_child_proven(f, s, parent, name, out, proof);
	ov_put(s);
	return err;
}

static int ov_dir(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id id,
		  struct ov_dir **out)
{
	struct ov_dir *d = ov_find(s, id);
	int err;
	struct ks_snapshot base = {0};
	stqr_id temporary = {{0}}, anchor = {{0}};
	bool pinned = false;
	struct buf attrs = {0};
	if (d) {
		if (d->node.removed)
			return -ENOENT;
		*out = d;
		return 0;
	}
	if (f->lower->references) {
		err = stqr_request_id(&temporary);
		if (!err)
			err = ks_object_reference(f->lower, id, temporary, 0);
		if (err)
			return err;
		pinned = true;
	}
	ks_cache_forget(f->lower, id);
	err = ks_plain_snapshot(f->lower, id, &base);
	if (err)
		goto fail;
	if (base.node.kind != 2 || base.node.removed) {
		err = -ENOTDIR;
		goto fail;
	}
	d = sq_alloc(sizeof(*d));
	if (!d) {
		err = -ENOMEM;
		goto fail;
	}
	d->id = id;
	d->lower = id;
	if (pinned) {
		err = anchor_reserve(f, id, id, &anchor);
		if (!err)
			err = ks_object_reference(f->lower, id, anchor, 4);
		if (!err)
			err = ov_lower_reference_attr(&base.node, anchor, &attrs);
	}
	else
		err = 0;
	if (!err)
		err = ov_node(d, &base.node);
	if (pinned)
		d->metadata = true;
	ks_snapshot_free(&base);
	if (!err)
		err = ov_add_dir(s, d);
	if (err) {
		drop(&d->image);
		sq_free(d);
		goto fail;
	}
	if (pinned)
		ks_object_reference(f->lower, id, temporary, 1);
	drop(&attrs);
	*out = d;
	return 0;
fail:
	ks_snapshot_free(&base);
	drop(&attrs);
	if (pinned)
		ks_object_reference(f->lower, id, temporary, 1);
	return err;
}
static int ov_inode_match(struct inode *, void *);
/* The VFS already indexes every live inode by the complete stable object ID.
 * Do not walk the mount list under inode_lock: an invalidation of N cached
 * inodes would otherwise perform N full list searches and block unrelated
 * inode insertion/eviction for the duration of each search. ilookup5 also
 * retains the inode and waits for I_NEW before its metadata is inspected. */
static struct inode *ov_inode_get(struct ks_fs *f, stqr_object_id id)
{
	unsigned long hash = 0;
	if (!f->sb)
		return NULL;
	for (unsigned k = 0; k < 32; k++)
		hash = hash * 31 + id.bytes[k];
	return ilookup5(f->sb, hash, ov_inode_match, id.bytes);
}
static bool ov_absent(struct ks_fs *f, stqr_object_id id, stqr_proof proof)
{
	struct inode *inode;
	bool absent = false;
	if (!proof.cursor || zero(proof.subscription.bytes, 16))
		return false;
	inode = ov_inode_get(f, id);
	if (inode) {
		mutex_lock(&KS_I(inode)->metadata_lock);
		absent = !memcmp(&KS_I(inode)->upper_absent, &proof, sizeof(proof));
		mutex_unlock(&KS_I(inode)->metadata_lock);
		iput(inode);
	}
	return absent;
}

static int ov_snapshot(struct ks_fs *f, struct ks_overlay_state *state, stqr_object_id id,
		       struct ks_snapshot *out)
{
	struct ov_dir *d = ov_find(state, id);
	struct ks_snapshot base = {0};
	stqr_info lower_info = {0};
	struct ks_directory_proof directory_proof = {0};
	struct ks_entry *entries = NULL;
	size_t n = 0;
	struct ks_node node;
	struct buf data = {0}, raw = {0};
	int err;
	memset(out, 0, sizeof(*out));
	out->id = id;
	if (!d) {
		err = (!ov_upper_ref(state, id) && ov_absent(f, id, state->info.proof))
			  ? -ENOENT
			  : ks_plain_snapshot(f, id, out);
		if (!err)
			return out->node.kind == 2 ? -EUCLEAN : 0;
		if (err != -ENOENT)
			return err;
		if (ov_upper_ref(state, id))
			return -ENOENT;
		err = ks_plain_snapshot(f->lower, id, &base);
		if (err)
			return err;
		if (base.node.kind != 2) {
			*out = base;
			out->node.inherited = true;
			out->node.upper_absent = state->info.proof;

			return 0;
		}
	} else if (!d->opaque && !d->metadata && !d->node.removed) {
		err = ks_plain_snapshot(f->lower, d->lower, &base);
		if (err)
			return err;
	}
	node = d ? d->node : base.node;
	if (base.raw)
		node = base.node;
	if (d && d->node.removed) {
		ks_snapshot_free(&base);
		return -ENOENT;
	}
	err = ov_entries_info(f, state, id, &entries, &n, &lower_info);
	if (err)
		goto done;
	put(&data, "STDR\1\0", 6);
	number(&data, n, 8);
	for (size_t i = 0; i < n; i++) {
		blob(&data, entries[i].name, strlen(entries[i].name));
		put(&data, entries[i].id.bytes, 32);
	}
	// A virtual directory is materialized for the VFS, never persisted as STFS.
	encode_node(&raw, &node);
	if (raw.err || data.err) {
		err = -ENOMEM;
		goto done;
	}
	size_t attrs_offset = raw.n;
	put(&raw, data.p, data.n);
	out->raw = raw.p;
	out->raw_len = raw.n;
	raw.p = NULL;
	err = decode_node(out->raw, attrs_offset, &out->node);

	if (!err) {
		out->node.kind = 2;
		out->node.size = data.n;
		out->node.depth = 0;
		out->node.inline_data = (u8 *)out->raw + attrs_offset;
		out->node.lower = false;
		out->node.inherited = false;
		if (!d)
			out->node.upper_absent = state->info.proof;
		out->info = state->info;
		out->info.length = data.n;
		directory_proof.has_lower = !d || !d->opaque;
		directory_proof.lower_id = d ? d->lower : id;
		directory_proof.valid =
		    ks_cache_stamp(f, ks_root_id, &state->info, &directory_proof.root) &&
		    (!directory_proof.has_lower ||
		     ks_cache_stamp(f->lower, directory_proof.lower_id, &lower_info,
				    &directory_proof.lower));
		if (directory_proof.valid) {
			out->directory_proof =
			    kmemdup(&directory_proof, sizeof(directory_proof), GFP_NOFS);
			out->node.directory_proof = out->directory_proof;
		}
		out->namespace_head = kmemdup(state->root_head.p, state->root_head.n, GFP_NOFS);
		if (!out->namespace_head)
			err = -ENOMEM;
		else
			out->namespace_head_len = state->root_head.n;
	}
done:
	ks_snapshot_free(&base);
	sq_free(entries);
	drop(&data);
	drop(&raw);
	if (err)
		ks_snapshot_free(out);
	return err;
}
/* Invalidators may consume a live cached snapshot, but must never fetch or
 * register subscriptions. Virtual directories and missing namespace proofs are
 * left for foreground resolution. Keep the same upper/absence rules as normal
 * overlay reads, including open-but-unlinked upper objects. */
static int ks_overlay_snapshot_cached(struct ks_fs *f, stqr_object_id id,
				     struct ks_snapshot *out)
{
	struct ks_snapshot root = {0};
	struct ks_overlay_state *state = NULL;
	int err = -ENODATA;
	memset(out, 0, sizeof(*out));
	if (ks_cache_peek(f, ks_root_id, &root.raw, &root.raw_len, &root.info))
		return err;
	mutex_lock(&f->overlay_cache_lock);
	state = f->overlay_state;
	if (state && state->root_head.n == root.raw_len &&
	    !memcmp(state->root_head.p, root.raw, root.raw_len) &&
	    !memcmp(state->info.runtime.bytes, root.info.runtime.bytes, 16))
		refcount_inc(&state->refs);
	else
		state = NULL;
	mutex_unlock(&f->overlay_cache_lock);
	if (!state || ov_find(state, id))
		goto done;
	if (!ov_upper_ref(state, id) && ov_absent(f, id, root.info.proof)) {
		err = ks_plain_snapshot_cached(f->lower, id, out);
		if (!err) {
			out->node.inherited = true;
			out->node.upper_absent = root.info.proof;
		}
	} else
		err = ks_plain_snapshot_cached(f, id, out);
	if (!err && out->node.kind == 2)
		err = -ENODATA;
	if (err)
		ks_snapshot_free(out);
done:
	ov_put(state);
	ks_snapshot_free(&root);
	return err;
}
int ks_overlay_snapshot(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *out)
{
	struct ks_overlay_state *s;
	int err = ov_state(f, &s);
	if (err)
		return err;
	err = ov_snapshot(f, s, id, out);
	ov_put(s);
	return err;
}
static bool ov_allowed(struct ks_overlay_state *s, const char *path, bool dir)
{
	if (!s->policies)
		return true;
	for (unsigned i = 0; i < s->policies; i++) {
		const char *r = s->policy[i];
		size_t n = strlen(r);
		if (r[n - 1] == '/') {
			if ((dir && strlen(path) == n - 1 && !memcmp(path, r, n - 1)) ||
			    !strncmp(path, r, n))
				return true;
		} else if (!dir && !strcmp(path, r))
			return true;
	}
	return false;
}
static int ov_path_id(struct ks_fs *f, struct ks_overlay_state *s, const char *path,
		      stqr_object_id *out)
{
	stqr_object_id id = ks_root_id;
	const char *at = path;
	int err;
	while (*at) {
		const char *end = strchr(at, '/');
		size_t n = end ? (size_t)(end - at) : strlen(at);
		char name[256];
		if (!n || n > 255)
			return -EINVAL;
		memcpy(name, at, n);
		name[n] = 0;
		err = ov_child(f, s, id, name, &id);
		if (err)
			return err;
		if (!end)
			break;
		at = end + 1;
	}
	*out = id;
	return 0;
}
static int ov_inode_match(struct inode *i, void *id)
{
	return !memcmp(KS_I(i)->id.bytes, id, 32);
}
static char *ov_vfs_path(struct ks_fs *f, stqr_object_id id)
{
	struct inode *i;
	struct dentry *d;
	char *buf, *name, *out = NULL;
	i = ov_inode_get(f, id);
	if (!i)
		return NULL;
	d = d_find_alias(i);
	iput(i);
	if (!d)
		return NULL;
	buf = sq_alloc(4097);
	if (!buf) {
		dput(d);
		return NULL;
	}
	name = dentry_path_raw(d, buf, 4097);
	dput(d);
	if (!IS_ERR(name) && name[0] == '/') {
		size_t n = strlen(name + 1) + 1;
		out = sq_alloc(n);
		if (out)
			memcpy(out, name + 1, n);
	}
	sq_free(buf);
	return out;
}
static int ov_walk_path(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id root,
			stqr_object_id target, char *path, unsigned unused, size_t *budget)
{
	struct frame {
		stqr_object_id id;
		struct ks_entry *entries;
		size_t count, at, path_length;
	};
	struct frame *stack = sq_alloc(1025 * sizeof(*stack));
	size_t level = 0;
	int err = -ENOENT;
	(void)unused;
	if (!stack)
		return -ENOMEM;
	stack[0].id = root;
	for (;;) {
		struct frame *frame = &stack[level];
		if (!frame->entries) {
			if (!*budget) {
				err = -E2BIG;
				break;
			}
			(*budget)--;
			err = ov_entries(f, s, frame->id, &frame->entries, &frame->count);
			if (err)
				break;
		}
		if (frame->at == frame->count) {
			sq_free(frame->entries);
			frame->entries = NULL;
			if (!level) {
				err = -ENOENT;
				break;
			}
			level--;
			path[stack[level].path_length] = 0;
			continue;
		}
		struct ks_entry *entry = &frame->entries[frame->at++];
		size_t len = frame->path_length, z = strlen(entry->name);
		if (len + !!len + z > 4096) {
			err = -ENAMETOOLONG;
			break;
		}
		if (len)
			path[len] = '/';
		strcpy(path + len + !!len, entry->name);
		if (eqid(entry->id, target)) {
			err = 0;
			break;
		}
		struct ks_snapshot node = {0};
		err = ov_snapshot(f, s, entry->id, &node);
		if (err)
			break;
		bool dir = node.node.kind == 2;
		ks_snapshot_free(&node);
		if (dir) {
			if (level == 1024) {
				err = -ELOOP;
				break;
			}
			for (size_t k = 0; k <= level; k++)
				if (eqid(stack[k].id, entry->id)) {
					err = -ELOOP;
					break;
				}
			if (err)
				break;
			level++;
			stack[level].id = entry->id;
			stack[level].path_length = strlen(path);
			stack[level].at = stack[level].count = 0;
		} else
			path[len] = 0;
	}
	for (size_t k = 0; k <= level; k++)
		sq_free(stack[k].entries);
	sq_free(stack);
	return err;
}
static int ov_path(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id id, char **out)
{
	char *path;
	stqr_object_id found;
	int err;
	if (eqid(id, ks_root_id)) {
		path = sq_alloc(1);
		if (!path)
			return -ENOMEM;
		*out = path;
		return 0;
	}
	path = ov_vfs_path(f, id);
	if (path) {
		err = ov_path_id(f, s, path, &found);
		if (!err && eqid(found, id)) {
			*out = path;
			return 0;
		}
		sq_free(path);
	}
	path = sq_alloc(4097);
	if (!path)
		return -ENOMEM;
	size_t budget = 1000000;
	err = ov_walk_path(f, s, ks_root_id, id, path, 0, &budget);
	if (err) {
		sq_free(path);
		return err;
	}
	*out = path;
	return 0;
}
static int ov_writable(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id id, char **out)
{
	struct ks_snapshot node = {0};
	char *path = NULL;
	int err = ov_snapshot(f, s, id, &node);
	if (err)
		return err;
	err = ov_path(f, s, id, &path);
	if (err == -ENOENT && node.node.kind != 2 && node.node.write_path_len) {
		path = sq_alloc(node.node.write_path_len + 1);
		if (!path)
			err = -ENOMEM;
		else {
			memcpy(path, node.node.write_path, node.node.write_path_len);
			err = 0;
		}
	}
	if (!err && !ov_allowed(s, path, node.node.kind == 2))
		err = -EROFS;
	ks_snapshot_free(&node);
	if (err)
		sq_free(path);
	else
		*out = path;
	return err;
}
int ks_overlay_check_write(struct ks_fs *f, stqr_object_id id)
{
	struct ks_overlay_state *s;
	char *path;
	int err;
	if (f->readonly)
		return -EROFS;
	if (!f->lower)
		return 0;
	err = ov_state(f, &s);
	if (err)
		return err;
	err = ov_writable(f, s, id, &path);
	if (!err)
		sq_free(path);
	ov_put(s);
	return err;
}
static int ov_destination(struct ks_fs *f, struct ks_overlay_state *s, stqr_object_id parent,
			  const char *name, bool dir, char **out)
{
	char *base, *path;
	struct ks_entry *entries;
	size_t n;
	int err;
	if (!valid_name(name))
		return -EINVAL;
	err = ov_entries(f, s, parent, &entries, &n);
	if (err)
		return err;
	sq_free(entries);
	err = ov_path(f, s, parent, &base);
	if (err)
		return err;
	size_t len = strlen(base), z = strlen(name);
	if (len + !!len + z > 4096) {
		sq_free(base);
		return -ENAMETOOLONG;
	}
	path = sq_alloc(len + !!len + z + 1);
	if (!path) {
		sq_free(base);
		return -ENOMEM;
	}
	memcpy(path, base, len);
	if (len)
		path[len++] = '/';
	strcpy(path + len, name);
	sq_free(base);
	if (!ov_allowed(s, path, dir)) {
		sq_free(path);
		return -EROFS;
	}
	*out = path;
	return 0;
}
static int ov_lower_reference_attr(struct ks_node *node, stqr_id token, struct buf *attrs)
{
	static const char name[] = "trusted.stqr.lower-reference";
	struct buf old = {.p = (u8 *)node->xattrs, .n = node->xattrs_size};
	unsigned count = old.p ? readnum(&old, 4) : 0;
	bool exists = false, inserted = false;
	for (unsigned i = 0; i < count; i++) {
		size_t kn, vn;
		const u8 *key = readblob(&old, &kn);
		readblob(&old, &vn);
		if (kn == sizeof(name) - 1 && !memcmp(key, name, kn))
			exists = true;
	}
	if (old.err || (!exists && count >= 128))
		return old.err ? old.err : -ENOSPC;
	number(attrs, count + !exists, 4);
	old.pos = old.p ? 4 : 0;
	for (unsigned i = 0; i < count; i++) {
		size_t kn, vn;
		const u8 *key = readblob(&old, &kn), *value = readblob(&old, &vn);
		int cmp = memcmp(name, key, min_t(size_t, sizeof(name) - 1, kn));
		if (!cmp)
			cmp = (sizeof(name) - 1 > kn) - (sizeof(name) - 1 < kn);
		if (!inserted && cmp <= 0) {
			blob(attrs, name, sizeof(name) - 1);
			blob(attrs, token.bytes, 16);
			inserted = true;
		}
		if (cmp) {
			blob(attrs, key, kn);
			blob(attrs, value, vn);
		}
	}
	if (!inserted) {
		blob(attrs, name, sizeof(name) - 1);
		blob(attrs, token.bytes, 16);
	}
	if (attrs->err)
		return attrs->err;
	node->xattrs = attrs->p;
	node->xattrs_size = attrs->n;
	return 0;
}
static int ov_copy_inode(struct ks_fs *f, stqr_object_id id, const char *path, bool data_write)
{
	struct ks_snapshot *images = sq_alloc(2 * sizeof(*images));
	struct ks_snapshot *upper = images, *base = images ? images + 1 : NULL;
	struct ks_node n;
	struct buf raw = {0}, attrs = {0};
	stqr_id anchor = {{0}}, temporary = {{0}};
	bool pinned = false;
	int err;
	if (!images)
		return -ENOMEM;
	err = ks_plain_snapshot(f, id, upper);
	if (!err) {
		ks_snapshot_free(upper);
		sq_free(images);
		return 0;
	}
	if (err != -ENOENT) {
		sq_free(images);
		return err;
	}
	if (f->lower->references) {
		err = stqr_request_id(&temporary);
		if (!err)
			err = ks_object_reference(f->lower, id, temporary, 0);
		if (err) {
			sq_free(images);
			return err;
		}
		pinned = true;
	}
	ks_cache_forget(f->lower, id);
	err = ks_plain_snapshot(f->lower, id, base);
	if (err)
		goto done;
	n = base->node;
	if (n.kind == 2 || (n.kind == 1 && n.links > 1) || (data_write && n.kind != 1)) {
		err = -EOPNOTSUPP;
		goto done;
	}
	n.write_path = (u8 *)path;
	n.write_path_len = strlen(path);
	if (n.kind == 1 && n.depth)
		n.lower = true;
	if (n.lower) {
		n.lower_owner = id;
		err = lower_fingerprint(&base->node, n.lower_digest);
		if (err)
			goto done;
		if (pinned) {
			err = anchor_reserve(f, id, id, &anchor);
			if (!err)
				err = ks_object_reference(f->lower, id, anchor, 4);
			if (!err)
				err = ov_lower_reference_attr(&n, anchor, &attrs);
			if (err)
				goto done;
		}
	}
	encode_node(&raw, &n);
	err = raw.err ? raw.err : cas_object(f, id, raw.p, raw.n, NULL, 0, true);
	if (err == -EAGAIN)
		err = 0;
done:
	/* An inherited token may have been copied into bucket COW snapshots.
	 * Keep published anchor pins until graph-aware offline recovery. */
	if (pinned)
		ks_object_reference(f->lower, id, temporary, 1);
	drop(&raw);
	drop(&attrs);
	ks_snapshot_free(base);
	ks_snapshot_free(upper);
	sq_free(images);
	return err;
}
static int ov_mark_orphan(struct ks_fs *f, struct ks_overlay_state *s,
			  stqr_object_id id, bool virtual_directory)
{
	struct ks_snapshot *old;
	struct ks_node next;
	int err;
	if (!f->references)
		return 0;
	if (virtual_directory) {
		s->orphan = true;
		s->orphan_id = id;
		return 0;
	}
	old = sq_alloc(sizeof(*old));
	if (!old)
		return -ENOMEM;
	ks_cache_forget(f, id);
	err = ks_plain_snapshot(f, id, old);
	if (err == -ENOENT)
		err = 0; /* lower-only whiteouts own no upper payload */
	else if (!err) {
		next = old->node;
		next.links = 0;
		next.ctime = ktime_get_real_ns();
		err = member_set(&s->orphan_member, old, &next);
		if (!err) {
			s->orphan = s->orphan_has_inode = true;
			s->orphan_id = id;
		}
	}
	ks_snapshot_free(old);
	sq_free(old);
	return err;
}
static int ks_overlay_prune_orphan(struct ks_fs *f, stqr_object_id id)
{
	int err = -EAGAIN;
	mutex_lock(&f->overlay_lock);
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_overlay_state *s = NULL;
		err = ov_edit(f, &s);
		if (err)
			break;
		for (size_t i = 0; i < s->count; i++) {
			struct ov_dir *d = s->dirs[i];
			if (!eqid(d->id, id) || !d->node.removed)
				continue;
			/* Only the specifically claimed, removed virtual directory is
			 * pruned. Keep parent whiteouts and all other namespace entries. */
			drop(&d->image);
			sq_free(d->entries);
			sq_free(d);
			memmove(s->dirs + i, s->dirs + i + 1,
				(s->count - i - 1) * sizeof(*s->dirs));
			s->count--;
			err = ov_publish(f, s);
			break;
		}
		ov_put(s);
		if (err != -EAGAIN)
			break;
	}
	mutex_unlock(&f->overlay_lock);
	return err;
}
int ks_overlay_copyup(struct ks_fs *f, stqr_object_id id)
{
	int err = -EAGAIN;
	mutex_lock(&f->overlay_lock);
	// The common path only mutates an already published upper inode. It must
	// neither rewrite STOM nor clone the whole namespace for each data write.
	struct ks_overlay_state *visible = NULL;
	if (!ov_state(f, &visible)) {
		char *path = NULL;
		struct ks_snapshot upper = {0};
		int checked = ov_writable(f, visible, id, &path);
		if (!checked && !ks_plain_snapshot(f, id, &upper)) {
			stqr_object_id mapped;
			int found = ov_path_id(f, visible, path, &mapped);
			if (ov_upper_ref(visible, id) || found == -ENOENT ||
			    (!found && !eqid(mapped, id))) {
				ks_snapshot_free(&upper);
				sq_free(path);
				ov_put(visible);
				mutex_unlock(&f->overlay_lock);
				return 0;
			}
		}
		ks_snapshot_free(&upper);
		sq_free(path);
		ov_put(visible);
		if (checked) {
			mutex_unlock(&f->overlay_lock);
			return checked;
		}
	}

	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_overlay_state *s = NULL;
		struct ov_dir *d;
		char *path = NULL, *slash, *parent_path;
		stqr_object_id parent, here;
		err = ov_edit(f, &s);
		if (err)
			break;
		err = ov_writable(f, s, id, &path);
		if (err)
			goto end;
		err = ov_copy_inode(f, id, path, true);
		if (err)
			goto end;
		slash = strrchr(path, '/');
		parent_path = sq_alloc(strlen(path) + 1);
		if (!parent_path) {
			err = -ENOMEM;
			goto end;
		}
		if (slash)
			memcpy(parent_path, path, slash - path);
		err = ov_path_id(f, s, parent_path, &parent);
		sq_free(parent_path);
		if (err == -ENOENT) {
			err = 0;
			goto end;
		}
		if (err)
			goto end;
		err = ov_child(f, s, parent, slash ? slash + 1 : path, &here);
		if (err == -ENOENT) {
			err = 0;
			goto end;
		}
		if (err)
			goto end;
		if (!eqid(here, id)) {
			err = 0;
			goto end;
		}
		d = ov_find(s, parent);
		if (d) {
			long at = find_entry(d->entries, d->count, slash ? slash + 1 : path);
			if (at >= 0 && eqid(d->entries[at].id, id)) {
				err = 0;
				goto end;
			}
		}
		err = ov_dir(f, s, parent, &d);
		if (!err)
			err = ov_entry(d, slash ? slash + 1 : path, id);
		if (!err)
			err = ov_publish(f, s);
	end:
		sq_free(path);
		ov_put(s);
		if (err != -EAGAIN)
			break;
	}
	mutex_unlock(&f->overlay_lock);
	return err;
}
static int ov_touch(struct ks_fs *f, struct ov_dir *d)
{
	struct ks_snapshot base = {0};
	struct ks_node n = d->node;
	if (!d->opaque && !d->metadata) {
		int err = ks_plain_snapshot(f->lower, d->lower, &base);
		if (err)
			return err;
		n = base.node;
	}
	n.mtime = n.ctime = ktime_get_real_ns();
	d->metadata = true;
	int err = ov_node(d, &n);
	ks_snapshot_free(&base);
	return err;
}
int ks_overlay_newnode(struct ks_fs *f, stqr_object_id parent, const char *name, unsigned mode,
		       u32 uid, u32 gid, unsigned kind, const char *target, stqr_object_id *created)
{
	int err = -EAGAIN;
	stqr_object_id id = {{0}};
	if (f->readonly)
		return -EROFS;
	if (kind < 1 || kind > 5 || (kind == 3 && (!target || !*target || strlen(target) > 4095)))
		return -EINVAL;
	mutex_lock(&f->overlay_lock);
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_overlay_state *s = NULL;
		struct ov_dir *d;
		struct ks_node node = {.kind = kind,
				       .mode = mode & 07777,
				       .uid = uid,
				       .gid = gid,
				       .links = kind == 2 ? 2 : 1};
		char *path = NULL;
		stqr_object_id old;
		struct buf raw = {0};
		err = ov_edit(f, &s);
		if (err)
			break;
		err = ov_destination(f, s, parent, name, kind == 2, &path);
		if (err)
			goto end;
		err = ov_child(f, s, parent, name, &old);
		if (!err) {
			err = -EEXIST;
			goto end;
		}
		if (err != -ENOENT)
			goto end;
		err = 0;
		node.atime = node.mtime = node.ctime = ktime_get_real_ns();
		if (kind == 2) {
			struct ov_dir *child = sq_alloc(sizeof(*child));
			if (!child) {
				err = -ENOMEM;
				goto end;
			}
			sq_random(id.bytes, 32);
			child->id = id;
			child->opaque = child->metadata = true;
			err = ov_node(child, &node);
			if (!err)
				err = ov_add_dir(s, child);
			if (err) {
				drop(&child->image);
				sq_free(child);
				goto end;
			}
		} else if (nullid(id)) {
			node.write_path = (u8 *)path;
			node.write_path_len = strlen(path);
			if (kind == 3) {
				node.inline_data = (u8 *)target;
				node.size = strlen(target);
			}
			encode_node(&raw, &node);
			err = raw.err ? raw.err : immutable(f, raw.p, raw.n, &id);
			if (err)
				goto end;
		}
		err = ov_dir(f, s, parent, &d);
		if (!err)
			err = ov_entry(d, name, id);
		if (!err)
			err = ov_touch(f, d);
		if (!err)
			err = ov_publish(f, s);
	end:
		drop(&raw);
		sq_free(path);
		ov_put(s);
		if (err != -EAGAIN)
			break;
	}
	mutex_unlock(&f->overlay_lock);
	if (!err)
		*created = id;
	return err;
}
int ks_overlay_remove(struct ks_fs *f, stqr_object_id parent, const char *name, bool dir)
{
	int err = -EAGAIN;
	if (f->readonly)
		return -EROFS;
	if (!valid_name(name))
		return -EINVAL;
	mutex_lock(&f->overlay_lock);
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_overlay_state *s = NULL;
		struct ov_dir *d;
		struct ks_snapshot child = {0};
		stqr_object_id id;
		char *path = NULL;
		struct ks_entry *entries = NULL;
		size_t n;
		err = ov_edit(f, &s);
		if (err)
			break;
		err = ov_child(f, s, parent, name, &id);
		if (err)
			goto end;
		err = ov_snapshot(f, s, id, &child);
		if (err)
			goto end;
		if ((child.node.kind == 2) != dir) {
			err = dir ? -ENOTDIR : -EISDIR;
			goto end;
		}
		err = ov_destination(f, s, parent, name, dir, &path);
		if (err)
			goto end;
		if (dir) {
			err = ov_entries(f, s, id, &entries, &n);
			if (err)
				goto end;
			if (n) {
				err = -ENOTEMPTY;
				goto end;
			}
			err = ov_dir(f, s, id, &d);
			if (err)
				goto end;
			struct ks_node removed = d->node;
			removed.removed = true;
			d->metadata = true;
			err = ov_node(d, &removed);
			if (err)
				goto end;
		}
		err = ov_dir(f, s, parent, &d);
		if (!err)
			err = ov_entry(d, name, (stqr_object_id){{0}});
		if (!err)
			err = ov_touch(f, d);
		if (!err)
			err = ov_mark_orphan(f, s, id, dir);
		if (!err)
			err = ov_publish(f, s);
	end:
		ks_snapshot_free(&child);
		sq_free(path);
		sq_free(entries);
		ov_put(s);
		if (err != -EAGAIN)
			break;
	}
	mutex_unlock(&f->overlay_lock);
	return err;
}
static bool ov_below(const char *path, const char *parent)
{
	size_t n = strlen(parent);
	return !strncmp(path, parent, n) && path[n] == '/';
}
int ks_overlay_rename(struct ks_fs *f, stqr_object_id from, const char *name, stqr_object_id to,
		      const char *newname, unsigned flags)
{
	int err = -EAGAIN;
	if (f->readonly)
		return -EROFS;
	if (!valid_name(name) || !valid_name(newname))
		return -EINVAL;
	if (flags & ~(RENAME_EXCHANGE | RENAME_NOREPLACE) ||
	    flags == (RENAME_EXCHANGE | RENAME_NOREPLACE))
		return -EINVAL;
	mutex_lock(&f->overlay_lock);
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_overlay_state *s = NULL;
		struct ks_snapshot source = {0}, dest = {0};
		stqr_object_id id, other = {{0}};
		char *oldpath = NULL, *newpath = NULL, *back = NULL;
		bool exists;
		struct ov_dir *left, *right, *d;
		struct ks_entry *entries = NULL;
		size_t n;
		err = ov_edit(f, &s);
		if (err)
			break;
		err = ov_child(f, s, from, name, &id);
		if (err)
			goto end;
		err = ov_snapshot(f, s, id, &source);
		if (err)
			goto end;
		err = ov_destination(f, s, from, name, source.node.kind == 2, &oldpath);
		if (err)
			goto end;
		err = ov_destination(f, s, to, newname, source.node.kind == 2, &newpath);
		if (err)
			goto end;
		err = ov_child(f, s, to, newname, &other);
		exists = !err;
		if (err && err != -ENOENT)
			goto end;
		err = 0;
		if ((flags & RENAME_NOREPLACE) && exists) {
			err = -EEXIST;
			goto end;
		}
		if ((flags & RENAME_EXCHANGE) && !exists) {
			err = -ENOENT;
			goto end;
		}
		if (eqid(from, to) && !strcmp(name, newname))
			goto end;
		if (source.node.kind == 2 && ov_below(newpath, oldpath)) {
			err = -EINVAL;
			goto end;
		}
		if (exists) {
			err = ov_snapshot(f, s, other, &dest);
			if (err)
				goto end;
			if (flags & RENAME_EXCHANGE) {
				err = ov_destination(f, s, from, name, dest.node.kind == 2, &back);
				if (err)
					goto end;
				if (dest.node.kind == 2 && ov_below(oldpath, newpath)) {
					err = -EINVAL;
					goto end;
				}
			} else {
				if ((source.node.kind == 2) != (dest.node.kind == 2)) {
					err = source.node.kind == 2 ? -ENOTDIR : -EISDIR;
					goto end;
				}
				if (dest.node.kind == 2) {
					err = ov_entries(f, s, other, &entries, &n);
					if (err)
						goto end;
					if (n) {
						err = -ENOTEMPTY;
						goto end;
					}
				}
			}
		}
		if (source.node.kind == 2)
			err = ov_dir(f, s, id, &d);
		else
			err = ov_copy_inode(f, id, oldpath, false);
		if (err)
			goto end;
		if (exists) {
			if (dest.node.kind == 2)
				err = ov_dir(f, s, other, &d);
			else
				err = ov_copy_inode(f, other, newpath, false);
			if (err)
				goto end;
		}
		err = ov_dir(f, s, from, &left);
		if (!err)
			err = ov_dir(f, s, to, &right);
		if (!err)
			err = ov_entry(left, name,
				       (flags & RENAME_EXCHANGE) ? other : (stqr_object_id){{0}});
		if (!err)
			err = ov_entry(right, newname, id);
		if (!err && exists && !(flags & RENAME_EXCHANGE) && dest.node.kind == 2) {
			d = ov_find(s, other);
			struct ks_node removed = d->node;
			removed.removed = true;
			d->metadata = true;
			err = ov_node(d, &removed);
		}
		if (!err && exists && !(flags & RENAME_EXCHANGE))
			err = ov_mark_orphan(f, s, other, dest.node.kind == 2);
		if (!err)
			err = ov_touch(f, left);
		if (!err && left != right)
			err = ov_touch(f, right);
		if (!err)
			err = ov_publish(f, s);
	end:
		ks_snapshot_free(&source);
		ks_snapshot_free(&dest);
		sq_free(oldpath);
		sq_free(newpath);
		sq_free(back);
		sq_free(entries);
		ov_put(s);
		if (err != -EAGAIN)
			break;
	}
	mutex_unlock(&f->overlay_lock);
	return err;
}
static int ks_overlay_node_cas(struct ks_fs *f, const struct ks_snapshot *old,
			       const struct ks_node *next)
{
	struct ks_overlay_state *s;
	struct ov_dir *d;
	char *path = NULL;
	int err;
	mutex_lock(&f->overlay_lock);
	err = ov_edit(f, &s);
	if (err)
		goto done;

	err = ov_writable(f, s, old->id, &path);
	if (!err)
		err = ov_dir(f, s, old->id, &d);
	if (!err && (s->root_head.n != old->namespace_head_len ||
		     memcmp(s->root_head.p, old->namespace_head, old->namespace_head_len)))
		err = -EAGAIN;
	if (!err) {
		d->metadata = true;
		err = ov_node(d, next);
	}
	if (!err)
		err = ov_publish(f, s);
	sq_free(path);
	ov_put(s);
done:
	mutex_unlock(&f->overlay_lock);
	return err;
}
int ks_overlay_setattr(struct ks_fs *f, stqr_object_id id, struct iattr *attr)
{
	struct ks_snapshot s;
	int err = ks_overlay_check_write(f, id);
	if (err)
		return err;
	err = ks_overlay_snapshot(f, id, &s);
	if (err)
		return err;
	bool dir = s.node.kind == 2;
	ks_snapshot_free(&s);
	if (!dir) {
		err = ks_overlay_copyup(f, id);
		if (err)
			return err;
	}
	return ks_plain_setattr(f, id, attr);
}
int ks_overlay_setxattr(struct ks_fs *f, stqr_object_id id, const char *name, const void *value,
			size_t size, int flags)
{
	struct ks_snapshot s;
	int err = ks_overlay_check_write(f, id);
	if (err)
		return err;
	err = ks_overlay_snapshot(f, id, &s);
	if (err)
		return err;
	bool dir = s.node.kind == 2;
	ks_snapshot_free(&s);
	if (!dir) {
		err = ks_overlay_copyup(f, id);
		if (err)
			return err;
	}
	return ks_plain_setxattr(f, id, name, value, size, flags);
}
static int ov_lower_kind(struct ks_fs *f, const char *path, unsigned *kind)
{
	stqr_object_id id = ks_root_id;
	const char *at = path;
	int err = 0;
	while (*at) {
		struct ks_snapshot dir = {0};
		struct ks_entry *entries = NULL;
		size_t count;
		char name[256];
		const char *end = strchr(at, '/');
		size_t n = end ? (size_t)(end - at) : strlen(at);
		if (n > 255 || !n)
			return -EINVAL;
		memcpy(name, at, n);
		name[n] = 0;
		err = ks_plain_snapshot(f->lower, id, &dir);
		if (err)
			return err;
		err = ks_directory(f->lower, &dir.node, &entries, &count);
		ks_snapshot_free(&dir);
		if (err)
			return err;
		long found = find_entry(entries, count, name);
		if (found < 0) {
			sq_free(entries);
			return -ENOENT;
		}
		id = entries[found].id;
		sq_free(entries);
		if (!end)
			break;
		at = end + 1;
	}
	struct ks_snapshot node = {0};
	err = ks_plain_snapshot(f->lower, id, &node);
	if (!err) {
		*kind = node.node.kind;
		ks_snapshot_free(&node);
	}
	return err;
}
static int ov_policy(struct ks_fs *f, struct ks_overlay_state *s, const char *whitelist)
{
	int err = 0;
	if (whitelist && *whitelist) {
		char *text = kstrdup(whitelist, GFP_NOFS), *cursor = text, *part;
		if (!text) {
			err = -ENOMEM;
			return err;
		}
		unsigned total = 1;
		for (const char *p = whitelist; *p; p++)
			if (*p == ',')
				total++;
		if (total > 4096) {
			kfree(text);
			err = -E2BIG;
			return err;
		}
		s->policy = sq_alloc(total * sizeof(char *));
		if (!s->policy) {
			kfree(text);
			err = -ENOMEM;
			return err;
		}
		while ((part = strsep(&cursor, ","))) {
			if (!ov_path_valid(part, true)) {
				err = -EINVAL;
				break;
			}
			s->policy[s->policies] = sq_alloc(strlen(part) + 1);
			if (!s->policy[s->policies]) {
				err = -ENOMEM;
				break;
			}
			strcpy(s->policy[s->policies++], part);
		}
		kfree(text);
		if (err)
			return err;
		// Policy normalization is shared with the Go adapter: existing directory
		// names without a trailing slash become directory rules.
		for (unsigned i = 0; i < s->policies; i++) {
			size_t n = strlen(s->policy[i]);
			unsigned kind;
			if (s->policy[i][n - 1] == '/')
				continue;
			int found = ov_lower_kind(f, s->policy[i], &kind);
			if (found && found != -ENOENT)
				return found;
			if (!found) {
				if (kind == 2) {
					char *p = sq_alloc(n + 2);
					if (!p) {
						err = -ENOMEM;
						return err;
					}
					memcpy(p, s->policy[i], n);
					p[n] = '/';
					sq_free(s->policy[i]);
					s->policy[i] = p;
				}
			}
		}
		// Canonical lexical order and duplicates are checked by the decoder too.
		for (unsigned i = 0; i < s->policies; i++)
			for (unsigned j = i + 1; j < s->policies; j++)
				if (strcmp(s->policy[i], s->policy[j]) > 0) {
					char *tmp = s->policy[i];
					s->policy[i] = s->policy[j];
					s->policy[j] = tmp;
				}
		unsigned keep = 0;
		for (unsigned i = 0; i < s->policies; i++) {
			if (keep && !strcmp(s->policy[keep - 1], s->policy[i]))
				sq_free(s->policy[i]);
			else
				s->policy[keep++] = s->policy[i];
		}
		s->policies = keep;
	}
	return 0;
}
int ks_overlay_initialize(struct ks_fs *f, const char *whitelist)
{
	struct ks_overlay_state *existing = NULL, *s;
	struct ks_snapshot root = {0};
	struct ov_dir *dir;
	int err = ov_state(f, &existing);
	if (!err) {
		ov_put(existing);
		return 0;
	}
	if (err != -ENOENT)
		return err;
	if (f->readonly)
		return -EROFS;
	s = sq_alloc(sizeof(*s));
	if (!s)
		return -ENOMEM;
	refcount_set(&s->refs, 1);
	err = ks_plain_snapshot(f->lower, ks_root_id, &root);
	if (err)
		goto done;
	if (root.node.kind != 2) {
		err = -ENOTDIR;
		goto done;
	}
	dir = sq_alloc(sizeof(*dir));
	if (!dir) {
		err = -ENOMEM;
		goto done;
	}
	dir->id = dir->lower = ks_root_id;
	err = ov_node(dir, &root.node);
	if (!err)
		err = ov_add_dir(s, dir);
	if (err) {
		drop(&dir->image);
		sq_free(dir);
		goto done;
	}
	err = ov_policy(f, s, whitelist);
	if (err)
		goto done;
	err = ov_publish(f, s);
done:
	ks_snapshot_free(&root);
	ov_put(s);
	return err;
}

int ks_overlay_validate_policy(struct ks_fs *f, const char *text)
{
	struct ks_overlay_state *s = NULL, *requested;
	int err;
	if (!text)
		return 0;
	err = ov_state(f, &s);
	if (err)
		return err;
	requested = sq_alloc(sizeof(*requested));
	if (!requested) {
		ov_put(s);
		return -ENOMEM;
	}
	refcount_set(&requested->refs, 1);
	err = ov_policy(f, requested, text);
	if (!err) {
		if (requested->policies != s->policies)
			err = -EINVAL;
		else
			for (unsigned i = 0; i < s->policies; i++)
				if (strcmp(requested->policy[i], s->policy[i])) {
					err = -EINVAL;
					break;
				}
	}
	ov_put(s);
	ov_put(requested);
	return err;
}
