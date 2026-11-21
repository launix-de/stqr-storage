// SPDX-License-Identifier: GPL-3.0-only
/* Included after core.c: reuse its checked canonical codec, not another ABI. */
#include "kstqr.h"
#include <linux/ktime.h>
#include <linux/overflow.h>
#include <linux/sort.h>
const stqr_object_id ks_root_id = {{0x3f, 0xda, 0xbc, 0x50, 0xba, 0xc2, 0xa0, 0xe8,
				    0x3a, 0x0a, 0x4b, 0x32, 0x46, 0xca, 0x79, 0xbe,
				    0x13, 0x7a, 0x00, 0x19, 0x40, 0x39, 0x44, 0x2a,
				    0x8d, 0x27, 0x51, 0x1e, 0x3b, 0x14, 0x21, 0xbe}};
struct segment {
	u64 length, offset;
	stqr_object_id id;
};
struct segments {
	struct segment *p;
	size_t n, cap;
};
struct member {
	stqr_object_id id;
	u64 version;
	struct buf before, after;
};
static bool eqid(stqr_object_id a, stqr_object_id b) { return !memcmp(a.bytes, b.bytes, 32); }
static bool nullid(stqr_object_id id) { return zero(id.bytes, 32); }
static int get_object(struct ks_fs *f, stqr_object_id id, void **p, size_t *n, stqr_info *info)
{
	int err = stqr_get(f->client, id, p, n, info);
	if (err == -ENOTCONN || err == -ECONNRESET || err == -EPIPE || err == -ETIMEDOUT ||
	    (err == -EAGAIN && !stqr_status(f->client))) {
		err = stqr_reconnect(f->client);
		if (!err)
			err = stqr_get(f->client, id, p, n, info);
	}
	return err;
}
static int cas_object(struct ks_fs *f, stqr_object_id id, const void *p, size_t n, u64 version,
		      bool absent)
{
	stqr_options o = {0};
	stqr_info info;
	int err;
	if (f->readonly)
		return -EROFS;
	o.has_version = !absent;
	o.version = version;
	o.absent = absent;
	err = stqr_request_id(&o.request_id);
	if (err)
		return err;
	err = stqr_cas(f->client, id, 1, 0, NULL, 0, p, n, &o, &info);
	if (err == -EINPROGRESS) {
		if (!stqr_reconnect(f->client))
			err = stqr_cas(f->client, id, 1, 0, NULL, 0, p, n, &o, &info);
		if (err == -EINPROGRESS)
			err = -EIO;
	}
	return err;
}
static int immutable(struct ks_fs *f, const void *p, size_t n, stqr_object_id *id)
{
	if (n > SQ_CHUNK)
		return -E2BIG;
	sq_random(id->bytes, 32);
	return cas_object(f, *id, p, n, 0, true);
}
static int decode_node(const void *p, size_t n, struct ks_node *node)
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
	if (version < 1 || version > 4)
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
	} else
		return -EOPNOTSUPP; /* Overlay roots/content require a lower connection. */
	if (version == 2) {
		node->removed = boolean(&d);
		if (!node->removed)
			return -EUCLEAN;
	}
	if (version >= 3) {
		node->removed = boolean(&d);
		readblob(&d, &length);
		if (length)
			return -EOPNOTSUPP;
	}
	if (version == 4) {
		node->atime = readnum(&d, 8);
		node->ctime = readnum(&d, 8);
	} else
		node->atime = node->ctime = node->mtime;
	if (done(&d) || (node->kind != 1 && node->kind != 2) ||
	    (node->removed && node->kind != 2) || (node->mode & ~07777U) || node->size > S64_MAX ||
	    node->atime > S64_MAX || node->mtime > S64_MAX || node->ctime > S64_MAX)
		return -EUCLEAN;
	return 0;
}
static void encode_node(struct buf *b, const struct ks_node *node)
{
	put(b, "STFS", 4);
	number(b, 4, 2);
	number(b, node->kind, 1);
	number(b, node->mode, 4);
	number(b, node->uid, 4);
	number(b, node->gid, 4);
	number(b, node->mtime, 8);
	number(b, node->size, 8);
	number(b, node->depth ? 1 : 0, 1);
	if (node->depth) {
		number(b, node->depth, 1);
		put(b, node->root.bytes, 32);
	} else
		blob(b, node->inline_data, node->size);
	number(b, node->removed, 1);
	blob(b, NULL, 0);
	number(b, node->atime, 8);
	number(b, node->ctime, 8);
}
void ks_snapshot_free(struct ks_snapshot *s)
{
	sq_free(s->raw);
	memset(s, 0, sizeof(*s));
}
static bool marker(const void *raw, size_t n, stqr_object_id *id)
{
	if (n != 38 || memcmp(raw, "STFL\1\0", 6))
		return false;
	memcpy(id->bytes, (const u8 *)raw + 6, 32);
	return !nullid(*id);
}
static int tx_decode(struct buf *raw, stqr_object_id txid, struct member **out, unsigned *count,
		     unsigned *state)
{
	struct buf d = {.p = raw->p, .n = raw->n};
	struct member *m;
	unsigned i;
	const u8 *p;
	size_t n;
	if (raw->n < 11 || raw->n > SQ_CHUNK || memcmp(raw->p, "STFT\1\0", 6))
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
		m[i].version = readnum(&d, 8);
		p = readblob(&d, &n);
		m[i].before.p = (u8 *)p;
		m[i].before.n = n;
		if (d.err || decode_node(p, n, &check))
			goto bad;
		p = readblob(&d, &n);
		m[i].after.p = (u8 *)p;
		m[i].after.n = n;
		if (d.err || decode_node(p, n, &check) || !m[i].version || nullid(m[i].id) ||
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
static int help_tx(struct ks_fs *f, stqr_object_id first)
{
	stqr_object_id *stack = sq_alloc(128 * sizeof(*stack));
	unsigned depth = 0, rounds = 0;
	int err = 0, first_result = -EAGAIN;
	if (!stack)
		return -ENOMEM;
	stack[0] = first;
	while (rounds++ < 8192) {
		struct buf raw = {0};
		stqr_info info;
		struct member *m = NULL;
		unsigned count = 0, state, i;
		int decision = 1;
		u8 mark[38] = "STFL\1\0";
		stqr_object_id id = stack[depth];
		memcpy(mark + 6, id.bytes, 32);
		err = get_object(f, id, (void **)&raw.p, &raw.n, &info);
		if (err)
			break;
		err = tx_decode(&raw, id, &m, &count, &state);
		if (err) {
			drop(&raw);
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
					err = cas_object(f, m[i].id, image->p, image->n, ci.version,
							 false);
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
				if (ci.version != m[i].version || head.n != m[i].before.n ||
				    memcmp(head.p, m[i].before.p, head.n)) {
					decision = 2;
					drop(&head);
					break;
				}
				err = cas_object(f, m[i].id, mark, sizeof(mark), ci.version, false);
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
			if (!depth)
				first_result = state == 1 ? 0 : -EAGAIN;
			else
				depth--;
		} else if (!err && !state && decision > 0) {
			raw.p[6] = decision;
			err = cas_object(f, id, raw.p, raw.n, info.version, false);
			if (err == -EAGAIN)
				err = 0;
		}
		sq_free(m);
		drop(&raw);
		if (err || (state && decision > 0 && eqid(id, first)))
			break;
		cond_resched();
	}
	sq_free(stack);
	return err ? err : first_result;
}
int ks_snapshot(struct ks_fs *f, stqr_object_id id, struct ks_snapshot *s)
{
	unsigned attempt;
	int err;
	memset(s, 0, sizeof(*s));
	s->id = id;
	for (attempt = 0; attempt < 128; attempt++) {
		stqr_object_id txid;
		err = get_object(f, id, &s->raw, &s->raw_len, &s->info);
		if (err)
			return err;
		if (!marker(s->raw, s->raw_len, &txid)) {
			err = decode_node(s->raw, s->raw_len, &s->node);
			if (err)
				ks_snapshot_free(s);
			return err;
		}
		if (f->readonly) {
			struct buf raw = {0};
			stqr_info info;
			struct member *m = NULL;
			unsigned n, state, i;
			struct buf selected = {0};
			err = get_object(f, txid, (void **)&raw.p, &raw.n, &info);
			if (!err)
				err = tx_decode(&raw, txid, &m, &n, &state);
			if (!err) {
				err = -EUCLEAN;
				for (i = 0; i < n; i++)
					if (eqid(m[i].id, id)) {
						struct buf *image =
						    state == 1 ? &m[i].after : &m[i].before;
						put(&selected, image->p, image->n);
						err = selected.err;
						break;
					}
			}
			sq_free(m);
			drop(&raw);
			sq_free(s->raw);
			s->raw = selected.p;
			s->raw_len = selected.n;
			if (!err)
				err = decode_node(s->raw, s->raw_len, &s->node);
			if (err)
				ks_snapshot_free(s);
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
static int index_segments(struct ks_fs *f, stqr_object_id id, u64 length, unsigned depth,
			  struct segments *out)
{
	struct buf b = {0};
	stqr_info info;
	unsigned version, count, i;
	u64 total = 0;
	int err;
	err = get_object(f, id, (void **)&b.p, &b.n, &info);
	if (err)
		return err;
	if (b.n < 10 || b.n > SQ_CHUNK || memcmp(b.p, "STIX", 4)) {
		err = -EUCLEAN;
		goto out;
	}
	take(&b, 4);
	version = readnum(&b, 2);
	count = readnum(&b, 4);
	if ((version != 1 && version != 2) || !count || count > 1024) {
		err = -EUCLEAN;
		goto out;
	}
	for (i = 0; i < count; i++) {
		struct segment s = {0};
		const u8 *p;
		u64 lower = 0;
		s.length = readnum(&b, 8);
		p = take(&b, 32);
		if (p)
			memcpy(s.id.bytes, p, 32);
		s.offset = readnum(&b, version == 1 ? 4 : 8);
		if (version == 2)
			lower = readnum(&b, 8);
		if (b.err || !s.length || total > length || s.length > length - total ||
		    s.offset > S64_MAX || s.length > S64_MAX - s.offset ||
		    (depth > 1 && (s.offset || lower)) || (nullid(s.id) && (s.offset || lower)) ||
		    (depth == 1 && !nullid(s.id) && !lower && s.offset + s.length > SQ_CHUNK)) {
			err = -EUCLEAN;
			goto out;
		}
		if (lower) {
			err = -EOPNOTSUPP;
			goto out;
		}
		total += s.length;
		if (depth > 1 && !nullid(s.id))
			err = index_segments(f, s.id, s.length, depth - 1, out);
		else
			err = segment_add(out, s);
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
static int segments(struct ks_fs *f, const struct ks_node *n, struct segments *s)
{
	struct segment part = {.length = n->size};
	int err;
	if (!n->size)
		return 0;
	if (n->depth)
		return index_segments(f, n->root, n->size, n->depth, s);
	err = immutable(f, n->inline_data, n->size, &part.id);
	return err ? err : segment_add(s, part);
}
static int index_read(struct ks_fs *f, stqr_object_id id, u64 length, unsigned depth, void *dst,
		      size_t count, u64 offset)
{
	struct segments s = {0};
	size_t i;
	int err = index_segments(f, id, length, depth, &s);
	if (err)
		goto out;
	for (i = 0; i < s.n && count; i++) {
		size_t n;
		struct segment *p = &s.p[i];
		if (offset >= p->length) {
			offset -= p->length;
			continue;
		}
		n = min_t(u64, count, p->length - offset);
		if (nullid(p->id))
			memset(dst, 0, n);
		else {
			void *data = NULL;
			size_t actual;
			stqr_info info;
			err = stqr_get_range(f->client, p->id, p->offset + offset, n, &data,
					     &actual, &info);
			if (!err && actual != n)
				err = -EUCLEAN;
			if (!err)
				memcpy(dst, data, n);
			sq_free(data);
			if (err)
				goto out;
		}
		dst = (u8 *)dst + n;
		count -= n;
		offset = 0;
	}
	if (count)
		err = -EUCLEAN;
out:
	sq_free(s.p);
	return err;
}
int ks_read(struct ks_fs *f, const struct ks_node *n, void *dst, size_t count, u64 offset)
{
	if (offset > n->size || count > n->size - offset)
		return -EINVAL;
	if (!count)
		return 0;
	if (!n->depth) {
		memcpy(dst, n->inline_data + offset, count);
		return 0;
	}
	return index_read(f, n->root, n->size, n->depth, dst, count, offset);
}
static int build_segments(struct ks_fs *f, struct ks_node *node, struct segments *s)
{
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
			put(&b, "STIX", 4);
			number(&b, 2, 2);
			number(&b, count, 4);
			for (j = i; j < i + count; j++) {
				number(&b, s->p[j].length, 8);
				put(&b, s->p[j].id.bytes, 32);
				number(&b, s->p[j].offset, 8);
				number(&b, 0, 8);
				part.length += s->p[j].length;
			}
			err = b.err ? b.err : immutable(f, b.p, b.n, &part.id);
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
	struct segments s = {0};
	size_t pos;
	int err = 0;
	if (n <= 4096) {
		node->size = n;
		node->depth = 0;
		node->inline_data = data;
		memset(&node->root, 0, 32);
		return 0;
	}
	for (pos = 0; pos < n; pos += SQ_CHUNK) {
		struct segment part = {.length = min_t(size_t, n - pos, SQ_CHUNK)};
		err = immutable(f, (const u8 *)data + pos, part.length, &part.id);
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
static int node_cas(struct ks_fs *f, struct ks_snapshot *old, const struct ks_node *next)
{
	struct buf b = {0};
	int err;
	encode_node(&b, next);
	err = b.err ? b.err : cas_object(f, old->id, b.p, b.n, old->info.version, false);
	drop(&b);
	return err;
}
int ks_write(struct ks_fs *f, stqr_object_id id, u64 *position, const void *data, size_t count,
	     bool append)
{
	unsigned attempt;
	int err;
	if (f->readonly)
		return -EROFS;
	for (attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot old;
		struct ks_node next;
		struct segments before = {0}, after = {0};
		u64 off, end;
		u8 *small = NULL;
		err = ks_snapshot(f, id, &old);
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
			struct segment part = {.length = count};
			err = segments(f, &old.node, &before);
			if (!err)
				err = slice_segments(&after, &before, 0, min(off, old.node.size));
			if (!err && off > old.node.size) {
				struct segment gap = {.length = off - old.node.size};
				err = segment_add(&after, gap);
			}
			if (!err && count) {
				err = immutable(f, data, count, &part.id);
				if (!err)
					err = segment_add(&after, part);
			}
			if (!err && end < old.node.size)
				err = slice_segments(&after, &before, end, old.node.size);
			if (!err)
				err = build_segments(f, &next, &after);
		}
		next.mtime = next.ctime = ktime_get_real_ns();
		if (!err)
			err = node_cas(f, &old, &next);
		ks_snapshot_free(&old);
		sq_free(small);
		sq_free(before.p);
		sq_free(after.p);
		if (err != -EAGAIN) {
			if (!err)
				*position = end;
			return err;
		}
		cond_resched();
	}
	return -EAGAIN;
}
int ks_setattr(struct ks_fs *f, stqr_object_id id, struct iattr *attr)
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
			} else {
				err = segments(f, &old.node, &in);
				if (!err)
					err = slice_segments(&out, &in, 0, min(size, next.size));
				if (!err && size > next.size) {
					struct segment gap = {.length = size - next.size};
					err = segment_add(&out, gap);
				}
				if (!err)
					err = build_segments(f, &next, &out);
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
		ks_snapshot_free(&old);
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
static long find_entry(struct ks_entry *e, size_t n, const char *name)
{
	size_t i;
	for (i = 0; i < n; i++)
		if (!strcmp(e[i].name, name))
			return i;
	return -1;
}
static int entry_cmp(const void *a, const void *b)
{
	return strcmp(((const struct ks_entry *)a)->name, ((const struct ks_entry *)b)->name);
}
static int directory_image(struct ks_fs *f, struct ks_node *node, struct ks_entry *e, size_t n,
			   struct buf *content)
{
	size_t i;
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
	return blob_bytes(f, node, content->p, content->n);
}
int ks_initialize(struct ks_fs *f, u32 uid, u32 gid)
{
	struct ks_node node = {.kind = 2, .mode = 0755, .uid = uid, .gid = gid};
	struct buf content = {0}, raw = {0};
	int err;
	node.atime = node.mtime = node.ctime = ktime_get_real_ns();
	put(&content, "STDR\1\0", 6);
	number(&content, 0, 8);
	node.size = content.n;
	node.inline_data = content.p;
	encode_node(&raw, &node);
	err = raw.err ? raw.err : cas_object(f, ks_root_id, raw.p, raw.n, 0, true);
	drop(&content);
	drop(&raw);
	return err;
}
int ks_create(struct ks_fs *f, stqr_object_id parent, const char *name, unsigned mode, u32 uid,
	      u32 gid, bool dir, stqr_object_id *created)
{
	unsigned attempt;
	int err;
	bool made = false;
	if (f->readonly)
		return -EROFS;
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
		if (!err && find_entry(entries, n, name) >= 0)
			err = -EEXIST;
		if (!err && !made) {
			struct ks_node child = {
			    .kind = dir ? 2 : 1, .mode = mode & 07777, .uid = uid, .gid = gid};
			struct buf raw = {0}, child_content = {0};
			child.mtime = child.atime = child.ctime = ktime_get_real_ns();
			if (dir) {
				put(&child_content, "STDR\1\0", 6);
				number(&child_content, 0, 8);
				child.inline_data = child_content.p;
				child.size = child_content.n;
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
}
static int member_set(struct member *m, struct ks_snapshot *s, const struct ks_node *after)
{
	m->id = s->id;
	m->version = s->info.version;
	put(&m->before, s->raw, s->raw_len);
	encode_node(&m->after, after);
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
	int err;
	if (!count || count > 1024)
		return -E2BIG;
	sort(m, count, sizeof(*m), member_cmp, NULL);
	put(&b, "STFT\1\0", 6);
	number(&b, 0, 1);
	number(&b, count, 4);
	for (i = 0; i < count; i++) {
		if (i && eqid(m[i - 1].id, m[i].id)) {
			drop(&b);
			return -EINVAL;
		}
		put(&b, m[i].id.bytes, 32);
		number(&b, m[i].version, 8);
		blob(&b, m[i].before.p, m[i].before.n);
		blob(&b, m[i].after.p, m[i].after.n);
	}
	err = b.err ? b.err : immutable(f, b.p, b.n, &id);
	drop(&b);
	return err ? err : help_tx(f, id);
}
int ks_remove(struct ks_fs *f, stqr_object_id parent, const char *name, bool dir)
{
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
		err = ks_snapshot(f, parent, &old);
		if (err)
			return err;
		next = old.node;
		err = ks_directory(f, &old.node, &entries, &n);
		at = err ? -1 : find_entry(entries, n, name);
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
		if (!err && dir) {
			struct ks_node removed = child.node;
			removed.removed = true;
			members = sq_alloc(2 * sizeof(*members));
			if (!members)
				err = -ENOMEM;
			else {
				err = member_set(&members[0], &old, &next);
				if (!err)
					err = member_set(&members[1], &child, &removed);
				if (!err)
					err = transact(f, members, 2);
			}
		} else if (!err)
			err = node_cas(f, &old, &next);
		if (members) {
			member_free(&members[0]);
			member_free(&members[1]);
			sq_free(members);
		}
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
		struct ks_node an, bn;
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
			ai = find_entry(ae, na, name);
			bi = find_entry(be, nb, newname);
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
			if (source->node.kind != dest->node.kind)
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
			if (!err && !(flags & RENAME_EXCHANGE) && dest->raw &&
			    dest->node.kind == 2) {
				struct ks_node removed = dest->node;
				removed.removed = true;
				err = member_set(&m[count++], dest, &removed);
			}
			if (!err && !same && source->node.kind == 2)
				err = guard_path(f, to, source->id, m, &count, to_path, to_length);
			if (!err && !same && (flags & RENAME_EXCHANGE) && dest->node.kind == 2)
				err = guard_path(f, from, dest->id, m, &count, from_path,
						 from_length);
			if (!err)
				err = transact(f, m, count);
		}
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
