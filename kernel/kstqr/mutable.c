// SPDX-License-Identifier: GPL-3.0-only
/* Mutable data/index pages: same addresses and redo format as the Go library.
 */
static bool segments_have(const struct segments *parts, stqr_object_id id)
{
	size_t i;
	for (i = 0; i < parts->n; i++)
		if (eqid(parts->p[i].id, id))
			return true;
	return false;
}
static bool owns_data_block(const struct ks_node *node, const struct segment *part,
			    u64 position)
{
	stqr_object_id owner;
	u64 base, unit = SQ_CHUNK;
	if (part->lower || nullid(part->id) || part->offset > position)
		return false;
	base = position - part->offset;
	if (!(base % unit) && !stable_id(node->identity, 0, 0, base / unit, &owner) &&
	    eqid(owner, part->id))
		return true;
	if (!(base % 4096) && !stable_id(node->identity, 0, 0, base / 4096, &owner) &&
	    eqid(owner, part->id))
		return true;
	/* Go materializes legacy lower slices into a separately derived owner
	 * namespace. Random/shared legacy IDs are never freed online. */
	return !(base % 4096) && !stable_id(node->identity, 2, 0, base / 4096, &owner) &&
	       eqid(owner, part->id);
}
static bool index_kept(const struct ks_node *node, size_t parts, stqr_object_id id)
{
	unsigned level = 0;
	size_t count = parts, slot;
	if (!node->depth)
		return false;
	do {
		count = (count + 1023) / 1024;
		level++;
		for (slot = 0; slot < count; slot++) {
			stqr_object_id want;
			if (!stable_id(node->identity, 1, level, slot, &want) && eqid(want, id))
				return true;
		}
	} while (count > 1 && level < 16);
	return false;
}
static int collect_index_ids(struct ks_fs *f, stqr_object_id id, unsigned depth,
			     struct segments *ids)
{
	struct buf b = {0};
	stqr_info info;
	struct segment mark = {.id = id, .length = 1};
	unsigned version, count, i;
	int err;
	if (!depth || nullid(id))
		return 0;
	err = segment_add(ids, mark);
	if (err || depth == 1)
		return err;
	err = get_object(f, id, (void **)&b.p, &b.n, &info);
	if (err)
		return err;
	if (b.n < 10 || memcmp(b.p, "STIX", 4)) {
		drop(&b);
		return -EUCLEAN;
	}
	take(&b, 4);
	version = readnum(&b, 2);
	count = readnum(&b, 4);
	if (version < 1 || version > 3 || !count || count > 1024) {
		drop(&b);
		return -EUCLEAN;
	}
	for (i = 0; i < count; i++) {
		stqr_object_id child;
		const u8 *p;
		bool lower = false;
		readnum(&b, 8);
		p = take(&b, 32);
		if (!p) {
			err = -EUCLEAN;
			break;
		}
		memcpy(child.bytes, p, 32);
		readnum(&b, version == 1 ? 4 : 8);
		if (version == 2)
			readnum(&b, 8);
		if (version == 3)
			lower = boolean(&b);
		if (b.err) {
			err = b.err;
			break;
		}
		if (!lower && !nullid(child)) {
			err = collect_index_ids(f, child, depth - 1, ids);
			if (err)
				break;
		}
	}
	drop(&b);
	return err;
}
static int retire_pages(struct ks_fs *f, const struct ks_node *old, struct ks_node *next,
			struct segments *parts)
{
	struct segments prior = {0}, indexes = {0};
	size_t i;
	u64 position = 0;
	int err = 0;
	if (old->lower)
		return 0;
	if (old->depth)
		err = index_segments(f, old->root, old->size, old->depth, &prior);
	if (!err)
		err = collect_index_ids(f, old->root, old->depth, &indexes);
	for (i = 0; !err && i < prior.n; i++) {
		if (owns_data_block(old, &prior.p[i], position) &&
		    !segments_have(parts, prior.p[i].id)) {
			size_t j;
			bool seen = false;
			for (j = 0; j < i; j++)
				if (eqid(prior.p[j].id, prior.p[i].id))
					seen = true;
			if (!seen)
				err = stage_block(f, next, prior.p[i].id, 0, NULL, 0, false, true);
		}
		position += prior.p[i].length;
	}
	for (i = 0; !err && i < indexes.n; i++)
		if (index_kept(old, prior.n, indexes.p[i].id) &&
		    !index_kept(next, parts->n, indexes.p[i].id))
			err = stage_block(f, next, indexes.p[i].id, 0, NULL, 0, false, true);
	sq_free(prior.p);
	sq_free(indexes.p);
	return err;
}
static int prepare_mutable_write(struct ks_fs *f, struct ks_snapshot *old, struct ks_node *next,
				 u64 off, const void *data, size_t count)
{
	struct segments before = {0}, after = {0};
	u64 end = off + count, length = max(old->node.size, end), first = off / SQ_CHUNK * SQ_CHUNK,
	    stop = (end - 1) / SQ_CHUNK * SQ_CHUNK, at;
	int err = 0;
	if (old->node.depth) {
		err = index_segments(old->node.lower ? f->lower : f, old->node.root, old->node.size,
				     old->node.depth, &before);
		if (!err && old->node.lower)
			for (size_t j = 0; j < before.n; j++)
				if (!nullid(before.p[j].id))
					before.p[j].lower = true;
	}
	if (!err)
		err = slice_segments(&after, &before, 0, min(first, old->node.size));
	if (!err && !old->node.depth && old->node.size && first) {
		struct segment prefix = {.length = old->node.size};
		err = stable_id(next->identity, 0, 0, 0, &prefix.id);
		if (!err)
			err = stage_block(f, next, prefix.id, 0, old->node.inline_data,
					  old->node.size, true, false);
		if (!err)
			err = segment_add(&after, prefix);
	}
	if (!err && first > old->node.size) {
		struct segment gap = {.length = first - old->node.size};
		err = segment_add(&after, gap);
	}
	for (at = first; !err && at <= stop; at += SQ_CHUNK) {
		struct segment part = {.length = min_t(u64, SQ_CHUNK, length - at)};
		u64 a = max(off, at), z = min(end, at + part.length), pos = 0;
		bool canonical = false;
		size_t i;
		err = stable_id(next->identity, 0, 0, at / SQ_CHUNK, &part.id);
		if (err)
			break;
		for (i = 0; i < before.n; i++) {
			if (!before.p[i].lower && pos == at && !before.p[i].offset &&
			    eqid(before.p[i].id, part.id)) {
				canonical = true;
				break;
			}
			pos += before.p[i].length;
		}
		if (canonical)
			err = stage_block(f, next, part.id, a - at, (const u8 *)data + (a - off),
					  z - a, false, false);
		else {
			u8 *body = sq_alloc(part.length);
			size_t n =
			    min_t(u64, part.length, old->node.size - min(at, old->node.size));
			if (!body) {
				err = -ENOMEM;
				break;
			}
			if (n)
				err = ks_read(f, &old->node, body, n, at);
			if (!err) {
				memcpy(body + (a - at), (const u8 *)data + (a - off), z - a);
				err = stage_block(f, next, part.id, 0, body, part.length, true,
						  false);
			}
			sq_free(body);
		}
		if (!err)
			err = segment_add(&after, part);
	}
	if (!err && stop + SQ_CHUNK < old->node.size)
		err = slice_segments(&after, &before, stop + SQ_CHUNK, old->node.size);
	// build_segments consumes its input, so retain a small reference-only copy
	// for retirement decisions before publication.
	if (!err) {
		struct segments keep = {0};
		size_t i;
		for (i = 0; i < after.n && !err; i++)
			err = segment_add(&keep, after.p[i]);
		if (!err)
			err = build_segments(f, next, &after);
		if (!err)
			err = retire_pages(f, &old->node, next, &keep);
		sq_free(keep.p);
	}
	sq_free(before.p);
	sq_free(after.p);
	return err;
}
static int prepare_mutable_resize(struct ks_fs *f, struct ks_snapshot *old, struct ks_node *next,
				  u64 size)
{
	struct segments before = {0}, after = {0}, keep = {0};
	int err = 0;
	size_t i;
	if (old->node.depth) {
		err = index_segments(old->node.lower ? f->lower : f, old->node.root, old->node.size,
				     old->node.depth, &before);
		if (!err && old->node.lower)
			for (size_t j = 0; j < before.n; j++)
				if (!nullid(before.p[j].id))
					before.p[j].lower = true;
	} else if (old->node.size) {
		struct segment part = {.length = old->node.size};
		err = stable_id(next->identity, 0, 0, 0, &part.id);
		if (!err)
			err = stage_block(f, next, part.id, 0, old->node.inline_data,
					  old->node.size, true, false);
		if (!err)
			err = segment_add(&before, part);
	}
	if (!err)
		err = slice_segments(&after, &before, 0, min(size, old->node.size));
	if (!err && size < old->node.size && after.n) {
		struct segment *last = &after.p[after.n - 1];
		u64 at = 0;
		stqr_object_id want;
		for (i = 0; i + 1 < after.n; i++)
			at += after.p[i].length;
		err = stable_id(next->identity, 0, 0, at / SQ_CHUNK, &want);
		if (!err && !last->lower && !last->offset && eqid(last->id, want)) {
			u8 *body = sq_alloc(last->length);
			if (!body)
				err = -ENOMEM;
			else {
				err = ks_read(f, &old->node, body, last->length, at);
				if (!err)
					err = stage_block(f, next, last->id, 0, body, last->length,
							  true, false);
				sq_free(body);
			}
		}
	}
	if (!err && size > old->node.size) {
		struct segment gap = {.length = size - old->node.size};
		err = segment_add(&after, gap);
	}
	for (i = 0; !err && i < after.n; i++)
		err = segment_add(&keep, after.p[i]);
	if (!err)
		err = build_segments(f, next, &after);
	if (!err)
		err = retire_pages(f, &old->node, next, &keep);
	sq_free(before.p);
	sq_free(after.p);
	sq_free(keep.p);
	return err;
}
