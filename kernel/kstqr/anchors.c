// SPDX-License-Identifier: GPL-3.0-only
/* Durable canonical lower anchors; shared STLA ledger with the Go client. */
static int anchor_reserve(struct ks_fs *f, stqr_object_id owner, stqr_object_id target,
			  stqr_id *token)
{
	static const char domain[] = "STQR filesystem lower anchors v1";
	stqr_object_id root;
	int err = sq_hash(256, domain, sizeof(domain) - 1, root.bytes);
	if (err)
		return err;
	for (unsigned attempt = 0; attempt < 128; attempt++) {
		struct ks_snapshot *old = sq_alloc(sizeof(*old));
		struct buf body = {0}, nextbody = {0};
		struct member *member = sq_alloc(sizeof(*member));
		struct ks_node next;
		u8 key[64];
		u32 count = 0;
		size_t at = 0;
		if (!old || !member) {
			sq_free(old);
			sq_free(member);
			return -ENOMEM;
		}
		ks_cache_forget(f, root);
		err = ks_plain_snapshot(f, root, old);
		if (err == -ENOENT) {
			struct ks_node empty = {.kind = 1, .links = 1, .mode = 0600};
			struct buf raw = {0};
			empty.inline_data = (u8 *)"STLA\1\0\0\0\0\0";
			empty.size = 10;
			encode_node(&raw, &empty);
			err = raw.err ? raw.err : cas_object(f, root, raw.p, raw.n, NULL, 0, true);
			drop(&raw);
			ks_snapshot_free(old);
			sq_free(old);
			sq_free(member);
			if (!err || err == -EAGAIN)
				continue;
			return err;
		}
		if (err)
			goto out;
		if (old->node.kind != 1 || old->node.size < 10 || old->node.size > KS_DIR_LIMIT) {
			err = -EUCLEAN;
			goto out;
		}
		body.p = sq_alloc(old->node.size);
		if (!body.p) {
			err = -ENOMEM;
			goto out;
		}
		body.n = body.cap = old->node.size;
		err = ks_read(f, &old->node, body.p, body.n, 0);
		if (err)
			goto out;
		if (memcmp(take(&body, 4), "STLA", 4) || readnum(&body, 2) != 1) {
			err = -EUCLEAN;
			goto out;
		}
		count = readnum(&body, 4);
		if (body.err || (u64)count * 80 != body.n - 10) {
			err = -EUCLEAN;
			goto out;
		}
		for (u32 i = 0; i < count; i++) {
			u8 *row = body.p + 10 + i * 80;
			if (zero(row, 32) || zero(row + 32, 32) || zero(row + 64, 16) ||
			    (i && memcmp(row - 80, row, 64) >= 0)) {
				err = -EUCLEAN;
				goto out;
			}
		}
		memcpy(key, owner.bytes, 32);
		memcpy(key + 32, target.bytes, 32);
		for (; at < count && memcmp(body.p + 10 + at * 80, key, 64) < 0; at++)
			;
		if (at < count && !memcmp(body.p + 10 + at * 80, key, 64)) {
			memcpy(token->bytes, body.p + 10 + at * 80 + 64, 16);
			err = 0;
			goto out;
		}
		err = stqr_request_id(token);
		if (err)
			goto out;
		put(&nextbody, "STLA\1\0", 6);
		number(&nextbody, count + 1, 4);
		put(&nextbody, body.p + 10, at * 80);
		put(&nextbody, key, 64);
		put(&nextbody, token->bytes, 16);
		put(&nextbody, body.p + 10 + at * 80, (count - at) * 80);
		next = old->node;
		err = nextbody.err ? nextbody.err : nextbody.n > KS_DIR_LIMIT ? -E2BIG : 0;
		if (!err)
			err = blob_bytes(f, &next, nextbody.p, nextbody.n);
		if (!err)
			err = sq_hash(256, nextbody.p, nextbody.n, next.digest);
		next.mtime = next.ctime = ktime_get_real_ns();
		if (!err)
			err = member_set(member, old, &next);
		edits_free(next.edits);
		if (!err)
			err = transact(f, member, 1);
out:
		member_free(member);
		sq_free(member);
		drop(&body);
		drop(&nextbody);
		ks_snapshot_free(old);
		sq_free(old);
		if (err != -EAGAIN)
			return err;
	}
	return -EAGAIN;
}
