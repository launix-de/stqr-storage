// SPDX-License-Identifier: GPL-3.0-only
/* Uses the exact SDK codec and opaque object operations, never server formats. */
#include "block.h"
int ks_bd_claim(stqr_client *c, bool readonly) { return stqr_object_lease(c, readonly); }
static int ks_bd_decode(const void *raw, size_t n, stqr_object_id address, struct ks_bd_meta *m)
{
	struct buf b = {.p = (void *)raw, .n = n}, key = {0};
	const u8 *p;
	u8 hash[32];
	size_t len;
	int err;
	p = take(&b, 8);
	if (!p || memcmp(p, "STNBD001", 8) || readnum(&b, 2) != 1 || readnum(&b, 4) != KS_BD_STRIPE)
		return -EPROTO;
	m->size = readnum(&b, 8);
	p = take(&b, 16);
	if (p)
		memcpy(m->id.bytes, p, 16);
	p = readblob(&b, &len);
	if (!p || !len || len > 255 || memchr(p, 0, len) || memchr(p, '\n', len) ||
	    memchr(p, '\r', len))
		return -EPROTO;
	memcpy(m->name, p, len);
	m->name[len] = 0;
	for (unsigned j = 0; j < 3; j++) {
		p = readblob(&b, &len);
		if (!p || len > 255 || memchr(p, 0, len) || memchr(p, '\r', len) ||
		    memchr(p, '\n', len))
			return -EPROTO;
	}
	if (done(&b) || !m->size || m->size > S64_MAX || m->size % 512 || zero(m->id.bytes, 16))
		return -EPROTO;
	put(&key, "STQR NBD metadata v1\0", sizeof("STQR NBD metadata v1\0") - 1);
	put(&key, m->name, strlen(m->name));
	err = key.err ? key.err : sq_hash(256, key.p, key.n, hash);
	drop(&key);
	if (!err && (!zero(address.bytes, 8) || memcmp(address.bytes + 8, hash, 24)))
		err = -EPROTO;
	return err;
}
int ks_bd_inventory(stqr_client *c, stqr_object_id *after, bool *more, struct ks_bd_meta *entries,
		    unsigned *count)
{
	struct buf b = {0}, r = {0};
	stqr_id rid = {{0}};
	unsigned n;
	int err;
	*count = 0;
	reference(c, &b, (stqr_object_id){{0}});
	number(&b, *more, 1);
	if (*more)
		put(&b, after->bytes, 32);
	number(&b, 32, 4);
	err = request(c, 0x117, &b, rid, 0, &r, NULL);
	if (err)
		goto out;
	n = readnum(&r, 4);
	if (n > 32) {
		err = -EPROTO;
		goto out;
	}
	for (unsigned j = 0; j < n && !err; j++) {
		stqr_object_id id;
		stqr_info info;
		const u8 *p = take(&r, 32);
		void *raw;
		size_t length;
		if (!p) {
			err = -EPROTO;
			break;
		}
		memcpy(id.bytes, p, 32);
		info_decode(&r, &info);
		if (r.err) {
			err = r.err;
			break;
		}
		if ((*more || j) && memcmp(id.bytes, after->bytes, 32) <= 0) {
			err = -EPROTO;
			break;
		}
		*after = id;
		/* Descriptors occupy only the zero prefix. Do not scan stripe payload. */
		if (!zero(id.bytes, 8))
			continue;
		if (info.length > 2048) {
			err = -EPROTO;
			break;
		}
		err = stqr_get(c, id, &raw, &length, &info);
		if (!err) {
			err = ks_bd_decode(raw, length, id, entries + *count);
			stqr_free(raw);
		}
		if (!err)
			(*count)++;
	}
	if (!err) {
		bool next = boolean(&r);
		const u8 *p = next ? take(&r, 32) : NULL;
		if (next && (!p || memcmp(p, after->bytes, 32) < 0))
			err = -EPROTO;
		if (!err)
			err = done(&r);
		if (!err) {
			if (next)
				memcpy(after->bytes, p, 32);
			*more = next && zero(after->bytes, 8);
		}
	}
out:
	drop(&b);
	drop(&r);
	return err;
}
int ks_bd_address(stqr_id device, u64 index, stqr_object_id *id)
{
	u8 raw[sizeof("STQR NBD stripe v1\0") - 1 + 16 + 8], hash[32];
	size_t at = sizeof("STQR NBD stripe v1\0") - 1;
	memcpy(raw, "STQR NBD stripe v1\0", at);
	memcpy(raw + at, device.bytes, 16);
	at += 16;
	for (unsigned j = 0; j < 8; j++)
		raw[at + j] = index >> (56 - j * 8);
	int err = sq_hash(256, raw, sizeof(raw), hash);
	if (err)
		return err;
	memcpy(id->bytes, hash, 16);
	if (zero(id->bytes, 8))
		id->bytes[0] = 1;
	memcpy(id->bytes + 16, device.bytes, 8);
	memcpy(id->bytes + 24, raw + at, 8);
	return 0;
}
