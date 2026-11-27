/* SPDX-License-Identifier: GPL-3.0-only */
/* Private helpers shared with the standalone algorithm regression. Cached STOM
 * snapshots are immutable and decode validates strict directory/name ordering.
 * Editing clones are NOT sorted after insertion: always use their old scans.
 * This optional RAM index proves membership only, never namespace visibility.
 * In particular it retains references from removed directories and aliases,
 * matching the previous scan. No extra payload, disk index or sync is involved. */
#define OV_REF_INDEX_MAX_BYTES (4U << 20)
static struct ov_dir *ov_find(struct ks_overlay_state *s, stqr_object_id id)
{
	if (s->immutable) {
		size_t lo = 0, hi = s->count;
		while (lo < hi) {
			size_t mid = lo + (hi - lo) / 2;
			int cmp = memcmp(s->dirs[mid]->id.bytes, id.bytes, 32);
			if (!cmp)
				return s->dirs[mid];
			if (cmp < 0)
				lo = mid + 1;
			else
				hi = mid;
		}
		return NULL;
	}
	for (size_t i = 0; i < s->count; i++)
		if (eqid(s->dirs[i]->id, id))
			return s->dirs[i];
	return NULL;
}
static long ov_entry_at(struct ks_overlay_state *s, struct ov_dir *d, const char *name)
{
	if (!s->immutable)
		return find_entry(d->entries, d->count, name);
	size_t lo = 0, hi = d->count;
	while (lo < hi) {
		size_t mid = lo + (hi - lo) / 2;
		int cmp = strcmp(d->entries[mid].name, name);
		if (!cmp)
			return mid;
		if (cmp < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return -1;
}
static int ov_ref_cmp(const void *a, const void *b)
{
	return memcmp(a, b, sizeof(stqr_object_id));
}
static void ov_index_snapshot(struct ks_overlay_state *s)
{
	size_t count = 0;
	/* Called exactly once, after COMPLETE decode validation and before sharing.
	 * Failure to allocate or excessive input simply keeps the existing scan. */
	s->immutable = true;
	for (size_t i = 0; i < s->count; i++)
		for (size_t j = 0; j < s->dirs[i]->count; j++)
			if (!nullid(s->dirs[i]->entries[j].id)) {
				if (count == OV_REF_INDEX_MAX_BYTES / sizeof(*s->upper_ids))
					return;
				count++;
			}
	if (!count) {
		s->upper_indexed = true;
		return;
	}
	s->upper_ids = sq_alloc(count * sizeof(*s->upper_ids));
	if (!s->upper_ids)
		return;
	for (size_t i = 0; i < s->count; i++)
		for (size_t j = 0; j < s->dirs[i]->count; j++)
			if (!nullid(s->dirs[i]->entries[j].id))
				s->upper_ids[s->upper_count++] = s->dirs[i]->entries[j].id;
	sort(s->upper_ids, s->upper_count, sizeof(*s->upper_ids), ov_ref_cmp, NULL);
	/* Duplicate references are aliases; presence does not pick an owner. */
	size_t n = 1;
	for (size_t i = 1; i < s->upper_count; i++)
		if (!eqid(s->upper_ids[n - 1], s->upper_ids[i]))
			s->upper_ids[n++] = s->upper_ids[i];
	s->upper_count = n;
	s->upper_indexed = true;
}
static bool ov_upper_ref(struct ks_overlay_state *s, stqr_object_id id)
{
	if (nullid(id))
		return false;
	if (s->immutable && s->upper_indexed) {
		size_t lo = 0, hi = s->upper_count;
		while (lo < hi) {
			size_t mid = lo + (hi - lo) / 2;
			int cmp = memcmp(s->upper_ids[mid].bytes, id.bytes, 32);
			if (!cmp)
				return true;
			if (cmp < 0)
				lo = mid + 1;
			else
				hi = mid;
		}
		return false;
	}
	for (size_t i = 0; i < s->count; i++)
		for (size_t j = 0; j < s->dirs[i]->count; j++)
			if (eqid(s->dirs[i]->entries[j].id, id))
				return true;
	return false;
}
