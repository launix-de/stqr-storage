/* SPDX-License-Identifier: GPL-3.0-only */
/* Mutable overlay editing clones append names without sorting. Preserve their
 * first-match scan; only ks_directory's strictly validated STDR arrays qualify
 * for find_sorted_entry, before any caller appends or renames an entry. */
static long find_entry(const struct ks_entry *entries, size_t count, const char *name)
{
	for (size_t i = 0; i < count; i++)
		if (!strcmp(entries[i].name, name))
			return i;
	return -1;
}

static long find_sorted_entry(const struct ks_entry *entries, size_t count, const char *name)
{
	size_t lo = 0, hi = count;
	while (lo < hi) {
		size_t mid = lo + (hi - lo) / 2;
		int cmp = strcmp(entries[mid].name, name);
		if (!cmp)
			return mid;
		if (cmp < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return -1;
}
