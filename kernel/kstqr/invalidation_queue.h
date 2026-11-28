/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef KS_INVALIDATION_QUEUE_H
#define KS_INVALIDATION_QUEUE_H
/* A bounded set, protected by the mount inode_lock. Duplicate notifications
 * must not exhaust the queue and invalidate every unrelated mapped file.
 * Slots store one-based indices into ids; no allocation or network I/O. */
#define KS_INVALIDATION_CAPACITY 128
#define KS_INVALIDATION_SLOTS 256
struct ks_invalidation_queue {
	stqr_object_id ids[KS_INVALIDATION_CAPACITY];
	unsigned short slots[KS_INVALIDATION_SLOTS];
	unsigned count;
	bool all;
};
static inline void ks_invalidation_clear(struct ks_invalidation_queue *q)
{
	q->count = 0;
	q->all = false;
	memset(q->slots, 0, sizeof(q->slots));
}
static inline void ks_invalidation_add(struct ks_invalidation_queue *q, const stqr_object_id *id)
{
	unsigned hash = 2166136261U, slot;
	if (q->all)
		return;
	if (!id)
		goto all;
	for (unsigned i = 0; i < sizeof(id->bytes); i++)
		hash = (hash ^ id->bytes[i]) * 16777619U;
	slot = hash & (KS_INVALIDATION_SLOTS - 1);
	while (q->slots[slot]) {
		if (!memcmp(q->ids[q->slots[slot] - 1].bytes, id->bytes, sizeof(id->bytes)))
			return;
		slot = (slot + 1) & (KS_INVALIDATION_SLOTS - 1);
	}
	if (q->count == KS_INVALIDATION_CAPACITY)
		goto all;
	q->ids[q->count] = *id;
	q->slots[slot] = ++q->count;
	return;
all:
	ks_invalidation_clear(q);
	q->all = true;
}
#endif
