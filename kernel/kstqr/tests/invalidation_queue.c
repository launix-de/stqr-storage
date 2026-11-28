/* SPDX-License-Identifier: GPL-3.0-only */
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
typedef struct {
	unsigned char bytes[32];
} stqr_object_id;
#include "../invalidation_queue.h"
#define CHECK(x)                                                                                   \
	do {                                                                                       \
		if (!(x)) {                                                                        \
			fprintf(stderr, "invalidation queue line%d: %s\n", __LINE__, #x);          \
			exit(1);                                                                   \
		}                                                                                  \
	} while (0)
static stqr_object_id id(unsigned n)
{
	stqr_object_id out = {0};
	for (unsigned i = 0; i < 4; i++)
		out.bytes[i] = (unsigned char)(n >> (8 * i));
	return out;
}
static unsigned slot(stqr_object_id v)
{
	unsigned hash = 2166136261U;
	for (unsigned i = 0; i < sizeof(v.bytes); i++)
		hash = (hash ^ v.bytes[i]) * 16777619U;
	return hash & (KS_INVALIDATION_SLOTS - 1);
}
int main(void)
{
	struct ks_invalidation_queue q = {0};
	stqr_object_id a = id(42);
	for (unsigned i = 0; i < 100000; i++)
		ks_invalidation_add(&q, &a);
	CHECK(q.count == 1 && !q.all);
	CHECK(!memcmp(q.ids[0].bytes, a.bytes, 32));
	ks_invalidation_clear(&q);
	/* Deliberate collisions exercise probing and duplicate lookup beyond slot0.
	 */
	unsigned selected = 0;
	for (unsigned i = 0; selected < KS_INVALIDATION_CAPACITY; i++) {
		stqr_object_id v = id(i);
		if (slot(v) != slot(a))
			continue;
		ks_invalidation_add(&q, &v);
		CHECK(q.count == ++selected && !q.all);
	}
	for (unsigned i = 0; i < KS_INVALIDATION_CAPACITY; i++) {
		stqr_object_id v = q.ids[i];
		for (unsigned repeat = 0; repeat < 8; repeat++)
			ks_invalidation_add(&q, &v);
	}
	CHECK(q.count == KS_INVALIDATION_CAPACITY && !q.all);
	/* Only an additional distinct leaf or explicit loss of all coverage
	 * escalates. */
	stqr_object_id extra = id(0xfefefefe);
	ks_invalidation_add(&q, &extra);
	CHECK(q.all && !q.count);
	ks_invalidation_add(&q, &a);
	CHECK(q.all && !q.count);
	ks_invalidation_clear(&q);
	CHECK(!q.all && !q.count);
	ks_invalidation_add(&q, &a);
	CHECK(!q.all && q.count == 1);
	ks_invalidation_add(&q, NULL);
	CHECK(q.all && !q.count);
	ks_invalidation_clear(&q);
	ks_invalidation_add(&q, &extra);
	CHECK(!q.all && q.count == 1);
	CHECK(!memcmp(q.ids[0].bytes, extra.bytes, 32));
	puts("INVALIDATION_QUEUE_PASS duplicate_burst_quiet collision_safe "
	     "distinct_overflow_all explicit_loss_all drain_reset");
	return 0;
}
