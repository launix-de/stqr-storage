// SPDX-License-Identifier: GPL-3.0-only
/* A helper never removes inode markers while another helper may replay REDO.
 * Intent/decision/head phase ordering uses accepted visibility completions;
 * none of these normal client phases requests a physical checkpoint. */
static int drop_pinned_intent(struct ks_fs *f, stqr_object_id id, struct buf *head,
			      stqr_info *info, unsigned state)
{
	int err = delete_orphan_object(f, id);
	if (err) {
		intent_retire(f, id, head, info, state);
		return 0; /* WRITE without DELETE retains the compact receipt for fsck. */
	}
	if (head->n >= 15 && !memcmp(head->p, "STFJ\1\0", 6)) {
		struct buf d = *head;
		take(&d, 11);
		unsigned count = readnum(&d, 4);
		for (unsigned i = 0; i < count; i++) {
			stqr_object_id part;
			const u8 *raw = take(&d, 32);
			if (!raw)
				return -EUCLEAN;
			memcpy(part.bytes, raw, 32);
			err = delete_orphan_object(f, part);
			if (err)
				return err;
		}
	}
	return 0;
}
/* Once an exclusive descriptor claim is held, admitted in-place REDO may
 * outlive a cancelled syscall. Finish on mount-owned work rather than inheriting
 * the caller's fatal signal. The caller waits uninterruptibly and pins this
 * stack job and the VFS operation until the worker (including claim release)
 * has exited. put_super drains the same mount queue before closing transports.
 * This is a visibility completion, not a persistence request.
 */
struct ks_claimed_completion {
	struct work_struct work;
	struct completion done;
	struct ks_fs *fs;
	stqr_object_id id;
	stqr_id claim;
	int error;
};
static int finish_claimed_intent(struct ks_fs *f, stqr_object_id id)
{
	struct buf raw = {0}, journal = {0};
	struct member *m = NULL;
	stqr_info info;
	unsigned count = 0, state = 0;
	u8 mark[38] = "STFL\1\0";
	int err;
	memcpy(mark + 6, id.bytes, 32);
	err = intent_get(f, id, &raw, &journal, &info);
	if (err == -ENOENT) {
		err = 0;
		goto out;
	}
	if (!err && raw.n == 7 && !memcmp(raw.p, "STFD\1\0", 6))
		goto out;
	if (!err)
		err = tx_decode(&raw, id, &m, &count, &state);
	if (err || !state) {
		if (!err)
			err = -EUCLEAN;
		goto out;
	}
	/* Finish every REDO write before publishing the first inode. */
	u64 index = 0;
	for (unsigned i = 0; i < count && !err; i++) {
		struct buf head = {0};
		stqr_info hi;
		err = get_object(f, m[i].id, (void **)&head.p, &head.n, &hi);
		if (err == -ENOENT && state == 2)
			err = 0;
		bool owns = !err && head.n == sizeof(mark) && !memcmp(head.p, mark, sizeof(mark));
		if (owns && state == 1 && m[i].writes.n)
			err = apply_edits(f, id, &m[i].writes, m[i].writes_legacy, &index);
		else if (m[i].writes.n) {
			struct buf scan = m[i].writes;
			index += readnum(&scan, 4);
		}
		drop(&head);
	}
	for (unsigned i = 0; i < count && !err; i++) {
		for (;;) {
			struct buf head = {0};
			stqr_info hi;
			err = get_object(f, m[i].id, (void **)&head.p, &head.n, &hi);
			if (err == -ENOENT && state == 2)
				err = 0;
			bool owns =
			    !err && head.n == sizeof(mark) && !memcmp(head.p, mark, sizeof(mark));
			if (!owns || err) {
				drop(&head);
				break;
			}
			struct buf *image = state == 1 ? &m[i].after : &m[i].before;
			err = cas_object(f, m[i].id, image->p, image->n, head.p, head.n, false);
			drop(&head);
			if (err != -EAGAIN)
				break;
		}
	}
	if (!err)
		err = drop_pinned_intent(f, id, &journal, &info, state);
out:
	sq_free(m);
	drop(&raw);
	drop(&journal);
	return err;
}
static void claimed_completion_worker(struct work_struct *work)
{
	struct ks_claimed_completion *job = container_of(work, struct ks_claimed_completion, work);
	unsigned saved = memalloc_nofs_save();
	int released;
	job->error = finish_claimed_intent(job->fs, job->id);
	released = ks_object_reference(job->fs, job->id, job->claim, 3);
	if (!job->error)
		job->error = released;
	memalloc_nofs_restore(saved);
	complete(&job->done);
}
static int complete_claimed_intent(struct ks_fs *f, stqr_object_id id, stqr_id claim)
{
	struct ks_claimed_completion job = {.fs = f, .id = id, .claim = claim};
	INIT_WORK(&job.work, claimed_completion_worker);
	init_completion(&job.done);
	queue_work(f->reference_queue, &job.work);
	wait_for_completion(&job.done);
	flush_work(&job.work);
	return job.error;
}
static int help_tx_pinned(struct ks_fs *f, stqr_object_id first, stqr_id *origin)
{
	stqr_object_id *stack = sq_alloc(128 * sizeof(*stack));
	unsigned depth = 0;
	int err = 0, result = -EAGAIN;
	bool observed = false;
	if (!stack)
		return -ENOMEM;
	stack[0] = first;
	for (unsigned round = 0; round < 8192; round++) {
		struct buf raw = {0}, journal = {0};
		struct member *m = NULL;
		stqr_info info;
		stqr_object_id id = stack[depth];
		stqr_id pin = {{0}}, claim = {{0}};
		unsigned count = 0, state = 0;
		int decision = 1;
		bool pinned = false, finished = false;
		bool own = origin && !zero(origin->bytes, 16) && eqid(id, first);
		u8 mark[38] = "STFL\1\0";
		memcpy(mark + 6, id.bytes, 32);
		if (own) {
			pin = *origin;
			pinned = true;
		} else {
			err = stqr_request_id(&pin);
			if (!err)
				err = ks_object_reference(f, id, pin, 0);
			if (err == -EBUSY) {
				err = sq_pause(1);
				if (!err)
					continue;
			}
			if (err)
				break;
			pinned = true;
		}
		err = intent_get(f, id, &raw, &journal, &info);
		if (err == -ENOENT && !own) {
			err = 0;
			if (depth)
				depth--;
			else {
				if (!observed)
					result = 0; /* Caller must reload its now-unmarked inode. */
				finished = true;
			}
			goto cleanup;
		}
		if (err)
			goto cleanup;
		if (raw.n == 7 && !memcmp(raw.p, "STFD\1\0", 6)) {
			state = raw.p[6];
			if (eqid(id, first)) {
				result = state == 1 ? 0 : -EAGAIN;
				observed = finished = true;
			} else
				depth--;
			if (own) {
				err = ks_retire_pin(f, id, pin);
				if (!err) {
					memset(origin, 0, sizeof(*origin));
					pinned = false;
				}
			}
			goto cleanup;
		}
		err = tx_decode(&raw, id, &m, &count, &state);
		if (err)
			goto cleanup;
		if (state) {
			if (eqid(id, first)) {
				result = state == 1 ? 0 : -EAGAIN;
				observed = true;
			}
			err = ks_retire_pin(f, id, pin);
			if (err)
				goto cleanup;
			pinned = false;
			if (own)
				memset(origin, 0, sizeof(*origin));
			err = stqr_request_id(&claim);
			/* Drain helper pins in one direction. Re-entering the outer loop
			 * would PIN this decided intent again while other helpers try to
			 * claim it, so sufficiently many helpers can keep each other busy
			 * forever. Keep one claim token and remain unpinned while waiting,
			 * exactly as the Go filesystem helper does. A dependency still
			 * blocks retirement; cancellation is an error, never permission to
			 * replay REDO or publish heads without the exclusive claim. */
			while (!err) {
				err = ks_object_reference(f, id, claim, 2);
				if (err != -EBUSY)
					break;
				err = sq_pause(1);
			}
			if (err)
				goto cleanup;
			/* Ownership includes releasing the claim; never do that from a
			 * cancelled foreground caller while admitted REDO is unresolved. */
			err = complete_claimed_intent(f, id, claim);
			if (!err) {
				if (depth)
					depth--;
				else
					finished = true;
			}
			goto cleanup;
		}
		for (unsigned i = 0; i < count; i++) {
			struct buf head = {0};
			stqr_info hi;
			stqr_object_id other;
			err = get_object(f, m[i].id, (void **)&head.p, &head.n, &hi);
			if (err == -ENOENT) {
				err = 0;
				decision = 2;
			} else if (!err && !(head.n == sizeof(mark) && !memcmp(head.p, mark, sizeof(mark)))) {
				if (marker(head.p, head.n, &other)) {
					if (depth == 127)
						err = -ELOOP;
					else {
						stack[++depth] = other;
						decision = 0;
					}
				} else if (head.n != m[i].before.n || memcmp(head.p, m[i].before.p, head.n))
					decision = 2;
				else {
					err = cas_object(f, m[i].id, mark, sizeof(mark), head.p, head.n, false);
					if (err == -EAGAIN) {
						err = 0;
						decision = 0;
					}
				}
			}
			drop(&head);
			if (err || decision != 1)
				break;
		}
		if (!err && decision) {
			struct buf next = {0};
			put(&next, journal.p, journal.n);
			if (!next.err) {
				next.p[6] = decision;
				err = cas_object(f, id, next.p, next.n, journal.p, journal.n, false);
			} else
				err = next.err;
			drop(&next);
			if (err == -EAGAIN)
				err = 0;
		}
cleanup:
		if (pinned && !own) {
			int released = ks_retire_pin(f, id, pin);
			if (!err)
				err = released;
		}
		sq_free(m);
		drop(&raw);
		drop(&journal);
		if (err || finished)
			break;
		cond_resched();
	}
	sq_free(stack);
	return err ? err : result;
}
