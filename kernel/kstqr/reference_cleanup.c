// SPDX-License-Identifier: GPL-3.0-only
/* Included by objects.c. An acknowledged PIN outlives its submitting task.
 * Cancellation never authorizes forgetting its token: release it from
 * mount-owned work without inheriting the fatal signal. The foreground VFS
 * operation pins the mount and stack job until both completion and work exit.
 * This only retires a known PIN; it never infers an unknown acquisition outcome,
 * releases somebody else's claim, or requests a persistence checkpoint.
 */
/* Lower connections borrow their containing mount's workqueue. They are not
 * standalone mounts and deliberately have no separately allocated queue. Keep
 * the RPC target on the lower, but retain work in its owning mount until drained. */
static struct workqueue_struct *ks_reference_workqueue(struct ks_fs *f)
{
	while (f->parent)
		f = f->parent;
	return f->reference_queue;
}
struct ks_pin_retirement {
	struct work_struct work;
	struct completion done;
	struct ks_fs *fs;
	stqr_object_id id;
	stqr_id token;
	int error;
};
static void ks_pin_retirement_worker(struct work_struct *work)
{
	struct ks_pin_retirement *job = container_of(work, struct ks_pin_retirement, work);
	unsigned saved = memalloc_nofs_save();
	job->error = ks_object_reference(job->fs, job->id, job->token, 1);
	memalloc_nofs_restore(saved);
	complete(&job->done);
}
static int ks_retire_pin(struct ks_fs *f, stqr_object_id id, stqr_id token)
{
	struct ks_pin_retirement job = {.fs = f, .id = id, .token = token};
	if (!fatal_signal_pending(current)) {
		int err = ks_object_reference(f, id, token, 1);
		/* A fatal signal may arrive after the first check but before the
		 * transport submit. UNPIN is token-idempotent, so retry that same
		 * retirement on the worker rather than losing the acknowledged PIN. */
		if (err != -EINTR || !fatal_signal_pending(current))
			return err;
	}
	INIT_WORK(&job.work, ks_pin_retirement_worker);
	init_completion(&job.done);
	queue_work(ks_reference_workqueue(f), &job.work);
	wait_for_completion(&job.done);
	flush_work(&job.work);
	return job.error;
}
