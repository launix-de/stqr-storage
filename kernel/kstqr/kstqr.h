/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef KSTQR_H
#define KSTQR_H
#include "stqr.h"
#include <linux/fs.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/spinlock.h>
#include <linux/workqueue.h>
#define KS_RPC_SLOTS 4
struct ks_cache;
struct ks_overlay_state;
struct ks_rpc {
	struct mutex lock;
	stqr_client *client;
};
#define KS_DIR_LIMIT (16U << 20)
struct ks_node {
	unsigned kind, mode, uid, gid, depth;
	u64 size, mtime, atime, ctime;
	stqr_object_id source;
	u64 source_version, upper_absent_version;
	const u8 *write_path;
	size_t write_path_len;
	bool inherited;
	bool removed;
	u32 links;
	const u8 *xattrs;
	size_t xattrs_size;
	stqr_object_id root;
	const u8 *inline_data;
};
struct ks_snapshot {
	stqr_object_id id;
	stqr_info info;
	struct ks_node node;
	void *raw;
	size_t raw_len;
};
struct ks_entry {
	stqr_object_id id;
	char name[256];
};
struct ks_fs {
	struct ks_fs *lower, *parent;
	struct mutex overlay_lock, overlay_cache_lock;
	struct ks_overlay_state *overlay_state;
	struct file *boot_lifetime;
	struct ks_rpc rpc[KS_RPC_SLOTS];
	struct mutex sync_lock;
	stqr_commit_mark durable;
	struct ks_cache *cache;
	size_t cache_budget;
	struct super_block *sb;
	spinlock_t inode_lock;
	struct list_head inodes;
	struct work_struct invalidation_work;
	stqr_object_id invalidations[128];
	unsigned invalidation_count;
	bool invalidate_all;
	u64 invalidation_generation;
	char *url;
	stqr_id bucket;
	bool readonly, failed, stopping;
	atomic64_t cache_hits, cache_misses, rpc_gets, rpc_ranges, rpc_cas, read_windows;
	atomic64_t writeback_batches, writeback_folios, writeback_bytes, writeback_errors,
	    writeback_ns;
	atomic64_t write_snapshot_ns, write_prepare_ns, write_publish_ns, write_retries;
};
struct ks_inode {
	struct mutex metadata_lock;
	struct list_head mount_list;
	u64 invalidated_generation;
	u64 version;
	bool invalidate;
	u64 upper_absent_version;
	stqr_object_id id;
	struct inode inode;
};
static inline struct ks_inode *KS_I(struct inode *i)
{
	return container_of(i, struct ks_inode, inode);
}
int ks_checkpoint(struct ks_fs *);
int ks_cache_sync(struct ks_fs *);
int ks_plain_snapshot(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
int ks_overlay_snapshot(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
int ks_overlay_initialize(struct ks_fs *, const char *);
int ks_overlay_validate_policy(struct ks_fs *, const char *);
int ks_overlay_check_write(struct ks_fs *, stqr_object_id);
void ks_overlay_stop(struct ks_fs *);
int ks_overlay_newnode(struct ks_fs *, stqr_object_id, const char *, unsigned, u32, u32, unsigned,
		       const char *, stqr_object_id *);
int ks_overlay_remove(struct ks_fs *, stqr_object_id, const char *, bool);
int ks_overlay_rename(struct ks_fs *, stqr_object_id, const char *, stqr_object_id, const char *,
		      unsigned);
int ks_overlay_copyup(struct ks_fs *, stqr_object_id);
int ks_overlay_setattr(struct ks_fs *, stqr_object_id, struct iattr *);
int ks_overlay_setxattr(struct ks_fs *, stqr_object_id, const char *, const void *, size_t, int);
int ks_cache_mark(struct ks_fs *, stqr_commit_mark *);
void ks_cache_note_durable(struct ks_fs *, const stqr_commit_mark *);
struct ks_rpc *ks_rpc_lock(struct ks_fs *, stqr_object_id);
int ks_cache_start(struct ks_fs *, const char *, const char *, unsigned);
void ks_cache_stop(struct ks_fs *);
int ks_cache_peek(struct ks_fs *, stqr_object_id, void **, size_t *, stqr_info *);
int ks_snapshot_cached(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
int ks_cache_get(struct ks_fs *, stqr_object_id, void **, size_t *, stqr_info *);
bool ks_cache_version(struct ks_fs *, stqr_object_id, u64 *);
void ks_cache_forget(struct ks_fs *, stqr_object_id);
void ks_queue_invalidation(struct ks_fs *, const stqr_object_id *);
extern const stqr_object_id ks_root_id;
void ks_snapshot_free(struct ks_snapshot *s);
int ks_snapshot(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
int ks_initialize(struct ks_fs *, u32 uid, u32 gid);
int ks_read(struct ks_fs *, const struct ks_node *, void *, size_t, u64);
int ks_directory(struct ks_fs *, const struct ks_node *, struct ks_entry **, size_t *);
int ks_create(struct ks_fs *, stqr_object_id, const char *, unsigned, u32, u32, bool,
	      stqr_object_id *);
int ks_newnode(struct ks_fs *, stqr_object_id, const char *, unsigned, u32, u32, unsigned,
	       const char *, stqr_object_id *);
int ks_link(struct ks_fs *, stqr_object_id, const char *, stqr_object_id);
int ks_getxattr(struct ks_fs *, stqr_object_id, const char *, void *, size_t);
int ks_listxattr(struct ks_fs *, stqr_object_id, char *, size_t, bool);
int ks_setxattr(struct ks_fs *, stqr_object_id, const char *, const void *, size_t, int);
int ks_remove(struct ks_fs *, stqr_object_id, const char *, bool);
int ks_rename(struct ks_fs *, stqr_object_id, const char *, stqr_object_id, const char *, unsigned,
	      const stqr_object_id *, size_t, const stqr_object_id *, size_t);
int ks_write(struct ks_fs *, stqr_object_id, u64 *, const void *, size_t, bool);
int ks_setattr(struct ks_fs *, stqr_object_id, struct iattr *);
#endif
