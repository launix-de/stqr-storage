/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef KSTQR_H
#define KSTQR_H
#include "stqr.h"
#include <linux/fs.h>
#include <linux/mutex.h>
#define KS_DIR_LIMIT (16U << 20)
struct ks_node {
	unsigned kind, mode, uid, gid, depth;
	u64 size, mtime, atime, ctime;
	bool removed;
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
	struct mutex mutex;
	stqr_client *client;
	char *url;
	stqr_id bucket;
	bool readonly, failed;
};
struct ks_inode {
	stqr_object_id id;
	struct inode inode;
};
static inline struct ks_inode *KS_I(struct inode *i)
{
	return container_of(i, struct ks_inode, inode);
}
extern const stqr_object_id ks_root_id;
void ks_snapshot_free(struct ks_snapshot *s);
int ks_snapshot(struct ks_fs *, stqr_object_id, struct ks_snapshot *);
int ks_initialize(struct ks_fs *, u32 uid, u32 gid);
int ks_read(struct ks_fs *, const struct ks_node *, void *, size_t, u64);
int ks_directory(struct ks_fs *, const struct ks_node *, struct ks_entry **, size_t *);
int ks_create(struct ks_fs *, stqr_object_id, const char *, unsigned, u32, u32, bool,
	      stqr_object_id *);
int ks_remove(struct ks_fs *, stqr_object_id, const char *, bool);
int ks_rename(struct ks_fs *, stqr_object_id, const char *, stqr_object_id, const char *, unsigned,
	      const stqr_object_id *, size_t, const stqr_object_id *, size_t);
int ks_write(struct ks_fs *, stqr_object_id, u64 *, const void *, size_t, bool);
int ks_setattr(struct ks_fs *, stqr_object_id, struct iattr *);
#endif
