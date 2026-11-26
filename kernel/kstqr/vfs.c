// SPDX-License-Identifier: GPL-3.0-only
#include "kstqr.h"
#include "block.h"
#include "port.h"
#include <linux/backing-dev.h>
#include <linux/file.h>
#include <linux/fs_context.h>
#include <linux/fs_parser.h>
#include <linux/hex.h>
#include <linux/ktime.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/nsproxy.h>
#include <linux/pagemap.h>
#include <linux/sched/mm.h>
#include <linux/seq_file.h>
#include <linux/splice.h>
#include <linux/statfs.h>
#include <linux/uio.h>
#include <linux/writeback.h>
#include <linux/xattr.h>
#include <net/net_namespace.h>
#define KS_MAGIC 0x53545152
struct ks_context {
	char *url, *username, *password;
	stqr_id bucket, lower;
	char *whitelist, *device;
	struct file *boot_lifetime;
	unsigned timeout, cache_bytes;
	bool create, readonly, bd;
};
static const struct inode_operations ks_dir_ops, ks_file_ops, ks_symlink_ops;
static const struct xattr_handler *const ks_xattrs[];
static const struct file_operations ks_dir_fops, ks_file_fops;
static const struct address_space_operations ks_aops;
static unsigned enter_fs(struct ks_fs *f)
{
	unsigned flags = memalloc_nofs_save();
	(void)f;
	return flags;
}
static void leave_fs(struct ks_fs *f, unsigned flags)
{
	(void)f;
	memalloc_nofs_restore(flags);
}
static umode_t node_mode(const struct ks_node *n)
{
	unsigned type = S_IFREG;
	switch (n->kind) {
	case 2:
		type = S_IFDIR;
		break;
	case 3:
		type = S_IFLNK;
		break;
	case 4:
		type = S_IFIFO;
		break;
	case 5:
		type = S_IFSOCK;
		break;
	}
	return type | n->mode;
}
static void update_inode(struct inode *i, const struct ks_node *n, const stqr_info *info)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct ks_cache_stamp stamp = {0};
	bool covered = n->kind == 1 &&
		ks_cache_stamp(n->inherited && f->lower ? f->lower : f, KS_I(i)->id, info, &stamp);
	stqr_proof proof = info->proof;

	mutex_lock(&KS_I(i)->metadata_lock);
	if (n->kind == 1 && (!covered || memcmp(&KS_I(i)->data_stamp, &stamp, sizeof(stamp))))
		WRITE_ONCE(KS_I(i)->invalidate, true);
	KS_I(i)->data_stamp = stamp;
	if (n->directory_proof)
		KS_I(i)->directory_proof = *n->directory_proof;
	else
		memset(&KS_I(i)->directory_proof, 0, sizeof(KS_I(i)->directory_proof));
	/* Only a live subscription proves cached contents across calls. */
	if (!proof.cursor || memcmp(&KS_I(i)->proof, &proof, sizeof(proof)))
		WRITE_ONCE(KS_I(i)->invalidate, true);
	KS_I(i)->proof = proof;
	KS_I(i)->upper_absent = n->upper_absent;
	i->i_mode = node_mode(n);
	set_nlink(i, n->removed ? 0 : n->links);
	i_uid_write(i, n->uid);
	i_gid_write(i, n->gid);
	if (!mapping_tagged(i->i_mapping, PAGECACHE_TAG_DIRTY) &&
	    !mapping_tagged(i->i_mapping, PAGECACHE_TAG_WRITEBACK)) {
		i_size_write(i, n->size);
		inode_set_mtime_to_ts(i, ns_to_timespec64(n->mtime));
		inode_set_atime_to_ts(i, ns_to_timespec64(n->atime));
		inode_set_ctime_to_ts(i, ns_to_timespec64(n->ctime));
	}
	i->i_blocks = DIV_ROUND_UP_ULL(n->size, 512);
	mutex_unlock(&KS_I(i)->metadata_lock);
}
static int inode_match(struct inode *i, void *data) { return !memcmp(KS_I(i)->id.bytes, data, 32); }
static int inode_set(struct inode *i, void *data)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	memcpy(KS_I(i)->id.bytes, data, 32);
	spin_lock(&f->inode_lock);
	list_add_tail(&KS_I(i)->mount_list, &f->inodes);
	spin_unlock(&f->inode_lock);
	return 0;
}
static struct inode *get_inode(struct super_block *sb, stqr_object_id id, const struct ks_node *n,
			       const stqr_info *info)
{
	struct inode *i;
	unsigned long hash = 0;
	unsigned k;
	for (k = 0; k < 32; k++)
		hash = hash * 31 + id.bytes[k];
	i = iget5_locked(sb, hash, inode_match, inode_set, id.bytes);
	if (!i)
		return ERR_PTR(-ENOMEM);
	if (inode_state_read_once(i) & I_NEW) {
		i->i_ino = !memcmp(id.bytes, ks_root_id.bytes, 32) ? 1 : get_next_ino();
		i->i_op = n->kind == 2	 ? &ks_dir_ops
			  : n->kind == 3 ? &ks_symlink_ops
					 : &ks_file_ops;
		i->i_fop = n->kind == 2 ? &ks_dir_fops : &ks_file_fops;
		i->i_mapping->a_ops = &ks_aops;
		mapping_set_stable_writes(i->i_mapping);
		if (n->kind == 4 || n->kind == 5)
			init_special_inode(i, node_mode(n), 0);
		update_inode(i, n, info);
		unlock_new_inode(i);
	} else
		update_inode(i, n, info);
	return i;
}
/* Both namespace layers must still have uninterrupted subscription coverage. */
static bool overlay_proof_current(struct ks_fs *f, const struct ks_directory_proof *p)
{
	return p->valid && ks_cache_stamp_current(f, ks_root_id, &p->root) &&
	       (!p->has_lower || ks_cache_stamp_current(f->lower, p->lower_id, &p->lower));
}
static int refresh_inode(struct inode *i)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	bool attrs_current = false;
	if (f->lower && S_ISDIR(i->i_mode)) {
		struct ks_directory_proof *p = &KS_I(i)->directory_proof;
		mutex_lock(&KS_I(i)->metadata_lock);
		attrs_current = overlay_proof_current(f, p);
		mutex_unlock(&KS_I(i)->metadata_lock);
		if (attrs_current)
			return 0;
	}
	struct ks_snapshot s;
	int err = ks_snapshot(i->i_sb->s_fs_info, KS_I(i)->id, &s);
	if (!err) {
		update_inode(i, &s.node, &s.info);
		ks_snapshot_free(&s);
	}
	return err;
}
struct ks_dentry {
	struct ks_directory_proof overlay;
	stqr_proof parent_proof;
};
static struct dentry *ks_lookup(struct inode *parent, struct dentry *d, unsigned flags)
{
	struct ks_fs *f = parent->i_sb->s_fs_info;
	struct ks_snapshot s = {0}, child = {0};
	struct ks_directory_proof lookup_proof = {0};
	struct ks_entry *entries = NULL;
	struct inode *i = NULL;
	size_t n, k;
	unsigned saved = enter_fs(f);
	int err;
	(void)flags;
	if (f->lower) {
		stqr_object_id id;
		err = ks_overlay_lookup(f, KS_I(parent)->id, d->d_name.name, &id, &lookup_proof);
		if (err == -ENOENT) {
			err = 0;
			n = 0;
		} else if (!err) {
			entries = sq_alloc(sizeof(*entries));
			if (!entries)
				err = -ENOMEM;
			else {
				strscpy(entries[0].name, d->d_name.name, 256);
				entries[0].id = id;
				n = 1;
			}
		}
	} else {
		err = ks_snapshot(f, KS_I(parent)->id, &s);
		if (!err)
			err = ks_directory(f, &s.node, &entries, &n);
	}
	if (!err)
		for (k = 0; k < n; k++)
			if (!strcmp(entries[k].name, d->d_name.name)) {
				err = ks_snapshot(f, entries[k].id, &child);
				if (!err && child.node.removed)
					err = -ENOENT;
				if (!err) {
					i = get_inode(parent->i_sb, entries[k].id, &child.node,
						      &child.info);
					if (IS_ERR(i)) {
						err = PTR_ERR(i);
						i = NULL;
					}
				}
				break;
			}
	if (!err) {
		struct ks_dentry *proof = kmalloc(sizeof(*proof), GFP_NOFS);
		if (proof) {
			proof->overlay = lookup_proof;
			proof->parent_proof = s.info.proof;
			d->d_fsdata = proof;
		}
	}
	ks_snapshot_free(&s);
	ks_snapshot_free(&child);
	sq_free(entries);
	leave_fs(f, saved);
	if (err)
		return ERR_PTR(err);
	return d_splice_alias(i, d);
}
static int ks_revalidate(struct inode *parent, const struct qstr *name, struct dentry *d,
			 unsigned flags)
{
	struct ks_dentry *proof;
	struct ks_fs *f = parent->i_sb->s_fs_info;
	stqr_proof cached_proof;
	(void)name;
	if (flags & LOOKUP_RCU)
		return -ECHILD;
	if (f->lower) {
		struct ks_directory_proof binding = {0}, observed = {0};
		stqr_object_id id;
		int err;
		spin_lock(&d->d_lock);
		if (d->d_fsdata)
			binding = ((struct ks_dentry *)d->d_fsdata)->overlay;
		spin_unlock(&d->d_lock);
		if (overlay_proof_current(f, &binding))
			return d_inode(d) ? (refresh_inode(d_inode(d)) ? 0 : 1) : 1;
		/* A stale proof requires a fresh name check, not invalidation of an
		 * unchanged directory alias. Spliced aliases can retain the old proof. */
		err = ks_overlay_lookup(f, KS_I(parent)->id, d->d_name.name, &id, &observed);
		if (err && err != -ENOENT)
			return err;
		if (d_inode(d) &&
		    (err == -ENOENT || memcmp(id.bytes, KS_I(d_inode(d))->id.bytes, 32)))
			return 0;
		if (!d_inode(d) && err != -ENOENT)
			return 0;
		spin_lock(&d->d_lock);
		if (d->d_fsdata)
			((struct ks_dentry *)d->d_fsdata)->overlay = observed;
		spin_unlock(&d->d_lock);
		return d_inode(d) ? (refresh_inode(d_inode(d)) ? 0 : 1) : 1;
	}

	proof = d->d_fsdata;
	if (!proof || !ks_cache_proof(f, KS_I(parent)->id, &cached_proof) ||
	    memcmp(&cached_proof, &proof->parent_proof, sizeof(cached_proof)))
		return 0;
	/* Namespace proof also covers negative dentries. Refresh a positive inode
	 * from its live subscription before permission checks use its attributes. */
	if (d_inode(d) &&
	    (!ks_cache_proof(f, KS_I(d_inode(d))->id, &cached_proof) || refresh_inode(d_inode(d))))
		return 0;
	return 1;
}
static void ks_dentry_release(struct dentry *d) { kfree(d->d_fsdata); }
static const struct dentry_operations ks_dentry_ops = {.d_revalidate = ks_revalidate,
						       .d_release = ks_dentry_release};
static int ks_iterate(struct file *file, struct dir_context *ctx)
{
	struct inode *i = file_inode(file);
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct ks_snapshot s = {0};
	struct ks_entry *entries = NULL;
	size_t n, k;
	unsigned saved;
	int err;
	if (!dir_emit_dots(file, ctx))
		return 0;
	saved = enter_fs(f);
	err = ks_snapshot(f, KS_I(i)->id, &s);
	if (!err)
		err = ks_directory(f, &s.node, &entries, &n);
	leave_fs(f, saved);
	if (!err)
		for (k = ctx->pos - 2; k < n; k++) {
			struct ks_snapshot child = {0};
			struct inode *ci;
			saved = enter_fs(f);
			err = ks_snapshot(f, entries[k].id, &child);
			if (err == -ENOENT) {
				leave_fs(f, saved);
				err = 0;
				ctx->pos = k + 3;
				continue;
			}
			if (err) {
				leave_fs(f, saved);
				break;
			}
			ci = get_inode(i->i_sb, entries[k].id, &child.node, &child.info);
			ks_snapshot_free(&child);
			leave_fs(f, saved);
			if (IS_ERR(ci)) {
				err = PTR_ERR(ci);
				break;
			}
			if (!dir_emit(ctx, entries[k].name, strlen(entries[k].name), ci->i_ino,
				      (ci->i_mode & S_IFMT) >> 12)) {
				iput(ci);
				break;
			}
			iput(ci);
			ctx->pos = k + 3;
		}
	ks_snapshot_free(&s);
	sq_free(entries);
	return err;
}
static int create_common(struct mnt_idmap *idmap, struct inode *parent, struct dentry *d,
			 umode_t mode, unsigned kind, const char *target)
{
	struct ks_fs *f = parent->i_sb->s_fs_info;
	stqr_object_id id;
	struct ks_snapshot s;
	u32 uid = from_kuid(&init_user_ns, current_fsuid());
	u32 gid = from_kgid(&init_user_ns, current_fsgid());
	struct inode *i = NULL;
	unsigned saved;
	int err;
	if (idmap != &nop_mnt_idmap)
		return -EOPNOTSUPP;
	if (parent->i_mode & S_ISGID) {
		gid = i_gid_read(parent);
		if (kind == 2)
			mode |= S_ISGID;
	}
	saved = enter_fs(f);
	err = ks_newnode(f, KS_I(parent)->id, d->d_name.name, mode, uid, gid, kind, target, &id);
	if (!err) {
		err = ks_snapshot(f, id, &s);
		if (!err) {
			i = get_inode(parent->i_sb, id, &s.node, &s.info);
			ks_snapshot_free(&s);
			if (IS_ERR(i))
				err = PTR_ERR(i);
		}
	}
	if (!err) {
		d_instantiate(d, i);
		refresh_inode(parent);
	}
	leave_fs(f, saved);
	return err;
}
static int ks_create_file(struct mnt_idmap *idmap, struct inode *parent, struct dentry *d,
			  umode_t mode, bool excl)
{
	(void)excl;
	return create_common(idmap, parent, d, mode, 1, NULL);
}
static struct dentry *ks_mkdir(struct mnt_idmap *idmap, struct inode *parent, struct dentry *d,
			       umode_t mode)
{
	int err = create_common(idmap, parent, d, mode, 2, NULL);
	return err ? ERR_PTR(err) : NULL;
}
static int ks_symlink(struct mnt_idmap *idmap, struct inode *parent, struct dentry *d,
		      const char *target)
{
	return create_common(idmap, parent, d, 0777, 3, target);
}
static int ks_mknod(struct mnt_idmap *idmap, struct inode *parent, struct dentry *d, umode_t mode,
		    dev_t dev)
{
	unsigned kind;
	switch (mode & S_IFMT) {
	case S_IFREG:
		kind = 1;
		break;
	case S_IFIFO:
		kind = 4;
		break;
	case S_IFSOCK:
		kind = 5;
		break;
	default:
		return -EOPNOTSUPP;
	}
	if (dev)
		return -EOPNOTSUPP;
	return create_common(idmap, parent, d, mode, kind, NULL);
}
static int ks_link_vfs(struct dentry *old, struct inode *parent, struct dentry *d)
{
	struct ks_fs *f = parent->i_sb->s_fs_info;
	struct ks_snapshot s;
	struct inode *i;
	unsigned saved = enter_fs(f);
	int err = ks_link(f, KS_I(parent)->id, d->d_name.name, KS_I(d_inode(old))->id);
	if (!err) {
		err = ks_snapshot(f, KS_I(d_inode(old))->id, &s);
		if (!err) {
			i = get_inode(parent->i_sb, s.id, &s.node, &s.info);
			ks_snapshot_free(&s);
			if (IS_ERR(i))
				err = PTR_ERR(i);
			else
				d_instantiate(d, i);
		}
		if (!err)
			refresh_inode(parent);
	}
	leave_fs(f, saved);
	return err;
}
static const char *ks_get_link(struct dentry *d, struct inode *i, struct delayed_call *done)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct ks_snapshot s;
	unsigned saved;
	int err;
	char *target = NULL;
	if (!d)
		return ERR_PTR(-ECHILD);
	saved = enter_fs(f);
	err = ks_snapshot(f, KS_I(i)->id, &s);
	if (!err) {
		if (s.node.kind != 3)
			err = -EINVAL;
		else {
			target = kmemdup_nul(s.node.inline_data, s.node.size, GFP_NOFS);
			if (!target)
				err = -ENOMEM;
		}
		ks_snapshot_free(&s);
	}
	leave_fs(f, saved);
	if (err)
		return ERR_PTR(err);
	set_delayed_call(done, kfree_link, target);
	return target;
}
static int remove_common(struct inode *parent, struct dentry *d, bool dir)
{
	struct ks_fs *f = parent->i_sb->s_fs_info;
	unsigned saved = enter_fs(f);
	int err = ks_remove(f, KS_I(parent)->id, d->d_name.name, dir);
	if (!err) {
		refresh_inode(d_inode(d));
		refresh_inode(parent);
		ks_schedule_orphans(f);
	}
	leave_fs(f, saved);
	return err;
}
static int ks_unlink(struct inode *parent, struct dentry *d)
{
	return remove_common(parent, d, false);
}
static int ks_rmdir(struct inode *parent, struct dentry *d)
{
	return remove_common(parent, d, true);
}
static int capture_path(struct dentry *d, stqr_object_id *ids, size_t *length)
{
	struct dentry *at = dget(d);
	*length = 0;
	for (;;) {
		struct dentry *parent;
		if (*length == 1024) {
			dput(at);
			return -E2BIG;
		}
		ids[(*length)++] = KS_I(d_inode(at))->id;
		parent = dget_parent(at);
		if (parent == at) {
			dput(parent);
			dput(at);
			return 0;
		}
		dput(at);
		at = parent;
	}
}
static int ks_rename_vfs(struct mnt_idmap *idmap, struct inode *old, struct dentry *od,
			 struct inode *new, struct dentry *nd, unsigned flags)
{
	struct ks_fs *f = old->i_sb->s_fs_info;
	stqr_object_id *paths;
	size_t from_length, to_length;
	unsigned saved;
	int err;
	if (idmap != &nop_mnt_idmap)
		return -EOPNOTSUPP;
	paths = sq_alloc(2048 * sizeof(*paths));
	if (!paths)
		return -ENOMEM;
	err = capture_path(od->d_parent, paths, &from_length);
	if (!err)
		err = capture_path(nd->d_parent, paths + 1024, &to_length);
	if (err) {
		sq_free(paths);
		return err;
	}
	saved = enter_fs(f);
	err = ks_rename(f, KS_I(old)->id, od->d_name.name, KS_I(new)->id, nd->d_name.name, flags,
			paths, from_length, paths + 1024, to_length);
	if (!err) {
		if (!(flags & RENAME_EXCHANGE) && d_inode(nd))
			refresh_inode(d_inode(nd));
		refresh_inode(old);
		if (new != old)
			refresh_inode(new);
		ks_schedule_orphans(f);
	}
	leave_fs(f, saved);
	sq_free(paths);
	return err;
}
static int ks_getattr(struct mnt_idmap *idmap, const struct path *path, struct kstat *stat,
		      u32 mask, unsigned flags)
{
	struct inode *i = d_inode(path->dentry);
	struct ks_fs *f = i->i_sb->s_fs_info;
	unsigned saved = enter_fs(f);
	int err = refresh_inode(i);
	(void)flags;
	if (!err)
		generic_fillattr(idmap, mask, i, stat);
	leave_fs(f, saved);
	return err;
}
static int ks_setattr_vfs(struct mnt_idmap *idmap, struct dentry *d, struct iattr *attr)
{
	struct inode *i = d_inode(d);
	struct ks_fs *f = i->i_sb->s_fs_info;
	bool resize = attr->ia_valid & ATTR_SIZE;
	unsigned saved;
	int err;
	if (idmap != &nop_mnt_idmap)
		return -EOPNOTSUPP;
	err = setattr_prepare(idmap, d, attr);
	if (err)
		return err;
	if (resize) {
		filemap_invalidate_lock(i->i_mapping);
		err = filemap_write_and_wait(i->i_mapping);
		if (err)
			goto out_resize;
	}
	saved = enter_fs(f);
	err = ks_setattr(f, KS_I(i)->id, attr);
	if (!err)
		err = refresh_inode(i);
	leave_fs(f, saved);
	if (!err && resize) {
		truncate_setsize(i, attr->ia_size);
		saved = enter_fs(f);
		WRITE_ONCE(KS_I(i)->invalidate, false);
		leave_fs(f, saved);
	}
out_resize:
	if (resize)
		filemap_invalidate_unlock(i->i_mapping);
	return err;
}
/* Refresh ordinary reads without discarding locally dirty mmap data. No RPC
 * mutex is held while waiting for writeback or locking page-cache folios.
 */
static int ks_revalidate_cache(struct file *file)
{
	struct inode *i = file_inode(file);
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct address_space *mapping = i->i_mapping;
	unsigned saved;
	int err;
	if (mapping_tagged(mapping, PAGECACHE_TAG_DIRTY) ||
	    mapping_tagged(mapping, PAGECACHE_TAG_WRITEBACK))
		return 0;
	saved = enter_fs(f);
	err = refresh_inode(i);
	leave_fs(f, saved);
	if (err || !READ_ONCE(KS_I(i)->invalidate))
		return err;
	inode_lock(i);
	filemap_invalidate_lock(mapping);
	err = filemap_write_and_wait(mapping);
	if (!err) {
		truncate_inode_pages(mapping, 0);
		saved = enter_fs(f);
		err = refresh_inode(i);
		if (!err)
			WRITE_ONCE(KS_I(i)->invalidate, false);
		leave_fs(f, saved);
	}
	filemap_invalidate_unlock(mapping);
	inode_unlock(i);
	return err;
}
static ssize_t ks_read_iter(struct kiocb *iocb, struct iov_iter *to)
{
	int err;
	if (iocb->ki_flags & IOCB_NOWAIT)
		return -EAGAIN;
	if (iocb->ki_flags & IOCB_DIRECT)
		return -EOPNOTSUPP;
	if (!iov_iter_count(to))
		return 0;
	err = ks_revalidate_cache(iocb->ki_filp);
	return err ? err : generic_file_read_iter(iocb, to);
}
static ssize_t ks_write_iter(struct kiocb *iocb, struct iov_iter *from)
{
	struct inode *i = file_inode(iocb->ki_filp);
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct address_space *mapping = i->i_mapping;
	unsigned saved;
	void *buffer;
	ssize_t total = 0, checked;
	int err = 0;
	if (iocb->ki_flags & IOCB_NOWAIT)
		return -EAGAIN;
	if (iocb->ki_flags & IOCB_DIRECT)
		return -EOPNOTSUPP;
	if (f->lower) {
		err = ks_overlay_check_write(f, KS_I(i)->id);
		if (err)
			return err;
	}

	if (f->readonly)
		return -EROFS;
	size_t chunk = min_t(size_t, iov_iter_count(from), 1U << 20);
	buffer = sq_alloc(chunk);
	if (!buffer)
		return -ENOMEM;
	inode_lock(i);
	checked = generic_write_checks(iocb, from);
	if (checked > 0)
		err = file_remove_privs(iocb->ki_filp);
	inode_unlock(i);
	if (checked <= 0) {
		total = checked;
		goto out;
	}
	if (err)
		goto out;
	while (iov_iter_count(from)) {
		size_t n = min_t(size_t, chunk, iov_iter_count(from)), copied;
		u64 position = iocb->ki_pos;
		/* The source can itself be a kstqr mapping. Fault it before taking
		 * inode/invalidation/RPC locks needed by its fault handler.
		 */
		copied = copy_from_iter(buffer, n, from);
		if (copied != n) {
			iov_iter_revert(from, copied);
			err = -EFAULT;
			break;
		}
		inode_lock(i);
		filemap_invalidate_lock(mapping);
		err = filemap_write_and_wait(mapping);
		if (!err) {
			saved = enter_fs(f);
			err = ks_write(f, KS_I(i)->id, &position, buffer, n,
				       !!(iocb->ki_flags & IOCB_APPEND));
			if (!err) {
				iocb->ki_pos = position;
				total += n;
				refresh_inode(i);
			}
			leave_fs(f, saved);
		}
		if (!err) {
			truncate_inode_pages(mapping, 0);
			saved = enter_fs(f);
			WRITE_ONCE(KS_I(i)->invalidate, false);
			leave_fs(f, saved);
		}
		filemap_invalidate_unlock(mapping);
		inode_unlock(i);
		if (err) {
			iov_iter_revert(from, n);
			break;
		}
	}
out:
	sq_free(buffer);
	if (total && (iocb->ki_flags & (IOCB_DSYNC | IOCB_SYNC))) {
		int synced = ks_checkpoint(f);
		if (synced)
			return synced;
	}
	return total ? total : err;
}
static int ks_read_folio(struct file *file, struct folio *folio)
{
	struct inode *i = folio->mapping->host;
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct ks_snapshot s = {0};
	unsigned saved;
	void *buffer;
	size_t n = 0;
	int err;
	(void)file;
	buffer = sq_alloc(folio_size(folio));
	if (!buffer) {
		folio_unlock(folio);
		return -ENOMEM;
	}
	saved = enter_fs(f);
	err = ks_snapshot(f, KS_I(i)->id, &s);
	if (!err && folio_pos(folio) < s.node.size) {
		n = min_t(u64, folio_size(folio), s.node.size - folio_pos(folio));
		err = ks_read(f, &s.node, buffer, n, folio_pos(folio));
	}
	if (!err) {
		memcpy_to_folio(folio, 0, buffer, folio_size(folio));
		folio_mark_uptodate(folio);
	}
	ks_snapshot_free(&s);
	leave_fs(f, saved);
	sq_free(buffer);
	folio_unlock(folio);
	return err;
}
/* Queue contiguous windows without waiting in the caller. A bounded worker
 * pool allocates the payload buffers only when it runs; queued jobs retain
 * locked folios, not an extra MiB buffer per speculative read. No disk format
 * or object identity changes are involved. */
#define KS_READ_BYTES (1U << 20)
#define KS_READ_FOLIOS (KS_READ_BYTES / PAGE_SIZE)
struct ks_read_window {
 struct work_struct work;
 struct ks_fs *fs;
 stqr_object_id id;
 loff_t offset;
 size_t length;
 u64 queued;
 unsigned count;
 struct folio *folios[KS_READ_FOLIOS];
};
static void ks_read_window_worker(struct work_struct *work)
{
 struct ks_read_window *job = container_of(work, struct ks_read_window, work);
 struct ks_fs *f = job->fs;
 struct ks_snapshot snapshot = {0};
 u64 started = ktime_get_ns();
 size_t pos = 0, bytes = 0;
 unsigned saved = enter_fs(f);
 void *buffer = sq_alloc(job->length);
 int err = buffer ? ks_snapshot(f, job->id, &snapshot) : -ENOMEM;
 atomic64_inc(&f->read_windows);
 atomic64_add(started - job->queued, &f->read_queue_ns);
 if (!err && job->offset < snapshot.node.size) {
  bytes = min_t(u64, job->length, snapshot.node.size - job->offset);
  err = ks_read(f, &snapshot.node, buffer, bytes, job->offset);
 }
 ks_snapshot_free(&snapshot);
 if (!err) atomic64_add(bytes, &f->read_bytes);
 atomic64_add(ktime_get_ns() - started, &f->read_ns);
 // All filesystem/RPC work finishes before releasing the last locked folio;
 // inode eviction may proceed immediately after that unlock.
 for (unsigned n = 0; n < job->count; n++) {
  struct folio *folio = job->folios[n];
  size_t length = folio_size(folio);
  if (!err) {
   memcpy_to_folio(folio, 0, (u8 *)buffer + pos, length);
   folio_mark_uptodate(folio);
  }
  pos += length;
  folio_unlock(folio);
  folio_put(folio);
 }
 sq_free(buffer);
 leave_fs(f, saved);
 kfree(job);
}
static void ks_readahead(struct readahead_control *rac)
{
 struct inode *i = rac->mapping->host;
 struct ks_fs *f = i->i_sb->s_fs_info;
 while (readahead_count(rac)) {
  struct ks_read_window *job = kzalloc(sizeof(*job), GFP_NOFS);
  struct folio *folio;
  if (!job) return; // VFS falls back to read_folio for unconsumed folios.
  job->fs = f;
  job->id = KS_I(i)->id;
  job->offset = readahead_pos(rac);
  job->queued = ktime_get_ns();
  while (job->length < KS_READ_BYTES && job->count < KS_READ_FOLIOS &&
         (folio = readahead_folio(rac))) {
   folio_get(folio);
   job->folios[job->count++] = folio;
   job->length += folio_size(folio);
  }
  if (!job->count) { kfree(job); return; }
  INIT_WORK(&job->work, ks_read_window_worker);
  queue_work(f->read_queue, &job->work);
 }
}
#define KS_WRITEBACK_BYTES (1U << 20)
#define KS_WRITEBACK_FOLIOS (KS_WRITEBACK_BYTES / PAGE_SIZE)
struct ks_write_batch {
	struct folio *folios[KS_WRITEBACK_FOLIOS];
	void *data;
	loff_t start;
	size_t length, limit;
	unsigned count;
};
/* Folios are unlocked before the RPC. Their writeback bits and our references
 * remain held until ACK/error completion. In particular, completion must never
 * take their locks: truncate and page_mkwrite may hold those locks waiting for
 * writeback. The writeback bit prevents removal from the mapping until we finish.
 */
static int ks_flush_write_batch(struct address_space *mapping, struct writeback_control *wbc,
				struct ks_write_batch *batch)
{
	struct inode *i = mapping->host;
	struct ks_fs *f = i->i_sb->s_fs_info;
	u64 position = batch->start, started;
	unsigned saved;
	int err;
	if (!batch->count)
		return 0;
	started = ktime_get_ns();
	atomic64_inc(&f->writeback_batches);
	atomic64_add(batch->count, &f->writeback_folios);
	atomic64_add(batch->length, &f->writeback_bytes);
	saved = enter_fs(f);
	err = ks_write(f, KS_I(i)->id, &position, batch->data, batch->length, false);
	leave_fs(f, saved);
	atomic64_add(ktime_get_ns() - started, &f->writeback_ns);
	if (err) {
		atomic64_inc(&f->writeback_errors);
		mapping_set_error(mapping, err);
	}
	for (unsigned n = 0; n < batch->count; n++) {
		struct folio *folio = batch->folios[n];
		if (err)
			folio_redirty_for_writepage(wbc, folio);
		folio_end_writeback(folio);
		folio_put(folio);
	}
	batch->count = 0;
	batch->length = 0;
	return err;
}
static int ks_writepages(struct address_space *mapping, struct writeback_control *wbc)
{
	struct ks_write_batch *batch = kzalloc(sizeof(*batch), GFP_NOFS);
	struct folio *folio = NULL;
	int error = 0, first_error = 0;
	if (!batch)
		return -ENOMEM;
	/* Keep small-file fsync inexpensive while bounding large writes at 1 MiB. */
	batch->limit = min_t(u64, KS_WRITEBACK_BYTES,
			     max_t(u64, PAGE_SIZE, (u64)READ_ONCE(mapping->nrpages) << PAGE_SHIFT));
	batch->data = sq_alloc(batch->limit);
	if (!batch->data) {
		kfree(batch);
		return -ENOMEM;
	}
	while ((folio = writeback_iter(mapping, wbc, folio, &error))) {
		loff_t size = i_size_read(mapping->host), offset = folio_pos(folio);
		size_t n = offset < size ? min_t(u64, folio_size(folio), size - offset) : 0;
		error = 0;
		if (batch->count &&
		    (!n || offset != batch->start + batch->length ||
		     n > batch->limit - batch->length || batch->count == KS_WRITEBACK_FOLIOS))
			error = ks_flush_write_batch(mapping, wbc, batch);
		if (error || n > batch->limit) {
			if (!error)
				error = -E2BIG;
			if (!first_error)
				first_error = error;
			mapping_set_error(mapping, error);
			folio_redirty_for_writepage(wbc, folio);
			folio_unlock(folio);
			continue;
		}
		/* writeback_iter has cleared dirty state and protected writable PTEs.
		 * Copy while locked; later stores fault and wait for our writeback bit. */
		if (n < folio_size(folio))
			folio_zero_range(folio, n, folio_size(folio) - n);
		folio_start_writeback(folio);
		if (!n) {
			folio_end_writeback(folio);
			folio_unlock(folio);
			continue;
		}
		if (!batch->count)
			batch->start = offset;
		folio_get(folio);
		memcpy_from_folio(batch->data + batch->length, folio, 0, n);
		batch->folios[batch->count++] = folio;
		batch->length += n;
		folio_unlock(folio);
		if (batch->length == batch->limit || batch->count == KS_WRITEBACK_FOLIOS) {
			error = ks_flush_write_batch(mapping, wbc, batch);
			if (error && !first_error)
				first_error = error;
		}
	}
	if (error && !first_error)
		first_error = error;
	error = ks_flush_write_batch(mapping, wbc, batch);
	if (error && !first_error)
		first_error = error;
	sq_free(batch->data);
	kfree(batch);
	return first_error;
}
static vm_fault_t ks_page_mkwrite(struct vm_fault *vmf)
{
	struct file *file = vmf->vma->vm_file;
	struct inode *i = file_inode(file);
	struct address_space *mapping = i->i_mapping;
	struct folio *folio = page_folio(vmf->page);
	vm_fault_t result = VM_FAULT_NOPAGE;
	int err;
	struct ks_fs *f = i->i_sb->s_fs_info;
	if (f->lower && ks_overlay_check_write(f, KS_I(i)->id))
		return VM_FAULT_SIGBUS;
	if (sb_rdonly(i->i_sb))
		return VM_FAULT_SIGBUS;
	sb_start_pagefault(i->i_sb);
	balance_dirty_pages_ratelimited(mapping);
	err = file_update_time(file);
	if (err) {
		result = vmf_error(err);
		goto out;
	}
	filemap_invalidate_lock_shared(mapping);
	if (folio_lock_killable(folio))
		goto out_invalidate;
	if (folio->mapping != mapping || folio_mkwrite_check_truncate(folio, i) < 0)
		goto out_folio;
	if (folio_wait_writeback_killable(folio))
		goto out_folio;
	if (!folio_test_uptodate(folio)) {
		result = VM_FAULT_SIGBUS;
		goto out_folio;
	}
	folio_mark_dirty(folio);
	result = VM_FAULT_LOCKED;
	goto out_invalidate;
out_folio:
	folio_unlock(folio);
out_invalidate:
	filemap_invalidate_unlock_shared(mapping);
out:
	sb_end_pagefault(i->i_sb);
	return result;
}
/* Eviction deliberately leaves inode metadata stale instead of resubscribing
 * from the invalidator. A fault must refresh that metadata before filemap_fault
 * tests i_size, otherwise a remote truncate could return zeroes past EOF rather
 * than SIGBUS. Fault-around must defer while that refresh is outstanding. */
static vm_fault_t ks_filemap_fault(struct vm_fault *vmf)
{
	struct file *file = vmf->vma->vm_file;
	if (READ_ONCE(KS_I(file_inode(file))->invalidate)) {
		int err = ks_revalidate_cache(file);
		if (err)
			return vmf_error(err);
	}
	return filemap_fault(vmf);
}
static vm_fault_t ks_filemap_map_pages(struct vm_fault *vmf, pgoff_t start, pgoff_t end)
{
	if (READ_ONCE(KS_I(file_inode(vmf->vma->vm_file))->invalidate))
		return 0;
	return filemap_map_pages(vmf, start, end);
}
static const struct vm_operations_struct ks_vm_ops = {
    .fault = ks_filemap_fault, .map_pages = ks_filemap_map_pages, .page_mkwrite = ks_page_mkwrite};
static int ks_mmap(struct file *file, struct vm_area_struct *vma)
{
	struct inode *i = file_inode(file);
	int err;
	struct ks_fs *f = i->i_sb->s_fs_info;
	if (f->lower && (vma->vm_flags & VM_SHARED) && (vma->vm_flags & VM_WRITE)) {
		err = ks_overlay_check_write(f, KS_I(i)->id);
		if (err)
			return err;
	}

	if ((vma->vm_flags & VM_SHARED) && (vma->vm_flags & VM_WRITE) && sb_rdonly(i->i_sb))
		return -EROFS;
	if ((vma->vm_flags & VM_SHARED) && (vma->vm_flags & VM_MAYWRITE)) {
		inode_lock(i);
		err = file_remove_privs(file);
		inode_unlock(i);
		if (err)
			return err;
	}
	err = ks_revalidate_cache(file);
	if (err)
		return err;
	err = generic_file_mmap(file, vma);
	if (!err)
		vma->vm_ops = &ks_vm_ops;
	return err;
}
static int ks_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	(void)datasync;
	int err = file_write_and_wait_range(file, start, end);
	return err ? err : ks_checkpoint(file_inode(file)->i_sb->s_fs_info);
}
struct ks_file_reference {
	stqr_id upper, lower;
	bool upper_pinned, lower_pinned;
};
static int ks_open_file(struct inode *inode, struct file *file)
{
	struct ks_fs *f = inode->i_sb->s_fs_info;
	struct ks_file_reference *ref;
	struct ks_snapshot snapshot = {0};
	stqr_object_id id = KS_I(inode)->id;
	unsigned saved;
	int err = generic_file_open(inode, file);
	if (err)
		return err;
	ref = kzalloc(sizeof(*ref), GFP_NOFS);
	if (!ref)
		return -ENOMEM;
	saved = enter_fs(f);
	err = 0;
	if (f->references) {
		err = stqr_request_id(&ref->upper);
		if (!err)
			err = ks_object_reference(f, id, ref->upper, 0);
		ref->upper_pinned = !err;
	}
	if (!err && f->lower && f->lower->references) {
		err = stqr_request_id(&ref->lower);
		if (!err)
			err = ks_object_reference(f->lower, id, ref->lower, 0);
		ref->lower_pinned = !err;
	}
	if (!err) {
		ks_cache_forget(f, id);
		if (f->lower)
			ks_cache_forget(f->lower, id);
		err = ks_snapshot(f, id, &snapshot);
	}
	if (!err && (snapshot.node.removed || !snapshot.node.links))
		err = -ENOENT;
	ks_snapshot_free(&snapshot);
	if (err) {
		if (ref->lower_pinned)
			ks_object_reference(f->lower, id, ref->lower, 1);
		if (ref->upper_pinned)
			ks_object_reference(f, id, ref->upper, 1);
		kfree(ref);
	} else
		file->private_data = ref;
	leave_fs(f, saved);
	return err;
}
static int ks_release_file(struct inode *inode, struct file *file)
{
	struct ks_fs *f = inode->i_sb->s_fs_info;
	struct ks_file_reference *ref = file->private_data;
	stqr_object_id id = KS_I(inode)->id;
	unsigned saved;
	int err, next;
	if (!ref)
		return 0;
	/* VMAs retain struct file, so this also runs only after the final mmap,
	 * dup and fork reference has gone. Never reclaim dirty unmapped data. */
	err = file_write_and_wait_range(file, 0, LLONG_MAX);
	saved = enter_fs(f);
	if (!err && ref->lower_pinned)
		err = ks_object_reference(f->lower, id, ref->lower, 1);
	if (!err && ref->upper_pinned) {
		next = ks_object_reference(f, id, ref->upper, 1);
		if (next)
			err = next;
	}
	if (err)
		pr_warn_ratelimited("kstqr: retaining durable open reference after close error: %d\n", err);
	else
		ks_schedule_orphans(f);
	leave_fs(f, saved);
	file->private_data = NULL;
	kfree(ref);
	return err;
}
void ks_schedule_orphans(struct ks_fs *f)
{
	if (f->references && !f->readonly && !READ_ONCE(f->stopping))
		queue_delayed_work(system_unbound_wq, &f->orphan_work, 0);
}
static void ks_orphan_worker(struct work_struct *work)
{
	struct ks_fs *f = container_of(to_delayed_work(work), struct ks_fs, orphan_work);
	bool pending = false;
	unsigned saved;
	int err;
	if (READ_ONCE(f->stopping))
		return;
	saved = enter_fs(f);
	err = ks_collect_orphans(f, &pending);
	leave_fs(f, saved);
	if (err)
		pr_warn_ratelimited("kstqr: orphan cleanup deferred: %d\n", err);
	if ((pending || (err && err != -EACCES && err != -EOPNOTSUPP)) &&
	    !READ_ONCE(f->stopping))
		queue_delayed_work(system_unbound_wq, &f->orphan_work, HZ);
}
static const struct address_space_operations ks_aops = {.read_folio = ks_read_folio,
							.readahead = ks_readahead,
							.writepages = ks_writepages,
							.dirty_folio = filemap_dirty_folio};
static const struct file_operations ks_file_fops = {
    .owner = THIS_MODULE,
    .open = ks_open_file,
    .release = ks_release_file,
    .llseek = generic_file_llseek,
    .read_iter = ks_read_iter,
    .splice_read = filemap_splice_read,
    .splice_write = iter_file_splice_write,
    .write_iter = ks_write_iter,
    .fsync = ks_fsync,
    .mmap = ks_mmap,
};
static const struct file_operations ks_dir_fops = {
    .owner = THIS_MODULE,
    .open = ks_open_file,
    .release = ks_release_file,
    .llseek = generic_file_llseek,
    .read = generic_read_dir,
    .iterate_shared = ks_iterate,
    .fsync = ks_fsync,
};
static int ks_xattr_get(const struct xattr_handler *handler, struct dentry *d, struct inode *i,
			const char *name, void *buffer, size_t size)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	unsigned saved;
	int err;
	(void)d;
	if (handler->flags && !capable(CAP_SYS_ADMIN))
		return -ENODATA;
	saved = enter_fs(f);
	err = ks_getxattr(f, KS_I(i)->id, xattr_full_name(handler, name), buffer, size);
	leave_fs(f, saved);
	return err;
}
static int ks_xattr_set(const struct xattr_handler *handler, struct mnt_idmap *idmap,
			struct dentry *d, struct inode *i, const char *name, const void *value,
			size_t size, int flags)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	unsigned saved;
	int err;
	(void)d;
	if (idmap != &nop_mnt_idmap)
		return -EOPNOTSUPP;
	if (handler->flags && !capable(CAP_SYS_ADMIN))
		return -EPERM;
	saved = enter_fs(f);
	err = ks_setxattr(f, KS_I(i)->id, xattr_full_name(handler, name), value, size, flags);
	if (!err)
		refresh_inode(i);
	leave_fs(f, saved);
	return err;
}
static ssize_t ks_listxattr_vfs(struct dentry *d, char *buffer, size_t size)
{
	struct inode *i = d_inode(d);
	struct ks_fs *f = i->i_sb->s_fs_info;
	unsigned saved = enter_fs(f);
	int err = ks_listxattr(f, KS_I(i)->id, buffer, size, capable(CAP_SYS_ADMIN));
	leave_fs(f, saved);
	return err;
}
static const struct xattr_handler ks_user_xattr = {
    .prefix = "user.", .get = ks_xattr_get, .set = ks_xattr_set};
static const struct xattr_handler ks_trusted_xattr = {
    .prefix = "trusted.", .flags = 1, .get = ks_xattr_get, .set = ks_xattr_set};
static const struct xattr_handler ks_security_xattr = {
    .prefix = "security.", .get = ks_xattr_get, .set = ks_xattr_set};
static const struct xattr_handler *const ks_xattrs[] = {&ks_user_xattr, &ks_trusted_xattr,
							&ks_security_xattr, NULL};
static const struct inode_operations ks_file_ops = {
    .getattr = ks_getattr, .setattr = ks_setattr_vfs, .listxattr = ks_listxattr_vfs};
static const struct inode_operations ks_dir_ops = {
    .lookup = ks_lookup,
    .symlink = ks_symlink,
    .link = ks_link_vfs,
    .mknod = ks_mknod,
    .listxattr = ks_listxattr_vfs,
    .create = ks_create_file,
    .mkdir = ks_mkdir,
    .unlink = ks_unlink,
    .rmdir = ks_rmdir,
    .rename = ks_rename_vfs,
    .getattr = ks_getattr,
    .setattr = ks_setattr_vfs,
};
static const struct inode_operations ks_symlink_ops = {.get_link = ks_get_link,
						       .getattr = ks_getattr,
						       .setattr = ks_setattr_vfs,
						       .listxattr = ks_listxattr_vfs};
void ks_queue_invalidation(struct ks_fs *f, const stqr_object_id *id)
{
	if (f->parent) {
		ks_queue_invalidation(f->parent, NULL);
		return;
	}
	if (f->lower && id && !memcmp(id->bytes, ks_root_id.bytes, 32))
		id = NULL;
	if (READ_ONCE(f->stopping))
		return;
	spin_lock(&f->inode_lock);
	if (!id || f->invalidation_count == ARRAY_SIZE(f->invalidations)) {
		f->invalidate_all = true;
		f->invalidation_count = 0;
	} else if (!f->invalidate_all)
		f->invalidations[f->invalidation_count++] = *id;
	spin_unlock(&f->inode_lock);
	/* Cache-pressure invalidation can scan many inodes and wait for folios.
	 * Keep it off the CPU-bound shared queue used by unrelated kernel work. */
	queue_work(system_unbound_wq, &f->invalidation_work);
}
/* An unrelated overlay/cache eviction must not throw away mapped program
 * text whose exact inode snapshot still has uninterrupted subscription coverage.
 * New subscriptions, changed cursors and missing namespace proofs do not qualify.
 */
static bool ks_cached_data_unchanged(struct inode *i, const struct ks_snapshot *snapshot)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	struct ks_cache_stamp stamp = {0};
	bool same;
	if (!S_ISREG(i->i_mode) || snapshot->node.kind != 1 ||
	    !ks_cache_stamp(snapshot->node.inherited && f->lower ? f->lower : f, KS_I(i)->id,
			    &snapshot->info, &stamp))
		return false;
	mutex_lock(&KS_I(i)->metadata_lock);
	same = !READ_ONCE(KS_I(i)->invalidate) &&
	       !memcmp(&KS_I(i)->data_stamp, &stamp, sizeof(stamp));
	mutex_unlock(&KS_I(i)->metadata_lock);
	return same;
}
static void ks_invalidate_one(struct inode *i)
{
	struct address_space *mapping = i->i_mapping;
	struct ks_snapshot snapshot;
	int err = ks_snapshot_cached(i->i_sb->s_fs_info, KS_I(i)->id, &snapshot);
	/* An overlay-root update can invalidate namespace proofs without changing
	 * this file. Resolve it before taking the mapping lock, otherwise unrelated
	 * creates repeatedly discard executable pages. This worker runs outside the
	 * subscription callback/wire lock; never move the RPC under the mapping lock.
	 * An unavailable or changed snapshot still takes the conservative path. */
	if (err && S_ISREG(i->i_mode) && READ_ONCE(mapping->nrpages))
		err = ks_snapshot(i->i_sb->s_fs_info, KS_I(i)->id, &snapshot);
	if (!err) {
		bool same = ks_cached_data_unchanged(i, &snapshot);
		ks_snapshot_free(&snapshot);
		if (same)
			return;
	}
	/* Do not publish asynchronously copied metadata here: a foreground refresh
	 * may already have observed a newer truncate. Leave refresh pending until
	 * foreground access checks the current size before filemap_fault tests EOF.
	 */
	WRITE_ONCE(KS_I(i)->invalidate, true);
	if (S_ISREG(i->i_mode)) {
		filemap_invalidate_lock(mapping);
		if (!mapping_tagged(mapping, PAGECACHE_TAG_DIRTY) &&
		    !mapping_tagged(mapping, PAGECACHE_TAG_WRITEBACK)) {
			unmap_mapping_range(mapping, 0, 0, 0);
			truncate_inode_pages(mapping, 0);
		}
		filemap_invalidate_unlock(mapping);
	}
}
static void ks_invalidation_worker(struct work_struct *work)
{
	struct ks_fs *f = container_of(work, struct ks_fs, invalidation_work);
	stqr_object_id *ids = kmalloc_array(128, sizeof(*ids), GFP_NOFS);
	for (;;) {
		unsigned count;
		bool all;
		u64 generation;
		spin_lock(&f->inode_lock);
		count = f->invalidation_count;
		all = f->invalidate_all;
		if (count && ids)
			memcpy(ids, f->invalidations, count * sizeof(*ids));
		if (count && !ids) {
			all = true;
			count = 0;
		}
		f->invalidation_count = 0;
		f->invalidate_all = false;
		generation = ++f->invalidation_generation;
		spin_unlock(&f->inode_lock);
		if (!count && !all) {
			kfree(ids);
			return;
		}
		for (;;) {
			struct inode *batch[32];
			struct ks_inode *ki;
			unsigned n = 0;
			spin_lock(&f->inode_lock);
			list_for_each_entry(ki, &f->inodes, mount_list)
			{
				bool match = all;
				if (inode_state_read_once(&ki->inode) & I_NEW)
					continue;
				if (ki->invalidated_generation == generation)
					continue;
				ki->invalidated_generation = generation;
				for (unsigned k = 0; !match && k < count; k++)
					match = !memcmp(ki->id.bytes, ids[k].bytes, 32);
				if (match) {
					struct inode *i = igrab(&ki->inode);
					if (i)
						batch[n++] = i;
					if (n == ARRAY_SIZE(batch))
						break;
				}
			}
			spin_unlock(&f->inode_lock);
			if (!n)
				break;
			for (unsigned k = 0; k < n; k++) {
				ks_invalidate_one(batch[k]);
				iput(batch[k]);
			}
			cond_resched();
		}
	}
}
static struct inode *ks_alloc_inode(struct super_block *sb)
{
	struct ks_inode *i = kzalloc(sizeof(*i), GFP_KERNEL);
	(void)sb;
	if (!i)
		return NULL;
	mutex_init(&i->metadata_lock);
	INIT_LIST_HEAD(&i->mount_list);
	inode_init_once(&i->inode);
	return &i->inode;
}
static void ks_free_inode(struct inode *i) { kfree(KS_I(i)); }
static void ks_evict(struct inode *i)
{
	struct ks_fs *f = i->i_sb->s_fs_info;
	spin_lock(&f->inode_lock);
	list_del_init(&KS_I(i)->mount_list);
	spin_unlock(&f->inode_lock);
	truncate_inode_pages_final(&i->i_data);
	clear_inode(i);
}
static void ks_close_lower(struct ks_fs *f)
{
	if (!f)
		return;
	WRITE_ONCE(f->stopping, true);
	ks_cache_stop(f);
	stqr_fence_close(f->fence);
	f->fence = NULL;
	for (unsigned slot = 0; slot < KS_RPC_SLOTS; slot++) {
		stqr_close(f->rpc[slot].client);
	}
	kfree(f->url);
	kfree(f);
}
static void ks_put_super(struct super_block *sb)
{
	struct ks_fs *f = sb->s_fs_info;
	if (!f)
		return;
	WRITE_ONCE(f->stopping, true);
	cancel_delayed_work_sync(&f->orphan_work);
 if (f->read_queue) {
  destroy_workqueue(f->read_queue);
  f->read_queue = NULL;
 }
	if (!f->readonly) {
		int err = ks_checkpoint(f);
		if (err)
			pr_err("kstqr: final durability checkpoint failed: %d\n", err);
	}
	WRITE_ONCE(f->stopping, true);
	ks_cache_stop(f);
	stqr_fence_close(f->fence);
	f->fence = NULL;
	cancel_work_sync(&f->invalidation_work);
	ks_close_lower(f->lower);
	f->lower = NULL;
	for (unsigned slot = 0; slot < KS_RPC_SLOTS; slot++) {
		stqr_close(f->rpc[slot].client);
	}
	ks_overlay_stop(f);
	if (f->boot_lifetime)
		fput(f->boot_lifetime);
	kfree(f->url);
	kfree(f);
	sb->s_fs_info = NULL;
}
static int ks_show_options(struct seq_file *m, struct dentry *root)
{
	struct ks_fs *f = root->d_sb->s_fs_info;
	char bucket[33];
	bin2hex(bucket, f->bucket.bytes, 16);
	bucket[32] = 0;
	seq_show_option(m, "url", f->url);
	seq_show_option(m, "bucket", bucket);
	if (f->lower) {
		bin2hex(bucket, f->lower->bucket.bytes, 16);
		bucket[32] = 0;
		seq_show_option(m, "readonly_bucket", bucket);
	}

	seq_printf(m, ",metadata_cache_bytes=%zu", f->cache_budget);
	return 0;
}
static int ks_show_stats(struct seq_file *m, struct dentry *root)
{
	struct ks_fs *f = root->d_sb->s_fs_info;
	seq_printf(m,
		   "\tstqr: cache_hits=%lld cache_misses=%lld rpc_get=%lld rpc_range=%lld "
		   "rpc_cas=%lld read_windows=%lld read_bytes=%lld read_ns=%lld read_queue_ns=%lld writeback_batches=%lld writeback_folios=%lld "
		   "writeback_bytes=%lld writeback_errors=%lld writeback_ns=%lld "
		   "write_snapshot_ns=%lld write_prepare_ns=%lld write_publish_ns=%lld "
		   "write_retries=%lld\n",
		   atomic64_read(&f->cache_hits), atomic64_read(&f->cache_misses),
		   atomic64_read(&f->rpc_gets), atomic64_read(&f->rpc_ranges),
		   atomic64_read(&f->rpc_cas), atomic64_read(&f->read_windows),
 atomic64_read(&f->read_bytes), atomic64_read(&f->read_ns), atomic64_read(&f->read_queue_ns),
		   atomic64_read(&f->writeback_batches), atomic64_read(&f->writeback_folios),
		   atomic64_read(&f->writeback_bytes), atomic64_read(&f->writeback_errors),
		   atomic64_read(&f->writeback_ns), atomic64_read(&f->write_snapshot_ns),
		   atomic64_read(&f->write_prepare_ns), atomic64_read(&f->write_publish_ns),
		   atomic64_read(&f->write_retries));
	return 0;
}
static int ks_statfs(struct dentry *d, struct kstatfs *s)
{
	struct ks_fs *f = d->d_sb->s_fs_info;
	stqr_space space;
	int err;
	mutex_lock(&f->rpc[0].lock);
	for (;;) {
		err = stqr_bucket_space(f->rpc[0].client, &space);
		if (!ks_transport_retryable(f->rpc[0].client, err))
			break;
		err = ks_reconnect_wait(f, f->rpc[0].client);
		if (err)
			break;
	}
	mutex_unlock(&f->rpc[0].lock);
	if (err)
		return err;
	memset(s, 0, sizeof(*s));
	s->f_type = KS_MAGIC;
	s->f_bsize = 4096;
	s->f_frsize = 4096;
	s->f_namelen = 255;
	s->f_blocks = space.total / 4096;
	s->f_bfree = space.free / 4096;
	s->f_bavail = space.available / 4096;
	return 0;
}
static int ks_sync_fs(struct super_block *sb, int wait)
{
	return wait ? ks_checkpoint(sb->s_fs_info) : 0;
}
/* A forced local unmount is an explicit cancellation of hard reconnects.
 * Ordinary unmounts keep their durability/wait-on-node contract. */
static void ks_umount_begin(struct super_block *sb)
{
	struct ks_fs *f = sb->s_fs_info;
	WRITE_ONCE(f->stopping, true);
	if (f->lower)
		WRITE_ONCE(f->lower->stopping, true);
}
static const struct super_operations ks_super_ops = {
    .umount_begin = ks_umount_begin,
    .alloc_inode = ks_alloc_inode,
    .free_inode = ks_free_inode,
    .evict_inode = ks_evict,
    .put_super = ks_put_super,
    .sync_fs = ks_sync_fs,
    .show_options = ks_show_options,
    .show_stats = ks_show_stats,
    .statfs = ks_statfs,
};
static int ks_fill_super(struct super_block *sb, struct fs_context *fc)
{
	struct ks_context *c = fc->fs_private;
	struct ks_fs *f;
	struct ks_snapshot root;
	struct inode *inode;
	int err;
	unsigned saved;
	if (!c->url || !c->username || !c->password || !memcmp(c->bucket.bytes, (u8[16]){0}, 16))
		return -EINVAL;
	if (current->nsproxy->net_ns != &init_net)
		return -EOPNOTSUPP;
	if (c->bd) {
		if (c->create || c->whitelist || c->boot_lifetime ||
		    memcmp(c->lower.bytes, (u8[16]){0}, 16))
			return -EINVAL;
		err = ks_bd_fill_super(sb, c->url, c->username, c->password, c->bucket, c->timeout,
				       c->readonly || !!(fc->sb_flags & SB_RDONLY), c->device);
		if (err && sb->s_fs_info && !sb->s_root)
			sb->s_op->put_super(sb);
		return err;
	}
	if (c->device)
		return -EINVAL;
	err = super_setup_bdi(sb);
	if (err)
		return err;
	f = kzalloc(sizeof(*f), GFP_KERNEL);
	if (!f)
		return -ENOMEM;
	for (unsigned slot = 0; slot < KS_RPC_SLOTS; slot++)
		mutex_init(&f->rpc[slot].lock);
	mutex_init(&f->sync_lock);
	mutex_init(&f->overlay_lock);
	mutex_init(&f->overlay_cache_lock);
	spin_lock_init(&f->inode_lock);
	INIT_LIST_HEAD(&f->inodes);
	INIT_WORK(&f->invalidation_work, ks_invalidation_worker);
	INIT_DELAYED_WORK(&f->orphan_work, ks_orphan_worker);
	f->sb = sb;
	sb->s_bdi->ra_pages = (1U << 20) / PAGE_SIZE;
	f->readonly = c->readonly || !!(fc->sb_flags & SB_RDONLY);
	if (f->readonly && c->create) {
		kfree(f);
		return -EINVAL;
	}
	f->cache_budget = c->cache_bytes;
	if (c->boot_lifetime)
		f->boot_lifetime = get_file(c->boot_lifetime);
	f->bucket = c->bucket;
	f->url = kstrdup(c->url, GFP_KERNEL);
	if (!f->url) {
		if (f->boot_lifetime)
			fput(f->boot_lifetime);
		kfree(f);
		return -ENOMEM;
	}
	sb->s_xattr = ks_xattrs;
	sb->s_fs_info = f;
	sb->s_op = &ks_super_ops;
	sb->s_blocksize = PAGE_SIZE;
	sb->s_blocksize_bits = PAGE_SHIFT;
	sb->s_magic = KS_MAGIC;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_time_gran = 1;
	sb->s_flags |= SB_NOATIME;
	if (f->readonly)
		sb->s_flags |= SB_RDONLY;
	set_default_d_op(sb, &ks_dentry_ops);
 f->read_queue = alloc_workqueue("kstqr-read", WQ_UNBOUND | WQ_MEM_RECLAIM, KS_RPC_SLOTS);
 if (!f->read_queue) { ks_put_super(sb); return -ENOMEM; }

	saved = enter_fs(f);
	err = 0;
	for (unsigned slot = 0; slot < KS_RPC_SLOTS && !err; slot++)
		err = stqr_dial(&f->rpc[slot].client, c->url, c->username, c->password, c->bucket, "fs",
				c->timeout);
	if (!err)
		f->references = !!(stqr_capabilities(f->rpc[0].client) & STQR_CAP_OBJECT_REFERENCES);
	if (!err)
		err = ks_cache_start(f, c->username, c->password, c->timeout);
	if (!err && memcmp(c->lower.bytes, (u8[16]){0}, 16)) {
		if (!memcmp(c->lower.bytes, c->bucket.bytes, 16))
			err = -EINVAL;
		else {
			struct ks_fs *lower = kzalloc(sizeof(*lower), GFP_KERNEL);
			if (!lower)
				err = -ENOMEM;
			else {
				f->lower = lower;
				lower->parent = f;
				lower->readonly = true;
				lower->bucket = c->lower;
				lower->cache_budget = c->cache_bytes;
				lower->url = kstrdup(c->url, GFP_KERNEL);
				for (unsigned slot = 0; slot < KS_RPC_SLOTS; slot++)
					mutex_init(&lower->rpc[slot].lock);
				mutex_init(&lower->sync_lock);
				spin_lock_init(&lower->inode_lock);
				INIT_LIST_HEAD(&lower->inodes);
				INIT_WORK(&lower->invalidation_work, ks_invalidation_worker);
				if (!lower->url)
					err = -ENOMEM;
				for (unsigned slot = 0; slot < KS_RPC_SLOTS && !err; slot++)
					err =
					    stqr_dial(&lower->rpc[slot].client, c->url, c->username,
						      c->password, c->lower, "fs", c->timeout);
				if (!err)
					lower->references = !!(stqr_capabilities(lower->rpc[0].client) &
							      STQR_CAP_OBJECT_REFERENCES);
				if (!err)
					err = ks_cache_start(lower, c->username, c->password,
							     c->timeout);
			}
		}
	}

	if (!err)
		err = ks_snapshot(f, ks_root_id, &root);
	if (err == -ENOENT && c->create) {
		err = f->lower ? ks_overlay_initialize(f, c->whitelist)
			       : ks_initialize(f, from_kuid(&init_user_ns, current_fsuid()),
					       from_kgid(&init_user_ns, current_fsgid()));
		if (!err || err == -EAGAIN)
			err = ks_snapshot(f, ks_root_id, &root);
	}
	if (!err && f->lower) {
		err = ks_overlay_validate_policy(f, c->whitelist);
		if (err)
			ks_snapshot_free(&root);
	}

	if (!err) {
		if (root.node.kind != 2 || root.node.removed)
			err = -ENOTDIR;
		else {
			inode = get_inode(sb, ks_root_id, &root.node, &root.info);
			if (IS_ERR(inode))
				err = PTR_ERR(inode);
			else {
				sb->s_root = d_make_root(inode);
				if (!sb->s_root)
					err = -ENOMEM;
			}
		}
		ks_snapshot_free(&root);
	}
	leave_fs(f, saved);
	if (err)
		ks_put_super(sb);
	else
		ks_schedule_orphans(f);
	return err;
}
enum {
	Opt_url,
	Opt_bucket,
	Opt_credentials,
	Opt_timeout,
	Opt_init,
	Opt_readonly,
	Opt_cache,
	Opt_lower,
	Opt_whitelist,
	Opt_boot_lifetime,
	Opt_bd,
	Opt_device
};
static const struct fs_parameter_spec ks_parameters[] = {
    fsparam_string("url", Opt_url),
    fsparam_string("bucket", Opt_bucket),
    fsparam_fd("credentials", Opt_credentials),
    fsparam_u32("timeout_ms", Opt_timeout),
    fsparam_u32("metadata_cache_bytes", Opt_cache),
    fsparam_string("readonly_bucket", Opt_lower),
    fsparam_string("whitelist", Opt_whitelist),
    fsparam_fd("boot_lifetime", Opt_boot_lifetime),
    fsparam_flag("bd", Opt_bd),
    fsparam_string("device", Opt_device),
    fsparam_flag("init", Opt_init),
    fsparam_flag("readonly", Opt_readonly),
    {}};
static int credentials(struct ks_context *c, struct file *file)
{
	struct inode *i = file_inode(file);
	char *raw, *line, *next;
	loff_t position = 0;
	ssize_t size;
	int err = 0;
	if (!S_ISREG(i->i_mode) || !(file->f_mode & FMODE_READ) || (i->i_mode & 0077) ||
	    !uid_eq(i->i_uid, current_fsuid()) || i_size_read(i) <= 0 || i_size_read(i) > 8192 ||
	    c->username || c->password)
		return -EACCES;
	raw = kzalloc(8193, GFP_KERNEL);
	if (!raw)
		return -ENOMEM;
	size = kernel_read(file, raw, 8192, &position);
	if (size <= 0) {
		err = size ? size : -EINVAL;
		goto out;
	}
	if (memchr(raw, 0, size)) {
		err = -EINVAL;
		goto out;
	}
	raw[size] = 0;
	line = raw;
	while (line && *line) {
		char *value;
		next = strchr(line, '\n');
		if (next)
			*next++ = 0;
		value = strchr(line, '=');
		if (!value) {
			err = -EINVAL;
			break;
		}
		*value++ = 0;
		if (!strcmp(line, "username") && !c->username)
			c->username = kstrdup(value, GFP_KERNEL);
		else if (!strcmp(line, "password") && !c->password)
			c->password = kstrdup(value, GFP_KERNEL);
		else {
			err = -EINVAL;
			break;
		}
		if ((!strcmp(line, "username") && !c->username) ||
		    (!strcmp(line, "password") && !c->password)) {
			err = -ENOMEM;
			break;
		}
		line = next;
	}
	if (!err && (!c->username || !c->password || !*c->username || !*c->password))
		err = -EINVAL;
out:
	memzero_explicit(raw, 8193);
	kfree(raw);
	return err;
}
static int ks_parse(struct fs_context *fc, struct fs_parameter *param)
{
	struct ks_context *c = fc->fs_private;
	struct fs_parse_result result;
	int opt = fs_parse(fc, ks_parameters, param, &result);
	if (opt < 0)
		return opt;
	switch (opt) {
	case Opt_url:
		kfree(c->url);
		c->url = param->string;
		param->string = NULL;
		break;
	case Opt_bucket:
		return stqr_parse_id(param->string, &c->bucket);
	case Opt_lower: {
		int err = stqr_parse_id(param->string, &c->lower);
		return err ? err : memcmp(c->lower.bytes, (u8[16]){0}, 16) ? 0 : -EINVAL;
	}
	case Opt_whitelist:
		kfree(c->whitelist);
		c->whitelist = param->string;
		param->string = NULL;
		break;
	case Opt_boot_lifetime:
		if (param->type != fs_value_is_file || !S_ISSOCK(file_inode(param->file)->i_mode) ||
		    c->boot_lifetime)
			return -EINVAL;
		c->boot_lifetime = get_file(param->file);
		break;
	case Opt_credentials:
		if (param->type != fs_value_is_file)
			return -EINVAL;
		return credentials(c, param->file);
	case Opt_timeout:
		if (!result.uint_32 || result.uint_32 > 3600000)
			return -EINVAL;
		c->timeout = result.uint_32;
		break;
	case Opt_cache:
		if (result.uint_32 > (64U << 20))
			return -EINVAL;
		c->cache_bytes = result.uint_32;
		break;
	case Opt_bd:
		c->bd = true;
		break;
	case Opt_device:
		kfree(c->device);
		c->device = param->string;
		param->string = NULL;
		break;
	case Opt_init:
		c->create = true;
		break;
	case Opt_readonly:
		c->readonly = true;
		break;
	}
	return 0;
}
static int ks_get_tree(struct fs_context *fc) { return get_tree_nodev(fc, ks_fill_super); }
static void ks_free_context(struct fs_context *fc)
{
	struct ks_context *c = fc->fs_private;
	if (c) {
		kfree(c->url);
		kfree(c->whitelist);
		kfree(c->device);
		if (c->boot_lifetime)
			fput(c->boot_lifetime);
		kfree_sensitive(c->username);
		kfree_sensitive(c->password);
		kfree(c);
	}
}
static const struct fs_context_operations ks_context_ops = {
    .free = ks_free_context, .parse_param = ks_parse, .get_tree = ks_get_tree};
static int ks_init_context(struct fs_context *fc)
{
	struct ks_context *c = kzalloc(sizeof(*c), GFP_KERNEL);
	if (!c)
		return -ENOMEM;
	c->timeout = 30000;
	c->cache_bytes = 8U << 20;
	fc->fs_private = c;
	fc->ops = &ks_context_ops;
	return 0;
}
static void ks_kill_sb(struct super_block *sb)
{
	if (sb->s_magic == 0x53544244)
		ks_bd_kill_sb(sb);
	else
		kill_anon_super(sb);
}
static struct file_system_type ks_type = {
    .owner = THIS_MODULE,
    .name = "kstqr",
    .init_fs_context = ks_init_context,
    .parameters = ks_parameters,
    .kill_sb = ks_kill_sb,
};
static int __init ks_init(void)
{
	int err = ks_transport_start();
	if (err)
		return err;
	err = ks_bd_init();
	if (err) {
		ks_transport_stop();
		return err;
	}
	err = register_filesystem(&ks_type);
	if (err) {
		ks_bd_exit();
		ks_transport_stop();
	}
	return err;
}
static void __exit ks_exit(void)
{
	unregister_filesystem(&ks_type);
	ks_bd_exit();
	rcu_barrier();
	ks_transport_stop();
}
module_init(ks_init);
module_exit(ks_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Standalone STQR network filesystem client");
MODULE_ALIAS_FS("kstqr");
