// SPDX-License-Identifier: GPL-3.0-only
#include "block.h"
#include "kstqr.h"
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/kref.h>
#include <linux/completion.h>
#include <linux/hashtable.h>
#include <linux/hex.h>
#include <linux/module.h>
#include <linux/pagemap.h>
#include <linux/sched/mm.h>
#include <linux/seq_file.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/vmalloc.h>

struct ks_bd {
	struct kref refs;
	struct list_head disks;
	struct workqueue_struct *queue;
	struct ks_rpc rpc;
    DECLARE_HASHTABLE(pending, 8);

	bool readonly, failed, stopping;
	struct super_block *sb;

	char *url, *selected;
	stqr_id bucket;
};
struct ks_disk {
	struct list_head list;
	struct ks_bd *owner;
	struct ks_bd_meta meta;
	struct gendisk *disk;
	bool added;
	struct mutex pin_lock;
	struct list_head pins;
	struct dentry *entry;
};
struct ks_bio_work {
	struct work_struct work;
	struct bio *bio;
	struct ks_disk *disk;
};
struct ks_bd_pin {
	struct list_head list;
	struct work_struct work;
	struct super_block *sb;
};
static struct workqueue_struct *ks_bd_releases;

/* bdev_file_open_by_path drops its pathname reference after looking up dev_t.
 * Pin the backend superblock explicitly while the block layer holds a device.
 * Release outside the block open mutex: final super teardown deletes disks. */
static void ks_bd_unpin(struct work_struct *work)
{
	struct ks_bd_pin *pin = container_of(work, struct ks_bd_pin, work);
	deactivate_super(pin->sb);
	kfree(pin);
}
static int ks_disk_open(struct gendisk *disk, blk_mode_t mode)
{
	struct ks_disk *d = disk->private_data;
	struct ks_bd_pin *pin;
	if (READ_ONCE(d->owner->stopping) || READ_ONCE(d->owner->failed))
		return -EIO;
	if (mode & BLK_OPEN_WRITE && d->owner->readonly)
		return -EROFS;
	pin = kmalloc(sizeof(*pin), GFP_KERNEL);
	if (!pin)
		return -ENOMEM;
	if (!atomic_inc_not_zero(&d->owner->sb->s_active)) {
		kfree(pin);
		return -ENXIO;
	}
	pin->sb = d->owner->sb;
	INIT_WORK(&pin->work, ks_bd_unpin);
	mutex_lock(&d->pin_lock);
	list_add(&pin->list, &d->pins);
	mutex_unlock(&d->pin_lock);
	return 0;
}
static void ks_disk_release(struct gendisk *disk)
{
	struct ks_disk *d = disk->private_data;
	struct ks_bd_pin *pin;
	mutex_lock(&d->pin_lock);
	pin = list_first_entry(&d->pins, struct ks_bd_pin, list);
	list_del(&pin->list);
	mutex_unlock(&d->pin_lock);
	queue_work(ks_bd_releases, &pin->work);
}
int ks_bd_init(void)
{
	ks_bd_releases = alloc_workqueue("kstqr-bd-release", WQ_UNBOUND | WQ_MEM_RECLAIM, 0);
	return ks_bd_releases ? 0 : -ENOMEM;
}
void ks_bd_exit(void) { destroy_workqueue(ks_bd_releases); }

struct ks_bd_pending {
 struct hlist_node hash;
 stqr_object_id id;
 u64 start,end;
 struct completion done;
 unsigned refs;
};
static void ks_bd_free(struct kref *ref)
{
	struct ks_bd *b = container_of(ref, struct ks_bd, refs);
	/* Every queued bio has completed before the mount drops its reference. */
	ks_rpc_stop(&b->rpc);
    {
        struct ks_bd_pending *p;struct hlist_node *tmp;unsigned bucket;
        hash_for_each_safe(b->pending,bucket,tmp,p,hash){hash_del(&p->hash);kfree(p);}
    }
    stqr_close(b->rpc.client);
	kfree(b->url);
	kfree(b->selected);
	kfree(b);
}
static void ks_disk_free(struct gendisk *disk)
{
	struct ks_disk *d = disk->private_data;
	kref_put(&d->owner->refs, ks_bd_free);
	kfree(d);
}
struct ks_bd_result {
 struct completion done;
 int error,status;
 void *data;
 size_t size;
 stqr_info info;
};
static void ks_bd_completed(void *arg,int error,int status,void *data,size_t size,const stqr_info *info)
{
 struct ks_bd_result *r=arg;
 r->error=error;r->status=status;r->data=data;r->size=size;
 if(info)r->info=*info;
 complete(&r->done);
}
static u32 ks_bd_hash(stqr_object_id id)
{
 u32 hash;memcpy(&hash,id.bytes,sizeof(hash));return hash;
}
static int ks_bd_sync_locked(struct ks_bd *b)
{
 struct ks_bd_result r={0};int err;
 if(!b->rpc.receive_started || !(stqr_capabilities(b->rpc.client)&STQR_CAP_CONNECTION_SYNC))
  return stqr_sync(b->rpc.client);
 init_completion(&r.done);
 err=stqr_connection_sync_async(b->rpc.client,ks_bd_completed,&r);
 mutex_unlock(&b->rpc.lock);
 if(!err){wait_for_completion(&r.done);err=r.error;}
 stqr_free(r.data);
 mutex_lock(&b->rpc.lock);
  return err;
}
static int ks_bd_dependency(struct ks_bd *b,stqr_object_id id,u64 start,u64 end,
                            bool write,struct ks_bd_pending **active)
{
 struct ks_bd_pending *p;
again:
 hash_for_each_possible(b->pending,p,hash,ks_bd_hash(id))
  if(!memcmp(p->id.bytes,id.bytes,32)&&start<p->end&&p->start<end) {
   /* An overlapping write needs the earlier visibility completion, never a
    * persistence barrier. Unrelated ranges remain in flight on this socket. */
   p->refs++;mutex_unlock(&b->rpc.lock);wait_for_completion(&p->done);
   mutex_lock(&b->rpc.lock);if(!--p->refs)kfree(p);goto again;
  }
 if(!write)return 0;
 p=kzalloc(sizeof(*p),GFP_NOFS);if(!p)return -ENOMEM;
 p->id=id;p->start=start;p->end=end;p->refs=1;init_completion(&p->done);
 hash_add(b->pending,&p->hash,ks_bd_hash(id));*active=p;return 0;
}
static void ks_bd_visible(struct ks_bd_pending *p)
{
 if(!p)return;
 hash_del(&p->hash);complete_all(&p->done);if(!--p->refs)kfree(p);
}
static int ks_bd_put_async_locked(struct ks_bd *b,stqr_object_id id,u64 off,
                                  const void *data,size_t n,const stqr_options *o,
                                  stqr_info *info,bool full)
{
 struct ks_bd_result r={0};int err;init_completion(&r.done);
 err=full?stqr_put_result_async(b->rpc.client,id,data,n,o,ks_bd_completed,&r):
          stqr_put_range_result_async(b->rpc.client,id,off,data,n,o,ks_bd_completed,&r);
 mutex_unlock(&b->rpc.lock);
 if(!err){wait_for_completion(&r.done);err=r.error;}
 stqr_free(r.data);mutex_lock(&b->rpc.lock);if(info)*info=r.info;return err;
}
static int ks_bd_delete_async_locked(struct ks_bd *b,stqr_object_id id,const stqr_options *o)
{
 struct ks_bd_result r={0};int err;init_completion(&r.done);
 err=stqr_delete_async(b->rpc.client,id,o,ks_bd_completed,&r);
 mutex_unlock(&b->rpc.lock);
 if(!err){wait_for_completion(&r.done);err=r.error;}
 stqr_free(r.data);mutex_lock(&b->rpc.lock);return err;
}
static int ks_bd_get_async_locked(struct ks_bd *b,stqr_object_id id,u64 off,u64 length,
                                 void **data,size_t *actual,stqr_info *info,bool stat)
{
 struct ks_bd_result r={0};int err;init_completion(&r.done);
 err=stat?stqr_stat_async(b->rpc.client,id,ks_bd_completed,&r):
          stqr_get_range_async(b->rpc.client,id,off,length,ks_bd_completed,&r);
 mutex_unlock(&b->rpc.lock);
 if(!err){wait_for_completion(&r.done);err=r.error;}
 if(!stat&&r.status==400)err=-ERANGE;
 mutex_lock(&b->rpc.lock);
 if(info)*info=r.info;
 if(data)*data=r.data;else stqr_free(r.data);
 if(actual)*actual=r.size;
 return err;
}
static const struct block_device_operations ks_disk_ops;
static int ks_bd_range(struct ks_disk *d, unsigned op, u64 off, void *data, size_t size)
{
	struct ks_bd *b = d->owner;
	int err = 0;
	if (off > d->meta.size || size > d->meta.size - off)
		return -EINVAL;
	while (size && !err) {
		stqr_object_id id;
		stqr_info info;
		stqr_options o = {.accepted = 1};
        struct ks_bd_pending *active=NULL;
		size_t at = off % KS_BD_STRIPE, n = min_t(size_t, size, KS_BD_STRIPE - at);
		err = ks_bd_address(d->meta.id, off / KS_BD_STRIPE, &id);
		if (err)
			break;
        if(stqr_capabilities(b->rpc.client)&STQR_CAP_CONNECTION_SYNC) {
            err=ks_bd_dependency(b,id,at,at+n,op!=REQ_OP_READ,&active);
            if(err)break;
        }
		if (op == REQ_OP_READ) {
			void *raw = NULL;
			size_t actual = 0;
			memset(data, 0, n);
			err = ks_bd_get_async_locked(b,id,at,n,&raw,&actual,&info,false);
			if (err == -ENOENT)
				err = 0;
			else if (err == -ERANGE || stqr_status(b->rpc.client) == 400) {
				/* A stored short stripe has an implicit zero tail. Verify that
				 * fact. */
				err = ks_bd_get_async_locked(b,id,0,0,NULL,NULL,&info,true);
				if (!err && info.length > KS_BD_STRIPE)
					err = -EPROTO;
				else if (!err && at < info.length) {
					err = ks_bd_get_async_locked(b,id,at,min_t(u64,n,info.length-at),
                                                     &raw,&actual,&info,false);
					if (!err && actual > n)
						err = -EPROTO;
					if (!err && actual)
						memcpy(data, raw, actual);
				}
				if (err == -ENOENT)
					err = 0;
			} else if (!err) {
				if (actual > n || info.length > KS_BD_STRIPE)
					err = -EPROTO;
				else if (actual)
					memcpy(data, raw, actual);
			}
			stqr_free(raw);
		} else {
			err = stqr_request_id(&o.request_id);
			if (err) {ks_bd_visible(active);break;}
			if (op == REQ_OP_WRITE)
				err = at == 0 && n == KS_BD_STRIPE
					  ? ks_bd_put_async_locked(b,id,0,data,n,&o,&info,true)
                      : ks_bd_put_async_locked(b,id,at,data,n,&o,&info,false);
			else {
				err = ks_bd_get_async_locked(b,id,0,0,NULL,NULL,&info,true);
				if (err == -ENOENT)
					err = 0;
				else if (!err) {
					if (info.length > KS_BD_STRIPE)
						err = -EPROTO;
					else if (at == 0 && n == KS_BD_STRIPE) {
						err = ks_bd_delete_async_locked(b,id,&o);
						if (err == -ENOENT)
							err = 0;
						/* WRITE without DELETE may still zero the logical
						 * contents. */
						if (err == -EACCES)
							err = ks_bd_put_async_locked(b,id,0,NULL,0,&o,&info,true);
					} else if (at < info.length) {
						size_t stored = min_t(u64, n, info.length - at);
						memset(data, 0, stored);
						err = ks_bd_put_async_locked(b,id,at,data,stored,&o,&info,false);
					}
				}
			}
		}
        ks_bd_visible(active);
		off += n;
		size -= n;
		data = (u8 *)data + n;
	}
	return err;
}
static void ks_bio_copy(struct bio *bio, void *data, bool to_bio)
{
	struct bio_vec vec;
	struct bvec_iter it;
	size_t at = 0;
	bio_for_each_segment(vec, bio, it)
	{
		void *p = kmap_local_page(vec.bv_page);
		if (to_bio)
			memcpy((u8 *)p + vec.bv_offset, (u8 *)data + at, vec.bv_len);
		else
			memcpy((u8 *)data + at, (u8 *)p + vec.bv_offset, vec.bv_len);
		kunmap_local(p);
		at += vec.bv_len;
	}
}
static void ks_bio_worker(struct work_struct *work)
{
	struct ks_bio_work *job = container_of(work, struct ks_bio_work, work);
	struct bio *bio = job->bio;
	struct ks_disk *d = job->disk;
	struct ks_bd *b = d->owner;
	unsigned op = bio_op(bio), nofs = memalloc_nofs_save();
	size_t n = bio->bi_iter.bi_size;
	u64 off = (u64)bio->bi_iter.bi_sector << 9;
	void *data = NULL;
	int err = 0;
	mutex_lock(&b->rpc.lock);
	if (b->failed)
		err = -EIO;
	else if (op != REQ_OP_READ && op != REQ_OP_FLUSH && b->readonly)
		err = -EROFS;
	else if (op != REQ_OP_READ && op != REQ_OP_WRITE && op != REQ_OP_FLUSH &&
		 op != REQ_OP_DISCARD && op != REQ_OP_WRITE_ZEROES)
		err = -EOPNOTSUPP;
	if (!err && (bio->bi_opf & REQ_PREFLUSH || op == REQ_OP_FLUSH))
		err = ks_bd_sync_locked(b);
	if (!err && op != REQ_OP_FLUSH && n) {
		if (n > KS_BD_STRIPE)
			err = -E2BIG;
		else {
			data = kvmalloc(n, GFP_NOFS);
			if (!data)
				err = -ENOMEM;
			else {
				unsigned effective_op = op;
				if (op == REQ_OP_WRITE)
					ks_bio_copy(bio, data, false);
				else if (op == REQ_OP_WRITE_ZEROES && bio->bi_opf & REQ_NOUNMAP) {
					memset(data, 0, n);
					effective_op = REQ_OP_WRITE;
				}
				err = ks_bd_range(d, effective_op, off, data, n);
				if (!err && op == REQ_OP_READ)
					ks_bio_copy(bio, data, true);
			}
		}
	}
	if (!err && bio->bi_opf & REQ_FUA)
		err = ks_bd_sync_locked(b);
	/* Never silently reacquire a lost lease or replay an uncertain mutation. */
	if (err == -EINPROGRESS || err == -ENOTCONN || err == -ECONNRESET || err == -EPIPE ||
	    err == -ETIMEDOUT || err == -ESTALE || err == -EPROTO || err == -EBADMSG ||
	    err == -EIO || err == -EACCES)
		b->failed = true;
	mutex_unlock(&b->rpc.lock);
	kvfree(data);
	memalloc_nofs_restore(nofs);
	bio->bi_status = errno_to_blk_status(err);
	bio_endio(bio);
	kfree(job);
}
static void ks_submit_bio(struct bio *bio)
{
	struct ks_disk *d;
	struct ks_bio_work *job;
	bio = bio_split_to_limits(bio);
	if (!bio)
		return;
	d = bio->bi_bdev->bd_disk->private_data;
	if (READ_ONCE(d->owner->stopping)) {
		bio_io_error(bio);
		return;
	}
	job = kmalloc(sizeof(*job), GFP_NOIO);
	if (!job) {
		bio->bi_status = BLK_STS_RESOURCE;
		bio_endio(bio);
		return;
	}
	job->bio = bio;
	job->disk = d;
	INIT_WORK(&job->work, ks_bio_worker);
	queue_work(d->owner->queue, &job->work);
}
static const struct block_device_operations ks_disk_ops = {
    .owner = THIS_MODULE,
    .submit_bio = ks_submit_bio,
    .free_disk = ks_disk_free,
    .open = ks_disk_open,
    .release = ks_disk_release,
};
static void ks_bd_put_super(struct super_block *sb)
{
	struct ks_bd *b = sb->s_fs_info;
	struct ks_disk *d, *next;
	if (!b)
		return;
	WRITE_ONCE(b->stopping, true);
	list_for_each_entry(d, &b->disks, list) if (d->added) del_gendisk(d->disk);
	if (b->queue) {
		destroy_workqueue(b->queue);
		b->queue = NULL;
		if (!b->readonly && b->rpc.client) {
            int err;
            mutex_lock(&b->rpc.lock);
            err=ks_bd_sync_locked(b);
            mutex_unlock(&b->rpc.lock);
            if(err)pr_err("kstqr: block export final sync failed\n");
        }
	}
	list_for_each_entry_safe(d, next, &b->disks, list)
	{
		list_del(&d->list);
		put_disk(d->disk);
	}
	sb->s_fs_info = NULL;
	kref_put(&b->refs, ks_bd_free);
}
static int ks_bd_sync_fs(struct super_block *sb, int wait)
{
	struct ks_bd *b = sb->s_fs_info;
	int err;
	if (!wait)
		return 0;
	flush_workqueue(b->queue);
	mutex_lock(&b->rpc.lock);
	err = b->failed ? -EIO : ks_bd_sync_locked(b);
	mutex_unlock(&b->rpc.lock);
	return err;
}
static int ks_bd_show_options(struct seq_file *m, struct dentry *root)
{
	struct ks_bd *b = root->d_sb->s_fs_info;
	char bucket[33];
	bin2hex(bucket, b->bucket.bytes, 16);
	bucket[32] = 0;
	seq_puts(m, ",bd");
	seq_show_option(m, "url", b->url);
	seq_show_option(m, "bucket", bucket);
	if (b->selected)
		seq_show_option(m, "device", b->selected);
	return 0;
}
static const struct super_operations ks_bd_super_ops = {
    .statfs = simple_statfs,
    .put_super = ks_bd_put_super,
	.sync_fs = ks_bd_sync_fs,
	.show_options = ks_bd_show_options,
};
void ks_bd_kill_sb(struct super_block *sb)
{
	struct ks_bd *b = sb->s_fs_info;
	struct ks_disk *d;
	/* Synthetic positive dentries are pinned while exported, unlike the ordinary
	 * STFS lookup cache. Drop those references before generic shutdown shrinks it. */
	if (b)
		list_for_each_entry(d, &b->disks, list)
		{
			if (d->entry) {
				dput(d->entry);
				d->entry = NULL;
			}
		}
	kill_anon_super(sb);
}
static int ks_bd_add(struct super_block *sb, struct ks_bd_meta *m, const char *name)
{
	struct ks_bd *b = sb->s_fs_info;
	struct ks_disk *d;
	struct inode *inode;
	struct dentry *entry;
	struct queue_limits lim = {
	    .logical_block_size = 512,
	    .physical_block_size = 4096,
	    .io_min = 4096,
	    .io_opt = KS_BD_STRIPE,
	    .max_hw_sectors = KS_BD_STRIPE / 512,
	    .max_segments = 256,
	    .max_segment_size = KS_BD_STRIPE,
	    .max_hw_discard_sectors = KS_BD_STRIPE / 512,
	    .max_write_zeroes_sectors = KS_BD_STRIPE / 512,
		/* Splits of a long trim must align to allocation units, so fully
		 * covered stripes remain deletable. Partial trims still zero bytes. */
		.discard_granularity = KS_BD_STRIPE,
	    .features = BLK_FEAT_WRITE_CACHE | BLK_FEAT_FUA,
	};
	if (!*name || strchr(name, '/') || !strcmp(name, ".") || !strcmp(name, ".."))
		return -EINVAL;
	list_for_each_entry(d, &b->disks,
			    list) if (!memcmp(d->meta.id.bytes, m->id.bytes, 16)) return -EPROTO;
	d = kzalloc(sizeof(*d), GFP_KERNEL);
	if (!d)
		return -ENOMEM;
	mutex_init(&d->pin_lock);
	INIT_LIST_HEAD(&d->pins);
	d->owner = b;
	d->meta = *m;
	d->disk = blk_alloc_disk(&lim, NUMA_NO_NODE);
	if (IS_ERR(d->disk)) {
		int err = PTR_ERR(d->disk);
		kfree(d);
		return err;
	}
	kref_get(&b->refs);
	d->disk->private_data = d;
	d->disk->fops = &ks_disk_ops;
	snprintf(d->disk->disk_name, DISK_NAME_LEN, "kstqr%u", get_next_ino());
	set_capacity(d->disk, m->size / 512);
	set_disk_ro(d->disk, b->readonly);
	list_add_tail(&d->list, &b->disks);
	int err = add_disk(d->disk);
	if (err)
		return err;
	d->added = true;
	inode = new_inode(sb);
	if (!inode)
		return -ENOMEM;
	inode->i_ino = get_next_ino();
	inode->i_uid = GLOBAL_ROOT_UID;
	inode->i_gid = GLOBAL_ROOT_GID;
	init_special_inode(inode, S_IFBLK | (b->readonly ? 0400 : 0600), disk_devt(d->disk));
	entry = d_alloc_name(sb->s_root, name);
	if (!entry) {
		iput(inode);
		return -ENOMEM;
	}
	d_add(entry, inode);
	d->entry = entry;
	return 0;
}
int ks_bd_fill_super(struct super_block *sb, const char *url, const char *user,
		     const char *password, stqr_id bucket, unsigned timeout, bool readonly,
		     const char *selected)
{
	struct ks_bd *b;
	struct ks_bd_meta *page;
	struct inode *root;
	stqr_info info;
	stqr_object_id after = {{0}};
	bool more = false;
	unsigned count, found = 0;
	int err;
	b = kzalloc(sizeof(*b), GFP_KERNEL);
	if (!b)
		return -ENOMEM;
	kref_init(&b->refs);
	INIT_LIST_HEAD(&b->disks);
	b->readonly = readonly;
	b->sb = sb;
	b->bucket = bucket;
	mutex_init(&b->rpc.lock);
    hash_init(b->pending);
	sb->s_fs_info = b;
	sb->s_op = &ks_bd_super_ops;
	sb->s_magic = 0x53544244;
	sb->s_blocksize = PAGE_SIZE;
	sb->s_blocksize_bits = PAGE_SHIFT;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_flags |= SB_NOATIME;
	b->url = kstrdup(url, GFP_KERNEL);
	b->selected = selected ? kstrdup(selected, GFP_KERNEL) : NULL;
	if (!b->url || (selected && !b->selected))
		return -ENOMEM;
	b->queue = alloc_workqueue("kstqr-bd", WQ_UNBOUND | WQ_MEM_RECLAIM, 0);
	if (!b->queue)
		return -ENOMEM;
	err = stqr_dial(&b->rpc.client, url, user, password, bucket, "bd", timeout);
	if (err)
		return err;
	err = ks_bd_claim(b->rpc.client, readonly);
	if (err)
		return err;
	err = stqr_stat(b->rpc.client, ks_root_id, &info);
	if (err != -ENOENT)
		return err ? err : -EINVAL;
    ks_rpc_start(&b->rpc,NULL);
	root = new_inode(sb);
	if (!root)
		return -ENOMEM;
	root->i_ino = 1;
	root->i_mode = S_IFDIR | 0700;
	root->i_op = &simple_dir_inode_operations;
	root->i_fop = &simple_dir_operations;
	set_nlink(root, 2);
	sb->s_root = d_make_root(root);
	if (!sb->s_root)
		return -ENOMEM;
	page = kcalloc(32, sizeof(*page), GFP_KERNEL);
	if (!page)
		return -ENOMEM;
	do {
		mutex_lock(&b->rpc.lock);
		err = ks_bd_inventory(b->rpc.client, &after, &more, page, &count);
		mutex_unlock(&b->rpc.lock);
		for (unsigned j = 0; j < count && !err; j++) {
			if (selected && strcmp(selected, page[j].name))
				continue;
			if (++found > 256) {
				err = -E2BIG;
				break;
			}
			err = ks_bd_add(sb, &page[j], selected ? "bd" : page[j].name);
		}
	} while (!err && more);
	kfree(page);
	return err ? err : found ? 0 : -ENODEV;
}
