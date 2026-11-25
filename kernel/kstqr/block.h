/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef KSTQR_BLOCK_H
#define KSTQR_BLOCK_H
#include "stqr.h"
#include <linux/fs.h>
#define KS_BD_STRIPE (1U << 20)
struct ks_bd_meta {
	stqr_id id;
	u64 size;
	char name[256];
};
int ks_bd_fill_super(struct super_block *, const char *, const char *, const char *, stqr_id,
		     unsigned, bool, const char *);
int ks_bd_claim(stqr_client *, bool);
int ks_bd_inventory(stqr_client *, stqr_object_id *, bool *, struct ks_bd_meta *, unsigned *);
int ks_bd_address(stqr_id, u64, stqr_object_id *);
int ks_bd_init(void);
void ks_bd_exit(void);
void ks_bd_kill_sb(struct super_block *);
#endif
