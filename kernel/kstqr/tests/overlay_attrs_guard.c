/* SPDX-License-Identifier: GPL-3.0-only */
#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
typedef unsigned char u8;
typedef struct {
  u8 bytes[32];
} stqr_object_id;
struct ks_cache_stamp {
  unsigned token;
};
struct ks_directory_proof {
  struct ks_cache_stamp root, lower;
  stqr_object_id lower_id;
  bool valid, has_lower, attrs_valid, row_present;
  u8 row_digest[32];
};
struct ks_node {
  bool removed;
};
struct ov_dir {
  stqr_object_id lower;
  struct ks_node node;
  u8 row_digest[32];
};
struct ks_overlay_state {
  unsigned info;
};
struct ks_fs {
  struct ks_fs *lower;
};
static struct ks_overlay_state state;
static struct ov_dir row;
static bool present, lower_current, root_current;
static int read_error;
static unsigned captures, releases;
static stqr_object_id ks_root_id;
static int ov_state(struct ks_fs *f, struct ks_overlay_state **out) {
  (void)f;
  captures++;
  *out = &state;
  return read_error;
}
static struct ov_dir *ov_find(struct ks_overlay_state *s, stqr_object_id id) {
  (void)s;
  (void)id;
  return present ? &row : NULL;
}
static bool eqid(stqr_object_id a, stqr_object_id b) {
  return !memcmp(&a, &b, sizeof(a));
}
static bool ks_cache_stamp_current(struct ks_fs *f, stqr_object_id id,
                                   const struct ks_cache_stamp *s) {
  (void)f;
  (void)id;
  (void)s;
  return lower_current;
}
static bool ks_cache_stamp(struct ks_fs *f, stqr_object_id id, unsigned *info,
                           struct ks_cache_stamp *out) {
  (void)f;
  (void)id;
  (void)info;
  out->token = 9;
  return root_current;
}
static void ov_put(struct ks_overlay_state *s) {
  assert(s == &state);
  releases++;
}
#include "overlay_attrs_guard_actual.h"
static struct ks_directory_proof initial(void) {
  struct ks_directory_proof p = {.valid = true,
                                 .attrs_valid = true,
                                 .has_lower = true,
                                 .row_present = true,
                                 .lower_id = {{8}},
                                 .row_digest = {5}};
  present = lower_current = root_current = true;
  read_error = 0;
  row = (struct ov_dir){.lower = {{8}}, .row_digest = {5}};
  return p;
}
int main(void) {
  struct ks_fs lower = {0}, f = {.lower = &lower};
  struct ks_directory_proof before = initial(), after;
  stqr_object_id id = {{7}};
  assert(ks_overlay_refresh_directory_attrs(&f, id, &before, &after) == 1);
  assert(after.root.token == 9);
  assert(!memcmp(&after.lower, &before.lower, sizeof(before.lower)));
  row.row_digest[0]++;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  row.node.removed = true;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  present = false;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  before.row_present = false;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  present = false;
  before.lower_id = id;
  assert(ks_overlay_refresh_directory_attrs(&f, id, &before, &after) == 1);
  before = initial();
  lower_current = false;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  row.lower.bytes[0]++;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  root_current = false;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  before = initial();
  read_error = -EIO;
  assert(ks_overlay_refresh_directory_attrs(&f, id, &before, &after) == -EIO);
  before = initial();
  before.attrs_valid = false;
  unsigned n = captures;
  assert(!ks_overlay_refresh_directory_attrs(&f, id, &before, &after));
  assert(captures == n);
  assert(captures == releases + 1);
  puts("overlay attributes exact-row/lower/root/error guards passed");
  return 0;
}
