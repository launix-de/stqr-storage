/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "plain bind line %d: %s (%s)\n", __LINE__, #x, strerror(errno)); exit(1); } } while (0)
static void put(const char *path, const char *value) {
 int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0600);
 CHECK(fd >= 0); CHECK(write(fd, value, strlen(value)) == (ssize_t)strlen(value)); CHECK(!close(fd));
}
static int equals(const char *path, const char *value) {
 char buf[64]; int fd = open(path, O_RDONLY); if (fd < 0) return 0;
 ssize_t n = read(fd, buf, sizeof(buf)); CHECK(!close(fd));
 return n == (ssize_t)strlen(value) && !memcmp(buf, value, n);
}
static void expect(const char *path, const char *value) {
 for (unsigned k = 0; k < 100; k++) { if (equals(path, value)) return; usleep(20000); }
 CHECK(equals(path, value));
}
static void expect_tmpfs(const char *path, const char *marker, dev_t device) {
 struct stat st; struct statfs fs;
 CHECK(!stat(path, &st)); if(st.st_dev != device) fprintf(stderr,"mount lost path=%s expected=%lu actual=%lu\n",path,(unsigned long)device,(unsigned long)st.st_dev); CHECK(st.st_dev == device);
 CHECK(!statfs(path, &fs)); CHECK((unsigned long)fs.f_type == 0x01021994UL);
 expect(marker, "mounted-payload");
}
static dev_t tmpfs_at(const char *path, const char *marker) {
 struct stat st;
 CHECK(!mount("plain-bind-regression", path, "tmpfs", MS_NOSUID | MS_NODEV, "size=1m"));
 CHECK(!stat(path, &st));
 put(marker, "mounted-payload");
 return st.st_dev;
}
static void directory_mounts(void) {
 struct statfs other;
 CHECK(!mkdir("/mnt/mount-proof", 0700));
 CHECK(!mkdir("/mnt/mount-proof/usr", 0700));
 CHECK(!mkdir("/mnt/mount-proof/usr/lib", 0700));
 CHECK(!mkdir("/mnt/mount-proof/usr/lib/boot", 0700));
 CHECK(!symlink("usr/lib", "/mnt/mount-proof/lib"));
 CHECK(!mkdir("/mnt/mount-proof/sys", 0700));
 CHECK(!mkdir("/mnt/mount-proof/sys/fs", 0700));
 CHECK(!mkdir("/mnt/mount-proof/sys/fs/cgroup", 0700));
 put("/mnt/mount-proof/usr/lib/boot/underlying", "underlying-boot");
 put("/mnt/mount-proof/sys/underlying", "underlying-sys");
 put("/mnt/mount-proof/sys/fs/cgroup/underlying", "underlying-cgroup");
 /* The first mount resolves a merged-/usr alias. Two nested mounts then
  * model /sys and /sys/fs/cgroup: losing either changes the reached backend,
  * even when the covered STQR directories happen to contain the same names. */
 dev_t boot = tmpfs_at("/mnt/mount-proof/lib/boot", "/mnt/mount-proof/lib/boot/marker");
 dev_t sys = tmpfs_at("/mnt/mount-proof/sys", "/mnt/mount-proof/sys/marker");
 CHECK(!mkdir("/mnt/mount-proof/sys/fs", 0700));
 CHECK(!mkdir("/mnt/mount-proof/sys/fs/cgroup", 0700));
 dev_t cgroup = tmpfs_at("/mnt/mount-proof/sys/fs/cgroup", "/mnt/mount-proof/sys/fs/cgroup/marker");
 CHECK(boot != sys && sys != cgroup && boot != cgroup);
 /* Mounts are private to this mountpoint. The independently mounted writer
  * still reaches the covered STQR namespace and changes several parent proofs. */
 CHECK(!statfs("/other/mount-proof/sys/fs/cgroup", &other));
 CHECK((unsigned long)other.f_type == 0x53545152UL);
 for (unsigned k = 0; k < 4; k++) {
  char path[160];
  snprintf(path, sizeof(path), "/mnt/mount-proof/local-%u", k); put(path, "local-root");
  snprintf(path, sizeof(path), "/mnt/mount-proof/usr/local-%u", k); put(path, "local-usr");
  snprintf(path, sizeof(path), "/mnt/mount-proof/usr/lib/local-%u", k); put(path, "local-lib");
  snprintf(path, sizeof(path), "/other/mount-proof/remote-%u", k); put(path, "remote-root");
  expect(path, "remote-root");
  snprintf(path, sizeof(path), "/other/mount-proof/usr/remote-%u", k); put(path, "remote-usr");
  snprintf(path, sizeof(path), "/other/mount-proof/usr/lib/remote-%u", k); put(path, "remote-lib");
  snprintf(path, sizeof(path), "/other/mount-proof/sys/remote-%u", k); put(path, "covered-sys");
  snprintf(path, sizeof(path), "/other/mount-proof/sys/fs/remote-%u", k); put(path, "covered-fs");
  snprintf(path, sizeof(path), "/other/mount-proof/sys/fs/cgroup/remote-%u", k); put(path, "covered-cgroup");
  /* Allow the independent connection's subscription notifications to arrive. */
  usleep(300000);
  expect_tmpfs("/mnt/mount-proof/lib/boot", "/mnt/mount-proof/lib/boot/marker", boot);
  expect_tmpfs("/mnt/mount-proof/usr/lib/boot", "/mnt/mount-proof/usr/lib/boot/marker", boot);
  expect_tmpfs("/mnt/mount-proof/sys", "/mnt/mount-proof/sys/marker", sys);
  expect_tmpfs("/mnt/mount-proof/sys/fs/cgroup", "/mnt/mount-proof/sys/fs/cgroup/marker", cgroup);
 }
 CHECK(!umount("/mnt/mount-proof/sys/fs/cgroup"));
 CHECK(!umount("/mnt/mount-proof/sys"));
 CHECK(!umount("/mnt/mount-proof/lib/boot"));
 expect("/mnt/mount-proof/usr/lib/boot/underlying", "underlying-boot");
 expect("/mnt/mount-proof/sys/underlying", "underlying-sys");
 expect("/mnt/mount-proof/sys/fs/cgroup/underlying", "underlying-cgroup");
 puts("KSTQR_PLAIN_DIRECTORY_MOUNTS_PASS alias=preserved nested=preserved covered-writes=isolated");
}
int main(void) {
 directory_mounts();
 CHECK(!mkdir("/mnt/bind-proof", 0700));
 put("/tmp/bind-source", "bound-payload");
 put("/mnt/bind-proof/leaf", "underlying");
 CHECK(!mount("/tmp/bind-source", "/mnt/bind-proof/leaf", NULL, MS_BIND, NULL));
 expect("/mnt/bind-proof/leaf", "bound-payload");
 /* Local and independently mounted writers each change the parent proof. */
 put("/mnt/bind-proof/local-sibling", "local");
 expect("/mnt/bind-proof/leaf", "bound-payload");
 put("/other/bind-proof/remote-sibling", "remote");
 expect("/other/bind-proof/remote-sibling", "remote");
 usleep(300000);
 expect("/mnt/bind-proof/leaf", "bound-payload");
 CHECK(!umount("/mnt/bind-proof/leaf"));
 expect("/mnt/bind-proof/leaf", "underlying");
 /* A real name replacement must still invalidate the old positive dentry. */
 put("/other/bind-proof/replacement", "new-inode");
 CHECK(!rename("/other/bind-proof/replacement", "/other/bind-proof/leaf"));
 expect("/mnt/bind-proof/leaf", "new-inode");
 CHECK(!unlink("/other/bind-proof/leaf"));
 struct stat st; int gone = 0;
 for (unsigned k = 0; k < 100; k++) { if (stat("/mnt/bind-proof/leaf", &st) && errno == ENOENT) { gone = 1; break; } usleep(20000); }
 CHECK(gone);
 /* A cached negative must also become positive when another writer creates. */
 put("/other/bind-proof/leaf", "recreated");
 expect("/mnt/bind-proof/leaf", "recreated");
 puts("KSTQR_PLAIN_BIND_PASS sibling-bind=preserved replacement=visible removed=absent recreated=visible");
 return 0;
}
