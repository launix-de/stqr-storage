/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
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
int main(void) {
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
