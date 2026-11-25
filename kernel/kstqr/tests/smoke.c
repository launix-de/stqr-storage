/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>
#include "subscription_wait.h"
#define REQUIRE(x)                                                                                 \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "kstqr test line %d: %s: %s\n", __LINE__, #x, strerror(errno));        \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)
static void save(const char *path, const char *text)
{
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0640);
    REQUIRE(fd >= 0);
    REQUIRE(write(fd, text, strlen(text)) == (ssize_t)strlen(text));
    REQUIRE(!fsync(fd));
    REQUIRE(!close(fd));
}
int main(void)
{
    struct statvfs space;
    REQUIRE(!statvfs("/mnt", &space));
    REQUIRE(space.f_blocks > 0 && space.f_bavail > 0 && space.f_bfree <= space.f_blocks);
    REQUIRE(space.f_frsize == 4096 && space.f_namemax == 255);
    char buf[128] = {0};
    int fd, old, parent;
    struct stat st;
    char *mapped;
    setvbuf(stdout, NULL, _IOLBF, 0);
    puts("kstqr: Go-format read and cross-mount visibility");
    fd = open("/mnt/go.txt", O_RDWR);
    REQUIRE(fd >= 0);
    REQUIRE(read(fd, buf, sizeof(buf)) == 13 && !memcmp(buf, "hello from Go", 13));
    puts("kstqr: hardlinks, symlinks and xattrs");
    REQUIRE(!fstat(fd, &st) && st.st_nlink == 2);
    REQUIRE(readlink("/mnt/go-link", buf, sizeof(buf)) == 6 && !memcmp(buf, "go.txt", 6));
    REQUIRE(!link("/mnt/go.txt", "/mnt/third-link"));
    REQUIRE(!fstat(fd, &st) && st.st_nlink == 3);
    REQUIRE(!unlink("/mnt/third-link"));
    REQUIRE(!fstat(fd, &st) && st.st_nlink == 2);
    REQUIRE(getxattr("/mnt/go.txt", "user.from-go", buf, sizeof(buf)) == 11 &&
            !memcmp(buf, "Go metadata", 11));
    REQUIRE(!setxattr("/mnt/go.txt", "user.kernel", "kernel metadata", 15, 0));
    REQUIRE(setxattr("/mnt/go.txt", "user.kernel", "x", 1, XATTR_CREATE) < 0 && errno == EEXIST);
    REQUIRE(!setxattr("/mnt/go.txt", "user.empty", "", 0, 0));
    REQUIRE(getxattr("/mnt/go.txt", "user.empty", NULL, 0) == 0);
    REQUIRE(!removexattr("/mnt/go.txt", "user.empty"));
    REQUIRE(getxattr("/mnt/go.txt", "user.empty", buf, sizeof(buf)) < 0 && errno == ENODATA);
    {
        unsigned char *value = malloc(60000), *copy = malloc(60000);
        int j;
        REQUIRE(value && copy);
        for (j = 0; j < 60000; j++)
            value[j] = (unsigned char)(j * 13);
        REQUIRE(!setxattr("/mnt/go.txt", "user.large", value, 60000, 0));
        REQUIRE(getxattr("/mnt/go.txt", "user.large", NULL, 0) == 60000);
        REQUIRE(getxattr("/mnt/go.txt", "user.large", copy, 60000) == 60000 &&
                !memcmp(copy, value, 60000));
        REQUIRE(getxattr("/mnt/go.txt", "user.large", buf, 8) < 0 && errno == ERANGE);
        REQUIRE(listxattr("/mnt/go.txt", NULL, 0) > 0);
        free(value);
        free(copy);
    }
    old = open("/mnt/go.txt", O_RDONLY);
    REQUIRE(old >= 0);
    mapped = mmap(NULL, 13, PROT_READ, MAP_SHARED, old, 0);
    REQUIRE(mapped != MAP_FAILED);
    REQUIRE(!close(old));
    REQUIRE(mapped[0] == 'h');
    REQUIRE(pwrite(fd, "H", 1, 0) == 1);
    REQUIRE(mapped[0] == 'H');
    REQUIRE(!munmap(mapped, 13));
    REQUIRE(!close(fd));
    fd = open("/readonly/go.txt", O_RDONLY);
    REQUIRE(fd >= 0);
    REQUIRE(test_pread_eventually(fd,buf,13,0,"Hello from Go") == 13);
    REQUIRE(!close(fd));
    REQUIRE(open("/readonly/must-not-exist", O_CREAT | O_WRONLY, 0600) < 0 && errno == EROFS);
    puts("kstqr: namespace, durable rename and open-unlink");
    REQUIRE(!mkdir("/mnt/a", 0750));
    REQUIRE(!mkdir("/mnt/b", 0755));
    save("/mnt/a/source", "source");
    save("/mnt/b/target", "old");
    old = open("/mnt/b/target", O_RDONLY);
    REQUIRE(old >= 0);
    REQUIRE(!rename("/mnt/a/source", "/mnt/b/target"));
    REQUIRE(read(old, buf, sizeof(buf)) == 3 && !memcmp(buf, "old", 3));
    REQUIRE(!close(old));
    fd = open("/mnt/b/target", O_RDWR | O_APPEND);
    REQUIRE(fd >= 0);
    REQUIRE(write(fd, "!", 1) == 1);
    REQUIRE(!unlink("/mnt/b/target"));
    REQUIRE(pread(fd, buf, sizeof(buf), 0) == 7 && !memcmp(buf, "source!", 7));
    REQUIRE(write(fd, "!", 1) == 1);
    REQUIRE(!close(fd));
    REQUIRE(!rmdir("/mnt/a"));
    REQUIRE(!rmdir("/mnt/b"));
    REQUIRE(!mkdir("/mnt/moving", 0755));
    REQUIRE(!mkdir("/mnt/destination", 0755));
    REQUIRE(!rename("/mnt/moving", "/mnt/destination/moved"));
    REQUIRE(!rmdir("/mnt/destination/moved"));
    REQUIRE(!rmdir("/mnt/destination"));
    puts("kstqr: repeated overwrite, truncation and open-unlink lifetime");
    {
        unsigned char before[8192], after[8192];
        struct stat first, current;
        fd = open("/mnt/reuse-lifetime", O_CREAT | O_EXCL | O_RDWR, 0600);
        REQUIRE(fd >= 0);
        memset(before, 'a', sizeof(before));
        REQUIRE(pwrite(fd, before, sizeof(before), 0) == sizeof(before));
        REQUIRE(!fsync(fd) && !fstat(fd, &first));
        for (unsigned step = 0; step < 100; step++) {
            memset(before, (int)step, sizeof(before));
            REQUIRE(pwrite(fd, before, sizeof(before), 0) == sizeof(before));
            REQUIRE(!fsync(fd));
            REQUIRE(pread(fd, after, sizeof(after), 0) == sizeof(after));
            REQUIRE(!memcmp(before, after, sizeof(before)));
            REQUIRE(!fstat(fd, &current));
            REQUIRE(current.st_ino == first.st_ino && current.st_size == sizeof(before));
        }
        REQUIRE(!ftruncate(fd, 1024) && !fsync(fd));
        REQUIRE(!ftruncate(fd, sizeof(before)) && !fsync(fd));
        REQUIRE(pread(fd, after, sizeof(after), 0) == sizeof(after));
        REQUIRE(!memcmp(before, after, 1024));
        for (size_t at = 1024; at < sizeof(after); at++)
            REQUIRE(after[at] == 0);
        REQUIRE(!unlink("/mnt/reuse-lifetime"));
        REQUIRE(pread(fd, after, 1024, 0) == 1024 && !memcmp(before, after, 1024));
        REQUIRE(pwrite(fd, "still-open", 10, 0) == 10 && !fsync(fd));
        REQUIRE(!close(fd));
        REQUIRE(open("/mnt/reuse-lifetime", O_RDONLY) < 0 && errno == ENOENT);
    }
    puts("kstqr: readonly cross-mount open survives final namespace unlink");
    {
        unsigned char expected[8192], actual[8192];
        int reader = -1;
        memset(expected, 'r', sizeof(expected));
        fd = open("/mnt/readonly-orphan", O_CREAT | O_EXCL | O_RDWR, 0600);
        REQUIRE(fd >= 0);
        REQUIRE(write(fd, expected, sizeof(expected)) == sizeof(expected) && !fsync(fd));
        for (unsigned attempt = 0; attempt < 100 && reader < 0; attempt++) {
            reader = open("/readonly/readonly-orphan", O_RDONLY);
            if (reader < 0) {
                REQUIRE(errno == ENOENT);
                usleep(10000);
            }
        }
        REQUIRE(reader >= 0);
        REQUIRE(!unlink("/mnt/readonly-orphan"));
        REQUIRE(!close(fd));
        REQUIRE(pread(reader, actual, sizeof(actual), 0) == sizeof(actual));
        REQUIRE(!memcmp(expected, actual, sizeof(expected)));
        REQUIRE(!close(reader));
    }
    REQUIRE(!mkdir("/mnt/gone", 0755));
    parent = open("/mnt/gone", O_RDONLY | O_DIRECTORY);
    REQUIRE(parent >= 0);
    REQUIRE(!rmdir("/mnt/gone"));
    REQUIRE(mkdirat(parent, "late", 0700) < 0);
    REQUIRE(!close(parent));
    puts("kstqr: rename flags and directory replacement");
    save("/mnt/one", "one");
    save("/mnt/two", "two");
    REQUIRE(syscall(SYS_renameat2, AT_FDCWD, "/mnt/one", AT_FDCWD, "/mnt/two", RENAME_NOREPLACE) <
                0 &&
            errno == EEXIST);
    REQUIRE(!syscall(SYS_renameat2, AT_FDCWD, "/mnt/one", AT_FDCWD, "/mnt/two", RENAME_EXCHANGE));
    fd = open("/mnt/one", O_RDONLY);
    REQUIRE(fd >= 0);
    REQUIRE(read(fd, buf, sizeof(buf)) == 3 && !memcmp(buf, "two", 3));
    REQUIRE(!close(fd));
    REQUIRE(!mkdir("/mnt/dir1", 0755));
    REQUIRE(!mkdir("/mnt/dir2", 0755));
    REQUIRE(!rename("/mnt/dir1", "/mnt/dir2"));
    REQUIRE(!rmdir("/mnt/dir2"));
    puts("kstqr: segmented writes, sparse files and permissions");
    fd = open("/mnt/sparse", O_CREAT | O_RDWR | O_TRUNC, 0600);
    REQUIRE(fd >= 0);
    {
        size_t length = (2U << 20) + 37, j;
        unsigned char *data = malloc(length), *copy = malloc(length);
        REQUIRE(data && copy);
        for (j = 0; j < length; j++)
            data[j] = (unsigned char)(j * 17);
        REQUIRE(write(fd, data, length) == (ssize_t)length);
        REQUIRE(pread(fd, copy, length, 0) == (ssize_t)length && !memcmp(data, copy, length));
        free(data);
        free(copy);
    }
    REQUIRE(!ftruncate(fd, ((off_t)1 << 40)));
    REQUIRE(pread(fd, buf, 16, ((off_t)1 << 40) - 16) == 16);
    REQUIRE(!memcmp(buf, (char[16]){0}, 16));
    REQUIRE(pwrite(fd, "end", 3, ((off_t)1 << 40) - 3) == 3);
    REQUIRE(!fstat(fd, &st) && st.st_size == ((off_t)1 << 40));
    REQUIRE(pread(fd, buf, 3, ((off_t)1 << 40) - 3) == 3 && !memcmp(buf, "end", 3));
    REQUIRE(!ftruncate(fd, 3));
    REQUIRE(!fchmod(fd, 0640));
    REQUIRE(!fstat(fd, &st) && (st.st_mode & 0777) == 0640 && st.st_size == 3);
    REQUIRE(!fsync(fd));
    REQUIRE(!close(fd));
    puts("kstqr: concurrent append through independent mounts");
    save("/mnt/append", "");
    fd = open("/mnt/append", O_RDWR);
    REQUIRE(fd >= 0 && !ftruncate(fd, 8192) && !fsync(fd));
    REQUIRE(!close(fd));
    {
        pid_t child = fork();
        int j, status;
        REQUIRE(child >= 0);
        fd = child ? open("/mnt/append",O_WRONLY|O_APPEND) : test_open_eventually("/other/append",O_WRONLY|O_APPEND);
        REQUIRE(fd >= 0);
        memset(buf, child ? 'A' : 'B', 64);
        for (j = 0; j < 20; j++)
            REQUIRE(write(fd, buf, 64) == 64);
        REQUIRE(!close(fd));
        if (!child)
            _exit(0);
        REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
        fd = open("/mnt/append", O_RDONLY);
        REQUIRE(fd >= 0);
        {
            double end=test_monotonic()+2;
            do { REQUIRE(!fstat(fd,&st)); if(st.st_size==8192+2560)break;usleep(1000); }while(test_monotonic()<end);
            if(st.st_size!=8192+2560)fprintf(stderr,"concurrent append: actual size=%lld expected=%d\n",(long long)st.st_size,8192+2560);
            REQUIRE(st.st_size==8192+2560);
            REQUIRE(lseek(fd, 8192, SEEK_SET) == 8192);
            int a = 0, b = 0;
            for (j = 0; j < 40; j++) {
                int k;
                REQUIRE(read(fd, buf, 64) == 64);
                REQUIRE(buf[0] == 'A' || buf[0] == 'B');
                for (k = 1; k < 64; k++)
                    REQUIRE(buf[k] == buf[0]);
                if (buf[0] == 'A')
                    a++;
                else
                    b++;
            }
            REQUIRE(a == 20 && b == 20);
            REQUIRE(read(fd, buf, 64) == 0);
        }
        REQUIRE(!close(fd));
    }
    save("/mnt/kernel.txt", "written by kstqr");
    REQUIRE(!symlink("kernel.txt", "/mnt/kernel-link"));
    REQUIRE(!lstat("/mnt/kernel-link", &st) && S_ISLNK(st.st_mode));
    REQUIRE(!symlink("missing", "/mnt/broken"));
    REQUIRE(open("/mnt/broken", O_RDONLY) < 0 && errno == ENOENT);
    REQUIRE(!symlink("cycle-b", "/mnt/cycle-a"));
    REQUIRE(!symlink("cycle-a", "/mnt/cycle-b"));
    REQUIRE(open("/mnt/cycle-a", O_RDONLY) < 0 && errno == ELOOP);
    REQUIRE(getxattr("/readonly/go.txt", "user.kernel", buf, sizeof(buf)) == 15 &&
            !memcmp(buf, "kernel metadata", 15));
    REQUIRE(setxattr("/readonly/go.txt", "user.no", "x", 1, 0) < 0 && errno == EROFS);
    puts("kstqr: FIFO and local Unix sockets");
    REQUIRE(!mkfifo("/mnt/fifo", 0600));
    fd = open("/mnt/fifo", O_RDWR | O_NONBLOCK);
    REQUIRE(fd >= 0);
    REQUIRE(write(fd, "pipe", 4) == 4 && read(fd, buf, sizeof(buf)) == 4 &&
            !memcmp(buf, "pipe", 4));
    REQUIRE(!close(fd));
    REQUIRE(!lstat("/mnt/fifo", &st) && S_ISFIFO(st.st_mode));
    {
        struct sockaddr_un addr = {.sun_family = AF_UNIX};
        int listener, peer, client;
        strcpy(addr.sun_path, "/mnt/socket");
        listener = socket(AF_UNIX, SOCK_STREAM, 0);
        REQUIRE(listener >= 0);
        REQUIRE(!bind(listener, (struct sockaddr *)&addr, sizeof(addr)));
        REQUIRE(!listen(listener, 1));
        client = socket(AF_UNIX, SOCK_STREAM, 0);
        REQUIRE(client >= 0);
        REQUIRE(!connect(client, (struct sockaddr *)&addr, sizeof(addr)));
        peer = accept(listener, NULL, NULL);
        REQUIRE(peer >= 0);
        REQUIRE(write(client, "socket", 6) == 6);
        REQUIRE(read(peer, buf, sizeof(buf)) == 6 && !memcmp(buf, "socket", 6));
        REQUIRE(!close(peer));
        REQUIRE(!close(client));
        REQUIRE(!close(listener));
        REQUIRE(!lstat("/mnt/socket", &st) && S_ISSOCK(st.st_mode));
    }
    puts("KSTQR_SMOKE_PASS");
    return 0;
}
