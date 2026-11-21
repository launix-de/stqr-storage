/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
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
    char buf[128] = {0};
    int fd, old, parent;
    struct stat st;
    char *mapped;
    setvbuf(stdout, NULL, _IOLBF, 0);
    puts("kstqr: Go-format read and cross-mount visibility");
    fd = open("/mnt/go.txt", O_RDWR);
    REQUIRE(fd >= 0);
    REQUIRE(read(fd, buf, sizeof(buf)) == 13 && !memcmp(buf, "hello from Go", 13));
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
    REQUIRE(read(fd, buf, 13) == 13 && buf[0] == 'H');
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
    {
        pid_t child = fork();
        int j, status;
        REQUIRE(child >= 0);
        fd = open(child ? "/mnt/append" : "/other/append", O_WRONLY | O_APPEND);
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
    puts("KSTQR_SMOKE_PASS");
    return 0;
}
