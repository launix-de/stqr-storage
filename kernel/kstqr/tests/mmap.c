/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#define REQUIRE(x)                                                                                 \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "mmap test line %d: %s: %s\n", __LINE__, #x, strerror(errno));         \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)
static void sql(sqlite3 *db, const char *statement)
{
    char *message = NULL;
    int rc = sqlite3_exec(db, statement, NULL, NULL, &message);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQLite %s: %s (%d)\n", statement, message ? message : sqlite3_errmsg(db),
                rc);
        sqlite3_free(message);
        exit(1);
    }
}
static int rowcount(sqlite3 *db)
{
    sqlite3_stmt *stmt = NULL;
    int count;
    REQUIRE(sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &stmt, NULL) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    count = sqlite3_column_int(stmt, 0);
    REQUIRE(sqlite3_finalize(stmt) == SQLITE_OK);
    return count;
}
static void integrity(sqlite3 *db)
{
    sqlite3_stmt *stmt = NULL;
    REQUIRE(sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt, NULL) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW &&
            !strcmp((const char *)sqlite3_column_text(stmt, 0), "ok"));
    REQUIRE(sqlite3_finalize(stmt) == SQLITE_OK);
}
static void sqlite_test(void)
{
    sqlite3 *db = NULL;
    pid_t child;
    int j, status;
    puts("mmap: SQLite WAL and shared-memory locking");
    REQUIRE(sqlite3_open("/mnt/mmap-sqlite.db", &db) == SQLITE_OK);
    sqlite3_busy_timeout(db, 10000);
    sql(db, "PRAGMA journal_mode=WAL; PRAGMA mmap_size=1048576; CREATE TABLE t(id INTEGER PRIMARY "
            "KEY,value TEXT); INSERT INTO t VALUES(1,'start');");
    REQUIRE(sqlite3_close(db) == SQLITE_OK);
    child = fork();
    REQUIRE(child >= 0);
    REQUIRE(sqlite3_open("/mnt/mmap-sqlite.db", &db) == SQLITE_OK);
    sqlite3_busy_timeout(db, 10000);
    sql(db, "PRAGMA mmap_size=1048576");
    if (!child) {
        for (j = 0; j < 20; j++)
            sql(db, "INSERT INTO t(value) VALUES('shared mapped WAL')");
        REQUIRE(sqlite3_close(db) == SQLITE_OK);
        _exit(0);
    }
    for (j = 0; j < 20; j++) {
        int n = rowcount(db);
        REQUIRE(n >= 1 && n <= 21);
        struct timespec delay = {.tv_nsec = 1000000};
        nanosleep(&delay, NULL);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    REQUIRE(rowcount(db) == 21);
    integrity(db);
    REQUIRE(sqlite3_close(db) == SQLITE_OK);
}
static void control(const char *endpoint, const char *action)
{
    char url[256];
    pid_t child;
    int status;
    REQUIRE(snprintf(url, sizeof(url), "%s/__kstqr_test/%s", endpoint, action) < (int)sizeof(url));
    child = fork();
    REQUIRE(child >= 0);
    if (!child) {
        execl("/bin/wget", "wget", "-q", "-O", "/dev/null", url, (char *)NULL);
        _exit(127);
    }
    REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
}
static void failed_writeback(const char *endpoint)
{
    int fd, err, attempt;
    char *p;
    puts("mmap: failed writeback reports an error and retains dirty data");
    fd = open("/mnt/mmap-error", O_CREAT | O_TRUNC | O_RDWR, 0600);
    REQUIRE(fd >= 0);
    REQUIRE(!ftruncate(fd, 4096));
    p = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    REQUIRE(p != MAP_FAILED);
    memcpy(p, "initial", 7);
    REQUIRE(!msync(p, 4096, MS_SYNC));
    control(endpoint, "lock");
    memcpy(p, "recovered", 9);
    err = msync(p, 4096, MS_SYNC);
    REQUIRE(err < 0 && (errno == EACCES || errno == EIO || errno == EROFS));
    REQUIRE(!memcmp(p, "recovered", 9));
    control(endpoint, "unlock");
    for (attempt = 0; attempt < 3; attempt++) {
        err = fsync(fd);
        if (!err)
            break;
        REQUIRE(errno == EACCES || errno == EIO || errno == EROFS);
    }
    REQUIRE(!err);
    REQUIRE(!memcmp(p, "recovered", 9));
    REQUIRE(!munmap(p, 4096));
    REQUIRE(!close(fd));
}
int main(int argc, char **argv)
{
    const size_t length = 8192 + 37;
    int fd, other, status;
    char buf[64] = {0};
    char *a, *b;
    struct stat st;
    (void)argv;
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc > 1 && !strcmp(argv[1], "verify")) {
        sqlite3 *db = NULL;
        fd = open("/readonly/mmap-file", O_RDONLY);
        REQUIRE(fd >= 0);
        REQUIRE(read(fd, buf, 12) == 12 && !memcmp(buf, "mmap durable", 12));
        REQUIRE(!close(fd));
        fd = open("/readonly/mmap-background", O_RDONLY);
        REQUIRE(fd >= 0);
        REQUIRE(read(fd, buf, 10) == 10 && !memcmp(buf, "background", 10));
        REQUIRE(!close(fd));
        fd = open("/readonly/mmap-error", O_RDONLY);
        REQUIRE(fd >= 0);
        REQUIRE(read(fd, buf, 9) == 9 && !memcmp(buf, "recovered", 9));
        REQUIRE(!close(fd));
        REQUIRE(sqlite3_open_v2("file:/readonly/mmap-sqlite.db?immutable=1", &db,
                                SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK);
        REQUIRE(rowcount(db) == 21);
        integrity(db);
        REQUIRE(sqlite3_close(db) == SQLITE_OK);
        puts("KSTQR_MMAP_REMOUNT_PASS");
        return 0;
    }
    puts("mmap: shared stores, msync and native I/O coherence");
    fd = open("/mnt/mmap-file", O_CREAT | O_TRUNC | O_RDWR, 0600);
    REQUIRE(fd >= 0);
    REQUIRE(!ftruncate(fd, length));
    a = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    REQUIRE(a != MAP_FAILED);
    b = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    REQUIRE(b != MAP_FAILED);
    memcpy(a, "mapped", 6);
    memcpy(a + 4090, "cross-page", 10);
    REQUIRE(!memcmp(b, "mapped", 6) && !memcmp(b + 4090, "cross-page", 10));
    REQUIRE(pread(fd, buf, 6, 0) == 6 && !memcmp(buf, "mapped", 6));
    REQUIRE(!msync(a, length, MS_SYNC));
    other = open("/other/mmap-file", O_RDONLY);
    REQUIRE(other >= 0);
    REQUIRE(pread(other, buf, 10, 4090) == 10 && !memcmp(buf, "cross-page", 10));
    REQUIRE(!close(other));
    memcpy(a + 100, "before-native", 13);
    REQUIRE(pwrite(fd, "native", 6, 0) == 6);
    REQUIRE(!memcmp(a, "native", 6) && !memcmp(a + 100, "before-native", 13));
    puts("mmap: user I/O buffers faulting back into the same filesystem");
    REQUIRE(!msync(a, length, MS_SYNC));
    REQUIRE(!madvise(a, 4096, MADV_DONTNEED));
    REQUIRE(!posix_fadvise(fd, 0, 4096, POSIX_FADV_DONTNEED));
    REQUIRE(pwrite(fd, a + 100, 13, 200) == 13);
    REQUIRE(!memcmp(a + 200, "before-native", 13));
    REQUIRE(!madvise(a, 4096, MADV_DONTNEED));
    REQUIRE(!posix_fadvise(fd, 0, 4096, POSIX_FADV_DONTNEED));
    REQUIRE(pread(fd, a + 300, 6, 0) == 6 && !memcmp(a + 300, "native", 6));
    REQUIRE(!msync(a, length, MS_SYNC));
    {
        int dir = open("/mnt", O_RDONLY | O_DIRECTORY);
        REQUIRE(dir >= 0);
        REQUIRE(!madvise(a + 4096, 4096, MADV_DONTNEED));
        REQUIRE(!posix_fadvise(fd, 4096, 4096, POSIX_FADV_DONTNEED));
        REQUIRE(syscall(SYS_getdents64, dir, a + 4096, 4096) > 0);
        REQUIRE(!close(dir));
    }
    puts("mmap: forked shared atomics while writeback runs");
    {
        _Atomic unsigned *counter = (_Atomic unsigned *)(a + 4096 + 128);
        pid_t child;
        int j;
        atomic_store(counter, 0);
        REQUIRE(atomic_is_lock_free(counter));
        child = fork();
        REQUIRE(child >= 0);
        for (j = 0; j < 5000; j++) {
            atomic_fetch_add(counter, 1);
            if (j % 1000 == 0)
                REQUIRE(!msync(a, length, MS_SYNC));
        }
        if (!child)
            _exit(0);
        REQUIRE(waitpid(child, &status, 0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
        REQUIRE(atomic_load(counter) == 10000);
        REQUIRE(!fsync(fd));
    }
    puts("mmap: private mappings and protection changes");
    {
        char *private = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
        REQUIRE(private != MAP_FAILED);
        memcpy(private, "private", 7);
        REQUIRE(!fsync(fd));
        REQUIRE(pread(fd, buf, 6, 0) == 6 && !memcmp(buf, "native", 6));
        REQUIRE(!munmap(private, length));
        REQUIRE(!mprotect(b, length, PROT_READ));
        REQUIRE(!mprotect(b, length, PROT_READ | PROT_WRITE));
        b[10] = 'X';
        REQUIRE(!msync(b, length, MS_SYNC));
    }
    puts("mmap: truncate, EOF tail and SIGBUS");
    REQUIRE(!ftruncate(fd, 80));
    {
        pid_t child = fork();
        REQUIRE(child >= 0);
        if (!child) {
            volatile char *p = a + 4096;
            *p = '!';
            _exit(0);
        }
        REQUIRE(waitpid(child, &status, 0) == child && WIFSIGNALED(status) &&
                WTERMSIG(status) == SIGBUS);
    }
    a[100] = '!';
    REQUIRE(!msync(a, 4096, MS_SYNC));
    REQUIRE(!fstat(fd, &st) && st.st_size == 80);
    REQUIRE(!ftruncate(fd, length));
    REQUIRE(pread(fd, buf, sizeof(buf), 80) == sizeof(buf));
    REQUIRE(!memcmp(buf, (char[64]){0}, sizeof(buf)));
    memcpy(a, "mmap durable", 12);
    REQUIRE(!fsync(fd));
    REQUIRE(!munmap(b, length));
    REQUIRE(!close(fd));
    a[12] = '!';
    REQUIRE(!msync(a, length, MS_SYNC));
    REQUIRE(!munmap(a, length));
    other = open("/readonly/mmap-file", O_RDONLY);
    REQUIRE(other >= 0);
    REQUIRE(mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, other, 0) == MAP_FAILED &&
            errno == EACCES);
    b = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE, other, 0);
    REQUIRE(b != MAP_FAILED);
    b[0] = '?';
    REQUIRE(!munmap(b, 4096));
    REQUIRE(!close(other));
    fd = open("/mnt/mmap-background", O_CREAT | O_RDWR | O_TRUNC, 0600);
    REQUIRE(fd >= 0);
    REQUIRE(!ftruncate(fd, 4096));
    a = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    REQUIRE(a != MAP_FAILED);
    memcpy(a, "background", 10);
    REQUIRE(!munmap(a, 4096));
    REQUIRE(!close(fd));
    sqlite_test();
    REQUIRE(argc == 2);
    failed_writeback(argv[1]);
    puts("KSTQR_MMAP_PASS");
    return 0;
}
