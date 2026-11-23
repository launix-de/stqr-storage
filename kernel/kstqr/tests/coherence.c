/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr,"coherence line %d: %s: %s\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static void control(const char *url, const char *action)
{
    char address[512]; int status;
    snprintf(address,sizeof(address),"%s/__kstqr_test/%s",url,action);
    pid_t child = fork(); REQUIRE(child >= 0);
    if (!child) { execl("/bin/busybox","wget","-q","-O","/dev/null",address,NULL); _exit(127); }
    REQUIRE(waitpid(child,&status,0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
}
static double seconds(void)
{
    struct timespec ts; REQUIRE(!clock_gettime(CLOCK_MONOTONIC,&ts));
    return ts.tv_sec + ts.tv_nsec / 1e9;
}
static void asynchronous_change(const char *url, const char *action, const char *expected, const char *mapping)
{
    pid_t child = fork(); REQUIRE(child >= 0);
    if (!child) { control(url,action); _exit(0); }
    double deadline = seconds()+4;
    while (memcmp(mapping,expected,6) && seconds()<deadline) usleep(1000);
    REQUIRE(!memcmp(mapping,expected,6));
    int status; REQUIRE(waitpid(child,&status,0)==child && WIFEXITED(status) && !WEXITSTATUS(status));
}
int main(int argc, char **argv)
{
    struct stat st; char content[4096] = "before";
    int fd, ro;
    REQUIRE(argc == 2);
    /* Exercise the 1 MiB durable write path and an unaligned replacement. */
    char *large = malloc(2U << 20); REQUIRE(large); memset(large,'L',2U << 20);
    fd = open("/mnt/subscription-large",O_CREAT|O_TRUNC|O_RDWR,0600); REQUIRE(fd >= 0);
    REQUIRE(write(fd,large,2U<<20) == (2U<<20));
    REQUIRE(pwrite(fd,"middle",6,(1U<<20)-3) == 6);
    REQUIRE(pread(fd,large,16,(1U<<20)-8) == 16);
    REQUIRE(!memcmp(large,"LLLLLmiddleLLLLL",16));
    REQUIRE(!fsync(fd)); REQUIRE(!close(fd)); REQUIRE(!unlink("/mnt/subscription-large")); free(large);
    fd = open("/mnt/subscription.txt",O_CREAT|O_TRUNC|O_RDWR,0644); REQUIRE(fd >= 0);
    REQUIRE(write(fd,content,sizeof(content)) == sizeof(content)); REQUIRE(!close(fd));
    REQUIRE(stat("/readonly/subscription-created",&st) < 0 && errno == ENOENT);
    ro = open("/readonly/subscription.txt",O_RDONLY); REQUIRE(ro >= 0);
    char *mapping = mmap(NULL,4096,PROT_READ,MAP_SHARED,ro,0); REQUIRE(mapping != MAP_FAILED);
    REQUIRE(!memcmp(mapping,"before",6));
    /* Warm metadata and page cache. No stat/read is issued while awaiting the
     * remote update below: the mmap must be invalidated by the subscription. */
    for (int j=0;j<100;j++) REQUIRE(!stat("/readonly/subscription.txt",&st));
    control(argv[1],"subscription-update");
    double deadline = seconds()+3;
    while (memcmp(mapping,"after!",6) && seconds()<deadline) usleep(1000);
    REQUIRE(!memcmp(mapping,"after!",6));
    deadline = seconds()+3;
    while (stat("/readonly/subscription-created",&st) && seconds()<deadline) usleep(1000);
    REQUIRE(!stat("/readonly/subscription-created",&st));
    REQUIRE(!stat("/readonly/subscription.txt",&st) && (st.st_mode & 0777) == 0600);
    asynchronous_change(argv[1],"subscription-reconnect","again!",mapping);
    asynchronous_change(argv[1],"subscription-resync","resync",mapping);
    control(argv[1],"subscription-delete");
    deadline = seconds()+3;
    while (!stat("/readonly/subscription.txt",&st) && seconds()<deadline) usleep(1000);
    REQUIRE(stat("/readonly/subscription.txt",&st) < 0 && errno == ENOENT);
    REQUIRE(!memcmp(mapping,"resync",6)); /* open-unlink stays readable */
    REQUIRE(!munmap(mapping,4096)); REQUIRE(!close(ro));
    /* A tiny cache must evict proactively and must not resubscribe evicted
     * inodes from the asynchronous invalidation worker. */
    for (unsigned j=0;j<48;j++) {
        char path[128]; snprintf(path,sizeof(path),"/fresh/cache-%u",j);
        fd=open(path,O_CREAT|O_TRUNC|O_WRONLY,0600); REQUIRE(fd>=0);
        REQUIRE(write(fd,"x",1)==1); REQUIRE(!close(fd));
    }
    for (unsigned round=0;round<2;round++) for(unsigned j=0;j<48;j++) {
        char path[128],byte; snprintf(path,sizeof(path),"/fresh/cache-%u",j);
        fd=open(path,O_RDONLY); REQUIRE(fd>=0); REQUIRE(read(fd,&byte,1)==1 && byte=='x'); REQUIRE(!close(fd));
    }
    puts("kstqr: bounded-cache eviction and parallel RPC filesystem passed");
    puts("kstqr: remote namespace/permission and live mmap invalidation passed");
    return 0;
}
