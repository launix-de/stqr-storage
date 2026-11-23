/* SPDX-License-Identifier: GPL-3.0-only */
#ifndef KSTQR_TEST_SUBSCRIPTION_WAIT_H
#define KSTQR_TEST_SUBSCRIPTION_WAIT_H
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
/* A writer ACK is not an observation barrier on another mount. These helpers
 * require convergence within two seconds; local syscall checks stay immediate. */
static inline double test_monotonic(void) {
    struct timespec t; if(clock_gettime(CLOCK_MONOTONIC,&t))return 0;
    return t.tv_sec+t.tv_nsec/1e9;
}
static inline int test_open_eventually(const char *path,int flags) {
    double end=test_monotonic()+2;
    do { int fd=open(path,flags); if(fd>=0 || errno!=ENOENT)return fd; usleep(1000); }while(test_monotonic()<end);
    errno=ETIMEDOUT;return -1;
}
static inline ssize_t test_pread_eventually(int fd,void *buffer,size_t n,off_t offset,const void *expected) {
    double end=test_monotonic()+2;
    do { ssize_t got=pread(fd,buffer,n,offset); if(got<0)return got;
         if(got==(ssize_t)n && !memcmp(buffer,expected,n))return got;
         usleep(1000);
    }while(test_monotonic()<end);
    errno=ETIMEDOUT;return -1;
}
#endif
