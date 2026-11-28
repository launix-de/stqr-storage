/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"reference-limit: line%d assertion %s errno=%d\n",__LINE__,#x,errno); exit(1); } } while (0)
static double monotonic(void) { struct timespec t; CHECK(!clock_gettime(CLOCK_MONOTONIC,&t)); return t.tv_sec+t.tv_nsec/1e9; }
int main(void) {
 struct stat before,st;
 int fds[3];
 CHECK(!stat("/readonly/async-unpin",&before));
 // Bound the whole probe while allowing loaded VM hosts to complete RPCs.
 alarm(60);
 for(unsigned i=0;i<3;i++) {
  char data[16];
  double start=monotonic();
  fds[i]=open("/readonly/async-unpin",O_RDONLY);
  CHECK(fds[i]>=0);
  CHECK(!fstat(fds[i],&st));
  CHECK(st.st_ino==before.st_ino && st.st_size==16 && S_ISREG(st.st_mode));
  CHECK(pread(fds[i],data,sizeof(data),0)==sizeof(data));
  CHECK(!memcmp(data,"RRRRRRRRRRRRRRRR",sizeof(data)));
  printf("reference-limit: attempt=%u admitted=1 seconds=%.6f\n",i,monotonic()-start); fflush(stdout);
 }
 // Keep earlier descriptors live while later opens cross the old boundary.
 for(unsigned i=0;i<3;i++) {
  char data[16];
  CHECK(!fstat(fds[i],&st));
  CHECK(st.st_ino==before.st_ino && st.st_size==16);
  CHECK(pread(fds[i],data,sizeof(data),0)==sizeof(data));
  CHECK(!memcmp(data,"RRRRRRRRRRRRRRRR",sizeof(data)));
  CHECK(!close(fds[i]));
 }
 CHECK(!stat("/readonly/async-unpin",&st));
 alarm(0);
 puts("KSTQR_REFERENCE_LIMIT_PASS seeded=8192 attempts=3 admitted=3");
 return 0;
}
