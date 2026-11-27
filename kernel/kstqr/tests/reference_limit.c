/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"reference-limit: line%d assertion %s errno=%d\n",__LINE__,#x,errno); exit(1); } } while (0)
static double monotonic(void) { struct timespec t; CHECK(!clock_gettime(CLOCK_MONOTONIC,&t)); return t.tv_sec+t.tv_nsec/1e9; }
int main(void) {
 struct stat st;
 CHECK(!stat("/readonly/async-unpin",&st));
 for(unsigned i=0;i<3;i++) {
  double start=monotonic();
  alarm(5);
  int fd=open("/readonly/async-unpin",O_RDONLY);
  int error=errno;
  alarm(0);
  double elapsed=monotonic()-start;
  if(fd>=0)close(fd);
  printf("reference-limit: attempt=%u errno=%d seconds=%.6f\n",i,error,elapsed); fflush(stdout);
  CHECK(fd<0); CHECK(error==ENFILE); CHECK(elapsed<2.0);
 }
 // A terminal lifetime-capacity error must leave metadata reads usable.
 CHECK(!stat("/readonly/async-unpin",&st));
 puts("KSTQR_REFERENCE_LIMIT_PASS seeded=8192 attempts=3 terminal=ENFILE");
 return 0;
}
