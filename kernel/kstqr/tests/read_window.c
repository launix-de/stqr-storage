/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"read window line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static double now(void) { struct timespec ts; CHECK(!clock_gettime(CLOCK_MONOTONIC,&ts)); return ts.tv_sec+ts.tv_nsec/1e9; }
int main(int argc,char **argv) {
    unsigned char *data=malloc(1U<<20); CHECK(data); CHECK(argc==2);
    int fd=open("/readonly/read-window",O_RDONLY); CHECK(fd>=0);
    double start=now(); size_t at=0;
    while(at<(1U<<20)) { ssize_t got=read(fd,data+at,(1U<<20)-at); CHECK(got>0); at+=(size_t)got; }
    double elapsed=now()-start;
    for(unsigned i=0;i<(1U<<20);i++) CHECK(data[i]==(i/16384%8==7?0:(unsigned char)(i/16384+1)));
    CHECK(read(fd,data,1)==0); CHECK(!close(fd)); free(data);
    printf("KSTQR_READ_WINDOW_READ bytes=1048576 leaves=56 holes=8 reply_delay_ms=20 seconds=%.9f mib_s=%.3f\n",elapsed,1/elapsed);
    char address[512]; CHECK(snprintf(address,sizeof(address),"%s/__kstqr_test/read-window",argv[1])<(int)sizeof(address));
    pid_t child=fork(); CHECK(child>=0);
    if(!child) { execl("/bin/busybox","wget","-qO-",address,NULL); _exit(127); }
    int status; CHECK(waitpid(child,&status,0)==child && WIFEXITED(status) && !WEXITSTATUS(status));
    puts("KSTQR_READ_WINDOW_PASS");
    return 0;
}
