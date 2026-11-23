/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"bench line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static double now(void) { struct timespec ts; CHECK(!clock_gettime(CLOCK_MONOTONIC,&ts)); return ts.tv_sec+ts.tv_nsec/1e9; }
static void report(const char *name,int rep,double start,double units) {
    double elapsed=now()-start;
    printf("KSTQR_BENCH {\"test\":\"%s\",\"rep\":%d,\"seconds\":%.9f,\"rate\":%.3f}\n",name,rep,elapsed,units/elapsed);
}
static void path(char *p,unsigned i) { snprintf(p,128,"/readonly/bench-%03u.php",i%32); }
static void small_read(unsigned i) {
    char p[128],data[4096]; path(p,i);
    int fd=open(p,O_RDONLY); CHECK(fd>=0); CHECK(read(fd,data,sizeof(data))==sizeof(data));
    CHECK(data[0]=='x' && data[4095]=='x'); CHECK(!close(fd));
}
int main(void) {
    char p[128],data[65536]; struct stat st; double start;
    setvbuf(stdout,NULL,_IOLBF,0);
    for(int rep=0;rep<3;rep++) {
        start=now(); int fd=open("/readonly/bench-sequential",O_RDONLY); CHECK(fd>=0);
        size_t n=0; ssize_t got;
        while((got=read(fd,data,sizeof(data)))>0) { CHECK(data[0]=='s' && data[got-1]=='s'); n+=got; }
        CHECK(!got && n==(8U<<20)); CHECK(!close(fd)); report("seq_read_mib_s",rep,start,8);
        start=now(); for(unsigned i=0;i<512;i++) {path(p,i);CHECK(!stat(p,&st));CHECK(st.st_size==4096);}
        report("stat_ops_s",rep,start,512);
        start=now(); for(unsigned i=0;i<256;i++) CHECK(stat("/readonly/bench-missing",&st)<0 && errno==ENOENT);
        report("missing_stat_ops_s",rep,start,256);
        start=now(); for(unsigned i=0;i<128;i++) small_read(i);
        report("small_read_files_s",rep,start,128);
        start=now(); for(unsigned k=0;k<4;k++) { pid_t child=fork(); CHECK(child>=0); if(!child) { for(unsigned i=0;i<128;i++)small_read(i+k*7); _exit(0); } }
        for(unsigned k=0;k<4;k++) {int status; CHECK(wait(&status)>0 && WIFEXITED(status) && !WEXITSTATUS(status));}
        report("parallel_small_read_files_s",rep,start,512);
    }
    return 0;
}
