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
static const char *endpoint;
static unsigned long long backend_syncs(void) {
    char address[512],json[4096];int status;
    if(!endpoint)return 0;
    CHECK(snprintf(address,sizeof(address),"%s/__kstqr_test/io-stats",endpoint)<(int)sizeof(address));
    pid_t child=fork();CHECK(child>=0);
    if(!child){execl("/bin/busybox","wget","-q","-O","/tmp/stqr-read-bench-io.json",address,NULL);_exit(127);}
    CHECK(waitpid(child,&status,0)==child&&WIFEXITED(status)&&!WEXITSTATUS(status));
    FILE *f=fopen("/tmp/stqr-read-bench-io.json","r");CHECK(f);
    size_t n=fread(json,1,sizeof(json)-1,f);json[n]=0;CHECK(!ferror(f));CHECK(!fclose(f));
    const char *p=strstr(json,"\"sync_ops\":");CHECK(p);
    return strtoull(p+strlen("\"sync_ops\":"),NULL,10);
}
static void report(const char *name,int rep,double start,double units,unsigned long long before) {
    double elapsed=now()-start;unsigned long long after=backend_syncs();
    printf("KSTQR_BENCH {\"test\":\"%s\",\"rep\":%d,\"seconds\":%.9f,\"rate\":%.3f,\"backend_syncs\":%llu}\n",name,rep,elapsed,units/elapsed,after-before);
}
static void path(char *p,unsigned i) { snprintf(p,128,"/readonly/bench-%03u.php",i%32); }
static void small_read(unsigned i) {
    char p[128],data[4096]; path(p,i);
    int fd=open(p,O_RDONLY); CHECK(fd>=0); CHECK(read(fd,data,sizeof(data))==sizeof(data));
    CHECK(data[0]=='x' && data[4095]=='x'); CHECK(!close(fd));
}
int main(int argc,char **argv) {
    char p[128],data[65536]; struct stat st; double start;
    setvbuf(stdout,NULL,_IOLBF,0);
    if(argc>1)endpoint=argv[1];
    unsigned long long before;
    for(int rep=0;rep<3;rep++) {
        before=backend_syncs();start=now(); int fd=open("/readonly/bench-sequential",O_RDONLY); CHECK(fd>=0);
        size_t n=0; ssize_t got;
        while((got=read(fd,data,sizeof(data)))>0) { CHECK(data[0]=='s' && data[got-1]=='s'); n+=got; }
        CHECK(!got && n==(8U<<20)); CHECK(!close(fd)); report("seq_read_mib_s",rep,start,8,before);
        before=backend_syncs();start=now(); for(unsigned i=0;i<512;i++) {path(p,i);CHECK(!stat(p,&st));CHECK(st.st_size==4096);}
        report("stat_ops_s",rep,start,512,before);
        before=backend_syncs();start=now(); for(unsigned i=0;i<256;i++) CHECK(stat("/readonly/bench-missing",&st)<0 && errno==ENOENT);
        report("missing_stat_ops_s",rep,start,256,before);
        before=backend_syncs();start=now(); for(unsigned i=0;i<128;i++) small_read(i);
        report("small_read_files_s",rep,start,128,before);
        before=backend_syncs();start=now(); for(unsigned k=0;k<4;k++) { pid_t child=fork(); CHECK(child>=0); if(!child) { for(unsigned i=0;i<128;i++)small_read(i+k*7); _exit(0); } }
        for(unsigned k=0;k<4;k++) {int status; CHECK(wait(&status)>0 && WIFEXITED(status) && !WEXITSTATUS(status));}
        report("parallel_small_read_files_s",rep,start,512,before);
    }
    return 0;
}
