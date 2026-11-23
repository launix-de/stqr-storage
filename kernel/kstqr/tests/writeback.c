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
#include "subscription_wait.h"
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"writeback line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static double now(void) { struct timespec ts; CHECK(!clock_gettime(CLOCK_MONOTONIC,&ts)); return ts.tv_sec+ts.tv_nsec/1e9; }
struct counters { unsigned long long cas, batches, folios, snapshot, prepare, publish, retries; };
static unsigned long long field(const char *text,const char *key) { const char *p=strstr(text,key); return p?strtoull(p+strlen(key),NULL,10):0; }
static struct counters counters(void) {
    char *line=NULL; size_t cap=0; struct counters c={0}; int found=0;
    FILE *f=fopen("/proc/self/mountstats","r"); CHECK(f);
    while(getline(&line,&cap,f)>0) {
        if(strstr(line," mounted on /mnt ")) found=1;
        else if(strstr(line,"device ")) found=0;
        if(found && strstr(line,"stqr:")) {
            c.cas=field(line,"rpc_cas="); c.batches=field(line,"writeback_batches="); c.folios=field(line,"writeback_folios=");
            c.snapshot=field(line,"write_snapshot_ns="); c.prepare=field(line,"write_prepare_ns="); c.publish=field(line,"write_publish_ns="); c.retries=field(line,"write_retries="); break;
        }
    }
    free(line); CHECK(!fclose(f)); return c;
}
static void report(const char *test,int rep,double start,double units,const char *unit,struct counters a) {
    double elapsed=now()-start; struct counters b=counters();
    printf("KSTQR_WRITE_BENCH {\"test\":\"%s\",\"rep\":%d,\"seconds\":%.9f,\"rate\":%.3f,\"unit\":\"%s\",\"cas\":%llu,\"batches\":%llu,\"folios\":%llu,\"snapshot_ns\":%llu,\"prepare_ns\":%llu,\"publish_ns\":%llu,\"retries\":%llu}\n",test,rep,elapsed,units/elapsed,unit,b.cas-a.cas,b.batches-a.batches,b.folios-a.folios,b.snapshot-a.snapshot,b.prepare-a.prepare,b.publish-a.publish,b.retries-a.retries);
}
static int newfile(const char *name,int rep,size_t size) {
    char path[128]; snprintf(path,sizeof(path),"/mnt/wb-%s-%d",name,rep);
    int fd=open(path,O_CREAT|O_TRUNC|O_RDWR,0600); CHECK(fd>=0); CHECK(!ftruncate(fd,size)); return fd;
}
static void verify(int fd,const char *expected,size_t size) {
    char *copy=malloc(size); CHECK(copy); CHECK(pread(fd,copy,size,0)==(ssize_t)size); CHECK(!memcmp(copy,expected,size)); free(copy);
}
static void benchmark(void) {
    const size_t length=1U<<20; char *source=malloc(length); CHECK(source); memset(source,'D',length);
    for(int rep=0;rep<3;rep++) {
        int fd=newfile("direct",rep,0); struct counters c=counters(); double start=now();
        for(int n=0;n<8;n++)CHECK(write(fd,source,length)==(ssize_t)length);
        CHECK(!fsync(fd)); report("direct_write_fsync",rep,start,8,"MiB/s",c); CHECK(!close(fd));
        fd=newfile("dense",rep,length); char *p=mmap(NULL,length,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0); CHECK(p!=MAP_FAILED);
        c=counters(); start=now(); memset(p,'M',length); CHECK(!msync(p,length,MS_SYNC)); CHECK(!fsync(fd));
        report("mmap_dense_msync",rep,start,1,"MiB/s",c);
        memset(source,'M',length); verify(fd,source,length); CHECK(!munmap(p,length)); CHECK(!close(fd)); memset(source,'D',length);
        fd=newfile("sparse",rep,length); p=mmap(NULL,length,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0); CHECK(p!=MAP_FAILED);
        c=counters(); start=now(); for(int n=0;n<32;n++)memset(p+n*32768,'H',4096);
        CHECK(!msync(p,length,MS_SYNC)); CHECK(!fsync(fd)); report("mmap_sparse_msync",rep,start,32.0*4096/(1U<<20),"MiB/s",c);
        memset(source,0,length); for(int n=0;n<32;n++)memset(source+n*32768,'H',4096); verify(fd,source,length);
        CHECK(!munmap(p,length)); CHECK(!close(fd)); memset(source,'D',length);
        fd=newfile("commit4k",rep,4096); p=mmap(NULL,4096,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0); CHECK(p!=MAP_FAILED);
        c=counters(); start=now(); for(int n=0;n<32;n++){memset(p,'a'+n%26,4096);CHECK(!msync(p,4096,MS_SYNC));}
        CHECK(!fsync(fd)); report("mmap_4k_commit",rep,start,32,"commits/s",c); CHECK(!munmap(p,4096)); CHECK(!close(fd));
    }
    free(source);
}
static void correctness(void) {
    /* Gaps, clipped msync ranges, partial EOF and a batch crossing 1 MiB. */
    size_t length=(2U<<20)+37; char *expected=malloc(length); CHECK(expected); memset(expected,'B',length);
    int fd=newfile("edges",0,length); CHECK(pwrite(fd,expected,length,0)==(ssize_t)length);
    char *p=mmap(NULL,length,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0); CHECK(p!=MAP_FAILED);
    memset(p,'A',4096); memset(expected,'A',4096);
    memset(p+8192,'C',4096); memset(expected+8192,'C',4096);
    memset(p+(1U<<20)-4096,'X',12288); memset(expected+(1U<<20)-4096,'X',12288);
    memset(p+(2U<<20),'Z',37); memset(expected+(2U<<20),'Z',37);
    CHECK(!msync(p,12288,MS_SYNC));
    /* A second mount observes exactly the completed range. Unsubmitted gaps
     * and still-dirty distant pages must not be accidentally included. */
    int other=test_open_eventually("/other/wb-edges-0",O_RDONLY); CHECK(other>=0); char data[4096];
    CHECK(pread(other,data,4096,4096)==4096); for(int j=0;j<4096;j++)CHECK(data[j]=='B');
    CHECK(pread(other,data,4096,(1U<<20)-4096)==4096); for(int j=0;j<4096;j++)CHECK(data[j]=='B');
    CHECK(!msync(p,length,MS_SYNC)); CHECK(!fsync(fd));
    verify(fd,expected,length);
    char *observed=malloc(length);CHECK(observed);
    CHECK(test_pread_eventually(other,observed,length,0,expected)==(ssize_t)length);free(observed);
    CHECK(!munmap(p,length)); CHECK(!close(fd)); CHECK(!close(other)); free(expected);
    puts("KSTQR_WRITEBACK_EDGES_PASS");
}
int main(int argc,char **argv) {
    setvbuf(stdout,NULL,_IOLBF,0);
    if(argc>1 && !strcmp(argv[1],"benchmark"))benchmark(); else correctness();
    return 0;
}
