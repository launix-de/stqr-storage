/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"cache reopen line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static unsigned long long read_bytes(void) {
    FILE *f=fopen("/proc/self/mountstats","r"); CHECK(f);
    char *line=NULL; size_t capacity=0; int selected=0;
    unsigned long long result=0; int found=0;
    while(getline(&line,&capacity,f)>0) {
        if(!strncmp(line,"device ",7)) selected=strstr(line,"mounted on /readonly with fstype kstqr")!=NULL;
        char *p=selected?strstr(line,"read_bytes="):NULL;
        if(p) { result=strtoull(p+strlen("read_bytes="),NULL,10); found=1; break; }
    }
    free(line); CHECK(!ferror(f)); CHECK(!fclose(f)); CHECK(found); return result;
}
static void read_file(void) {
    unsigned char data[65536]; size_t at=0; ssize_t got;
    int fd=open("/readonly/cache-reopen",O_RDONLY); CHECK(fd>=0);
    while((got=read(fd,data,sizeof(data)))>0) {
        for(ssize_t i=0;i<got;i++) { CHECK(data[i]=='R'); }
        at+=(size_t)got;
    }
    CHECK(!got && at==(1U<<20)); CHECK(!close(fd));
}
int main(void) {
    read_file(); unsigned long long before=read_bytes();
    for(unsigned i=0;i<8;i++)read_file();
    unsigned long long delta=read_bytes()-before;
    printf("KSTQR_CACHE_REOPEN bytes=%llu opens=8\n",delta);
    /* The warm file has uninterrupted metadata subscription coverage and no
     * writers. Allow a conservative extra window, not one full reload/open. */
    CHECK(delta<=(1U<<20)); puts("KSTQR_CACHE_REOPEN_PASS"); return 0;
}
