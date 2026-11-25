/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"cache capacity line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static unsigned long long misses(void) {
    char line[2048]; int selected=0; unsigned long long value=0;
    FILE *file=fopen("/proc/self/mountstats","r"); CHECK(file);
    while(fgets(line,sizeof(line),file)) {
        if(!strncmp(line,"device ",7)) selected=strstr(line," mounted on /mnt with fstype kstqr")!=NULL;
        if(selected) { char *p=strstr(line,"cache_misses="); if(p) {CHECK(sscanf(p,"cache_misses=%llu",&value)==1);CHECK(!fclose(file));return value;} }
    }
    CHECK(0);return 0;
}
static void scan(void) {
    for(unsigned i=0;i<600;i++) {char path[128];struct stat info;snprintf(path,sizeof(path),"/mnt/cache-working-set/file-%04u",i);CHECK(!lstat(path,&info));CHECK(S_ISREG(info.st_mode)&&info.st_size==0);}
}
int main(void) {
    CHECK(!mkdir("/mnt/cache-working-set",0700));
    for(unsigned i=0;i<600;i++) {char path[128];snprintf(path,sizeof(path),"/mnt/cache-working-set/file-%04u",i);int fd=open(path,O_CREAT|O_EXCL|O_WRONLY,0600);CHECK(fd>=0);CHECK(!close(fd));}
    scan(); unsigned long long before=misses(); scan(); unsigned long long after=misses();
    CHECK(before==after);
    printf("kstqr: 600-inode working set stays cached within the byte budget (misses=%llu)\n",after);
    return 0;
}
