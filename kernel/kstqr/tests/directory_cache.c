/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"directory cache line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
#define FILES 160
struct counters { unsigned long long get, range; };
static struct counters counters(const char *mount) {
    FILE *f=fopen("/proc/self/mountstats","r"); CHECK(f);
    char *line=NULL, needle[128]; size_t cap=0; int selected=0, found=0;
    struct counters result={0};
    CHECK(snprintf(needle,sizeof(needle),"mounted on %s with fstype kstqr",mount)>0);
    while(getline(&line,&cap,f)>0) {
        if(!strncmp(line,"device ",7)) selected=strstr(line,needle)!=NULL;
        if(selected && strstr(line,"rpc_get=")) {
            result.get=strtoull(strstr(line,"rpc_get=")+8,NULL,10);
            CHECK(strstr(line,"rpc_range="));
            result.range=strtoull(strstr(line,"rpc_range=")+10,NULL,10);
            found=1; break;
        }
    }
    free(line); CHECK(!ferror(f)); CHECK(!fclose(f)); CHECK(found); return result;
}
static void path(char *out,size_t size,const char *mount,unsigned i) {
    int n=snprintf(out,size,"%s/directory-cache/item%04u",mount,i);
    CHECK(n>0 && (size_t)n<size);
}
static void exists(const char *mount,unsigned i) {
    char name[160]; struct stat st; path(name,sizeof(name),mount,i);
    CHECK(!stat(name,&st)); CHECK(S_ISREG(st.st_mode));
}
int main(int argc,char **argv) {
    CHECK(argc==2);
    if(!strcmp(argv[1],"setup")) {
        CHECK(!mkdir("/mnt/directory-cache",0755));
        for(unsigned i=0;i<FILES;i++) {
            char name[160]; path(name,sizeof(name),"/mnt",i);
            int fd=open(name,O_CREAT|O_EXCL|O_WRONLY,0600); CHECK(fd>=0); CHECK(!close(fd));
        }
        puts("directory-cache: populated non-inline STDR directory"); return 0;
    }
    CHECK(!strcmp(argv[1],"check"));
    exists("/readonly",0); /* Populate and validate one exact directory head. */
    struct counters before=counters("/readonly");
    for(unsigned i=1;i<FILES;i++) exists("/readonly",i);
    struct counters after=counters("/readonly");
    printf("KSTQR_DIRECTORY_CACHE names=%u get=%llu range=%llu\n",FILES-1,after.get-before.get,after.range-before.range);
    /* Every cold name still validates the mutable inode at its owner. Cached
     * names remove body/index reads, never the concurrent-writer guard. */
    CHECK(after.get-before.get>=FILES/2);
    CHECK(after.range-before.range<=4);

    /* Same-size rename must invalidate by exact head, not inode/size alone. */
    CHECK(!rename("/mnt/directory-cache/item0000","/mnt/directory-cache/new_0000"));
    struct stat st; errno=0;
    CHECK(stat("/readonly/directory-cache/item0000",&st)<0 && errno==ENOENT);
    CHECK(!stat("/readonly/directory-cache/new_0000",&st));
    int fd=open("/mnt/directory-cache/added",O_CREAT|O_EXCL|O_WRONLY,0600);
    CHECK(fd>=0); CHECK(!close(fd));
    CHECK(!stat("/readonly/directory-cache/added",&st));
    exists("/readonly",FILES-1);

    /* A 4-KiB metadata budget cannot admit this decoded body. Fallback is a
     * real positive control: lookups stay correct and perform payload reads. */
    exists("/tiny",1); before=counters("/tiny");
    for(unsigned i=2;i<18;i++) exists("/tiny",i);
    after=counters("/tiny");
    printf("KSTQR_DIRECTORY_CACHE tiny_get=%llu tiny_range=%llu\n",after.get-before.get,after.range-before.range);
    CHECK(after.range>before.range);
    CHECK(!stat("/tiny/directory-cache/new_0000",&st));
    errno=0; CHECK(stat("/tiny/directory-cache/item0000",&st)<0 && errno==ENOENT);
    puts("KSTQR_DIRECTORY_CACHE_PASS"); return 0;
}
