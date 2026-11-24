/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>
#define CHECK(x) do {if(!(x)){fprintf(stderr,"native overlay check failed: %s:%d %s (errno=%d)\n",__FILE__,__LINE__,#x,errno);exit(1);}}while(0)
int main(void){
 char value[64];
 CHECK(!setxattr("/mnt","user.native","directory",9,0));
 CHECK(getxattr("/mnt","user.native",value,sizeof value)==9&&!memcmp(value,"directory",9));
 CHECK(!setxattr("/mnt/new-file","user.native","regular",7,0));
 int fd=open("/mnt/overlay-source",O_RDWR);CHECK(fd>=0);
 char *map=mmap(NULL,2U<<20,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);CHECK(map!=MAP_FAILED);
 CHECK(map[4096]=='L'&&map[1048580]=='Q');memcpy(map+4096,"MM",2);
 CHECK(!msync(map,2U<<20,MS_SYNC));CHECK(!munmap(map,2U<<20));CHECK(!close(fd));
 fd=open("/fresh/go.txt",O_RDWR);CHECK(fd>=0);errno=0;
 map=mmap(NULL,4096,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);CHECK(map==MAP_FAILED&&errno==EROFS);close(fd);
 fd=open("/mnt/orphan",O_CREAT|O_RDWR|O_EXCL,0600);CHECK(fd>=0);CHECK(write(fd,"before",6)==6);
 CHECK(!unlink("/mnt/orphan"));CHECK(pwrite(fd,"after!",6,0)==6);CHECK(!fsync(fd));CHECK(pread(fd,value,6,0)==6&&!memcmp(value,"after!",6));close(fd);CHECK(access("/mnt/orphan",F_OK)<0&&errno==ENOENT);
 CHECK(!mkfifo("/mnt/fifo",0600));CHECK(!unlink("/mnt/fifo"));
 puts("KSTQR_NATIVE_OVERLAY_MMAP_XATTR_PASS");return 0;
}
