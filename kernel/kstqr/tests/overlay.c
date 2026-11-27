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
static unsigned long long writeback_batches(void)
{
 char *line=NULL;size_t size=0;int mounted=0,found=0;unsigned long long count=0;
 FILE *f=fopen("/proc/self/mountstats","r");CHECK(f);
 while(getline(&line,&size,f)>0){
  if(strstr(line," mounted on /mnt "))mounted=1;
  else if(strstr(line,"device "))mounted=0;
  if(mounted&&strstr(line,"stqr:")){
   char *field=strstr(line,"writeback_batches=");CHECK(field);
   count=strtoull(field+strlen("writeback_batches="),NULL,10);found=1;break;
  }
 }
 free(line);CHECK(!fclose(f));CHECK(found);return count;
}
static void mutable_sparse_read(void)
{
 int fd=open("/mnt/mutable-sparse-read",O_CREAT|O_EXCL|O_RDWR,0600);CHECK(fd>=0);
 char original[4096];memset(original,'A',sizeof original);
 CHECK(write(fd,original,sizeof original)==(ssize_t)sizeof original);
 CHECK(!ftruncate(fd,8192));
 char *p=mmap(NULL,4096,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);CHECK(p!=MAP_FAILED);
 p[0]='Z';CHECK(!msync(p,4096,MS_SYNC));CHECK(!fsync(fd));
 char data[8192];ssize_t got=pread(fd,data,sizeof data,0);
 printf("KSTQR_MUTABLE_SPARSE_READ bytes=%zd errno=%d\n",got,errno);
 CHECK(got==(ssize_t)sizeof data);CHECK(data[0]=='Z'&&data[4095]=='A');
 for(unsigned i=4096;i<sizeof data;i++)CHECK(!data[i]);
 CHECK(!posix_fadvise(fd,4096,4096,POSIX_FADV_DONTNEED));
 memset(data,0x5a,4096);got=pread(fd,data,4096,4096);
 CHECK(got==4096);for(unsigned i=0;i<4096;i++)CHECK(!data[i]);
 CHECK(!munmap(p,4096));CHECK(!close(fd));CHECK(!unlink("/mnt/mutable-sparse-read"));
}
static void mmap_growth(void)
{
 int fd=open("/mnt/mmap-grow",O_CREAT|O_EXCL|O_RDWR,0600);CHECK(fd>=0);
 CHECK(!ftruncate(fd,4096));
 char *p=mmap(NULL,4096,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);CHECK(p!=MAP_FAILED);
 p[0]='A';p[2047]='B';p[3072]='C';
 unsigned long long before=writeback_batches();
 CHECK(!ftruncate(fd,8192));
 CHECK(writeback_batches()==before); /* Growth must not force dirty mmap I/O. */
 struct stat st;CHECK(!fstat(fd,&st)&&st.st_size==8192);
 CHECK(p[0]=='A'&&p[2047]=='B'&&p[3072]=='C');
 CHECK(!msync(p,4096,MS_SYNC));CHECK(!fsync(fd));
 char data[8192];CHECK(pread(fd,data,sizeof data,0)==(ssize_t)sizeof data);
 CHECK(data[0]=='A'&&data[2047]=='B'&&data[3072]=='C');
 for(unsigned i=4096;i<sizeof data;i++)CHECK(!data[i]);
 p[0]='D';p[2047]='E';
 CHECK(!ftruncate(fd,2048)); /* Shrink must still drain pending writeback. */
 CHECK(!fstat(fd,&st)&&st.st_size==2048);
 CHECK(!ftruncate(fd,8192));CHECK(!fsync(fd));
 CHECK(pread(fd,data,sizeof data,0)==(ssize_t)sizeof data);
 CHECK(data[0]=='D'&&data[2047]=='E');
 for(unsigned i=2048;i<sizeof data;i++)CHECK(!data[i]);
 CHECK(!munmap(p,4096));CHECK(!close(fd));CHECK(!unlink("/mnt/mmap-grow"));
 puts("KSTQR_OVERLAY_MMAP_GROWTH_PASS");
}
int main(void){
 mutable_sparse_read();
 mmap_growth();
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
