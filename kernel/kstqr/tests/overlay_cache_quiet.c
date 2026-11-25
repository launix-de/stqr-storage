/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x) do { if(!(x)) {fprintf(stderr,"overlay quiet line %d: %s (%s)\n",__LINE__,#x,strerror(errno));exit(1);} } while(0)
static unsigned long long calls(void) {
 char line[4096];int selected=0;FILE *f=fopen("/proc/self/mountstats","r");CHECK(f);
 while(fgets(line,sizeof(line),f)) {
  if(!strncmp(line,"device ",7))selected=strstr(line," mounted on /mnt with fstype kstqr")!=NULL;
  if(selected) {
   char *m=strstr(line,"cache_misses="),*g=strstr(line,"rpc_get=");
   if(m&&g){unsigned long long n=strtoull(m+13,NULL,10)+strtoull(g+8,NULL,10);CHECK(!fclose(f));return n;}
  }
 }
 CHECK(0);return 0;
}
int main(void) {
 char initial[4096];memset(initial,'x',sizeof(initial));
 int original=open("/mnt/mmap-evicted",O_CREAT|O_TRUNC|O_RDWR,0600);CHECK(original>=0);
 CHECK(write(original,initial,sizeof(initial))==sizeof(initial));CHECK(!fsync(original));
 char *mapping=mmap(NULL,sizeof(initial),PROT_READ,MAP_SHARED,original,0);CHECK(mapping!=MAP_FAILED);CHECK(mapping[0]=='x');
 for(unsigned i=0;i<48;i++) {
  char path[128];snprintf(path,sizeof(path),"/mnt/quiet-cache-%u",i);
  int fd=open(path,O_CREAT|O_TRUNC|O_WRONLY,0600);CHECK(fd>=0);CHECK(write(fd,"q",1)==1);CHECK(!close(fd));
 }
 for(unsigned i=0;i<48;i++) {
  char path[128];struct stat st;snprintf(path,sizeof(path),"/mnt/quiet-cache-%u",i);CHECK(!stat(path,&st));
 }
 sleep(2);unsigned long long before=calls();sleep(1);unsigned long long after=calls();
 printf("overlay idle cache/network calls: before=%llu after=%llu\n",before,after);fflush(stdout);
 CHECK(after==before);
 int other=open("/other/mmap-evicted",O_WRONLY|O_TRUNC);CHECK(other>=0);CHECK(!close(other));
 pid_t child=fork();CHECK(child>=0);
 if(!child){volatile char value=mapping[0];(void)value;_exit(0);}
 int status;CHECK(waitpid(child,&status,0)==child);CHECK(WIFSIGNALED(status)&&WTERMSIG(status)==SIGBUS);
 CHECK(!munmap(mapping,sizeof(initial)));CHECK(!close(original));
 puts("kstqr: evicted overlay metadata stays idle and mmap observes remote truncation");
 return 0;
}
