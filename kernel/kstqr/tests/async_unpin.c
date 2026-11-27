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
#define CHECK(x) do{if(!(x)){fprintf(stderr,"async unpin line%d: %s (%s)\n",__LINE__,#x,strerror(errno));exit(1);}}while(0)
static double now(void){struct timespec t;CHECK(!clock_gettime(CLOCK_MONOTONIC,&t));return t.tv_sec+t.tv_nsec/1e9;}
static int control(const char *base,const char *path){char url[512];CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/unpin-%s",base,path)<(int)sizeof(url));pid_t c=fork();CHECK(c>=0);if(!c){execl("/bin/busybox","wget","-qO-",url,NULL);_exit(127);}int s;CHECK(waitpid(c,&s,0)==c);return WIFEXITED(s)&&!WEXITSTATUS(s);}
static void eventually(const char *base,const char *path){for(unsigned i=0;i<200;i++){if(control(base,path))return;usleep(10000);}CHECK(0);}
int main(int argc,char **argv){CHECK(argc==2);int fd=open("/readonly/async-unpin",O_RDONLY);CHECK(fd>=0);char data[16];CHECK(read(fd,data,sizeof(data))==sizeof(data)&&data[0]=='R');CHECK(control(argv[1],"begin"));double start=now();CHECK(!close(fd));double elapsed=now()-start;printf("KSTQR_ASYNC_UNPIN_CLOSE seconds=%.9f\n",elapsed);CHECK(elapsed<0.25);eventually(argv[1],"seen");CHECK(control(argv[1],"drop"));eventually(argv[1],"retired");fd=open("/readonly/async-unpin",O_RDONLY);CHECK(fd>=0);CHECK(read(fd,data,sizeof(data))==sizeof(data)&&data[0]=='R');CHECK(!close(fd));puts("KSTQR_ASYNC_UNPIN_PASS");return 0;}
