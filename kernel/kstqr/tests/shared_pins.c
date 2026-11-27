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
static int control(const char *base,const char *path){char url[512];CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/unpin-%s",base,path)<(int)sizeof(url));pid_t c=fork();CHECK(c>=0);if(!c){execl("/bin/busybox","wget","-qO-",url,NULL);_exit(127);}int s;CHECK(waitpid(c,&s,0)==c);return WIFEXITED(s)&&!WEXITSTATUS(s);}
static void eventually(const char *base,const char *path){for(unsigned i=0;i<200;i++){if(control(base,path))return;usleep(10000);}CHECK(0);}
int main(int argc,char **argv){CHECK(argc==2);int fds[16];for(unsigned i=0;i<16;i++){fds[i]=open("/readonly/async-unpin",O_RDONLY);CHECK(fds[i]>=0);}CHECK(control(argv[1],"one"));for(unsigned i=0;i<15;i++)CHECK(!close(fds[i]));CHECK(control(argv[1],"one"));CHECK(control(argv[1],"begin"));CHECK(!close(fds[15]));eventually(argv[1],"seen");int fresh=open("/readonly/async-unpin",O_RDONLY);CHECK(fresh>=0);CHECK(control(argv[1],"two"));CHECK(control(argv[1],"drop"));eventually(argv[1],"one");int after=open("/readonly/async-unpin",O_RDONLY);CHECK(after>=0);CHECK(control(argv[1],"one"));CHECK(control(argv[1],"renewed"));CHECK(!close(after));char data[16];CHECK(read(fresh,data,sizeof(data))==sizeof(data)&&data[0]=='R');CHECK(!close(fresh));eventually(argv[1],"retired");puts("KSTQR_SHARED_PINS_PASS opens=16 initial_tokens=1 reopen_tokens=2 retired_old_leaves=1");return 0;}
