/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(x) do {if(!(x)){fprintf(stderr,"statfs async line%d %s: %s\n",__LINE__,#x,strerror(errno));exit(1);}}while(0)
static int control(const char *base,const char *action) {
 char url[512];CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/statfs/%s",base,action)<(int)sizeof(url));
 pid_t child=fork();CHECK(child>=0);
 if(!child){int quiet=open("/dev/null",O_WRONLY);CHECK(quiet>=0);CHECK(dup2(quiet,STDOUT_FILENO)>=0);CHECK(dup2(quiet,STDERR_FILENO)>=0);close(quiet);execl("/bin/wget","wget","-q","-O","/dev/null",url,(char*)NULL);_exit(127);}
 int status;CHECK(waitpid(child,&status,0)==child);return WIFEXITED(status)&&WEXITSTATUS(status)==0;
}
int main(int argc,char **argv) {
 CHECK(argc==2);alarm(20);
 int fd=open("/statfs/statfs-async-data",O_CREAT|O_EXCL|O_RDWR,0600);CHECK(fd>=0);
 CHECK(control(argv[1],"begin"));
 pid_t child=fork();CHECK(child>=0);
 if(!child){struct statfs space;_exit(statfs("/statfs",&space)?1:0);}
 unsigned tries;for(tries=0;tries<300;tries++){if(control(argv[1],"seen"))break;usleep(10000);}CHECK(tries<300);
 CHECK(!kill(child,SIGKILL));
 /* Both metadata paths issue real GET/CAS on this mount's sole RPC socket.
  * The proxy verifies that they arrive BEFORE releasing the STATFS reply. */
 CHECK(!mkdir("/statfs/statfs-independent",0700));
 CHECK(!rmdir("/statfs/statfs-independent"));
 CHECK(control(argv[1],"verify"));
 CHECK(control(argv[1],"release"));
 int status;CHECK(waitpid(child,&status,0)==child);CHECK(WIFSIGNALED(status)&&WTERMSIG(status)==SIGKILL);
 CHECK(pwrite(fd,"yes",3,0)==3);CHECK(!fsync(fd));
 char data[3];CHECK(pread(fd,data,sizeof(data),0)==3);CHECK(!memcmp(data,"yes",3));
 CHECK(!close(fd));CHECK(!unlink("/statfs/statfs-async-data"));
 puts("KSTQR_ASYNC_STATFS_PASS delayed_reply_same_channel_GET_CAS killed_caller_no_ESTALE");return 0;
}
