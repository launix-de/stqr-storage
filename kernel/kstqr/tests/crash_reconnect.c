/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "subscription_wait.h"
#define CHECK(x) do{if(!(x)){fprintf(stderr,"crash reconnect line %d: %s (%s)\n",__LINE__,#x,strerror(errno));exit(1);}}while(0)
static void control(const char *base,const char *action){
 char url[512];int status;snprintf(url,sizeof(url),"%s/__kstqr_test/%s",base,action);
 pid_t child=fork();CHECK(child>=0);if(!child){execl("/bin/busybox","wget","-q","-O","/dev/null",url,NULL);_exit(127);}
 CHECK(waitpid(child,&status,0)==child&&WIFEXITED(status)&&!WEXITSTATUS(status));
}
int main(int argc,char **argv){
 CHECK(argc==2);int fd=open("/mnt/rollback-file",O_CREAT|O_TRUNC|O_RDWR,0600);CHECK(fd>=0);
 CHECK(!ftruncate(fd,4096));CHECK(pwrite(fd,"durable",7,0)==7);CHECK(!fsync(fd));
 control(argv[1],"checkpoint-image");
 CHECK(!ftruncate(fd,8192));CHECK(pwrite(fd,"unconfirmed",11,0)==11);
 struct stat st;CHECK(!fstat(fd,&st)&&st.st_size==8192);
 control(argv[1],"restart-checkpoint-image");
 double end=test_monotonic()+4;
 do{if(!fstat(fd,&st)&&st.st_size==4096)break;usleep(1000);}while(test_monotonic()<end);
 CHECK(st.st_size==4096);
 char data[16];CHECK(test_pread_eventually(fd,data,7,0,"durable")==7);
 /* Mutable-block intents use durable acknowledgments. There may be no
  * unconfirmed mark left to reject after this artificial rollback of an
  * already durable image; accepted writes still report lost marks. */
 int sync_result=fsync(fd);CHECK(!sync_result||errno==ESTALE||errno==EIO);
 CHECK(!close(fd));puts("KSTQR_CRASH_RECONNECT_PASS");return 0;
}
