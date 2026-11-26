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
 /* Even a read-only mount creates accepted pin/unpin receipts. Keep one
  * unconfirmed prefix on that connection before replacing the server boot. */
 int readonly=open("/readonly/go.txt",O_RDONLY);CHECK(readonly>=0);
 char readonly_data[32];CHECK(read(readonly,readonly_data,sizeof(readonly_data))>0);
 CHECK(!close(readonly));
 control(argv[1],"checkpoint-image");
 CHECK(!ftruncate(fd,8192));CHECK(pwrite(fd,"unconfirmed",11,0)==11);
 struct stat st;CHECK(!fstat(fd,&st)&&st.st_size==8192);
 control(argv[1],"restart-checkpoint-image");
 double end=test_monotonic()+4;
 do{if(!fstat(fd,&st)&&st.st_size==4096)break;usleep(1000);}while(test_monotonic()<end);
 CHECK(st.st_size==4096);
 char data[16];CHECK(test_pread_eventually(fd,data,7,0,"durable")==7);
 /* The earlier explicit fsync established the saved image. Later normal
  * writes/intents were only accepted, so rollback may discard them; reconnect
  * must preserve the saved bytes and never certify lost accepted marks. */
 int sync_result=fsync(fd);CHECK(!sync_result||errno==ESTALE||errno==EIO);
 CHECK(!close(fd));
 /* Losing that old prefix must not permanently veto fresh live pin/unpin
  * operations: ordinary opens/reads/closes never promised crash durability. */
 for(unsigned reopened=0;reopened<2;reopened++) {
  readonly=open("/readonly/go.txt",O_RDONLY);CHECK(readonly>=0);
  CHECK(read(readonly,readonly_data,sizeof(readonly_data))>0);CHECK(!close(readonly));
 }
 int fresh=open("/mnt/after-restart",O_CREAT|O_TRUNC|O_RDWR,0600);CHECK(fresh>=0);
 CHECK(write(fresh,"fresh",5)==5);CHECK(pread(fresh,data,5,0)==5&&!memcmp(data,"fresh",5));
 /* The uncertain old prefix is still not silently acknowledged by fsync. */
 sync_result=fsync(fresh);CHECK(sync_result<0&&(errno==ESTALE||errno==EIO));
 CHECK(!close(fresh));puts("KSTQR_CRASH_RECONNECT_PASS");return 0;
}
