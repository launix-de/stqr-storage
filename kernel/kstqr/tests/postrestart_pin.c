/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(x) do { if(!(x)) { fprintf(stderr,"postrestart line %d: %s (%s)\n",__LINE__,#x,strerror(errno));exit(1); } } while(0)
static int control(const char *base,const char *path) {
 char url[512]; CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/%s",base,path)<(int)sizeof(url));
 pid_t p=fork();CHECK(p>=0);if(!p){execl("/bin/busybox","wget","-qO-",url,NULL);_exit(127);}
 int status;CHECK(waitpid(p,&status,0)==p);return WIFEXITED(status)&&!WEXITSTATUS(status);
}
int main(int argc,char **argv) {
 CHECK(argc==2);setvbuf(stdout,NULL,_IOLBF,0);
 /* The fixture checkpoint snapshots only explicitly fenced metadata before PIN. */
 CHECK(control(argv[1],"checkpoint-image"));
 int old=open("/readonly/async-unpin",O_RDONLY);CHECK(old>=0);
 CHECK(control(argv[1],"unpin-one"));
 CHECK(control(argv[1],"unpin-metadata-begin"));
 pid_t child=fork();CHECK(child>=0);
 if(!child) {
   int fresh=open("/readonly/async-unpin",O_RDONLY);CHECK(fresh>=0);
   puts("POSTRESTART_NEW_OPEN_RETURNED_SUCCESS");
   CHECK(control(argv[1],"unpin-print-count"));
   CHECK(control(argv[1],"unpin-one"));
   CHECK(control(argv[1],"unpin-restart-renewed"));
   char data[16];CHECK(read(fresh,data,sizeof(data))==sizeof(data)&&data[0]=='R');
   CHECK(!close(fresh));_exit(0);
 }
 int held=0;for(unsigned i=0;i<200;i++){if(control(argv[1],"unpin-metadata-seen")){held=1;break;}usleep(10000);}CHECK(held);
 puts("POSTRESTART_FRESH_GET_HELD");
 CHECK(control(argv[1],"restart-checkpoint-image"));
 int status;CHECK(waitpid(child,&status,0)==child);
 CHECK(WIFEXITED(status)&&!WEXITSTATUS(status));
 CHECK(!close(old));
 int retired=0;for(unsigned i=0;i<200;i++){if(control(argv[1],"unpin-retired")){retired=1;break;}usleep(10000);}CHECK(retired);
 puts("KSTQR_POSTRESTART_PIN_PASS");return 0;
}
