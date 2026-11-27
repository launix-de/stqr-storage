/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"edit retry line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while (0)
static void control(const char *base,const char *mode,const char *action) {
 char url[512];int status;
 CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/edit-retry/%s/%s",base,mode,action)<(int)sizeof(url));
 pid_t child=fork();CHECK(child>=0);
 if(!child){execl("/bin/busybox","wget","-q","-O","/dev/null",url,NULL);_exit(127);}
 CHECK(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
}
static int poll_held(const char *base) {
 char url[512];int status;
 CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/edit-retry/fatal-caller/seen",base)<(int)sizeof(url));
 pid_t child=fork();CHECK(child>=0);
 if(!child){int sink=open("/dev/null",O_WRONLY);if(sink>=0)dup2(sink,2);execl("/bin/busybox","wget","-q","-O","/dev/null",url,NULL);_exit(127);}
 CHECK(waitpid(child,&status,0)==child);return WIFEXITED(status)&&WEXITSTATUS(status)==0;
}
static void fatal_claimed_completion(const char *base) {
 char data[8192],got[8192];memset(data,'A',sizeof(data));
 int fd=open("/mnt/fatal-claimed-redo",O_CREAT|O_RDWR,0600);CHECK(fd>=0);
 CHECK(write(fd,data,sizeof(data))==(ssize_t)sizeof(data));CHECK(!close(fd));
 control(base,"fatal-caller","begin");
 pid_t writer=fork();CHECK(writer>=0);
 if(!writer){int out=open("/mnt/fatal-claimed-redo",O_RDWR);if(out<0)_exit(101);memset(data,'B',sizeof(data));_exit(pwrite(out,data,sizeof(data),0)==(ssize_t)sizeof(data)?0:102);}
 unsigned attempt;
 for(attempt=0;attempt<400&&!poll_held(base);attempt++)usleep(10000);
 CHECK(attempt<400);
 CHECK(!kill(writer,SIGKILL));
 control(base,"fatal-caller","release");
 int status;CHECK(waitpid(writer,&status,0)==writer);CHECK(WIFSIGNALED(status)&&WTERMSIG(status)==SIGKILL);
 /* A killed task must not abandon the accepted REDO claim. A later writer
  * waits for completion, then its exact newer bytes remain visible. */
 fd=open("/mnt/fatal-claimed-redo",O_RDWR);CHECK(fd>=0);memset(data,'C',sizeof(data));
 CHECK(pwrite(fd,data,sizeof(data),0)==(ssize_t)sizeof(data));
 CHECK(pread(fd,got,sizeof(got),0)==(ssize_t)sizeof(got));CHECK(!memcmp(data,got,sizeof(data)));CHECK(!close(fd));
 control(base,"fatal-caller","verify");
 puts("KSTQR_FATAL_CLAIMED_COMPLETION_PASS");
}
int main(int argc,char **argv) {
 setvbuf(stdout,NULL,_IONBF,0);
 CHECK(argc==2);
 const char *modes[]={"503","disconnect-before","disconnect-after"};
 char initial[8192],replacement[211],observed[sizeof(initial)+sizeof(replacement)];
 memset(initial,'A',sizeof(initial));memset(replacement,'B',sizeof(replacement));
 for(unsigned i=0;i<3;i++) {
  char path[128];CHECK(snprintf(path,sizeof(path),"/mnt/edit-outage-%u",i)<(int)sizeof(path));
  int fd=open(path,O_CREAT|O_RDWR|O_APPEND,0600);CHECK(fd>=0);
  CHECK(write(fd,initial,sizeof(initial))==(ssize_t)sizeof(initial));
  printf("KSTQR_EDIT_RETRY mode=%s begin\n",modes[i]);
  control(argv[1],modes[i],"begin");
  /* Retry the selected stable payload edit, never this whole append again. */
  CHECK(write(fd,replacement,sizeof(replacement))==(ssize_t)sizeof(replacement));
  struct stat st;CHECK(!fstat(fd,&st)&&st.st_size==(off_t)sizeof(observed));
  CHECK(pread(fd,observed,sizeof(observed),0)==(ssize_t)sizeof(observed));
  CHECK(!memcmp(observed,initial,sizeof(initial)));
  CHECK(!memcmp(observed+sizeof(initial),replacement,sizeof(replacement)));
  control(argv[1],modes[i],"verify");
  printf("KSTQR_EDIT_RETRY mode=%s verified\n",modes[i]);
  CHECK(!close(fd));
 }
 fatal_claimed_completion(argv[1]);
 /* Visibility retries cannot certify the accepted prefix from a lost socket.
  * A later explicit persistence request must still surface that uncertainty. */
 int uncertain=open("/mnt/edit-outage-0",O_RDONLY);CHECK(uncertain>=0);
 CHECK(fsync(uncertain)<0&&errno==ESTALE);CHECK(!close(uncertain));
 puts("KSTQR_EDIT_RETRY_PASS rejected503=waited disconnect-before=retried disconnect-after=exact-once");
 return 0;
}
