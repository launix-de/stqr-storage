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
#define CHECK(x) do { if (!(x)) { fprintf(stderr,"edit retry line %d: %s (%s)\n",__LINE__,#x,strerror(errno)); exit(1); } } while (0)
static void control(const char *base,const char *mode,const char *action) {
 char url[512];int status;
 CHECK(snprintf(url,sizeof(url),"%s/__kstqr_test/edit-retry/%s/%s",base,mode,action)<(int)sizeof(url));
 pid_t child=fork();CHECK(child>=0);
 if(!child){execl("/bin/busybox","wget","-q","-O","/dev/null",url,NULL);_exit(127);}
 CHECK(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
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
 /* Visibility retries cannot certify the accepted prefix from a lost socket.
  * A later explicit persistence request must still surface that uncertainty. */
 int uncertain=open("/mnt/edit-outage-0",O_RDONLY);CHECK(uncertain>=0);
 CHECK(fsync(uncertain)<0&&errno==ESTALE);CHECK(!close(uncertain));
 puts("KSTQR_EDIT_RETRY_PASS rejected503=waited disconnect-before=retried disconnect-after=exact-once");
 return 0;
}
