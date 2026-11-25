/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr,"signals line %d: %s: %s\n",__LINE__,#x,strerror(errno)); exit(1); } } while(0)
static volatile sig_atomic_t received;
static void alarm_tick(int signo) { (void)signo; received++; }
int main(void)
{
 char expected[4096],actual[4096]; memset(expected,'s',sizeof(expected));
 int fd=open("/mnt/signal-rpc",O_CREAT|O_TRUNC|O_RDWR,0600); REQUIRE(fd>=0);
 REQUIRE(write(fd,expected,sizeof(expected))==sizeof(expected)); REQUIRE(!fsync(fd));
 struct sigaction action={.sa_handler=alarm_tick,.sa_flags=SA_RESTART};
 REQUIRE(!sigemptyset(&action.sa_mask)); REQUIRE(!sigaction(SIGALRM,&action,NULL));
 struct itimerval timer={.it_interval={.tv_usec=500},.it_value={.tv_usec=500}};
 REQUIRE(!setitimer(ITIMER_REAL,&timer,NULL));
 for(unsigned i=0;i<32;i++) {
  struct stat st;
  expected[i]=(char)i;
  REQUIRE(pwrite(fd,expected,sizeof(expected),0)==sizeof(expected));
  REQUIRE(!fsync(fd)); REQUIRE(!fstat(fd,&st) && st.st_size==sizeof(expected));
  REQUIRE(pread(fd,actual,sizeof(actual),0)==sizeof(actual));
  REQUIRE(!memcmp(expected,actual,sizeof(expected)));
 }
 memset(&timer,0,sizeof(timer)); REQUIRE(!setitimer(ITIMER_REAL,&timer,NULL));
 REQUIRE(received>0); REQUIRE(!close(fd)); REQUIRE(!unlink("/mnt/signal-rpc"));
 puts("kstqr: restartable signals preserve RPC frames and file contents");
 return 0;
}
