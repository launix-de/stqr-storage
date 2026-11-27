/* SPDX-License-Identifier: GPL-3.0-only */
/* Exercise real VFS requests: ordinary metadata/write/close does not invoke the
 * protocol checkpoint path; Linux persistence operations invoke it exactly once.
 * This runs inside the disposable kernel fixture, never on a host filesystem. */
#include <sys/mount.h>
static void explicit_sync_trace_control(const char *name,const char *value)
{
    char path[256];snprintf(path,sizeof(path),"/sys/kernel/tracing/%s",name);
    int fd=open(path,O_WRONLY|O_TRUNC);REQUIRE(fd>=0);
    REQUIRE(write(fd,value,strlen(value))==(ssize_t)strlen(value));REQUIRE(!close(fd));
}
static void explicit_sync_trace_start(void)
{
    explicit_sync_trace_control("tracing_on","0");
    explicit_sync_trace_control("trace","");
    explicit_sync_trace_control("tracing_on","1");
}
static void explicit_sync_trace_expect(unsigned expected)
{
    explicit_sync_trace_control("tracing_on","0");
    FILE *trace=fopen("/sys/kernel/tracing/trace","r");REQUIRE(trace);
    unsigned count=0;char line[1024];
    while(fgets(line,sizeof(line),trace))if(strstr(line,"ks_checkpoint"))count++;
    REQUIRE(!ferror(trace));REQUIRE(!fclose(trace));
    if(count!=expected)fprintf(stderr,"kstqr checkpoint trace: expected %u, got %u\n",expected,count);
    REQUIRE(count==expected);
}
static void explicit_sync_policy(void)
{
    REQUIRE(!mkdir("/sys/kernel/tracing",0755)||errno==EEXIST);
    REQUIRE(!mount("tracefs","/sys/kernel/tracing","tracefs",0,NULL)||errno==EBUSY);
    explicit_sync_trace_control("tracing_on","0");
    explicit_sync_trace_control("current_tracer","function");
    explicit_sync_trace_control("set_ftrace_filter","ks_checkpoint");
    explicit_sync_trace_start();
    int fd=open("/mnt/explicit-sync-policy",O_CREAT|O_RDWR|O_TRUNC,0600);REQUIRE(fd>=0);
    for(unsigned i=0;i<16;i++)REQUIRE(pwrite(fd,"ordinary",8,i*8)==8);
    REQUIRE(!ftruncate(fd,4096));REQUIRE(!fchmod(fd,0640));REQUIRE(!close(fd));
    explicit_sync_trace_expect(0);
    explicit_sync_trace_start();fd=open("/mnt/explicit-sync-policy",O_RDWR);REQUIRE(fd>=0);
    REQUIRE(!fsync(fd));REQUIRE(!close(fd));explicit_sync_trace_expect(1);
    explicit_sync_trace_start();fd=open("/mnt/explicit-sync-policy",O_RDWR);REQUIRE(fd>=0);
    REQUIRE(!fdatasync(fd));REQUIRE(!close(fd));explicit_sync_trace_expect(1);
    explicit_sync_trace_start();fd=open("/mnt/explicit-sync-policy",O_RDWR|O_DSYNC);REQUIRE(fd>=0);
    REQUIRE(pwrite(fd,"durable",7,0)==7);REQUIRE(!close(fd));explicit_sync_trace_expect(1);
    explicit_sync_trace_control("current_tracer","nop");
    explicit_sync_trace_control("set_ftrace_filter","");
    REQUIRE(!unlink("/mnt/explicit-sync-policy"));
    puts("kstqr: ordinary bursts/close have zero checkpoints; explicit VFS sync has one");
}
