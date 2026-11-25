/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/bpf.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "sandbox line %d: %s: %s\n", __LINE__, #x, strerror(errno)); exit(1); } } while (0)
static void move_to(const char *path) {
 int fd = open(path, O_WRONLY); CHECK(fd >= 0);
 CHECK(write(fd, "0", 1) == 1); CHECK(!close(fd));
}
int main(int argc, char **argv) {
 CHECK(argc == 3);
 CHECK(!mkdir("/cgroup", 0755));
 CHECK(!mount("none", "/cgroup", "cgroup2", 0, NULL));
 CHECK(!mkdir("/cgroup/sandbox", 0755));
 int cgroup = open("/cgroup/sandbox", O_RDONLY | O_DIRECTORY); CHECK(cgroup >= 0);
 struct bpf_insn instructions[] = {
  {.code = BPF_ALU64 | BPF_MOV | BPF_K, .dst_reg = BPF_REG_0, .imm = 0},
  {.code = BPF_JMP | BPF_EXIT},
 };
 char verifier[4096] = {0};
 union bpf_attr attr = {0};
 attr.prog_type = BPF_PROG_TYPE_CGROUP_SKB;
 attr.insn_cnt = sizeof(instructions) / sizeof(instructions[0]);
 attr.insns = (uintptr_t)instructions;
 attr.license = (uintptr_t)"GPL";
 attr.log_buf = (uintptr_t)verifier; attr.log_size = sizeof(verifier); attr.log_level = 1;
 int program = syscall(SYS_bpf, BPF_PROG_LOAD, &attr, sizeof(attr));
 if (program < 0) fprintf(stderr, "%s\n", verifier);
 CHECK(program >= 0);
 memset(&attr, 0, sizeof(attr)); attr.target_fd = cgroup;
 attr.attach_bpf_fd = program; attr.attach_type = BPF_CGROUP_INET_EGRESS;
 CHECK(!syscall(SYS_bpf, BPF_PROG_ATTACH, &attr, sizeof(attr)));
 pid_t child = fork(); CHECK(child >= 0);
 if (!child) {
  move_to("/cgroup/sandbox/cgroup.procs");
  /* Prove the filter is active for ordinary process sockets. */
  const char *colon = strrchr(argv[1], ':'); CHECK(colon);
  struct sockaddr_in peer = {.sin_family = AF_INET, .sin_port = htons(atoi(colon + 1))};
  CHECK(inet_pton(AF_INET, "10.0.2.2", &peer.sin_addr) == 1);
  int socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0); CHECK(socket_fd >= 0);
  CHECK(connect(socket_fd, (struct sockaddr *)&peer, sizeof(peer)) == -1 && errno == EINPROGRESS);
  struct pollfd poll_fd = {.fd = socket_fd, .events = POLLOUT};
  CHECK(poll(&poll_fd, 1, 250) == 0); CHECK(!close(socket_fd));
  pid_t helper = fork(); CHECK(helper >= 0);
  if (!helper) {
   execl("/mount.kstqr", "/mount.kstqr", "--uri", argv[1], "--bucket", argv[2],
         "--credentials", "/credentials", "--timeout-ms", "3000", "/fresh", NULL);
   _exit(127);
  }
  int status; CHECK(waitpid(helper, &status, 0) == helper && WIFEXITED(status) && !WEXITSTATUS(status));
  int fd = open("/fresh/sandbox-write", O_CREAT | O_RDWR | O_TRUNC, 0600); CHECK(fd >= 0);
  CHECK(write(fd, "storage", 7) == 7); CHECK(!fsync(fd));
  char data[7]; CHECK(pread(fd, data, 7, 0) == 7 && !memcmp(data, "storage", 7));
  CHECK(!close(fd)); CHECK(!unlink("/fresh/sandbox-write"));
  CHECK(!umount("/fresh")); _exit(0);
 }
 int status; CHECK(waitpid(child, &status, 0) == child && WIFEXITED(status) && !WEXITSTATUS(status));
 CHECK(!close(program)); CHECK(!close(cgroup));
 CHECK(!rmdir("/cgroup/sandbox")); CHECK(!umount("/cgroup")); CHECK(!rmdir("/cgroup"));
 puts("KSTQR_SANDBOX_PASS"); return 0;
}
