/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <security/pam_ext.h>
#include <security/pam_modules.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/wait.h>
#include <unistd.h>
#ifndef HELPER
#define HELPER "/usr/libexec/stqr-home"
#endif
struct state {
  char *password;
  char *username;
  char session[33];
  int opened;
};
static int invoke(const char *, const char *, const struct state *);
static void cleanup(pam_handle_t *p, void *data, int status) {
  (void)p;
  (void)status;
  struct state *s = data;
  if (s) {
    if (s->opened && s->username)
      invoke("close", s->username, s);
    free(s->username);
    if (s->password) {
      explicit_bzero(s->password, strlen(s->password));
      free(s->password);
    }
    explicit_bzero(s, sizeof(*s));
    free(s);
  }
}
static int invoke(const char *action, const char *user, const struct state *s) {
  int pipefd[2], status;
  pid_t pid;
  if (pipe2(pipefd, O_CLOEXEC))
    return PAM_SYSTEM_ERR;
  pid = fork();
  if (pid < 0) {
    close(pipefd[0]);
    close(pipefd[1]);
    return PAM_SYSTEM_ERR;
  }
  if (!pid) {
    dup2(pipefd[0], STDIN_FILENO);
    close(pipefd[0]);
    close(pipefd[1]);
    char *const env[] = {"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C", NULL};
    execle(HELPER, HELPER, action, "--", user, s ? s->session : "", NULL, env);
    _exit(127);
  }
  close(pipefd[0]);
  /* Small bounded payload; suppress SIGPIPE if the child rejects configuration.
   */
  sigset_t set, old;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  sigprocmask(SIG_BLOCK, &set, &old);
  if (s && s->password && strcmp(action, "close")) {
    size_t n = strlen(s->password);
    const char *p = s->password;
    while (n) {
      ssize_t w = write(pipefd[1], p, n);
      if (w < 0) {
        if (errno == EINTR)
          continue;
        break;
      }
      p += w;
      n -= (size_t)w;
    }
  }
  close(pipefd[1]);
  struct timespec zero = {0};
  sigtimedwait(&set, NULL, &zero);
  sigprocmask(SIG_SETMASK, &old, NULL);
  while (waitpid(pid, &status, 0) < 0) {
    if (errno != EINTR)
      return PAM_SYSTEM_ERR;
  }
  return WIFEXITED(status) && !WEXITSTATUS(status) ? PAM_SUCCESS : PAM_AUTH_ERR;
}
PAM_EXTERN int pam_sm_authenticate(pam_handle_t *p, int flags, int argc,
                                   const char **argv) {
  (void)flags;
  (void)argc;
  (void)argv;
  const char *user, *password;
  int r;
  if ((r = pam_get_user(p, &user, NULL)) != PAM_SUCCESS)
    return r;
  if ((r = pam_get_authtok(p, PAM_AUTHTOK, &password, NULL)) != PAM_SUCCESS)
    return r;
  if (!password || !password[0] || strlen(password) > 4096)
    return PAM_AUTH_ERR;
  struct state *s = calloc(1, sizeof(*s));
  if (!s)
    return PAM_BUF_ERR;
  s->username = strdup(user);
  if (!s->username) {
    cleanup(p, s, 0);
    return PAM_BUF_ERR;
  }
  s->password = strdup(password);
  if (!s->password) {
    cleanup(p, s, 0);
    return PAM_BUF_ERR;
  }
  unsigned char random[16];
  if (getrandom(random, sizeof(random), 0) != (ssize_t)sizeof(random)) {
    cleanup(p, s, 0);
    return PAM_SYSTEM_ERR;
  }
  for (unsigned n = 0; n < 16; n++) {
    s->session[n * 2] = "0123456789abcdef"[random[n] >> 4];
    s->session[n * 2 + 1] = "0123456789abcdef"[random[n] & 15];
  }
  r = invoke("check", user, s);
  if (r != PAM_SUCCESS) {
    cleanup(p, s, 0);
    return r;
  }
  r = pam_set_data(p, "stqr-state", s, cleanup);
  if (r != PAM_SUCCESS)
    cleanup(p, s, 0);
  return r;
}
PAM_EXTERN int pam_sm_setcred(pam_handle_t *p, int flags, int argc,
                              const char **argv) {
  (void)p;
  (void)flags;
  (void)argc;
  (void)argv;
  return PAM_SUCCESS;
}
PAM_EXTERN int pam_sm_open_session(pam_handle_t *p, int flags, int argc,
                                   const char **argv) {
  (void)flags;
  (void)argc;
  (void)argv;
  const void *data;
  const char *user;
  if (pam_get_user(p, &user, NULL) != PAM_SUCCESS ||
      pam_get_data(p, "stqr-state", &data) != PAM_SUCCESS)
    return PAM_CRED_UNAVAIL;
  struct state *s = (struct state *)data;
  if (s->opened)
    return PAM_SUCCESS;
  int r = invoke("open", user, s);
  if (r == PAM_SUCCESS) {
    s->opened = 1;
    explicit_bzero(s->password, strlen(s->password));
    free(s->password);
    s->password = NULL;
  }
  return r;
}
PAM_EXTERN int pam_sm_close_session(pam_handle_t *p, int flags, int argc,
                                    const char **argv) {
  (void)flags;
  (void)argc;
  (void)argv;
  const void *data;
  const char *user;
  if (pam_get_user(p, &user, NULL) != PAM_SUCCESS ||
      pam_get_data(p, "stqr-state", &data) != PAM_SUCCESS)
    return PAM_SUCCESS;
  struct state *s = (struct state *)data;
  if (!s->opened)
    return PAM_SUCCESS;
  int r = invoke("close", user, s);
  if (r == PAM_SUCCESS)
    s->opened = 0;
  return r;
}
