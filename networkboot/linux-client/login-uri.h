/* SPDX-License-Identifier: GPL-3.0-only */
/* Prepared templates are bounded, credential-free JSON written by install-login.
 * Only the reserved endpoint token is substituted, before PID1 starts. No
 * persistence fence belongs to this ephemeral per-boot configuration update. */
static char *login_read(const char *path, size_t *length) {
  int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) return NULL;
  struct stat st;
  if (fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size < 1 ||
      st.st_size > 1024 * 1024) {
    close(fd); errno = EINVAL; return NULL;
  }
  size_t n = (size_t)st.st_size, at = 0;
  char *data = malloc(n + 1);
  if (!data) { close(fd); return NULL; }
  while (at < n) {
    ssize_t got = read(fd, data + at, n - at);
    if (got < 0 && errno == EINTR) continue;
    if (got <= 0) { free(data); close(fd); errno = EIO; return NULL; }
    at += (size_t)got;
  }
  close(fd); data[n] = 0;
  if (memchr(data, 0, n)) { free(data); errno = EINVAL; return NULL; }
  *length = n;
  return data;
}
static int login_uri(const char *root, const char *server) {
  struct in6_addr numeric;
  int ipv6 = inet_pton(AF_INET6, server, &numeric) == 1;
  if (!ipv6 && inet_pton(AF_INET, server, &numeric) != 1) {
    errno = EINVAL; fail("login server");
  }
  char *template_path, *config_path, *temporary;
  if (asprintf(&template_path, "%s/usr/lib/stqr-networkboot/home-template.json", root) < 0 ||
      asprintf(&config_path, "%s/etc/security/stqr-home.json", root) < 0 ||
      asprintf(&temporary, "%s/etc/security/.stqr-home.XXXXXX", root) < 0)
    fail("login path");
  size_t n;
  char *source = login_read(template_path, &n);
  if (!source) fail("login template");
  const char token[] = "@STQR_BOOT_SERVER@";
  char *where = strstr(source, token), *result;
  size_t size;
  if (where) {
    if (strstr(where + sizeof(token) - 1, token)) {
      errno = EINVAL; fail("duplicate login endpoint");
    }
    char host[INET6_ADDRSTRLEN + 2];
    snprintf(host, sizeof host, ipv6 ? "[%s]" : "%s", server);
    size_t head = (size_t)(where - source), hostlen = strlen(host);
    size = n - (sizeof(token) - 1) + hostlen;
    result = malloc(size + 1);
    if (!result) fail("login allocation");
    memcpy(result, source, head);
    memcpy(result + head, host, hostlen);
    memcpy(result + head + hostlen, where + sizeof(token) - 1,
           n - head - (sizeof(token) - 1));
    result[size] = 0;
  } else { result = source; size = n; }
  size_t previous_n = 0;
  char *previous = login_read(config_path, &previous_n);
  struct stat prior;
  if (previous && previous_n == size && !memcmp(previous, result, size) &&
      !lstat(config_path, &prior) && (prior.st_mode & 0777) == 0600 && prior.st_uid == geteuid()) {
    free(previous); if (result != source) free(result); free(source);
    free(template_path); free(config_path); free(temporary); return 0;
  }
  free(previous);
  int fd = mkstemp(temporary);
  if (fd < 0) fail("login temporary");
  if (fchmod(fd, 0600) || full(fd, result, size) || close(fd) ||
      rename(temporary, config_path)) {
    unlink(temporary); fail("login endpoint update");
  }
  if (result != source) free(result);
  free(source); free(template_path); free(config_path); free(temporary);
  return 0;
}
