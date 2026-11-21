/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <linux/mount.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
static void usage(FILE *out)
{
    fputs("Usage: mount.kstqr --uri URI --bucket ID --credentials FILE [--init] [--readonly] "
          "MOUNTPOINT\n"
          "       mount -t kstqr URI MOUNTPOINT -o bucket=ID,credentials=FILE[,init][,ro]\n"
          "Credentials file: username=... and password=... on separate lines, mode 0600.\n"
          "This helper exits after mounting; the kernel maintains the connection.\n",
          out);
}
static int configure(int fd, unsigned command, const char *key, const char *value, int aux)
{
    return syscall(SYS_fsconfig, fd, command, key, value, aux);
}
static int resolve(const char *url, char out[512])
{
    const char *start, *end, *colon = NULL;
    char host[256], service[8] = "80", ip[INET6_ADDRSTRLEN];
    struct addrinfo hints = {0}, *list, *a;
    size_t n;
    if (!strncmp(url, "http://", 7))
        start = url + 7;
    else if (!strncmp(url, "ws://", 5))
        start = url + 5;
    else {
        errno = EINVAL;
        return -1;
    }
    end = strchr(start, '/');
    if (!end)
        end = start + strlen(start);
    if (*end && strcmp(end, "/") && strcmp(end, "/api/ws")) {
        errno = EINVAL;
        return -1;
    }
    n = end - start;
    if (*start == '[') {
        const char *close = memchr(start, ']', n);
        if (!close || (close + 1 < end && close[1] != ':')) {
            errno = EINVAL;
            return -1;
        }
        n = close - start - 1;
        start++;
        if (close + 1 < end)
            colon = close + 1;
    } else {
        colon = memchr(start, ':', n);
        if (colon)
            n = colon - start;
    }
    if (!n || n >= sizeof(host)) {
        errno = EINVAL;
        return -1;
    }
    memcpy(host, start, n);
    host[n] = 0;
    if (colon) {
        size_t len = end - colon - 1;
        char *tail;
        unsigned long port;
        if (!len || len >= sizeof(service)) {
            errno = EINVAL;
            return -1;
        }
        memcpy(service, colon + 1, len);
        service[len] = 0;
        port = strtoul(service, &tail, 10);
        if (*tail || !port || port > 65535) {
            errno = EINVAL;
            return -1;
        }
    }
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_NUMERICSERV;
    if (getaddrinfo(host, service, &hints, &list)) {
        errno = EHOSTUNREACH;
        return -1;
    }
    for (a = list; a; a = a->ai_next) {
        void *addr;
        if (a->ai_family == AF_INET)
            addr = &((struct sockaddr_in *)a->ai_addr)->sin_addr;
        else if (a->ai_family == AF_INET6)
            addr = &((struct sockaddr_in6 *)a->ai_addr)->sin6_addr;
        else
            continue;
        if (!inet_ntop(a->ai_family, addr, ip, sizeof(ip)))
            continue;
        snprintf(out, 512, a->ai_family == AF_INET6 ? "http://[%s]:%s" : "http://%s:%s", ip,
                 service);
        freeaddrinfo(list);
        return 0;
    }
    freeaddrinfo(list);
    errno = EHOSTUNREACH;
    return -1;
}
int main(int argc, char **argv)
{
    const char *uri = NULL, *bucket = NULL, *path = NULL, *target = NULL, *timeout = "30000";
    char *option_text = NULL, resolved[512];
    int create = 0, readonly = 0, opt, fsfd = -1, cred = -1, mountfd = -1, result = 1;
    unsigned attrs = 0;
    static const struct option opts[] = {{"uri", required_argument, NULL, 'u'},
                                         {"bucket", required_argument, NULL, 'b'},
                                         {"credentials", required_argument, NULL, 'c'},
                                         {"init", no_argument, NULL, 'i'},
                                         {"readonly", no_argument, NULL, 'r'},
                                         {"timeout-ms", required_argument, NULL, 't'},
                                         {"help", no_argument, NULL, 'h'},
                                         {NULL, 0, NULL, 0}};
    while ((opt = getopt_long(argc, argv, "o:h", opts, NULL)) != -1) {
        switch (opt) {
        case 'u':
            uri = optarg;
            break;
        case 'b':
            bucket = optarg;
            break;
        case 'c':
            path = optarg;
            break;
        case 'i':
            create = 1;
            break;
        case 'r':
            readonly = 1;
            break;
        case 't':
            timeout = optarg;
            break;
        case 'o':
            if (option_text) {
                usage(stderr);
                goto out;
            }
            option_text = strdup(optarg);
            if (!option_text)
                goto fail;
            break;
        case 'h':
            usage(stdout);
            result = 0;
            goto out;
        default:
            usage(stderr);
            goto out;
        }
    }
    if (option_text) {
        char *cursor = option_text, *token;
        while ((token = strsep(&cursor, ","))) {
            if (!strncmp(token, "bucket=", 7))
                bucket = token + 7;
            else if (!strncmp(token, "credentials=", 12))
                path = token + 12;
            else if (!strncmp(token, "timeout_ms=", 11))
                timeout = token + 11;
            else if (!strcmp(token, "init"))
                create = 1;
            else if (!strcmp(token, "ro"))
                readonly = 1;
            else if (!strcmp(token, "rw"))
                readonly = 0;
            else if (!strcmp(token, "nosuid"))
                attrs |= MOUNT_ATTR_NOSUID;
            else if (!strcmp(token, "nodev"))
                attrs |= MOUNT_ATTR_NODEV;
            else if (!strcmp(token, "noexec"))
                attrs |= MOUNT_ATTR_NOEXEC;
            else if (strcmp(token, "defaults") && strcmp(token, "_netdev") &&
                     strcmp(token, "noatime")) {
                fputs("mount.kstqr: unsupported mount option\n", stderr);
                goto out;
            }
        }
    }
    if (!uri && argc - optind == 2)
        uri = argv[optind++];
    if (argc - optind == 1)
        target = argv[optind];
    if (!uri || !bucket || !path || !target || strlen(bucket) != 32 || (create && readonly)) {
        usage(stderr);
        goto out;
    }
    if (resolve(uri, resolved))
        goto fail;
    cred = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (cred < 0)
        goto fail;
    {
        struct stat st;
        if (fstat(cred, &st))
            goto fail;
        if (!S_ISREG(st.st_mode) || st.st_uid != geteuid() || (st.st_mode & 0077) ||
            st.st_size <= 0 || st.st_size > 8192) {
            errno = EACCES;
            goto fail;
        }
    }
    fsfd = syscall(SYS_fsopen, "kstqr", FSOPEN_CLOEXEC);
    if (fsfd < 0)
        goto fail;
    if (configure(fsfd, FSCONFIG_SET_STRING, "url", resolved, 0) ||
        configure(fsfd, FSCONFIG_SET_STRING, "bucket", bucket, 0) ||
        configure(fsfd, FSCONFIG_SET_STRING, "timeout_ms", timeout, 0) ||
        configure(fsfd, FSCONFIG_SET_FD, "credentials", NULL, cred) ||
        (create && configure(fsfd, FSCONFIG_SET_FLAG, "init", NULL, 0)) ||
        (readonly && configure(fsfd, FSCONFIG_SET_FLAG, "readonly", NULL, 0)) ||
        configure(fsfd, FSCONFIG_CMD_CREATE, NULL, NULL, 0))
        goto fail;
    if (readonly)
        attrs |= MOUNT_ATTR_RDONLY;
    mountfd = syscall(SYS_fsmount, fsfd, FSMOUNT_CLOEXEC, attrs);
    if (mountfd < 0)
        goto fail;
    if (syscall(SYS_move_mount, mountfd, "", AT_FDCWD, target, MOVE_MOUNT_F_EMPTY_PATH))
        goto fail;
    result = 0;
    goto out;
fail:
    fprintf(stderr, "mount.kstqr: %s\n", strerror(errno));
out:
    if (mountfd >= 0)
        close(mountfd);
    if (fsfd >= 0)
        close(fsfd);
    if (cred >= 0)
        close(cred);
    free(option_text);
    return result;
}
