/* SPDX-License-Identifier: GPL-3.0-only */
/* Minimal generic-netlink NBD attachment; no libnl/runtime daemon required. */
#include <linux/genetlink.h>
#include <linux/nbd-netlink.h>
#include <linux/netlink.h>
#include <sys/time.h>

struct nl_request {
  struct nlmsghdr header;
  struct genlmsghdr generic;
  unsigned char attributes[512];
};
static void nl_init(struct nl_request *m, uint16_t family, uint8_t command,
                    uint32_t sequence, int ack) {
  memset(m, 0, sizeof(*m));
  m->header.nlmsg_len = NLMSG_HDRLEN + GENL_HDRLEN;
  m->header.nlmsg_type = family;
  m->header.nlmsg_flags = NLM_F_REQUEST | (ack ? NLM_F_ACK : 0);
  m->header.nlmsg_seq = sequence;
  m->generic.cmd = command;
  m->generic.version = 1;
}
static size_t nl_attr(struct nl_request *m, uint16_t type, const void *data,
                      size_t length) {
  size_t at = NLMSG_ALIGN(m->header.nlmsg_len);
  size_t end = at + NLA_ALIGN(NLA_HDRLEN + length);
  if (end > sizeof(*m)) { errno = EMSGSIZE; fail("netlink attribute"); }
  struct nlattr *a = (struct nlattr *)((char *)m + at);
  a->nla_type = type;
  a->nla_len = NLA_HDRLEN + length;
  if (length) memcpy((char *)a + NLA_HDRLEN, data, length);
  m->header.nlmsg_len = end;
  return at;
}
static void nl_end_nest(struct nl_request *m, size_t at) {
  ((struct nlattr *)((char *)m + at))->nla_len = m->header.nlmsg_len - at;
}
static void nl_send_request(int fd, struct nl_request *m) {
  if (send(fd, m, m->header.nlmsg_len, 0) != (ssize_t)m->header.nlmsg_len)
    fail("send NBD netlink request");
}
static uint16_t nl_reply(int fd, uint32_t sequence, int want_family) {
  union { struct nlmsghdr align; unsigned char data[16384]; } reply;
  for (;;) {
    ssize_t n = recv(fd, reply.data, sizeof(reply.data), 0);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) fail("receive NBD netlink reply");
    int remaining = (int)n;
    for (struct nlmsghdr *h = (struct nlmsghdr *)reply.data;
         NLMSG_OK(h, remaining); h = NLMSG_NEXT(h, remaining)) {
      if (h->nlmsg_seq != sequence) continue;
      if (h->nlmsg_type == NLMSG_ERROR) {
        if (h->nlmsg_len < NLMSG_LENGTH(sizeof(struct nlmsgerr))) {
          errno = EPROTO; fail("short NBD netlink acknowledgement");
        }
        struct nlmsgerr error;
        memcpy(&error, NLMSG_DATA(h), sizeof(error));
        if (error.error) { errno = -error.error; fail("NBD netlink configuration"); }
        if (!want_family) return 0;
      } else if (want_family && h->nlmsg_type == GENL_ID_CTRL &&
                 h->nlmsg_len >= NLMSG_HDRLEN + GENL_HDRLEN) {
        size_t left = h->nlmsg_len - NLMSG_HDRLEN - GENL_HDRLEN;
        struct nlattr *a = (struct nlattr *)((char *)h + NLMSG_HDRLEN + GENL_HDRLEN);
        while (left >= NLA_HDRLEN && a->nla_len >= NLA_HDRLEN && a->nla_len <= left) {
          if ((a->nla_type & NLA_TYPE_MASK) == CTRL_ATTR_FAMILY_ID &&
              a->nla_len >= NLA_HDRLEN + sizeof(uint16_t)) {
            uint16_t family;
            memcpy(&family, (char *)a + NLA_HDRLEN, sizeof(family));
            return family;
          }
          size_t step = NLA_ALIGN(a->nla_len);
          if (step > left) break;
          left -= step;
          a = (struct nlattr *)((char *)a + step);
        }
        errno = EPROTO; fail("NBD netlink family reply");
      }
    }
  }
}
static void root_netlink_attach(int sock, const char *device, uint64_t size,
                                uint64_t flags) {
  const char *name = strrchr(device, '/');
  name = name ? name + 1 : device;
  if (strncmp(name, "nbd", 3) != 0 || name[3] < '0' || name[3] > '9') {
    errno = EINVAL; fail("NBD device name");
  }
  char *end;
  unsigned long number = strtoul(name + 3, &end, 10);
  if (*end || number > UINT32_MAX) { errno = EINVAL; fail("NBD device index"); }
  uint32_t index = (uint32_t)number, socket_fd = (uint32_t)sock;
  uint64_t block_size = 4096;
  int nl = socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_GENERIC);
  if (nl < 0) fail("NBD netlink socket");
  struct timeval timeout = {.tv_sec = 10};
  if (setsockopt(nl, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)))
    fail("NBD netlink timeout");
  struct sockaddr_nl local = {.nl_family = AF_NETLINK};
  if (bind(nl, (struct sockaddr *)&local, sizeof(local)) ||
      connect(nl, (struct sockaddr *)&local, sizeof(local)))
    fail("NBD netlink bind/connect");
  struct nl_request m;
  nl_init(&m, GENL_ID_CTRL, CTRL_CMD_GETFAMILY, 1, 0);
  nl_attr(&m, CTRL_ATTR_FAMILY_NAME, NBD_GENL_FAMILY_NAME, sizeof(NBD_GENL_FAMILY_NAME));
  nl_send_request(nl, &m);
  uint16_t family = nl_reply(nl, 1, 1);
  nl_init(&m, family, NBD_CMD_CONNECT, 2, 1);
  nl_attr(&m, NBD_ATTR_INDEX, &index, sizeof(index));
  nl_attr(&m, NBD_ATTR_SIZE_BYTES, &size, sizeof(size));
  nl_attr(&m, NBD_ATTR_BLOCK_SIZE_BYTES, &block_size, sizeof(block_size));
  nl_attr(&m, NBD_ATTR_SERVER_FLAGS, &flags, sizeof(flags));
  /* No DISCONNECT_ON_CLOSE: the kernel owns the connection after this process
   * exits, including systemd's stop/continue passes during root shutdown. */
  size_t sockets = nl_attr(&m, NBD_ATTR_SOCKETS | NLA_F_NESTED, NULL, 0);
  size_t item = nl_attr(&m, NBD_SOCK_ITEM | NLA_F_NESTED, NULL, 0);
  nl_attr(&m, NBD_SOCK_FD, &socket_fd, sizeof(socket_fd));
  nl_end_nest(&m, item);
  nl_end_nest(&m, sockets);
  nl_send_request(nl, &m);
  nl_reply(nl, 2, 0);
  close(nl);
  for (unsigned attempt = 0; attempt < 500; attempt++) {
    int dev = open(device, O_RDONLY | O_CLOEXEC);
    if (dev >= 0) {
      uint64_t capacity = 0;
      int ready = ioctl(dev, BLKGETSIZE64, &capacity) == 0 && capacity == size;
      close(dev);
      if (ready) return;
    }
    usleep(10000);
  }
  errno = ETIMEDOUT;
  fail("NBD kernel capacity readiness");
}
