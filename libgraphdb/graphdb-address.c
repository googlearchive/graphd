/*
Copyright 2015 Google Inc. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include "libgraphdb/graphdbp.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#if __sun__
#define AF_LOCAL AF_UNIX
#endif

int graphdb_address_connect(graphdb_handle *graphdb,
                            graphdb_address const *addr, int fd) {
  int err = 0;

  errno = 0;
  switch (addr->addr_type) {
    case GRAPHDB_ADDRESS_TCP:
      err = connect(fd, (struct sockaddr *)&addr->addr_tcp_sockaddr_in,
                    sizeof(addr->addr_tcp_sockaddr_in));
      if (err == -1) err = errno ? errno : -1;
      break;

    case GRAPHDB_ADDRESS_LOCAL: {
      struct sockaddr_un sa;
      memset(&sa, 0, sizeof(sa));
      sa.sun_family = AF_LOCAL;
      if (strlen(addr->addr_local_path) >= sizeof(sa.sun_path)) return ERANGE;
      strcpy(sa.sun_path, addr->addr_local_path);
      err = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    }
      if (err == -1) err = errno ? errno : -1;
      break;

    default:
      graphdb_notreached(graphdb, "unexpected address type %d",
                         addr->addr_type);
      return -1;
  }
  return err;
}

int graphdb_address_set_nonblocking(graphdb_handle *graphdb,
                                    graphdb_address const *addr, int fd) {
  int socket_flags;

  if ((socket_flags = fcntl(fd, F_GETFL, 0)) < 0) {
    int err = errno;
    graphdb_log(graphdb, CL_LEVEL_ERROR, "fcntl(%d, F_GETFL, 0) fails: %s", fd,
                strerror(errno));
    return err;
  }
  socket_flags |= O_NONBLOCK;
  if (fcntl(fd, F_SETFL, socket_flags) != 0) {
    int err = errno;
    graphdb_log(graphdb, CL_LEVEL_ERROR, "fcntl(%d, F_SETFL, %x) fails: %s", fd,
                socket_flags, strerror(errno));
    close(fd);
    return err;
  }
  return 0;
}

/*  Disable/Enable "Nagle's algorithm" on the server socket.
 *
 *  Nagle's algorithm in the TCP stack delays sending of packets
 *  until either
 *  - there are no outstanding acknowledgements or
 *  - enough data has been buffered to fill an IP packet.
 *
 *  This helps make interactive applications over a network
 *  not swamp the network with packets carrying a single
 *  keystroke as payload; but for server applications like
 *  ours, it can - given patience and just the right client
 *  behavior (ask Tim Sturge for help) - produce noticable
 *  TCP delays.
 *
 *  Setting TCP_NODELAY to 1 turns *off* Nagle's algorithm.
 */
int graphdb_address_set_nodelay(graphdb_handle *graphdb,
                                graphdb_address const *addr, int fd) {
  int flag = 1;

  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag)) !=
      0)

    /* This fails on MacOS X with a secret EOPNOTSUPP that I have
     * to jump through various hoops to even access
     * (#define __DARWIN_UNIX03 does the trick - but what
     * *else* does that do?) so let's just ignore that this
     *  minor optimization fails.
     */
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "setsockopt(%d, IPPROTO_TCP, TCP_NODELAY, &1) "
                "fails: %s",
                fd, strerror(errno));

  return 0;
}

int graphdb_address_socket(graphdb_handle *graphdb,
                           graphdb_address const *addr) {
  int fd;

  switch (addr->addr_type) {
    case GRAPHDB_ADDRESS_TCP:
      if ((fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
        int err = errno;
        graphdb_log(graphdb, CL_LEVEL_ERROR,
                    "socket(PF_INET, SOCK_STREAM, 0) fails: %s",
                    strerror(errno));
        errno = err;
        return -1;
      }
      return fd;

    case GRAPHDB_ADDRESS_LOCAL:
      if ((fd = socket(PF_UNIX, SOCK_STREAM, 0)) == -1) {
        int err = errno;
        graphdb_log(graphdb, CL_LEVEL_ERROR,
                    "socket(PF_UNIX, SOCK_STREAM, 0) fails: %s",
                    strerror(errno));
        errno = err;
        return -1;
      }
      return fd;

    default:
      graphdb_notreached(graphdb, "unexpected address type %d",
                         addr->addr_type);
      return -1;
  }
  return fd;
}

static int xtov(char s) {
  if (!isalnum(s)) return -1;
  if (isdigit(s)) return s - '0';
  if (s >= 'a' && s <= 'f') return 10 + (s - 'a');
  if (s >= 'A' && s <= 'F') return 10 + (s - 'A');
  return -1;
}

static bool could_be_servicename(char const *s, char const *e) {
  char const *const s0 = s;

  if (s == e) return false;

  for (; s < e; s++) {
    if (!isascii(*s) || (!isalnum(*s) && *s != '-')) return false;
    if (*s == '-' && (s > s0 && s[-1] == '-')) return false;
  }
  return true;
}

static bool could_be_hostname(char const *s, char const *e) {
  char const *const s0 = s;

  if (s == e) return true; /* interpreted as "localhost". */

  for (; s < e; s++) {
    if (!isascii(*s) || (!isalnum(*s) && *s != '-' && *s != '.')) return false;

    if (s > s0 && !isalnum(s[0]) && !isalnum(s[-1])) return false;
  }
  return true;
}

static bool scan_tcp_address(char *s, char **server_s, char **server_e,
                             char **port_s, char **port_e) {
  char *p;

  *server_s = s + (strncasecmp(s, "tcp://", 6) == 0
                       ? 6
                       : (strncasecmp(s, "tcp:", 4) == 0 ? 4 : 0));
  if ((p = strrchr(*server_s, ':')) == NULL) {
    *server_e = *server_s + strlen(*server_s);

    if (!could_be_hostname(*server_s, *server_e)) return false;

    *port_s = NULL;
    *port_e = NULL;
  } else {
    if (!could_be_hostname(*server_s, *server_e = p)) return false;

    *port_s = p + 1;
    *port_e = p + strlen(p);

    if (!could_be_servicename(*port_s, *port_e)) return false;
  }
  return true;
}

static void scan_local_address(char *s, char const **path_s) {
  char *w;
  char const *r;

  if (strncasecmp(s, "local:", 6) == 0)
    s += 6;
  else if (strncasecmp(s, "unix:", 5) == 0)
    s += 5;

  /* Skip leading triple /// */
  while (*s == '/' && (s[1] == '/')) s++;

  /* convert %NN to their meaning. */
  for (r = w = s; *r != '\0';) {
    int a, b;

    if (*r == '%' && (a = xtov(r[1])) != -1 && (b = xtov(r[2])) != -1) {
      *w++ = (a << 4) | b;
      r += 3;
    } else
      *w++ = *r++;
  }
  *w = '\0';

  *path_s = s;
}

/*
 *  graphdb_address_resolve -- (Utility) resolve an address
 *
 *  Parameters:
 *	graphdb  -- handle created with graphdb_create()
 *	deadline -- deadline in milliseconds (ignored)
 *	text     -- address to resolve
 *	addr_out -- assign resolved address here.
 *
 *  Returns:
 *	NULL on error, otherwise a server address
 */

int graphdb_address_resolve(graphdb_handle *graphdb, long long deadline,
                            char const *text, graphdb_address **addr_out) {
  size_t text_n;
  graphdb_address *addr;
  char *server_s, *server_e;
  char *port_s, *port_e;

  if (!GRAPHDB_IS_HANDLE(graphdb) || text == NULL) return EINVAL;

  text_n = strlen(text);

  errno = 0;
  graphdb_assert(graphdb, graphdb->graphdb_heap);

  addr = cm_malloc(graphdb->graphdb_heap, sizeof(*addr) + text_n + 1);
  if (addr == NULL) return errno ? errno : ENOMEM;

  memset(addr, 0, sizeof(*addr));
  addr->addr_next = NULL;
  addr->addr_display_name = memcpy((char *)(addr + 1), text, text_n + 1);

  if (scan_tcp_address(addr->addr_display_name, &server_s, &server_e, &port_s,
                       &port_e)) {
    char tmp = '\0';
    struct sockaddr_in *s_in = &addr->addr_tcp_sockaddr_in;

    if (port_s != NULL && port_s > addr->addr_display_name) {
      tmp = port_s[-1];
      port_s[-1] = '\0';
    }

    memset(s_in, 0, sizeof(*s_in));

    s_in->sin_family = AF_INET;
    s_in->sin_addr.s_addr = INADDR_ANY;
    s_in->sin_port = htons(0);

    if (server_e > server_s && *server_s != '\0') {
#ifdef __sun__
      s_in->sin_addr.s_addr = inet_addr(server_s);
      if ((in_addr_t)(-1) != s_in->sin_addr.s_addr)
#else
      if (!inet_aton(server_s, &s_in->sin_addr))
#endif
      {
        struct hostent *he;

        he = gethostbyname(server_s);
        if (he == NULL) {
          graphdb_log(graphdb, CL_LEVEL_ERROR, "tcp: can't resolve \"%.*s\"",
                      (int)(server_e - server_s), server_s);
          cm_free(graphdb->graphdb_heap, addr);
          return ENOENT;
        }
        if (he->h_addrtype != AF_INET) {
          graphdb_log(graphdb, CL_LEVEL_ERROR,
                      "tcp: unfamiliar addrtype "
                      "%d for \"%.*s\""
                      " (XXX port me to IPV6!)",
                      he->h_addrtype, (int)(server_e - server_s), server_s);
          cm_free(graphdb->graphdb_heap, addr);
          return ENOENT;
        }
        memcpy(&s_in->sin_addr.s_addr, he->h_addr,
               sizeof(s_in->sin_addr.s_addr));
      }
    }

    if (tmp != '\0') port_s[-1] = tmp;

    if (port_s != NULL) {
      struct servent *se;

      unsigned short hu;

      if (sscanf(port_s, "%hu", &hu) == 1)
        s_in->sin_port = htons(hu);
      else if ((se = getservbyname(port_s, "tcp")) != NULL)
        s_in->sin_port = se->s_port;
      else {
        graphdb_log(graphdb, CL_LEVEL_ERROR,
                    "tcp: cannot resolve service "
                    "name \"%s\" (try using a number?)",
                    port_s);
        cm_free(graphdb->graphdb_heap, addr);
        return ENOENT;
      }
    } else {
      /* Use the default port. */
      s_in->sin_port = htons(GRAPHDB_DEFAULT_PORT);
    }
    addr->addr_type = GRAPHDB_ADDRESS_TCP;
    graphdb_log(graphdb, CL_LEVEL_DEBUG, "ip %d.%d.%d.%d, port %hu",
                (unsigned int)(s_in->sin_addr.s_addr >> 24),
                (unsigned char)(s_in->sin_addr.s_addr >> 16),
                (unsigned char)(s_in->sin_addr.s_addr >> 8),
                (unsigned char)s_in->sin_addr.s_addr, ntohs(s_in->sin_port));
  } else {
    scan_local_address(addr->addr_display_name, &addr->addr_local_path);
    addr->addr_type = GRAPHDB_ADDRESS_LOCAL;
  }

  *addr_out = addr;

  return 0;
}
