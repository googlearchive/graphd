/*
Copyright 2018 Google Inc. All rights reserved.
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

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sysexits.h>

#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* gpush -- write requests until you block.
 */

#include "libcl/cl.h"
#include "libcm/cm.h"
#include "libgraph/graph.h"
#include "libgraphdb/graphdb.h"
#include "libgraphdb/graphdbp.h"

#define ISXDIGIT(ch) (isascii(ch) && isxdigit(ch))

#define IS_LIT(lit, s, e) \
  (sizeof(lit) - 1 == (e) - (s) && !strncasecmp(s, lit, sizeof(lit) - 1))

/* Overall system state.
 */
typedef struct gpush_handle {
  graphdb_handle *gpush_graphdb;
  cl_handle *gpush_cl;
  cm_handle *gpush_cm;

  long gpush_timeout;
  long gpush_stall_wait_seconds;
  unsigned int gpush_silent : 1;

  long long gpush_count;

  char const *gpush_progname;
  char const *gpush_query;
  size_t gpush_query_n;

  int gpush_socket;

} gpush_handle;

static void usage(char const *progname) {
  fprintf(stderr,
          "usage: %s options....\n"
          "Options:\n"
          "   -h              print this brief message\n"
          "   -v              increase verbosity of debug output\n"
          "   -l file         log to <file>, if it exists\n"
          "   -z              run without printing dots and timeouts\n"
          "   -t timeout      wait this many milliseconds to connect or query\n"
          "   -w seconds      after stalling for this long, exit 0 (success)\n"
          "   -s server-url   connect to <server-url>\n"
          "\n",
          progname);
  exit(EX_USAGE);
}

static void wait_for(int fd, long msecs) {
  struct pollfd p;
  int err;

  p.fd = fd;
  p.events = POLLOUT;
  p.revents = 0;

  err = poll(&p, 1, msecs);

  if (err < 0) {
    fprintf(stderr, "wait_for: poll: %s\n", strerror(errno));
    exit(1);
  }
}

static void do_send(gpush_handle *gpush) {
  int fd = gpush->gpush_socket;
  size_t n_wait = 0;
  size_t cc = 0;
  int err;
  char const *message = gpush->gpush_query;
  size_t n = gpush->gpush_query_n;

  while (cc < n) {
    err = write(fd, message + cc, n - cc);
    if (err > 0) {
      cc += err;
      continue;
    }
    if (errno != EINPROGRESS && errno != EWOULDBLOCK && errno != EAGAIN) {
      fprintf(stderr, "write: %s\n", strerror(errno));
      exit(1);
    }

    /*  Wait for the file descriptor to
     *  turn writeable again.
     */
    wait_for(fd, 1000);
    n_wait++;

    if (gpush->gpush_stall_wait_seconds > 0 &&
        n_wait >= gpush->gpush_stall_wait_seconds) {
      /* Shut down. */
      (void)close(gpush->gpush_socket);
      graphdb_destroy(gpush->gpush_graphdb);

      /* Success! */
      exit(0);
    }

    if (gpush->gpush_silent) continue;

    if (n_wait < 10) {
      putc('0' + n_wait, stderr);
      fflush(stderr);
    } else if (n_wait < 1000) {
      if (n_wait % 10 == 0) {
        fprintf(stderr, "[%zu]", n_wait);
        if (n_wait % 100 == 0) putc('\n', stderr);
        fflush(stderr);
      }
    } else if (n_wait < 10000) {
      if (n_wait % 100 == 0) {
        fprintf(stderr, "[%zu]", n_wait);
        if (n_wait % 1000 == 0) putc('\n', stderr);
        fflush(stderr);
      }
    } else if (n_wait % 1000 == 0) {
      if (n_wait % 10000 == 0) putc('\n', stderr);
      fprintf(stderr, "[%zu]", n_wait);
      fflush(stderr);
    }
    continue;
  }
}

int main(int argc, char **argv) {
  int opt, err;
  size_t n_sent = 0;
  char const *logfile = NULL;
  char const *server_address[2] = {"tcp://127.0.0.1:8100", 0};
  char sbuf[GRAPHDB_SERVER_NAME_SIZE];
  char ebuf[GRAPHDB_ERROR_SIZE];

  int verbose = 0;
  gpush_handle gpush;
  cm_buffer qb;

  memset(&gpush, 0, sizeof(gpush));

  gpush.gpush_cl = cl_create();
  gpush.gpush_cm = cm_trace(cm_c());
  gpush.gpush_timeout = -1;
  gpush.gpush_count = -1;
  gpush.gpush_query = "write ()\n";

  if ((gpush.gpush_progname = strrchr(argv[0], '/')) != NULL)
    gpush.gpush_progname++;
  else
    gpush.gpush_progname = argv[0];

  while ((opt = getopt(argc, argv, "l:hq:s:t:n:vw:z")) != EOF) {
    switch (opt) {
      case 'z':
        gpush.gpush_silent = true;
        break;

      case 'l':
        logfile = optarg;
        break;

      case 'n':
        if (sscanf(optarg, "%lld", &gpush.gpush_count) != 1) {
          fprintf(stderr,
                  "%s: expected repeat count "
                  "got \"%s\"\n",
                  gpush.gpush_progname, optarg);
          exit(EX_USAGE);
        }
        break;

      case 's':
        server_address[0] = optarg;
        break;

      case 'q':
        gpush.gpush_query = optarg;
        break;

      case 't':
        if (sscanf(optarg, "%ld", &gpush.gpush_timeout) != 1) {
          fprintf(stderr,
                  "%s: expected timeout "
                  "(in milliseconds), got \"%s\"\n",
                  gpush.gpush_progname, optarg);
          exit(EX_USAGE);
        }
        break;

      case 'w':
        if (sscanf(optarg, "%ld", &gpush.gpush_stall_wait_seconds) != 1) {
          fprintf(stderr,
                  "%s: expected stall timeout "
                  "(in seconds), got \"%s\"\n",
                  gpush.gpush_progname, optarg);
          exit(EX_USAGE);
        }
        break;

      case 'v':
        verbose++;
        break;

      case 'h':
      case '?':
        usage(gpush.gpush_progname);
        break;

      default:
        break;
    }
  }

  gpush.gpush_query_n = strlen(gpush.gpush_query);
  if (verbose) {
    switch (verbose) {
      case 1:
        verbose = CL_LEVEL_DETAIL;
        break;
      case 2:
        verbose = CL_LEVEL_DEBUG;
        break;
      default:
        verbose = CL_LEVEL_SPEW;
        break;
    }

    cl_set_loglevel_full(gpush.gpush_cl, verbose);
  }

  if (logfile != NULL) cl_file(gpush.gpush_cl, logfile);

  /*  Create and parametrize a socket.
   */
  if ((gpush.gpush_graphdb = graphdb_create()) == NULL) {
    perror("graphdb_create()");
    exit(1);
  }

  graphdb_set_memory(gpush.gpush_graphdb, gpush.gpush_cm);
  graphdb_set_logging(gpush.gpush_graphdb, gpush.gpush_cl);
  graphdb_set_loglevel(gpush.gpush_graphdb, verbose);

  if (verbose) graphdb_set_loglevel(gpush.gpush_graphdb, CL_LEVEL_VERBOSE);

  err = graphdb_connect(gpush.gpush_graphdb, 0, server_address, 0);
  if (err != 0) {
    fprintf(stderr, "%s: cannot connect to %s: %s\n", gpush.gpush_progname,
            server_address[0], graphdb_strerror(err, ebuf, sizeof ebuf));
    graphdb_destroy(gpush.gpush_graphdb);
    return EX_UNAVAILABLE;
  }

  gpush.gpush_socket = graphdb_descriptor(gpush.gpush_graphdb);
  if (gpush.gpush_socket < 0) {
    fprintf(stderr, "%s: cannot get descriptor for %s: %s\n",
            gpush.gpush_progname,
            graphdb_server_name(gpush.gpush_graphdb, sbuf, sizeof sbuf),
            graphdb_strerror(err, ebuf, sizeof ebuf));
    graphdb_destroy(gpush.gpush_graphdb);
    return EX_UNAVAILABLE;
  }

  /* Repeatedly send. */

  cm_buffer_initialize(&qb, gpush.gpush_cm);

  long long count = gpush.gpush_count;
  for (; count; count--) {
    if (!gpush.gpush_silent) {
      putc('.', stderr);
      fflush(stderr);
    }

    do_send(&gpush);
    n_sent++;

    if (!gpush.gpush_silent)
      if (n_sent % 50 == 0) putchar('\n');
  }
  cm_buffer_finish(&qb);

  /* Shut down. */

  (void)close(gpush.gpush_socket);

  graphdb_destroy(gpush.gpush_graphdb);

  /* If we wanted to measure stall, and arrived
   * here, we didn't stall.
   */
  if (gpush.gpush_stall_wait_seconds > 0) {
    if (!gpush.gpush_silent)
      fprintf(stderr, "%s: didn't stall for %lu during first %lu writes.\n",
              gpush.gpush_progname,
              (unsigned long)gpush.gpush_stall_wait_seconds,
              (unsigned long)gpush.gpush_count);
    return EX_SOFTWARE;
  }
  return 0;
}
