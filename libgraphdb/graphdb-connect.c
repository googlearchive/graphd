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

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static char const *const graphd_connect_default_addrtext[] = {
    "tcp://127.0.0.1:8100", NULL};

static int graphdb_connect_io(graphdb_handle *graphdb, long long deadline) {
  struct pollfd pfd;
  int err;
  int timeout;
  char msg[200];
  socklen_t size;

  int peer_err;
  struct sockaddr_in si;
  socklen_t si_len;

  if (graphdb->graphdb_connected) return 0;

  pfd.fd = graphdb->graphdb_fd;
  pfd.events = POLLOUT | POLLIN;

  if (deadline <= 0)
    timeout = deadline;
  else {
    unsigned long long now = graphdb_time_millis();

    if (deadline < now)
      timeout = 0;
    else if (deadline - now > INT_MAX)
      timeout = INT_MAX;
    else
      timeout = deadline - now;
  }

  if ((err = poll(&pfd, 1, timeout)) == 0) {
    graphdb_connection_drop(graphdb, NULL, "time out while trying to connect",
                            ETIMEDOUT);
    return ETIMEDOUT;
  } else if (err < 0) {
    err = errno;
    snprintf(msg, sizeof msg,
             "(while waiting for connect to complete) poll: %s", strerror(err));
    graphdb_connection_drop(graphdb, NULL, msg, err);
    return err;
  }

  size = sizeof(err);
  err = 0;

  if ((pfd.revents & POLLERR) != 0) {
    if (getsockopt(pfd.fd, SOL_SOCKET, SO_ERROR, &err, &size) || err == 0)
      err = -1;
  } else {
    /*  Force the connect to fail, if it failed.
     *  Without this, I'm just getting "ready for writing"
     *  indications when there should be a POLLERR...
     */

    si_len = sizeof(si);
    peer_err = getpeername(pfd.fd, (struct sockaddr *)&si, &si_len);
    err = 0;
    size = sizeof(err);
    if (peer_err) {
      if (getsockopt(pfd.fd, SOL_SOCKET, SO_ERROR, &err, &size) || err == 0)
        err = -1;
    }
  }

  if (err != 0) {
    snprintf(msg, sizeof msg,
             "(while waiting for connect to complete) socket: %s",
             err == -1 ? "unspecified error" : strerror(err));
    graphdb_connection_drop(graphdb, NULL, msg, err);
    return err;
  }

  graphdb->graphdb_connected = 1;

  graphdb_assert(graphdb, graphdb->graphdb_address_current);
  graphdb_assert(graphdb, graphdb->graphdb_address_current->addr_display_name);

  graphdb_log(graphdb, CL_LEVEL_DETAIL, "graphdb: connected to %s\n",
              graphdb->graphdb_address_current->addr_display_name);
  return 0;
}

/*
 *  Reconnect or initially connect to a server.  Loop until up to <deadline>
 *  to have the connect go through.
 *
 *  We have the addresses in <graphdb_address_head, tail> to work with.
 *  If an existing connection drops, we start trying at the beginning.
 *  If a connect attempt fails, we try the next one in the list.
 *
 * @param graphdb	handle
 * @param deadline	If it takes longer than to that point,
 *			give up with ETIMEDOUT.
 */
int graphdb_connect_reconnect(graphdb_handle *graphdb, long long deadline) {
  graphdb_address const *a, *first = NULL;
  int fd, err = graphdb->graphdb_connect_errno;
  unsigned long long millis = 0;

  if (graphdb->graphdb_connected) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_connect_reconnect: already connected.");
    return 0;
  }

  graphdb_log(graphdb, CL_LEVEL_DEBUG,
              "graphdb_connect_reconnect: deadline %lld", deadline);

  if ((fd = graphdb->graphdb_fd) != -1) {
    a = graphdb->graphdb_address_current;
    goto wait_for_connect_events;
  }

  for (;;) {
    /*  Move to the next address, if we have one;
     *  if we don't, start at the beginning.
     */
    if (!graphdb_reconnect_address(graphdb)) {
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_connect_reconnect: run out of "
                  "things to try.");
      err = graphdb->graphdb_connect_errno;
      break;
    }
    a = graphdb->graphdb_address_current;
    graphdb->graphdb_address_last = a;
    if (first == NULL) first = a;

    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_connect_reconnect: trying \"%s\"",
                a->addr_display_name);

    /*  Open a socket, switch it to non-blocking,
     *  and initiate a connection to the destination.
     */
    if ((fd = graphdb_address_socket(graphdb, a)) == -1) {
      /*  Resource shortage -- the system is running
       *  out of file descriptors?
       */
      err = graphdb->graphdb_connect_errno = errno;
      break;
    }

    err = graphdb_address_set_nonblocking(graphdb, a, fd);
    if (err != 0) {
      /* This should never happen. */
      close(fd);
      continue;
    }

    err = graphdb_address_set_nodelay(graphdb, a, fd);
    if (err != 0) {
      /* This should never happen. */
      close(fd);
      continue;
    }

    err = graphdb_address_connect(graphdb, a, fd);

    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_connect_reconnect: "
                "connecting to %s on fd %d (%s)",
                a->addr_display_name, fd, err ? strerror(err) : "success");

    if (err == EINPROGRESS) {
      /*  The normal non-blocking connect case.
       *  We have until <deadline> to wait for the
       *  connection to go through.
       */
      graphdb->graphdb_fd = fd;

    wait_for_connect_events:
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_connect_reconnect: "
                  "waiting for connection events");

      err = graphdb_connect_io(graphdb, deadline);

      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_connect_reconnect: "
                  "got result: %s",
                  strerror(err));
    }
    if (err == 0) {
      /* Successful connection.
       */
      graphdb->graphdb_fd = fd;
      graphdb->graphdb_connected = 1;
      graphdb->graphdb_connect_errno = 0;

      return 0;
    }
    graphdb->graphdb_fd = -1;

    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_connect_reconnect: "
                "abandoning %s, fd %d: %s",
                a->addr_display_name, fd, strerror(err));

    close(fd);
    graphdb->graphdb_connected = 0;
    graphdb->graphdb_connect_errno = err;

    if (deadline >= 0 && (millis = graphdb_time_millis()) >= deadline) {
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_connect_reconnect: "
                  "out of time; giving up");
      err = ETIMEDOUT;
      break;
    }
  }

  /*  Give up.
   */
  graphdb_connection_drop_reconnects(graphdb);

  graphdb->graphdb_fd = -1;
  graphdb->graphdb_connected = 0;

  /*  Leave our "current"  pointer at the first address we
   * (unsuccessfully) tried to connect to.
   */
  graphdb->graphdb_address_last = graphdb->graphdb_address_current = first;

  return err ? err : ETIMEDOUT;
}

/**
 * @brief Connect to a running graph database server.
 *
 * If timeout is >= 0, if the connection cannot be completed
 * within @b timeout milliseconds, the call returns with an error.
 *
 * This time does not include hostname resolving - hostname
 * resolves are assumed to be instantaneous, for the purposes
 * of timeout calculation.   If you need truly instataneous
 * resolves, resolve asynchronously yourself and pass down
 * IP addresses.
 *
 * @param graphdb 	handle created with graphdb_create()
 * @param timeout 	timeout in milliseconds,
 *				#GRAPHDB_INFINITY for never.
 * @param addrtext 	NULL for default ("tcp://127.0.0.1:8100"),
 *			otherwise a NULL-terminated, argv-style array
 *			of URL-like addresses to connect to,
 *			most preferred first.
 * @param  flags 	for further study (at some point, we may support
 *			asynchronous connects).
 *
 * @return	0 		on success
 * @return	EINVAL		for NULL or invalid graphdb
 * @return	EALREADY	if already connected
 * @return	Other nonzero error values for other subsystem errors.
 */

int graphdb_connect(graphdb_handle *graphdb, long timeout,
                    char const *const *addrtext, int flags) {
  int err;
  long long deadline;
  graphdb_address const *addr;

  graphdb_log(graphdb, CL_LEVEL_DEBUG, "graphdb_connect %s%s, timeout %ld",
              (addrtext == NULL ? "null"
                                : (*addrtext == NULL ? "(null)" : addrtext[0])),
              addrtext && addrtext[0] && addrtext[1] ? ", ..." : "", timeout);

  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (graphdb->graphdb_connected) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG, "graphdb_connect: already connected");
    return EALREADY;
  }

  if ((err = graphdb_initialize(graphdb)) != 0) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_connect: initialization fails (%s)", strerror(err));
    return err;
  }

  /* Clean up after the most recent failed attempt. */
  if (graphdb->graphdb_address_head != NULL) {
    graphdb_address *a, *a_next;
    for (a = graphdb->graphdb_address_head; a != NULL; a = a_next) {
      a_next = a->addr_next;
      cm_free(graphdb->graphdb_heap, a);
    }
    graphdb->graphdb_address_head = NULL;
    graphdb->graphdb_address_tail = &graphdb->graphdb_address_head;
  }

  deadline = (timeout > 0 ? graphdb_time_millis() + timeout : timeout);

  /*  Resolve the text addresses to a chain of graphdb_address
   *  structures stored as part of the handle.
   */
  if (addrtext == NULL || *addrtext == NULL)
    addrtext = graphd_connect_default_addrtext;
  graphdb_assert(graphdb, *addrtext != NULL);

  graphdb->graphdb_address_current = NULL;
  graphdb->graphdb_address_last = NULL;
  graphdb->graphdb_address_tail = &graphdb->graphdb_address_head;
  for (; *addrtext != NULL; addrtext++) {
    err = graphdb_address_resolve(graphdb, deadline, *addrtext,
                                  graphdb->graphdb_address_tail);
    if (err == 0) {
      graphdb_log(graphdb, CL_LEVEL_DEBUG, "successfully resolved %s (type %d)",
                  *addrtext, (*graphdb->graphdb_address_tail)->addr_type);
      graphdb->graphdb_address_tail =
          &(*graphdb->graphdb_address_tail)->addr_next;
    } else {
      graphdb_log(graphdb, CL_LEVEL_ERROR, "%s: %s", *addrtext, strerror(err));
    }
  }
  if (graphdb->graphdb_address_head == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_connect: none of the addresses resolve: %s",
                strerror(err));
    return err;
  }

  /*  Count the number of resolved addresses.
   */
  graphdb->graphdb_address_n = 0;
  for (addr = graphdb->graphdb_address_head; addr != NULL;
       addr = addr->addr_next)
    graphdb->graphdb_address_n++;

  /* When reconnecting, that's how many
   * retries we get.
   */
  graphdb->graphdb_address_retries = graphdb->graphdb_address_n;

  return graphdb_connect_reconnect(graphdb, deadline);
}
