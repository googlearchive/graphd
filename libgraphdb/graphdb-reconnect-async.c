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


/*  A reconnect attempt to the server has
 *  end-to-end success -- bytes are actually
 *  sent (or are arriving).
 *
 *  Reset the retry counter.
 */
void graphdb_reconnect_success(graphdb_handle *g) {
  g->graphdb_address_retries = g->graphdb_address_n;
}

/*  Set graphdb->graphdb_address_current to the
 *  address to connect to.
 *
 *  Return true if we have at least one more try left,
 *  false if we're out of options and should return an
 *  error.
 */
bool graphdb_reconnect_address(graphdb_handle *g) {
  /* The first retry goes to the previous address,
   * if there is one.
   */
  if (g->graphdb_address_retries-- == 0) {
    /*  Out of options.
     *
     *  Reset the retry counter for the next round;
     *  Reset the "current host" to NULL.
     *
     *  (Since we've lost context, we might as
     *  well retry in the user's preferred order.)
     */
    g->graphdb_address_retries = g->graphdb_address_n;
    g->graphdb_address_current = NULL;

    return false;
  }

  /*  The first retry goes to the existing current address.
   *  If we're lucky, that server is still up and still has
   *  our context.  (Yeah, right.)
   */
  if (g->graphdb_address_retries == g->graphdb_address_n - 1 &&
      g->graphdb_address_current != NULL)
    return true;

  /* Slightly more complicated.
   */
  if (g->graphdb_address_last == NULL) {
    if (g->graphdb_address_current == NULL ||
        (g->graphdb_address_current = g->graphdb_address_current->addr_next) ==
            NULL)
      g->graphdb_address_current = g->graphdb_address_head;
  } else {
    /* Find an address, different from graphdb_address_last */
    /* If addr_next is null on address_last, start at the front
     * again.
     */

    for (g->graphdb_address_current = g->graphdb_address_last; true;
         g->graphdb_address_current = g->graphdb_address_current->addr_next) {
      if (g->graphdb_address_last == NULL ||
          strcmp(g->graphdb_address_current->addr_display_name,
                 g->graphdb_address_last->addr_display_name) != 0)
        break;

      if (g->graphdb_address_current->addr_next == NULL) {
        g->graphdb_address_current = g->graphdb_address_head;
        break;
      }
    }
  }

  return g->graphdb_address_current != NULL;
}

int graphdb_reconnect_async_io(graphdb_handle *graphdb) {
  struct pollfd pfd;
  int err;
  char msg[200];
  socklen_t size;

  int peer_err;
  struct sockaddr_in si;
  socklen_t si_len;

  if (graphdb->graphdb_connected) return 0;

  pfd.fd = graphdb->graphdb_fd;
  pfd.events = POLLOUT | POLLIN;

  /*  Timed out?  Not an error; we'll just come back later.
   */
  if ((err = poll(&pfd, 1, 0)) == 0)
    return 0;

  else if (err < 0) {
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

/**
 * @brief Begin reconnecting to a server asynchronously.
 *
 *  If the attempt ends with a connect in progress
 *  (graphd->graphd_connected == false, but
 *  graphd->graphd_fd != -1), the connect or failure
 *  notification is delivered via
 *  grapdb_reconnect_async_io.
 *
 * @param graphdb	handle
 * @return 0 on success, a nonzero error code on error.
 */
int graphdb_reconnect_async(graphdb_handle *graphdb) {
  graphdb_address const *a, *first = NULL;
  int fd, err = graphdb->graphdb_connect_errno;

  if (graphdb->graphdb_connected) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_reconnect_async: already connected.");
    return 0;
  }
  if (graphdb->graphdb_fd != -1) {
    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_reconnect_async: already waiting.");
    return 0;
  }
  graphdb_assert(graphdb,
                 graphdb->graphdb_fd == -1 && !graphdb->graphdb_connected);
  for (;;) {
    if (!graphdb_reconnect_address(graphdb)) {
      err = graphdb->graphdb_connect_errno;
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_reconnect_async: run out of "
                  "retries; giving up (%s)",
                  strerror(err));
      break;
    }
    a = graphdb->graphdb_address_current;
    graphdb_assert(graphdb, a != NULL);

    graphdb->graphdb_address_last = a;
    if (first == NULL) first = a;

    graphdb_log(graphdb, CL_LEVEL_DEBUG,
                "graphdb_reconnect_async: trying \"%s\"", a->addr_display_name);

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
                "graphdb_reconnect_async: "
                "connecting to %s on fd %d (%s)",
                a->addr_display_name, fd, err ? strerror(err) : "success");

    if (err == EINPROGRESS) {
      /*  The normal non-blocking connect case.
       */
      graphdb->graphdb_fd = fd;

      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_reconnect_async: "
                  "waiting for connection events");
      return 0;
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
                "graphdb_reconnect_async: "
                "abandoning %s, fd %d: %s",
                a->addr_display_name, fd, strerror(err));
    close(fd);
    graphdb->graphdb_connected = 0;
    graphdb->graphdb_connect_errno = err;

    /*  If the address we just tried was the last one,
     *  consider this attempt over.
     */
    if (graphdb->graphdb_address_current == NULL) {
      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_reconnect_async: "
                  "no current address; giving up");
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
