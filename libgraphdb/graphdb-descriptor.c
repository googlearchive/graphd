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
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>


/**
 * @brief Return a connection's file descriptor.
 * @warning Untested.
 *
 * An application working with poll() or select() can use the
 * file descriptor returned to wait for new input on a graph database
 * connection.  It should call graphdb_descriptor_events() to ask
 * which events the graph database is waiting for, and, once input arrives,
 * call graphdb_descriptor_io() to process the pending input.
 *
 * @param graphdb 	handle created with graphdb_create() and connected
 *		   	using graphdb_connect().
 * @returns -1 on error, a file handle otherwise
 */

int graphdb_descriptor(graphdb_handle* graphdb) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) {
    errno = EINVAL;
    return -1;
  }

  /*  If we don't have a file descriptor right now,
   *  begin a reconnect.
   */
  if (graphdb->graphdb_fd == -1) {
    int err;

    graphdb_log(graphdb, GRAPHDB_LEVEL_DEBUG,
                "graphdb_descriptor: -1 "
                "right now; begin asynchronous "
                "reconnect.");

    err = graphdb_reconnect_async(graphdb);
    if (err != 0) {
      graphdb_log(graphdb, GRAPHDB_LEVEL_DEBUG,
                  "graphdb_descriptor: "
                  "asynchronous reconnect fails: "
                  "%s",
                  strerror(err));
      graphdb_assert(graphdb, graphdb->graphdb_fd == -1);
      graphdb_assert(graphdb, !graphdb->graphdb_connected);
      errno = err;
    }
  }
  return graphdb->graphdb_app_fd = graphdb->graphdb_fd;
}

/**
 * @brief Do whatever I/O is called for on the graphd connection.
 *
 * @param graphdb 	handle created with graphdb_create() and
 *			initially connected using graphdb_connect().
 * @param events 	the events that the handle selected or polled
 *			ready for, an inclusive bitwise OR of
 *			#GRAPHDB_OUTPUT, #GRAPHDB_INPUT, and/or #GRAPHDB_ERROR.
 *
 * @returns	0 on success.
 * @returns	EBADF	if the file descriptor that the application
 *			was using is no longer used by the graph database.
 *			The application should call graphdb_descriptor()
 *			again and install the current file descriptor.
 * @returns	EINVAL	if the handle parameter is NULL or otherwise invalid.
 * @returns	nonzero error codes on other system errors.
 */

int graphdb_descriptor_io(graphdb_handle* graphdb, int events) {
  int err = 0;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (graphdb->graphdb_app_fd != graphdb->graphdb_fd) return EBADF;

  if (!graphdb->graphdb_connected) {
    if (graphdb->graphdb_fd != -1) err = graphdb_reconnect_async_io(graphdb);

    /*  If we don't have a fd, either we didn't have one
     *  all along, or the graphdb_reconnect_async_io
     *  failed.  Move to the next candidate.
     *
     *  graphdb_reconnect_async() will return an error
     *  if it runs out of candidates.
     */
    if (graphdb->graphdb_fd == -1) err = graphdb_reconnect_async(graphdb);

    if (err != 0) return err;
  } else {
    if (!(events & (GRAPHDB_OUTPUT | GRAPHDB_INPUT))) {
      err = graphdb_request_io(graphdb, 0);
      if (err == ETIMEDOUT || err == EALREADY) err = 0;
    } else if (events & GRAPHDB_OUTPUT) {
      err = graphdb_request_io_write(graphdb);
    } else if (events & GRAPHDB_INPUT) {
      err = graphdb_request_io_read(graphdb);
    }

    graphdb_assert(graphdb, !graphdb->graphdb_connected == (err != 0));

    if (!graphdb->graphdb_connected) {
      err = graphdb_reconnect_async(graphdb);

      if (err != 0) {
        graphdb_log(graphdb, GRAPHDB_LEVEL_DEBUG,
                    "graphdb_descriptor_io: "
                    "asynchronous reconnect fails: "
                    "%s",
                    strerror(err));
        graphdb_assert(graphdb, graphdb->graphdb_fd == 0);
        graphdb_assert(graphdb, !graphdb->graphdb_connected);
      }
    }
  }
  return err;
}

/**
 * @brief What events is the graph repository connection interested in?
 *
 * @warning Untested.
 * @param graphdb 	handle created with graphdb_create() and connected
 *		   	using graphdb_connect().
 * @returns as a bitmap of #GRAPHDB_OUTPUT and #GRAPHDB_INPUT, the kinds
 *		of events  that the graph repository wants to know about.
 */

int graphdb_descriptor_events(graphdb_handle* graphdb) {
  if (!GRAPHDB_IS_HANDLE(graphdb)) return 0;

  /*  If we don't have a file descriptor right now,
   *  begin a reconnect.
   */
  if (graphdb->graphdb_fd == -1) {
    int err;

    graphdb_log(graphdb, GRAPHDB_LEVEL_DEBUG,
                "graphdb_descriptor_events: -1 "
                "descriptor; begin asynchronous "
                "reconnect.");

    err = graphdb_reconnect_async(graphdb);
    if (err != 0) {
      graphdb_log(graphdb, GRAPHDB_LEVEL_DEBUG,
                  "graphdb_descriptor_events: "
                  "asynchronous reconnect fails: "
                  "%s",
                  strerror(err));
      graphdb_assert(graphdb, graphdb->graphdb_fd == 0);
      graphdb_assert(graphdb, !graphdb->graphdb_connected);
      errno = err;

      return 0;
    }
  }
  graphdb_assert(graphdb, graphdb->graphdb_fd != -1);

  if (!graphdb->graphdb_connected) return GRAPHDB_OUTPUT;

  return (graphdb->graphdb_request_unsent ? GRAPHDB_OUTPUT : 0) |
         (graphdb_request_io_want_input(graphdb) ? GRAPHDB_INPUT : 0);
}
