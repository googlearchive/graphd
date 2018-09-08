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


/*  Mark all unanswered, uncancelled requests as failed.
 */
void graphdb_connection_drop_reconnects(graphdb_handle *graphdb) {
  char const *server_name = "no server";
  graphdb_address const *addr;
  graphdb_request *req, *next;

  graphdb_log(graphdb, CL_LEVEL_SPEW, "graphdb_connection_drop_reconnects()");

  if ((addr = graphdb->graphdb_address_current) != NULL)
    server_name = addr->addr_display_name;
  graphdb_log(graphdb, CL_LEVEL_DETAIL, "%s: giving up - %s", server_name,
              strerror(graphdb->graphdb_connect_errno));

  graphdb->graphdb_request_unanswered = NULL;
  graphdb->graphdb_request_unsent = NULL;

  for (req = graphdb->graphdb_request_head; req != NULL; req = next) {
    next = req->req_next;

    if (req->req_cancelled) {
      /* Remove the request from the internal queues. */
      graphdb_request_unlink_req(graphdb, req);
      continue;
    }

    if (!req->req_answered) {
      req->req_answered = true;
      req->req_started = true;
      req->req_sent = true;

      if (req->req_errno == 0) req->req_errno = graphdb->graphdb_connect_errno;
    }

    graphdb_assert(graphdb, req->req_answered);
    graphdb_assert(graphdb, req->req_started);
    graphdb_assert(graphdb, req->req_sent);
  }
}

void graphdb_connection_drop(graphdb_handle *graphdb, graphdb_request *req,
                             char const *why, int why_err) {
  char const *server_name = "no server";
  graphdb_address const *addr;
  graphdb_request *next;

  graphdb_log(graphdb, CL_LEVEL_SPEW, "graphdb_connection_drop(req=%p, %s)",
              (void *)req, why ? why : "(null)");

  if (graphdb->graphdb_fd != -1) {
    (void)close(graphdb->graphdb_fd);
    graphdb->graphdb_fd = -1;

    /* Clear the input buffers - we don't want to re-read
     * input that made us drop the connection to begin with.
     */
    if (graphdb->graphdb_input_buf != NULL) {
      graphdb_buffer_free(graphdb, graphdb->graphdb_input_buf);
      graphdb->graphdb_input_buf = NULL;
    }
  }
  if ((addr = graphdb->graphdb_address_current) != NULL)
    server_name = addr->addr_display_name;

  if (req == NULL)
    graphdb_log(graphdb, CL_LEVEL_DETAIL, "%s: %s", server_name, why);
  else {
    /*  The connection fell down it in mid-request.  At this stage
     *  of development, that's probably because the _server_
     *  fell down it in mid-request...
     *
     *  Do we have the request text?  If yes, use it in
     *  the error message -- it may contain the operation that
     *  the server is crashing on.
     */

    if (req->req_in_head != NULL) {
      char const *request_name;
      int request_name_n;
      char const *ellipsis = "...";

      request_name = req->req_in_head->buf_data + req->req_in_head_i;
      request_name_n = ((req->req_in_head == req->req_in_tail)
                            ? req->req_in_tail_n
                            : req->req_in_head->buf_data_n) -
                       req->req_in_head_i;

      if (request_name_n > 80)
        request_name_n = 80;
      else
        ellipsis = "";

      graphdb_log(graphdb, CL_LEVEL_ERROR, "%s: %.*s%s: %s", server_name,
                  request_name_n, request_name, ellipsis, why);
    } else {
      graphdb_log(graphdb, CL_LEVEL_ERROR,
                  "%s: "
                  "request #%lu: %s",
                  server_name, req->req_id, why);
    }
  }

  if (graphdb->graphdb_connected) {
    graphdb->graphdb_connected = false;
    graphdb->graphdb_address_last = graphdb->graphdb_address_current;
    graphdb->graphdb_address_current = NULL;
    graphdb->graphdb_connect_errno = why_err;
  }

  /*  Resync the request queue:
   *  	- resend requests that haven't fully been answered;
   *  	- mark requests as sent if they *have* been answered;
   *  	- throw out cancelled requests.
   */
  graphdb->graphdb_request_unanswered = NULL;
  graphdb->graphdb_request_unsent = NULL;

  for (req = graphdb->graphdb_request_head; req != NULL; req = next) {
    bool was_started;

    was_started = req->req_started;
    next = req->req_next;

    if (req->req_cancelled) {
      /* Remove the request from the internal queues. */
      graphdb_request_chain_out(graphdb, req);
      continue;
    }

    if (!req->req_answered && req->req_started) {
      if (req->req_retries > 0) {
        graphdb_log(graphdb, CL_LEVEL_DETAIL,
                    "graphdb_connection_drop: "
                    "retrying request %p (%d retr%s left)",
                    req, req->req_retries, req->req_retries == 1 ? "y" : "ies");

        req->req_retries--;
        req->req_sent = req->req_started = req->req_answered;

      } else {
        graphdb_log(graphdb, CL_LEVEL_SPEW,
                    "graphdb_connection_drop: "
                    "giving up on %p",
                    req);

        req->req_started = true;
        req->req_answered = true;
        req->req_sent = true;
        req->req_errno = why_err;
      }
      if (!req->req_sent) {
        /*  Reset the send buffers.
         */
        graphdb_buffer *b;

        req->req_out_unsent = req->req_out;

        for (b = req->req_out_unsent; b != NULL; b = b->buf_next)

          b->buf_data_i = 0;
      }
    }

    if (graphdb->graphdb_request_unanswered == NULL && !req->req_answered)
      graphdb->graphdb_request_unanswered = req;

    if (graphdb->graphdb_request_unsent == NULL && !req->req_sent) {
      graphdb->graphdb_request_unsent = req;
    }

    if (!was_started) break;
  }
}
