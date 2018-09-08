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

/*
 *  This is an uncopying client, similar to the uncopying server
 *  described in graph/doc/gr-uncopying.txt.
 *
 *  Incoming buffers are linkcounted and co-owned by the parsing
 *  connection and by the requests whose replies they contain.
 *  More than one request can receive contents from more than one buffer,
 *  but request replies are contiguous.
 */

/*
 *  graphdb_request_wait_req -- wait for a reply
 *
 *  If the connection drops, graphdb_request_wait_req()
 *  automatically initiates a reconnect.
 *
 *  Parameters:
 *	graphdb 	-- handle created with graphdb_create(),
 *		   	   must be connected.
 *	request_out	-- return the answered request here, with
 *			   one reference to it.
 *	deadline 	-- until when to wait, in milliseconds,
 * 			   or -1 (infinity) or 0 (just poll)
 *
 */

int graphdb_request_wait_req(graphdb_handle *graphdb,
                             graphdb_request **request_inout,
                             long long deadline) {
  int err = 0;
  graphdb_request *req;
  graphdb_request *target;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (deadline < -1) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_wait_req: unexpected deadline %lld", deadline);
    return EINVAL;
  }
  target = *request_inout;

  /*  Pick off request that have already been answered.
   */
  for (;;) {
    int cancelled;

    if ((req = graphdb->graphdb_request_head) == NULL) {
      graphdb_log(graphdb, CL_LEVEL_FAIL,
                  "graphdb_request_wait_req: "
                  "nothing to wait for.");
      return ENOENT;
    }
    if (target != NULL) req = target;

    graphdb_assert(graphdb, req != NULL);
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "wait for data on request %p [slot id %lu]", (void *)req,
                (unsigned long)req->req_id);

    /*  We need to do more I/O before getting a
     *  reply to this request.
     */
    if (!req->req_answered || !req->req_sent) break;

    /*  We've already processed this request.  But - had it
     *  been cancelled by the application?
     */
    cancelled = req->req_cancelled;

    /*  Remove the connection to the infrastructure.
     *  We still may have a linkcount from the application.
     */

    /* Take a link if the caller doesn't hold one. */
    if (target == NULL) req->req_refcount++;

    /* Chain-out drops an infrastructure link.
     * Now we're holding the remaining link.
     */
    graphdb_request_chain_out(graphdb, req);

    if (!cancelled) {
      graphdb_log(graphdb, CL_LEVEL_SPEW,
                  "found answered request %p [slot id %lu], "
                  "request_inout=%p",
                  (void *)req, (unsigned long)req->req_id, request_inout);

      /*  Return the link we took to the caller.
       */
      *request_inout = req;
      return 0;
    }

    /*  The application no longer wanted this one.
     *  Drop the link to "req".
     */
    if (target != NULL) {
      /*  The application asked for this
       *  request by name, yet had cancelled
       *  it earlier.  "Huh?"
       */
      graphdb_log(graphdb, CL_LEVEL_SPEW,
                  "graphdb_request_wait_req: tagged "
                  "request had been cancelled?");
      return ECHILD;
    }
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_wait_req: "
                "skip cancelled request");

    /*  We didn't have a target -> we were holding
     *  the link; drop it now.
     */
    graphdb_request_unlink_req(graphdb, req);

    /* req is invalid at this point. */
  }

  /*  If we have the time, get more events, and cause
   *  more requests to be answered.
   */
  for (;;) {
    /*  Do some IO.  Some of that IO will, hopefully,
     *  involve reading replies to requests we're waiting
     *  for, but all kinds of other activities are involved
     *  in making that happen -- reconnecting, resending
     *  requests, and so on.
     */

    if (!graphdb->graphdb_connected) {
      if (deadline > 0)
        err = graphdb_connect_reconnect(graphdb, deadline);
      else {
        while ((err = graphdb_reconnect_async(graphdb)) == 0) {
          err = graphdb_reconnect_async_io(graphdb);
          if (err == 0) break;
        }
      }
    }

    if (graphdb->graphdb_connected) {
      err = graphdb_request_io(graphdb, deadline);
      if (err != 0) {
        graphdb_log(graphdb, CL_LEVEL_FAIL,
                    "error from graphdb_request_io: "
                    "%s",
                    strerror(err));
        break;
      }
    }

    req = target ? target : graphdb->graphdb_request_head;
    if (req == NULL) {
      if (err == 0) err = ENOENT;
      break;
    }

    /*  Did all that I/O get us our request answered?
     */
    if (req->req_answered && req->req_sent) {
      int cancelled;

      cancelled = req->req_cancelled;

      graphdb_log(graphdb, CL_LEVEL_SPEW, "answered request %p [slot id %lu]",
                  (void *)req, (unsigned long)req->req_id);

      /*  If we didn't get a passed-in target request,
       *  make another link to the internal request we're
       *  returning.
       */
      if (target == NULL) req->req_refcount++;

      /* Remove the request from the internal queues. */
      graphdb_request_chain_out(graphdb, req);

      if (!cancelled) {
        *request_inout = req;
        return 0;
      } else if (target != NULL)
        return ECHILD;

      /*  Drop the link we took, likely destroying
       *  the request in the process.
       */
      graphdb_request_unlink_req(graphdb, req);
    }
    if (deadline == 0 || (deadline > 0 && graphdb_time_millis() >= deadline))

      break;
  }
  *request_inout = NULL;

  graphdb_log(graphdb, CL_LEVEL_FAIL,
              "graphdb_request_wait_req: "
              "%s",
              strerror(err ? err : ETIMEDOUT));

  return err ? err : ETIMEDOUT;
}

/**
 * @brief Perform I/O and wait for a response.
 *
 * Depending on the @b request_id_inout parameter, this function can
 * be used to perform I/O until the response to a specific request
 * arrives and is reported, or until all requests have been
 * executed.
 *
 * Once a request result has been used, it must be free'd with
 * graphdb_request_free().
 *
 * @param graphdb 	handle created with graphdb_create(), connected
 *			with graphdb_connect().
 * @param request_id_inout	on entry, #GRAPHDB_REQUEST_ANY or the request
 *			   	caller is waiting for.  On exit, the request
 *			  	the caller gets.
 * @param timeout_millis 	how long to wait, in milliseconds.
 *				-1 means infinity, 0 means just poll.
 * @param application_data_out 	opaque data strored with request
 * @param text_out 		text of response; the text stays valid
 *				at least as long as the request id stays valid -
 *				don't free this pointerp; call
 *				graphdb_request_free() instead.
 * @param text_size_out		# of bytes pointed to by <*text_out>.
 *
 * @returns 0 on success
 * @returns ETIMEOUT if timeout_millis is 0 and there are no
 *	requests ready to be returned.
 * @returns ETIMEOUT if a request timed out.  (In that
 *	case, *request_id_inout will be something other than
 * 	GRAPHDB_REQUEST_ANY.)
 * @returns EINVAL if the handle is invalid or NULL.
 * @returns ENOMEM on allocation failure.
 * @returns other nonzero errnos on system errors
 */

int graphdb_request_wait(graphdb_handle *graphdb,
                         graphdb_request_id *request_id_inout,
                         long timeout_millis, void **application_data_out,
                         char const **text_out, size_t *text_size_out) {
  int err;
  size_t n;
  graphdb_buffer *buf;
  graphdb_request *req;
  graphdb_request_id req_id;
  char *w;

  char const *text;
  size_t text_size;

  if (!GRAPHDB_IS_HANDLE(graphdb)) return EINVAL;

  if (graphdb->graphdb_request_head == NULL) return ENOENT;

  req_id = (request_id_inout == NULL ? GRAPHDB_REQUEST_ANY : *request_id_inout);
  if (req_id == GRAPHDB_REQUEST_ANY)
    req = NULL;
  else if ((req = graphdb_request_lookup(graphdb, req_id)) == NULL) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_request_wait: unknown request #%ld", (long)req_id);
    return ENOENT;
  }

  /*  Take a link to this request.
   */
  if (req != NULL) req->req_refcount++;

  err = graphdb_request_wait_req(
      graphdb, &req, timeout_millis > 0 ? graphdb_time_millis() + timeout_millis
                                        : timeout_millis);
  if (err != 0) {
    if (req != NULL) graphdb_request_unlink_req(graphdb, req);

    graphdb_log(graphdb, CL_LEVEL_FAIL,
                "graphdb_request_wait: "
                "error from graphdb_request_wait_req: %s",
                strerror(err));
    return err;
  }

  /*  The req that we got a back from wait_req has one
   *  link on it.  That link will be free'ed by the application
   *  using graphdb_request_free().
   */
  graphdb_assert(graphdb, req != NULL);
  if (request_id_inout != NULL) *request_id_inout = req->req_id;
  if (application_data_out != NULL)
    *application_data_out = req->req_application_data;

  if (req->req_in_head == req->req_in_tail) {
    /* Single buffer -- common case. */

    if (req->req_in_head == NULL) {
      /* Well, I take this as a "no"... */

      text = "";
      text_size = 0;
    } else {
      text = req->req_in_head->buf_data + req->req_in_head_i;
      text_size = req->req_in_tail_n - req->req_in_head_i;
    }
  } else {
    /*  Measure the total size of the reply.
     */
    buf = req->req_in_head;
    n = buf->buf_data_n - req->req_in_head_i;

    for (buf = buf->buf_next; buf != req->req_in_tail; buf = buf->buf_next) {
      n += buf->buf_data_n;
    }
    n += req->req_in_tail_n;

    /*  Allocate space for it.
     */
    req->req_in_text = w = cm_malloc(req->req_heap, n + 1);
    if (req->req_in_text == NULL) return ENOMEM;

    /*  Consolidate the data into the allocated buffer.
     */
    buf = req->req_in_head;
    n = buf->buf_data_n - req->req_in_head_i;
    memcpy(w, buf->buf_data + req->req_in_head_i, n);
    w += n;

    for (buf = buf->buf_next; buf != req->req_in_tail; buf = buf->buf_next) {
      memcpy(w, buf->buf_data, buf->buf_data_n);
      w += buf->buf_data_n;
    }
    memcpy(w, buf->buf_data, req->req_in_tail_n);
    w += req->req_in_tail_n;
    *w = '\0';

    text = req->req_in_text;
    text_size = w - req->req_in_text;
  }

  err = req->req_errno;

  if (graphdb->graphdb_app_reply_callback != NULL)
    (*graphdb->graphdb_app_reply_callback)(
        graphdb->graphdb_app_reply_callback_data, graphdb, req->req_errno,
        req->req_application_data, req->req_id, text, text_size);

  if (text_out != NULL) *text_out = text;
  if (text_size_out != NULL) *text_size_out = text_size;

  /*  Don't access req after this callback - the callback
   *  may have already free'd the request.
   */
  return err;
}

int graphdb_request_wait_iterator_loc(graphdb_handle *graphdb,
                                      graphdb_request_id *request_id_inout,
                                      long timeout_millis,
                                      void **application_data_out,
                                      graphdb_iterator **it_out,
                                      char const *file, int line) {
  graphdb_request *req;
  int err = 0;
  unsigned long long deadline;

  if (graphdb == NULL) return EINVAL;

  graphdb_log(graphdb, CL_LEVEL_SPEW,
              "graphdb_request_wait_iterator: "
              "timeout is %ld",
              timeout_millis);

  if (it_out != NULL) *it_out = NULL;

  if (timeout_millis < -1) {
    graphdb_log(graphdb, CL_LEVEL_ERROR,
                "graphdb_(v)query: unexpected timeout %ld (expecting "
                "value >= -1)",
                timeout_millis);
    return EINVAL;
  }
  errno = 0;

  deadline = timeout_millis <= 0 ? timeout_millis
                                 : graphdb_time_millis() + timeout_millis;

  if (request_id_inout == NULL || *request_id_inout == GRAPHDB_REQUEST_ANY)
    req = NULL;
  else {
    req = graphdb_request_lookup(graphdb, *request_id_inout);
    if (req == NULL) return errno ? errno : ENOENT;

    /* We're holding a link to this request.
     */
    req->req_refcount++;
  }
  if ((err = graphdb_request_wait_req(graphdb, &req, deadline)) != 0)
    return err;

  graphdb_assert(graphdb, req != NULL);
  graphdb_assert(graphdb, req->req_refcount >= 1);
  graphdb_assert(graphdb, req->req_handle == graphdb);

  if (application_data_out != NULL)
    *application_data_out = req->req_application_data;
  if (request_id_inout != NULL) *request_id_inout = req->req_id;

  *it_out = graphdb_iterator_alloc_loc(req, NULL, file, line);
  if (*it_out == NULL) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_wait_iterator: iterator alloc "
                "fails; free request %p",
                (void *)req);
    graphdb_request_unlink_req(graphdb, req);

    return ENOMEM;
  }
  graphdb_request_unlink_req(graphdb, req);

  return 0;
}
