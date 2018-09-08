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
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>


#define GRAPHDB_INPUT_BUFFER_SIZE (8 * 1024)

#if __sun__
typedef void (*sig_t)(int);
#endif

/*  Given state in the handle and incoming bytes, find the end of
 *  a request response.
 *
 *  Rules:
 * 	Replies end on a newline, but not one in a ""-delimited string.
 *  	In strings, \ escapes a \ or a ".
 *
 *  If we add binary formats that don't fit into this pattern, we'll
 *  have to make this boundary detector smarter.
 */
static int graphdb_request_io_boundary(graphdb_handle *graphdb,
                                       char const **s_ptr, char const *e) {
  char const *s = *s_ptr;
  unsigned int state;

  for (state = graphdb->graphdb_input_state; s < e; s++) switch (state) {
      case 0:
        while (s < e && *s != '"' && *s != '\n') s++;
        if (s >= e) break;
        if (*s == '\n') {
          graphdb->graphdb_input_state = state;
          *s_ptr = s + 1;
          return 1;
        }
        state = 1;
        break;

      case 1:
        /* In a quoted string. */
        while (s < e && *s != '"' && *s != '\\') s++;
        if (s >= e) break;
        state = (*s == '"') ? 0 : 2;
        break;

      case 2:
        /* In a quoted string, after a \ */
        state = 1;
        break;
    }

  graphdb->graphdb_input_state = state;
  *s_ptr = s;
  return 0;
}

int graphdb_request_io_write(graphdb_handle *graphdb) {
  graphdb_request *req;
  graphdb_buffer *buf;
  ssize_t cc;
  sig_t old_handler;

  old_handler = signal(SIGPIPE, SIG_IGN);

  while ((req = graphdb->graphdb_request_unsent) != NULL) {
    while ((buf = req->req_out_unsent) != NULL) {
      req->req_started = true;

      if (buf->buf_data_i < buf->buf_data_n) {
        cc = write(graphdb->graphdb_fd, buf->buf_data + buf->buf_data_i,
                   buf->buf_data_n - buf->buf_data_i);
        if (cc <= 0 && (errno == EINPROGRESS || errno == EAGAIN)) {
          signal(SIGPIPE, old_handler);
          return 0;
        }
        if (cc <= 0) {
          char msg[200];
          int err = errno;
          if (err == 0) err = ECONNRESET;
          snprintf(msg, sizeof(msg), "write: %s",
                   cc == 0 ? "EOF" : strerror(err));
          graphdb_connection_drop(graphdb, req, msg, err);
          signal(SIGPIPE, old_handler);
          return err ? err : -1;
        }

        graphdb_reconnect_success(graphdb);
        graphdb_log(graphdb, CL_LEVEL_DETAIL, "C: %.*s", (int)cc,
                    buf->buf_data + buf->buf_data_i);
        buf->buf_data_i += cc;
      }

      if (buf->buf_data_i >= buf->buf_data_n)
        req->req_out_unsent = buf->buf_next;
    }

    /*  The request has been sent.
     */
    req->req_sent = 1;
    graphdb->graphdb_request_unsent = req->req_next;

    graphdb_log(graphdb, CL_LEVEL_VERBOSE,
                "graphdb_request_io_write: %p has been sent.  "
                "(New unsent: %p)",
                (void *)req, (void *)graphdb->graphdb_request_unsent);
  }
  signal(SIGPIPE, old_handler);
  return 0;
}

/*  After a buffer has been parked on req->req_input_buffer and has
 *  been filled with data, it is appended to the input buffer chain,
 *  where it waits to be parsed by the tokenizer.
 */
static void graphdb_request_append_input_buffer(graphdb_handle *graphdb,
                                                graphdb_request *req,
                                                graphdb_buffer *buf,
                                                char const *e) {
  graphdb_assert(graphdb, req != NULL);
  graphdb_assert(graphdb, e <= buf->buf_data + buf->buf_data_m);

  graphdb_log(graphdb, CL_LEVEL_SPEW, "add input to request %p [slot id %lu]",
              (void *)req, (unsigned long)req->req_id);

  req->req_started = true;
  if (req->req_in_tail != NULL)
    req->req_in_tail->buf_next = buf;
  else {
    req->req_in_tail = req->req_in_head = buf;
    req->req_in_head_i = buf->buf_data_i;
  }
  req->req_in_tail = graphdb_buffer_dup(graphdb, buf);
  req->req_in_tail_n = e - buf->buf_data;
}

/*
 *  Read more input into the current buffer.
 *
 *	0        i--scan-->    n--read()-->    m
 *	[bbbbbbbb|ccccccccccccc|...............]
 *
 *  	buf_data   	-- memory base
 *  	buf_data_i	-- we've scanned up to here
 *	buf_data_n 	-- read data up to here
 *	buf_data_m 	-- allocated up to here.
 */
int graphdb_request_io_read(graphdb_handle *graphdb) {
  int err;
  graphdb_buffer *buf;
  ssize_t cc;
  char const *s, *e;
  int input_might_be_pending;

  while ((buf = graphdb->graphdb_input_buf) != NULL) {
    graphdb_buffer_check(graphdb, buf);
    graphdb_assert(graphdb, buf->buf_data_n < buf->buf_data_m);

    /*  Read more data, advance <n>
     */
    cc = read(graphdb->graphdb_fd, buf->buf_data + buf->buf_data_n,
              buf->buf_data_m - buf->buf_data_n);
    if (cc <= 0) {
      char msg[200];

      if ((err = errno) == EAGAIN || err == EINPROGRESS) return 0;

      snprintf(msg, sizeof(msg), "read: %s", cc == 0 ? "EOF" : strerror(errno));
      graphdb_connection_drop(graphdb, NULL, msg, err ? err : ECONNRESET);
      return err ? err : ECONNRESET;
    }

    graphdb_reconnect_success(graphdb);
    graphdb_log(graphdb, CL_LEVEL_DETAIL, "S: %.*s", (int)cc,
                buf->buf_data + buf->buf_data_n);

    buf->buf_data_n += cc;
    graphdb_buffer_check(graphdb, buf);

    /*  Scan more data as requests, advancing <i>.
     */

    s = buf->buf_data + buf->buf_data_i;
    e = buf->buf_data + buf->buf_data_n;

    graphdb_log(
        graphdb, CL_LEVEL_SPEW,
        "%s:%d: scan %p from i=%llu to n=%llu (\"%.*s..%.*s\")", __FILE__,
        __LINE__, (void *)buf, (unsigned long long)buf->buf_data_i,
        (unsigned long long)buf->buf_data_n, (int)(e - s > 20 ? 20 : e - s), s,
        (int)(e - s > 20 ? 20 : 0), (e - s > 20 ? e - 20 : ""));

    while (s < e) {
      graphdb_request *req;

      if ((req = graphdb->graphdb_request_unanswered) == NULL) {
        /*  There are no unanswered requests,
         *  yet the server is talking.  Huh?
         */
        graphdb_log(graphdb, CL_LEVEL_ERROR,
                    "protocol error: S: \"%.*s\" without "
                    "pending request!",
                    (int)(e - s), s);
        graphdb_connection_drop(graphdb, NULL,
                                "protocol error -- server is sending "
                                "data without pending requests",
                                EINVAL);
        return EINVAL;
      }

      if (!graphdb_request_io_boundary(graphdb, &s, e)) {
        /* The request extends into the next buffer.
         */
        graphdb_request_append_input_buffer(graphdb, req, buf, e);
        buf->buf_data_i = e - buf->buf_data;
        break;
      }
      /*  The request ends at <s>.  It starts either
       *  at buf->buf_data_i -- where we started scanning
       *  in this round -- or in a buffer appended to the
       *  request during a previous round of processing.
       */
      graphdb_request_append_input_buffer(graphdb, req, buf, s);

      /*  Note that we're not going to the next buffer
       *  here; multiple requests are frequently stored
       *  in the same buffer.
       */

      req->req_answered = 1;
      graphdb->graphdb_request_unanswered = req->req_next;

      graphdb_log(graphdb, CL_LEVEL_DEBUG,
                  "graphdb_request_io_read: request %p has "
                  "been answered (new unanswered: %p)",
                  (void *)req, (void *)graphdb->graphdb_request_unanswered);
      {
        graphdb_buffer *b;
        graphdb_log(graphdb, CL_LEVEL_DEBUG, "request %p has been answered:",
                    (void *)req);
        b = req->req_in_head;
        graphdb_log(
            graphdb, CL_LEVEL_DEBUG, "- begin %p req_in_head_i %llu .. n=%llu",
            (void *)req->req_in_head, (unsigned long long)req->req_in_head_i,
            (unsigned long long)b->buf_data_n);
        while (b != req->req_in_tail) {
          b = b->buf_next;
          if (b != req->req_in_tail)
            graphdb_log(graphdb, CL_LEVEL_DEBUG, "- middle %p 0..n=%llu",
                        (void *)b, (unsigned long long)b->buf_data_n);
        }
        graphdb_log(graphdb, CL_LEVEL_DEBUG, "- end %p 0...req_in_tail_n %llu",
                    (void *)req->req_in_tail,
                    (unsigned long long)req->req_in_tail_n);
      }

      /*  Mark the boundary between the text we've
       *  parsed so far and the remaining input.
       */
      buf->buf_data_i = s - buf->buf_data;

    } /* end while (s < e) */

    /*  We're done parsing this buffer.  (If its contents were
     *  parts of request replies, those replies now hold
     *  reference-counted links to the buffer.)  Unlink it.
     */
    graphdb->graphdb_input_buf = NULL;
    input_might_be_pending = (buf->buf_data_n == buf->buf_data_m);
    graphdb_buffer_free(graphdb, buf);

    if (input_might_be_pending)
      graphdb->graphdb_input_buf = graphdb_buffer_alloc_heap(
          graphdb, graphdb->graphdb_cm, GRAPHDB_INPUT_BUFFER_SIZE);
  }
  return 0;
}

bool graphdb_request_io_want_input(graphdb_handle *graphdb) {
  graphdb_request *req;
  graphdb_buffer *buf;

  if ((req = graphdb->graphdb_request_unanswered) == NULL) {
    graphdb_log(graphdb, CL_LEVEL_VERBOSE,
                "graphdb_request_io_want_input: no, there are no "
                "unanswered requests.");
    return false;
  }

  if ((buf = graphdb->graphdb_input_buf) == NULL) {
    buf = graphdb_buffer_alloc_heap(graphdb, graphdb->graphdb_cm,
                                    GRAPHDB_INPUT_BUFFER_SIZE);
    if (buf == NULL) return false;

    graphdb->graphdb_input_buf = buf;
  }
  return buf->buf_data_n < buf->buf_data_m;
}

int graphdb_request_io(graphdb_handle *graphdb, long long deadline) {
  struct pollfd pfd;
  int err;
  int millis;
  long long now;

  pfd.fd = graphdb->graphdb_fd;
  pfd.events = 0;

  graphdb_assert(graphdb, deadline >= -1);

  if (deadline <= 0)
    millis = deadline;
  else {
    now = graphdb_time_millis();
    if (now >= deadline)
      millis = 0;
    else if (deadline - now > INT_MAX)
      millis = INT_MAX;
    else
      millis = deadline - now;
  }

  if (graphdb_request_io_want_input(graphdb)) pfd.events |= POLLIN;
  if (graphdb->graphdb_request_unsent != NULL) pfd.events |= POLLOUT;

  if (pfd.events == 0) {
    graphdb_log(graphdb, CL_LEVEL_SPEW,
                "graphdb_request_io: don't want input; "
                "no unsent requests");
    return EALREADY;
  }

  graphdb_log(graphdb, CL_LEVEL_SPEW,
              "graphdb_request_io: fd %d, events %d, milliseconds: %d", pfd.fd,
              pfd.events, millis);

  errno = 0;
  err = poll(&pfd, 1, millis);

  if (err == 0) return ETIMEDOUT;

  if (err < 0) {
    char msg[200];
    err = errno;
    snprintf(msg, sizeof msg, "(while waiting for reply) poll: %s",
             strerror(err));
    graphdb_connection_drop(graphdb, graphdb->graphdb_request_unanswered, msg,
                            err);
    return err;
  } else if (!(pfd.revents & (POLLIN | POLLOUT))) {
    char msg[200];
    socklen_t size;

    err = errno;
    size = sizeof(err);
    if (getsockopt(pfd.fd, SOL_SOCKET, SO_ERROR, &err, &size)) err = -1;

    snprintf(msg, sizeof msg, "(while waiting for reply) socket: %s",
             err == -1 ? "unspecified error" : strerror(err));
    graphdb_connection_drop(graphdb, graphdb->graphdb_request_unanswered, msg,
                            err);
    return err;
  }

  if ((pfd.revents & POLLOUT) && (err = graphdb_request_io_write(graphdb)))
    return err;

  if ((pfd.revents & POLLIN) && (err = graphdb_request_io_read(graphdb)))
    return err;

  return 0;
}
